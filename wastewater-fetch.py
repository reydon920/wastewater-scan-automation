#!/usr/bin/env python3
"""
WastewaterScan Data Exporter — Fixed Version

Fetches actual MEASUREMENT data (not just sites) from the WastewaterScan
GraphQL API. Decomposes timestamps into separate columns for clean CSV output.

Key fixes vs. original:
  1. Properly introspects the schema, discovers that `measurements` requires
     a `filter` argument, builds that filter with siteIds + date range,
     and falls back to per-site batching if a bulk query is rejected.
  2. Decomposes fetched_at_utc and sample_date into separate
     year / month / day / hour / minute / second / weekday columns.
  3. Strips GeoJSON cruft (geometry, type=Feature, coordinates) so every
     value lands in its designated column.
"""

import os
import json
import logging
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd

# ── Config ──────────────────────────────────────────────────────────────
BASE_URL  = os.getenv("WWS_BASE_URL", "https://data.wastewaterscan.org")
API_URL   = os.getenv("WWS_API_URL", "https://api.wastewaterscan.org/graphql")
OUT_DIR   = os.getenv("WWS_OUT_DIR", "output")
CSV_PATH  = os.path.join(OUT_DIR, f"wastewater_{datetime.now(timezone.utc):%Y%m%d}.csv")

LOOKBACK_DAYS      = int(os.getenv("WWS_LOOKBACK_DAYS", "14"))
THIRD_LATEST_ONLY  = os.getenv("WWS_THIRD_LATEST", "0") == "1"

EMAIL_ENABLED = os.getenv("WWS_EMAIL", "0") == "1"
SMTP_HOST = os.getenv("WWS_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("WWS_SMTP_PORT", "465"))
SMTP_USER = os.getenv("WWS_SMTP_USER", "")
SMTP_PASS = os.getenv("WWS_SMTP_PASS", "")
EMAIL_TO  = os.getenv("WWS_EMAIL_TO", "")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("wws")

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://data.wastewaterscan.org",
    "Referer": "https://data.wastewaterscan.org/",
})

# ── GraphQL Client ──────────────────────────────────────────────────────

def gql_execute(query, variables=None):
    """Execute a GraphQL query. Returns the `data` dict or None."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    try:
        r = session.post(API_URL, json=payload, timeout=60)
        r.raise_for_status()
        body = r.json()
        if body.get("errors"):
            for err in body["errors"]:
                log.error(f"  GraphQL error: {err.get('message', err)}")
            return None
        return body.get("data")
    except Exception as e:
        log.error(f"  GraphQL request failed: {e}")
        return None


def unwrap_type(type_ref):
    """Unwrap nested ofType wrappers → (type_name, kind)."""
    if not type_ref:
        return None, None
    while type_ref.get("ofType"):
        type_ref = type_ref["ofType"]
    return type_ref.get("name"), type_ref.get("kind")


def introspect_type(type_name):
    """Introspect a named GraphQL type (object or input)."""
    q = """
    query($name: String!) {
      __type(name: $name) {
        name
        kind
        inputFields {
          name
          type { name kind ofType { name kind ofType { name kind } } }
        }
        fields {
          name
          type { name kind ofType { name kind ofType { name kind } } }
        }
      }
    }
    """
    data = gql_execute(q, {"name": type_name})
    if data and data.get("__type"):
        return data["__type"]
    return None


# Geo / internal fields to strip so they don't create bunched columns
GEO_BLOCKLIST = {
    'geometry', 'geography', 'coordinates', 'geojson', 'geom',
    'the_geom', 'wkb_geometry', 'bounds', 'location', '__typename',
    'pageInfo', 'cursor', 'type', 'features', 'crs',
}


def build_field_selection(type_name, depth=0, max_depth=2):
    """Recursively build a GraphQL field-selection string, skipping geo fields."""
    if depth > max_depth:
        return ""
    t = introspect_type(type_name)
    if not t or not t.get("fields"):
        return ""
    parts = []
    for f in t["fields"]:
        fname = f["name"]
        if fname in GEO_BLOCKLIST or fname.startswith("__"):
            continue
        underlying_name, underlying_kind = unwrap_type(f["type"])
        if underlying_kind == "SCALAR":
            parts.append(fname)
        elif underlying_kind == "OBJECT" and depth < max_depth:
            nested = build_field_selection(underlying_name, depth + 1, max_depth)
            if nested:
                parts.append(f"{fname} {{ {nested} }}")
    return " ".join(parts)


def find_query_in_schema(query_name):
    """Search the schema's Query type for `query_name`.
    Returns (return_type_name, args_list) or (None, None)."""
    q = """
    {
      __schema {
        queryType {
          fields {
            name
            args {
              name
              type { name kind ofType { name kind ofType { name kind ofType { name kind } } } }
              defaultValue
            }
            type { name kind ofType { name kind ofType { name kind ofType { name kind } } } }
          }
        }
      }
    }
    """
    data = gql_execute(q)
    if not data:
        return None, None
    for f in data["__schema"]["queryType"]["fields"]:
        if f["name"] == query_name:
            ret_name, _ = unwrap_type(f["type"])
            return ret_name, f.get("args", [])
    return None, None


def _unwrap_edges(result):
    """If a result is a Relay connection {edges:[{node:…}]}, flatten to [node, …]."""
    if isinstance(result, dict) and "edges" in result:
        return [e["node"] for e in result["edges"] if isinstance(e, dict) and "node" in e]
    return result

# ── Fetch Sites ─────────────────────────────────────────────────────────

def fetch_sites():
    """Fetch all monitoring sites from the API."""
    log.info("→ Fetching sites…")

    ret_type, args = find_query_in_schema("sites")
    if ret_type:
        site_fields = build_field_selection(ret_type, max_depth=1)
        if not site_fields:
            site_fields = "id name state region siteId siteName"
        for q in [
            f"query {{ sites {{ {site_fields} }} }}",
            f"query {{ sites(limit: 10000) {{ {site_fields} }} }}",
            f"query {{ sites(first: 10000) {{ {site_fields} }} }}",
        ]:
            data = gql_execute(q)
            if data and data.get("sites"):
                sites = _unwrap_edges(data["sites"])
                log.info(f"  ✓ Found {len(sites)} sites")
                return sites

    # Hardcoded fallback
    log.info("  Trying hardcoded sites query…")
    q = """
    query { sites { id name state region siteId siteName } }
    """
    data = gql_execute(q)
    if data and data.get("sites"):
        sites = _unwrap_edges(data["sites"])
        log.info(f"  ✓ Found {len(sites)} sites (hardcoded)")
        return sites

    log.error("  ✗ Failed to fetch sites")
    return []

# ── Fetch Pathogens ─────────────────────────────────────────────────────

def fetch_pathogens():
    """Fetch available pathogens (optional)."""
    log.info("→ Fetching pathogens…")
    ret_type, _ = find_query_in_schema("pathogens")
    if ret_type:
        pf = build_field_selection(ret_type, max_depth=1)
        q = f"query {{ pathogens {{ {pf} }} }}"
        data = gql_execute(q)
        if data and data.get("pathogens"):
            p = _unwrap_edges(data["pathogens"])
            log.info(f"  ✓ Found {len(p)} pathogens")
            return p
    log.info("  Pathogens query not available — skipping")
    return []

# ── Fetch Measurements ──────────────────────────────────────────────────

def fetch_measurements(site_ids, pathogens=None):
    """Fetch measurement data using proper filter arguments.

    The WastewaterScan `measurements` query requires a `filter` input
    containing at minimum `siteIds` and a date range.  We introspect
    the filter input type, build the object dynamically, and fall back
    to per-site batching if a single bulk request is rejected.
    """
    log.info(f"→ Fetching measurements for {len(site_ids)} sites…")

    end_date   = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    start_str  = start_date.strftime("%Y-%m-%d")
    end_str    = end_date.strftime("%Y-%m-%d")

    # ── Locate the measurements query in the schema ──
    query_name = "measurements"
    ret_type, args = find_query_in_schema(query_name)
    if not ret_type:
        for alt in ["getMeasurements", "allMeasurements", "measurementResults"]:
            ret_type, args = find_query_in_schema(alt)
            if ret_type:
                query_name = alt
                break
    if not ret_type:
        log.error("  ✗ No measurements query found via introspection")
        return _try_hardcoded_measurements(site_ids, start_str, end_str)

    measurement_fields = build_field_selection(ret_type, max_depth=1)
    if not measurement_fields:
        measurement_fields = ("siteId date pathogen concentration "
                              "concentrationUnits pctDetectable rollingAverage "
                              "site { id name state region }")
    log.info(f"  Measurement fields: {measurement_fields}")

    # ── Parse argument metadata ──
    arg_map = {}
    for a in (args or []):
        t_name, t_kind = unwrap_type(a["type"])
        arg_map[a["name"]] = {"type_name": t_name, "kind": t_kind}
    log.info(f"  Query arguments: {json.dumps({k: v['type_name'] for k, v in arg_map.items()})}")

    all_measurements = []

    # ═══ Strategy 1: `filter` input argument ═══
    if "filter" in arg_map:
        filter_type = arg_map["filter"]["type_name"]
        log.info(f"  Filter input type: {filter_type}")

        # Introspect the filter input to discover its fields
        ft = introspect_type(filter_type)
        filter_fields = {}
        if ft and ft.get("inputFields"):
            for fi in ft["inputFields"]:
                fn, fk = unwrap_type(fi["type"])
                filter_fields[fi["name"]] = {"type_name": fn, "kind": fk}
        log.info(f"  Filter input fields: {json.dumps({k: v['type_name'] for k, v in filter_fields.items()})}")

        # Build the filter object using only fields the input accepts
        def _build_filter(ids):
            f = {}
            if "siteIds" in filter_fields:
                f["siteIds"] = ids
            if "siteId" in filter_fields and len(ids) == 1:
                f["siteId"] = ids[0]
            if "startDate" in filter_fields:
                f["startDate"] = start_str
            if "endDate" in filter_fields:
                f["endDate"] = end_str
            if "start" in filter_fields:
                f["start"] = start_str
            if "end" in filter_fields:
                f["end"] = end_str
            if "pathogens" in filter_fields and pathogens:
                f["pathogens"] = [p.get("name") if isinstance(p, dict) else str(p) for p in pathogens]
            return f

        query = f"""
        query GetMeasurements($filter: {filter_type}!) {{
          {query_name}(filter: $filter) {{
            {measurement_fields}
          }}
        }}
        """

        # Try bulk first
        filter_obj = _build_filter(site_ids)
        log.info(f"  Attempting bulk query ({len(site_ids)} sites)…")
        data = gql_execute(query, {"filter": filter_obj})
        if data and data.get(query_name):
            result = _unwrap_edges(data[query_name])
            if isinstance(result, list) and result:
                log.info(f"  ✓ Got {len(result)} measurements (bulk)")
                return result

        # Bulk failed → batch per-site
        log.info("  Bulk query failed. Trying per-site batches (batch=20)…")
        batch_size = 20
        for i in range(0, len(site_ids), batch_size):
            batch = site_ids[i:i + batch_size]
            data = gql_execute(query, {"filter": _build_filter(batch)})
            if data and data.get(query_name):
                result = _unwrap_edges(data[query_name])
                if isinstance(result, list):
                    all_measurements.extend(result)
                    log.info(f"    Batch {i // batch_size + 1}: "
                             f"+{len(result)} (running total: {len(all_measurements)})")
        if all_measurements:
            log.info(f"  ✓ Total via batches: {len(all_measurements)}")
            return all_measurements

    # ═══ Strategy 2: individual top-level arguments ═══
    if any(k in arg_map for k in ("siteIds", "startDate", "limit", "first")):
        var_defs, arg_strs, variables = [], [], {}
        if "siteIds" in arg_map:
            var_defs.append("$siteIds: [ID!]");  arg_strs.append("siteIds: $siteIds")
            variables["siteIds"] = site_ids
        if "startDate" in arg_map:
            var_defs.append("$startDate: String"); arg_strs.append("startDate: $startDate")
            variables["startDate"] = start_str
        if "endDate" in arg_map:
            var_defs.append("$endDate: String");   arg_strs.append("endDate: $endDate")
            variables["endDate"] = end_str
        if "limit" in arg_map:
            var_defs.append("$limit: Int");        arg_strs.append("limit: $limit")
            variables["limit"] = 100000
        if "first" in arg_map:
            var_defs.append("$first: Int");        arg_strs.append("first: $first")
            variables["first"] = 100000

        query = f"""
        query GetMeasurements({', '.join(var_defs)}) {{
          {query_name}({', '.join(arg_strs)}) {{ {measurement_fields} }}
        }}
        """
        data = gql_execute(query, variables)
        if data and data.get(query_name):
            result = _unwrap_edges(data[query_name])
            if isinstance(result, list) and result:
                log.info(f"  ✓ Got {len(result)} measurements (individual args)")
                return result

    # ═══ Strategy 3: no arguments ═══
    query = f"query {{ {query_name} {{ {measurement_fields} }} }}"
    data = gql_execute(query)
    if data and data.get(query_name):
        result = _unwrap_edges(data[query_name])
        if isinstance(result, list) and result:
            log.info(f"  ✓ Got {len(result)} measurements (no args)")
            return result

    # ═══ Strategy 4: hardcoded patterns ═══
    log.info("  Introspection-based queries all failed. Trying hardcoded patterns…")
    return _try_hardcoded_measurements(site_ids, start_str, end_str)


def _try_hardcoded_measurements(site_ids, start_str, end_str):
    """Last-resort: try known query shapes without relying on introspection."""
    patterns = [
        # filter with siteIds + dates
        {
            "query": """
                query($siteIds: [ID!], $startDate: String, $endDate: String) {
                  measurements(filter: { siteIds: $siteIds, startDate: $startDate, endDate: $endDate }) {
                    siteId date pathogen concentration concentrationUnits
                    pctDetectable rollingAverage
                    site { id name state region }
                  }
                }
            """,
            "variables": {"siteIds": site_ids, "startDate": start_str, "endDate": end_str},
        },
        # filter with dates only (no siteIds)
        {
            "query": """
                query($startDate: String, $endDate: String) {
                  measurements(filter: { startDate: $startDate, endDate: $endDate }) {
                    siteId date pathogen concentration concentrationUnits
                    pctDetectable rollingAverage
                    site { id name state region }
                  }
                }
            """,
            "variables": {"startDate": start_str, "endDate": end_str},
        },
        # no filter
        {
            "query": """
                query {
                  measurements {
                    siteId date pathogen concentration concentrationUnits
                    pctDetectable rollingAverage
                    site { id name state region }
                  }
                }
            """,
            "variables": None,
        },
        # Relay connection style
        {
            "query": """
                query {
                  measurements(first: 100000) {
                    edges {
                      node {
                        siteId date pathogen concentration concentrationUnits
                        pctDetectable rollingAverage
                        site { id name state region }
                      }
                    }
                  }
                }
            """,
            "variables": None,
        },
    ]
    for i, p in enumerate(patterns):
        log.info(f"  Hardcoded pattern {i + 1}…")
        data = gql_execute(p["query"], p["variables"])
        if data and data.get("measurements"):
            result = _unwrap_edges(data["measurements"])
            if isinstance(result, list) and result:
                log.info(f"  ✓ Got {len(result)} measurements (pattern {i + 1})")
                return result
    log.error("  ✗ All hardcoded patterns failed")
    return []

# ── Main Fetch ──────────────────────────────────────────────────────────

def fetch_data():
    """Fetch sites → pathogens → measurements, then enrich."""

    pathogens = fetch_pathogens()
    sites = fetch_sites()
    if not sites:
        raise RuntimeError("Failed to fetch sites — cannot proceed without site IDs")

    # Build site lookup
    site_ids = []
    site_lookup = {}
    for s in sites:
        sid = str(s.get("id") or s.get("siteId") or s.get("site_id") or s.get("name") or "")
        if sid:
            site_ids.append(sid)
            site_lookup[sid] = s
    log.info(f"  Collected {len(site_ids)} site IDs")

    measurements = fetch_measurements(site_ids, pathogens)
    if not measurements:
        raise RuntimeError("Failed to fetch measurements — all strategies exhausted")

    log.info(f"✓ Total measurements fetched: {len(measurements)}")

    # Enrich each measurement with its site's metadata
    for m in measurements:
        sid = str(m.get("siteId") or m.get("site_id") or m.get("locationId") or "")
        if sid in site_lookup:
            for k, v in site_lookup[sid].items():
                if k not in m and isinstance(v, (str, int, float, bool)):
                    m.setdefault(f"site_{k}", v)

    return measurements

# ── DataFrame Processing ────────────────────────────────────────────────

def to_df(records):
    """Convert raw measurement records → clean DataFrame.

    Every value is allocated to its designated column. Timestamps are
    decomposed into separate year/month/day/hour/minute/second columns.
    GeoJSON fields (geometry, type=Feature, coordinates) are stripped.
    """
    if not isinstance(records, list):
        records = [records]

    flat_list = []
    now = datetime.now(timezone.utc)

    for record in records:
        # ── Extract nested location/site object ──
        loc = record.get("location") or record.get("site") or record.get("properties") or {}
        if not isinstance(loc, dict):
            loc = {}

        # Merge in any site_ prefixed enrichment fields
        site_fields = {}
        for k, v in record.items():
            if k.startswith("site_") and isinstance(v, (str, int, float, bool)):
                site_fields[k[5:]] = v
        merged_loc = {**site_fields, **loc}

        # ── Raw sample date ──
        raw_date = (record.get("sampleDate") or record.get("sample_date")
                    or record.get("date") or record.get("sampledAt")
                    or record.get("collectedAt") or record.get("site_date"))

        # ════════════════════════════════════════════════════════════════
        # Build the flat record — each value in its proper column
        # ════════════════════════════════════════════════════════════════

        flat = {
            # ── Sample Date (primary) ──
            "sample_date": raw_date,

            # ── Pathogen ──
            "pathogen": (record.get("pathogen") or record.get("target")
                         or record.get("pathogenName")),

            # ── Concentration ──
            "concentration": (record.get("concentration") or record.get("value")
                              or record.get("concentrationValue") or record.get("result")),
            "concentration_units": (record.get("concentrationUnits") or record.get("units")
                                    or record.get("unit") or record.get("concentration_units")),

            # ── Detection Percentage ──
            "pct_detectable": (record.get("pctDetect") or record.get("pct_detectable")
                               or record.get("percentDetectable")
                               or record.get("pctDetectable")),

            # ── Rolling Average ──
            "rolling_average": (record.get("rollingAverage")
                                or record.get("rolling_average")
                                or record.get("rollingAvg")),

            # ── Location ──
            "location_id": (merged_loc.get("id") or merged_loc.get("siteId")
                            or merged_loc.get("site_id")
                            or record.get("location_id")
                            or record.get("site_id")
                            or record.get("siteId")),
            "location_name": (merged_loc.get("name") or merged_loc.get("siteName")
                              or merged_loc.get("site_name")
                              or record.get("location_name")
                              or record.get("site_name")
                              or record.get("siteName")),
            "region": (merged_loc.get("region") or record.get("region")
                       or record.get("site_region")),
            "state": (merged_loc.get("state") or record.get("state")
                      or record.get("site_state")),

            # ── Fetched At (full ISO 8601 timestamp) ──
            "fetched_at_utc": now.isoformat(),

            # ── Decomposed: Fetched At ──
            "fetched_year":     now.year,
            "fetched_month":    now.month,
            "fetched_day":      now.day,
            "fetched_hour":     now.hour,
            "fetched_minute":   now.minute,
            "fetched_second":   now.second,
            "fetched_timezone": "UTC",
            "fetched_date":     now.strftime("%Y-%m-%d"),
            "fetched_time":     now.strftime("%H:%M:%S"),
        }

        # ── Decomposed: Sample Date ──
        if raw_date:
            try:
                parsed = pd.to_datetime(raw_date, errors="coerce")
                if pd.notna(parsed):
                    flat["sample_year"]    = parsed.year
                    flat["sample_month"]   = parsed.month
                    flat["sample_day"]     = parsed.day
                    flat["sample_weekday"] = parsed.day_name()
                else:
                    flat.update(sample_year=None, sample_month=None,
                                sample_day=None, sample_weekday=None)
            except Exception:
                flat.update(sample_year=None, sample_month=None,
                            sample_day=None, sample_weekday=None)
        else:
            flat.update(sample_year=None, sample_month=None,
                        sample_day=None, sample_weekday=None)

        # ── Capture any remaining unmapped scalar fields ──
        mapped_keys = set(flat.keys())
        for k, v in record.items():
            if k in GEO_BLOCKLIST or k in mapped_keys or k.startswith("site_"):
                continue
            if isinstance(v, (str, int, float, bool)):
                flat[k] = v
            elif isinstance(v, dict):
                for nk, nv in v.items():
                    if nk in GEO_BLOCKLIST:
                        continue
                    if isinstance(nv, (str, int, float, bool)):
                        key = f"{k}_{nk}"
                        if key not in flat:
                            flat[key] = nv

        flat_list.append(flat)

    df = pd.DataFrame(flat_list)

    # Ensure sample_date is datetime
    df["sample_date"] = pd.to_datetime(df["sample_date"], errors="coerce")

    # Sort newest first
    if "sample_date" in df.columns and df["sample_date"].notna().any():
        df = df.sort_values("sample_date", ascending=False).reset_index(drop=True)

    # Optional: keep only the third-to-latest sample date
    if THIRD_LATEST_ONLY and "sample_date" in df.columns and df["sample_date"].notna().any():
        unique_dates = sorted(df["sample_date"].dropna().unique(), reverse=True)
        if len(unique_dates) >= 3:
            target = unique_dates[2]
            df = df[df["sample_date"] == target].reset_index(drop=True)
            log.info(f"  Filtered to third-to-latest date: {target.date()}")

    return df

# ── Output ──────────────────────────────────────────────────────────────

def save_csv(df, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False)
    log.info(f"✓ Saved {path}")
    log.info(f"  Rows: {len(df)}  |  Columns: {len(df.columns)}")
    log.info("  Column summary:")
    for i, col in enumerate(df.columns, 1):
        non_null = df[col].notna().sum()
        log.info(f"    {i:2d}. {col:30s}  ({non_null}/{len(df)} non-null)")


def maybe_email(path):
    if not EMAIL_ENABLED:
        return
    msg = EmailMessage()
    msg["Subject"] = f"WastewaterScan export — {os.path.basename(path)}"
    msg["From"], msg["To"] = SMTP_USER, EMAIL_TO
    msg.set_content("Weekly wastewater data export attached.")
    with open(path, "rb") as f:
        msg.add_attachment(f.read(), maintype="text", subtype="csv",
                           filename=os.path.basename(path))
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
    log.info(f"✓ Emailed to {EMAIL_TO}")

# ── Main ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  WastewaterScan Data Exporter")
    log.info(f"  API:       {API_URL}")
    log.info(f"  Lookback:  {LOOKBACK_DAYS} days")
    log.info(f"  3rd-latest filter: {'ON' if THIRD_LATEST_ONLY else 'OFF'}")
    log.info("=" * 70)

    records = fetch_data()
    df      = to_df(records)
    save_csv(df, CSV_PATH)
    maybe_email(CSV_PATH)

    log.info("\nDone!")
