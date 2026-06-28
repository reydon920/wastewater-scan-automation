#!/usr/bin/env python3
"""
WastewaterScan Data Exporter — fixed field allocation + measurement extraction.
"""

import os
import re
import json
import logging
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone
from urllib.parse import urljoin
from io import StringIO

import requests
import pandas as pd

# ── Config ──────────────────────────────────────────────────────────────
BASE_URL      = os.getenv("WWS_BASE_URL", "https://data.wastewaterscan.org")
GRAPHQL_URL   = os.getenv("WWS_API_URL", "https://api.wastewaterscan.org/graphql")
OUT_DIR       = os.getenv("WWS_OUT_DIR", "output")
CSV_PATH      = os.path.join(OUT_DIR, f"wastewater_{datetime.now(timezone.utc):%Y%m%d}.csv")

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
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://data.wastewaterscan.org",
    "Referer": "https://data.wastewaterscan.org/",
})

# ── Helpers ─────────────────────────────────────────────────────────────
def gql_post(query, timeout=30):
    r = session.post(
        GRAPHQL_URL,
        json={"query": query},
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    if r.status_code != 200:
        return None
    body = r.json()
    if body.get("errors"):
        log.info(f"    GraphQL errors: {body['errors'][0].get('message')}")
        return None
    return body.get("data")

# Field-name aliases so each value lands in the correct column
FIELD_ALIASES = {
    "sample_date": [
        "sampleDate", "sample_date", "date", "sampledAt", "collectedAt",
        "collectionDate", "sampleTime", "dateTime",
    ],
    "pathogen": [
        "pathogen", "target", "pathogenName", "pathogen_name", "metric",
    ],
    "concentration": [
        "concentration", "value", "concentrationValue", "result",
        "measuredValue",
    ],
    "concentration_units": [
        "concentrationUnits", "units", "unit", "uom",
    ],
    "pct_detectable": [
        "pctDetect", "pct_detectable", "percentDetectable",
        "pctDetectable", "detectPct",
    ],
    "rolling_average": [
        "rollingAverage", "rolling_average", "rollingAvg",
    ],
    "location_id":   ["id", "siteId", "site_id", "location_id"],
    "location_name": ["name", "siteName", "site_name", "location_name"],
    "region":        ["region"],
    "state":         ["state", "siteState"],
}

def pick(rec, aliases, default=None):
    for key in aliases:
        if key in rec and rec[key] not in (None, ""):
            return rec[key]
    return default

# ── GraphQL introspection ──────────────────────────────────────────────
INTROSPECTION = """{
  __schema {
    queryType { fields { name args { name type { kind name ofType { kind name } } } type { kind name ofType { kind name ofType { kind name } } } } }
  }
}"""

def unwrap_type(t):
    """Walk NON_NULL / LIST wrappers and return the innermost named type."""
    while t and t.get("ofType"):
        t = t["ofType"]
    return t

def get_type_fields(type_name):
    q = '{ __type(name: "%s") { fields { name type { name kind ofType { name kind ofType { name kind } } } } } }' % type_name
    data = gql_post(q)
    if not data or not data.get("__type"):
        return []
    return data["__type"]["fields"] or []

def build_selection(type_name, depth=0, max_depth=1):
    """Build a GraphQL selection string, expanding OBJECT fields one level."""
    if depth > max_depth:
        return ""
    fields = get_type_fields(type_name)
    if not fields:
        return ""
    parts = []
    for f in fields:
        fname = f["name"]
        if fname.startswith("__"):
            continue
        # skip geo fields that bloat CSVs
        if fname.lower() in {"geometry", "geography", "coordinates",
                             "geojson", "geom", "the_geom", "wkb_geometry", "bounds"}:
            continue
        ft = unwrap_type(f["type"])
        if ft["kind"] == "SCALAR":
            parts.append(fname)
        elif ft["kind"] == "OBJECT" and depth < max_depth:
            nested = build_selection(ft["name"], depth + 1, max_depth)
            if nested:
                parts.append(f"{fname} {{ {nested} }}")
    return " ".join(parts)

def build_args(args_meta):
    """Turn introspected args into a literal arg string with sane defaults."""
    if not args_meta:
        return ""
    pieces = []
    for a in args_meta:
        n = a["name"].lower()
        t = unwrap_type(a["type"])
        tn = (t.get("name") or "").lower()
        # skip required args we can't satisfy
        if a["type"].get("kind") == "NON_NULL":
            # try to satisfy required common ones
            if "limit" in n or "first" in n:
                pieces.append(f'{a["name"]}: 10000')
            elif "offset" in n or "skip" in n:
                pieces.append(f'{a["name"]}: 0')
            else:
                return None  # cannot satisfy a required arg → bail
            continue
        # optional args: skip filters we don't know values for
        if any(k in n for k in ("limit", "first")):
            pieces.append(f'{a["name"]}: 10000')
        elif any(k in n for k in ("offset", "skip")):
            pieces.append(f'{a["name"]}: 0')
        elif "orderby" in n or "order" in n:
            pieces.append(f'{a["name"]}: "sampleDate_DESC"')
    return ("," + " ".join(pieces)) if pieces else ""

# ── Fetch logic ────────────────────────────────────────────────────────
def fetch_measurements():
    log.info(f"→ Introspecting schema at {GRAPHQL_URL}")
    data = gql_post(INTROSPECTION)
    if not data:
        raise RuntimeError("GraphQL introspection failed.")

    root_fields = data["__schema"]["queryType"]["fields"]

    # Sort: measurement-like first
    def priority(f):
        n = f["name"].lower()
        if any(k in n for k in ("measurement", "sample", "metric", "timeseries")):
            return 0
        if "data" in n:
            return 1
        return 2
    root_fields.sort(key=priority)

    for field in root_fields:
        name = field["name"]
        if name.startswith("__"):
            continue
        ft = unwrap_type(field["type"])
        if ft["kind"] != "OBJECT":
            continue
        tn = ft["name"]

        # Skip pure location/site roots UNLESS they nest measurements
        is_site_root = any(k in name.lower() for k in ("site", "location", "place"))

        selection = build_selection(tn, depth=0, max_depth=1 if is_site_root else 0)
        if not selection:
            continue

        args_str = build_args(field.get("args") or [])

        # Build candidate queries
        candidates = []
        if args_str is not None:
            candidates.append(f'query {{ {name}({args_str.strip(", ")}) {{ {selection} }} }}')
            candidates.append(f'query {{ {name} {{ {selection} }} }}')

        for q in candidates:
            log.info(f"  Trying root '{name}' …")
            d = gql_post(q, timeout=60)
            if d and d.get(name):
                val = d[name]
                # If this is a site root, try to pull nested measurements out
                if is_site_root:
                    nested = pull_nested_measurements(val)
                    if nested:
                        log.info(f"    ✓✓✓ Nested measurements under '{name}'")
                        return nested
                    log.info(f"    (root '{name}' has no nested measurements, skipping)")
                    continue
                if isinstance(val, list) and val and looks_like_measurement(val[0]):
                    log.info(f"    ✓✓✓ FOUND measurement root: '{name}'")
                    return val
                if isinstance(val, dict) and looks_like_measurement(val):
                    return [val]
    raise RuntimeError("Could not retrieve measurement data from GraphQL.")

def pull_nested_measurements(val):
    """If `val` is a list of sites (with nested measurement lists), flatten them."""
    out = []
    items = val if isinstance(val, list) else [val]
    for site in items:
        if not isinstance(site, dict):
            continue
        site_ctx = {
            "site_id":   site.get("id") or site.get("siteId"),
            "site_name": site.get("name") or site.get("siteName"),
            "region":    site.get("region"),
            "state":     site.get("state"),
        }
        for k, v in site.items():
            if isinstance(v, list) and v and isinstance(v[0], dict) and looks_like_measurement(v[0]):
                for m in v:
                    if isinstance(m, dict):
                        merged = {**site_ctx, **m}
                        out.append(merged)
    return out

def looks_like_measurement(rec):
    if not isinstance(rec, dict):
        return False
    keys = {k.lower() for k in rec.keys()}
    has_date  = any("date" in k or "time" in k for k in keys)
    has_value = any(x in k for x in (
        "concentration", "value", "result", "average", "detect", "pct", "pathogen", "target"
    ))
    return has_date and has_value

# ── Processing ─────────────────────────────────────────────────────────
def to_df(records):
    if not isinstance(records, list):
        records = [records]
    flat_list = []
    for record in records:
        loc = (record.get("site") or record.get("location")
               or record.get("siteBySiteId") or record.get("properties") or {})

        now = datetime.now(timezone.utc)
        flat = {
            # ── Date / time ───────────────────────────────────────────
            "sample_date":         pick(record, FIELD_ALIASES["sample_date"]),
            # ── Pathogen / measurement ───────────────────────────────
            "pathogen":            pick(record, FIELD_ALIASES["pathogen"]),
            "concentration":       pick(record, FIELD_ALIASES["concentration"]),
            "concentration_units": pick(record, FIELD_ALIASES["concentration_units"]),
            "pct_detectable":      pick(record, FIELD_ALIASES["pct_detectable"]),
            "rolling_average":     pick(record, FIELD_ALIASES["rolling_average"]),
            # ── Location ─────────────────────────────────────────────
            "location_id":         pick(loc,   FIELD_ALIASES["location_id"])   or pick(record, FIELD_ALIASES["location_id"]),
            "location_name":       pick(loc,   FIELD_ALIASES["location_name"]) or pick(record, FIELD_ALIASES["location_name"]),
            "region":              pick(loc,   FIELD_ALIASES["region"])        or pick(record, FIELD_ALIASES["region"]),
            "state":               pick(loc,   FIELD_ALIASES["state"])         or pick(record, FIELD_ALIASES["state"]),
            # ── Fetched-at (split into 3 columns instead of one bunched ISO chunk) ──
            "fetched_at_date":     now.strftime("%Y-%m-%d"),
            "fetched_at_time":     now.strftime("%H:%M:%S"),
            "fetched_at_tz":       "UTC",
            "fetched_at_utc":      now.isoformat(timespec="seconds"),
        }

        # Capture any leftover scalar fields (prefixed so they don't collide)
        for k, v in record.items():
            if k in {a for al in FIELD_ALIASES.values() for a in al}:
                continue
            if isinstance(v, (str, int, float, bool)):
                flat.setdefault(f"extra_{k}", v)
        flat_list.append(flat)

    df = pd.DataFrame(flat_list)

    # Strict typing
    if "sample_date" in df:
        df["sample_date"] = pd.to_datetime(df["sample_date"], errors="coerce", utc=True)
    for col in ("concentration", "pct_detectable", "rolling_average"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── "third-to-latest location data" ────────────────────────────────
    # Sort by sample_date desc, group by location_name, take the 3rd most recent location.
    if "sample_date" in df and "location_name" in df and df["sample_date"].notna().any():
        df_sorted = df.sort_values("sample_date", ascending=False)
        latest_locs = (df_sorted.dropna(subset=["location_name"])
                               .drop_duplicates("location_name")["location_name"]
                               .tolist())
        if len(latest_locs) >= 3:
            target = latest_locs[2]  # third-to-latest
            log.info(f"→ Filtering to third-to-latest location: {target}")
            df = df[df["location_name"] == target].reset_index(drop=True)

    # Final column order
    preferred = [
        "sample_date", "pathogen", "concentration", "concentration_units",
        "pct_detectable", "rolling_average",
        "location_id", "location_name", "region", "state",
        "fetched_at_date", "fetched_at_time", "fetched_at_tz", "fetched_at_utc",
    ]
    cols = [c for c in preferred if c in df.columns] + \
           [c for c in df.columns if c not in preferred]
    return df[cols]

def save_csv(df, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False)
    log.info(f"✓ Saved {path}  ({len(df)} rows)")

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

if __name__ == "__main__":
    records = fetch_measurements()
    df      = to_df(records)
    save_csv(df, CSV_PATH)
    maybe_email(CSV_PATH)
