#!/usr/bin/env python3
"""
WastewaterScan Data Exporter - Fixed Dynamic Introspection Version
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
KNOWN_API_URL = os.getenv("WWS_API_URL", "")
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
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://data.wastewaterscan.org",
    "Referer": "https://data.wastewaterscan.org/",
})

# ── Full introspection (fields + args + types) ─────────────────────────
INTROSPECTION_QUERY = """
{
  __schema {
    queryType {
      fields {
        name
        args {
          name
          type { kind name ofType { kind name ofType { kind name ofType { kind name } } } }
        }
        type { kind name ofType { kind name ofType { kind name ofType { kind name } } } }
      }
    }
    types {
      name
      kind
      fields { name type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } }
    }
  }
}
"""

# ── Type helpers ───────────────────────────────────────────────────────
def unwrap_type(t):
    """Strip NON_NULL/LIST wrappers, return underlying type ref."""
    while t and t.get("kind") in ("NON_NULL", "LIST"):
        t = t.get("ofType")
    return t or {}

def type_name(t):
    if not t: return None
    if t.get("name"): return t["name"]
    return type_name(t.get("ofType"))

def is_list(t):
    if not t: return False
    if t.get("kind") == "LIST": return True
    return is_list(t.get("ofType"))

# ── Build args string from introspected args ───────────────────────────
def build_args_string(args):
    """Provide minimal valid values for any NON_NULL argument."""
    parts = []
    for a in args or []:
        tn = type_name(a["type"])
        # Only force values for NON_NULL args (else omit and let server default)
        if a["type"].get("kind") != "NON_NULL":
            continue
        if tn in ("Int", "Float"):
            parts.append(f'{a["name"]}: 1')
        elif tn == "Boolean":
            parts.append(f'{a["name"]}: false')
        elif tn == "String":
            parts.append(f'{a["name"]}: ""')
        elif tn in ("json", "jsonb", "JSON"):
            parts.append(f'{a["name"]}: {{}}')
        else:
            # Object input type — try empty input object
            parts.append(f'{a["name"]}: {{}}')
    return "(" + ", ".join(parts) + ")" if parts else ""

# ── Recursively build selection set ────────────────────────────────────
def build_selection_set(type_name, type_index, depth=0, max_depth=3):
    if depth >= max_depth:
        return ""
    t = type_index.get(type_name)
    if not t or t.get("kind") != "OBJECT":
        return ""
    fields = t.get("fields") or []
    if not fields:
        return ""
    parts = []
    for f in fields:
        if f["name"].startswith("__"):
            continue
        inner = unwrap_type(f["type"])
        if inner.get("kind") == "OBJECT":
            if depth + 1 >= max_depth:
                continue  # don't nest too deep
            sub = build_selection_set(inner.get("name"), type_index, depth + 1, max_depth)
            if sub:
                parts.append(f'{f["name"]} {{ {sub} }}')
        else:
            parts.append(f["name"])
    return " ".join(parts)

# ── Try each query field dynamically ───────────────────────────────────
def try_query_field(url, field, type_index):
    name = field["name"]
    args_str = build_args_string(field.get("args", []))
    ret_unwrapped = unwrap_type(field["type"])
    ret_kind = ret_unwrapped.get("kind")
    ret_name = ret_unwrapped.get("name")

    # Determine the named type whose fields we should select
    if ret_kind == "OBJECT" and ret_name:
        selection = build_selection_set(ret_name, type_index)
    elif is_list(field["type"]):
        inner_named = type_name(field["type"])
        selection = build_selection_set(inner_named, type_index) if inner_named else "id"
    elif ret_kind in ("OBJECT",):
        selection = build_selection_set(ret_name, type_index)
    else:
        selection = ""  # scalar root

    if selection:
        query = f"query {{ {name}{args_str} {{ {selection} }} }}"
    else:
        query = f"query {{ {name}{args_str} }}"

    try:
        r = session.post(url, json={"query": query},
                         headers={"Content-Type": "application/json"}, timeout=20)
        if r.status_code != 200:
            log.info(f"    {name}: HTTP {r.status_code}")
            return None
        body = r.json()
        if body.get("data") and body["data"].get(name) is not None:
            val = body["data"][name]
            if val:  # non-empty
                log.info(f"    ✓✓✓ FOUND VALID ROOT: '{name}'  (args='{args_str}')")
                return body
            else:
                log.info(f"    {name}: returned empty (args may be wrong)")
        if body.get("errors"):
            msg = body["errors"][0].get("message", "")[:140]
            log.info(f"    {name}: {msg}")
    except Exception as e:
        log.info(f"    {name}: exception {e}")
    return None

# ── GraphQL endpoint tester ────────────────────────────────────────────
def try_graphql(url):
    log.info(f"  Trying GraphQL: {url}")
    try:
        r = session.post(url, json={"query": INTROSPECTION_QUERY},
                         headers={"Content-Type": "application/json"}, timeout=15)
        if r.status_code == 403:
            log.info("    ✗ 403 Forbidden")
            return None
        if r.status_code != 200:
            log.info(f"    ✗ HTTP {r.status_code}")
            return None
        body = r.json()
        if not body.get("data") or not body["data"].get("__schema"):
            if body.get("errors"):
                log.info(f"    Introspection disabled: {body['errors'][0].get('message')}")
            return None

        schema = body["data"]["__schema"]
        fields = schema["queryType"]["fields"]
        type_index = {t["name"]: t for t in schema.get("types", []) if t.get("name")}

        field_names = [f["name"] for f in fields if not f["name"].startswith("__")]
        log.info(f"    🔥 INTROSPECTION SUCCESS! {len(field_names)} queries: {field_names}")

        priorities = [
            "measurements", "allMeasurements", "getMeasurements",
            "samples", "samplingEvents", "results", "timeseries",
            "sites", "allSites", "sitesData", "data", "pathogens",
        ]
        ordered = sorted(
            fields,
            key=lambda f: (0 if f["name"] in priorities else 1, f["name"]),
        )

        for field in ordered:
            if field["name"].startswith("__"):
                continue
            result = try_query_field(url, field, type_index)
            if result:
                return result
    except requests.exceptions.ConnectionError:
        log.info("    ✗ DNS/Connection failed")
    except Exception as e:
        log.info(f"    Introspection exception: {e}")
    return None

# ── Embedded-data extraction ───────────────────────────────────────────
def extract_records_from_json(payload):
    def _find(obj, depth=0):
        if depth > 10: return None
        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict) and len(obj[0].keys()) >= 1:
            return obj
        if isinstance(obj, dict):
            for v in obj.values():
                r = _find(v, depth + 1)
                if r is not None: return r
        return None
    return _find(payload)

def _extract_next_data(html):
    m = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            if 'props' in data and 'pageProps' in data['props']:
                return data['props']['pageProps']
            return data
        except json.JSONDecodeError: pass
    return None

def _harvest_urls(text, base, out):
    out.update(re.findall(r'["\'`](https?://[^"\'`]*graphql[^"\'`]*)["\'`]', text, re.I))
    out.update(re.findall(r'["\'`](https?://[^"\'`]*/api/[^"\'`]*)["\'`]', text, re.I))
    for p in re.findall(r'["\'`](/(?:api|graphql)/[^"\'`]*)["\'`]', text): out.add(urljoin(base, p))
    for p in re.findall(r'fetch\s*\(\s*["\'`]([^"\'`]+)["\'`]', text): out.add(p if p.startswith("http") else urljoin(base, p))

def discover_endpoints(base_url, html):
    candidates = set()
    _harvest_urls(html, base_url, candidates)
    ext_scripts = re.findall(r'<script[^>]+src="([^"]+)"', html)
    for src in ext_scripts[:25]:  # bumped from 15 → 25
        if not src.endswith('.js'): continue
        url = urljoin(base_url, src)
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200: _harvest_urls(r.text, base_url, candidates)
        except requests.exceptions.RequestException: pass
    # Common default paths (Hasura / Next.js API routes)
    for path in ['/graphql', '/api/graphql', '/v1/graphql', '/api/data',
                 '/api/measurements', '/data.json', '/api/sites',
                 '/api/measurements.json', '/api/v1/measurements']:
        candidates.add(urljoin(base_url, path))
    gql = sorted(c for c in candidates if 'graphql' in c.lower())
    rest = sorted(c for c in candidates if c not in gql)
    return gql + rest

# ── REST tester ────────────────────────────────────────────────────────
def try_rest(url):
    log.info(f"  Trying REST: {url}")
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            ct = r.headers.get('content-type', '')
            if 'json' in ct:
                data = r.json()
                if extract_records_from_json(data):
                    log.info("    ✓ REST JSON hit!")
                    return data
            if 'csv' in ct or (r.text and r.text[:1].isdigit()):
                log.info("    ✓ REST CSV hit!")
                return {"_csv": r.text}
    except requests.exceptions.ConnectionError:
        log.info("    ✗ DNS/Connection failed")
    except Exception: pass
    return None

# ── Main Fetch Logic ───────────────────────────────────────────────────
def fetch_data():
    if KNOWN_API_URL:
        log.info(f"Using WWS_API_URL = {KNOWN_API_URL}")
        data = try_graphql(KNOWN_API_URL) if 'graphql' in KNOWN_API_URL.lower() else try_rest(KNOWN_API_URL)
        if data: return data
        raise RuntimeError(f"Configured WWS_API_URL failed: {KNOWN_API_URL}")

    log.info(f"→ Fetching {BASE_URL}")
    try:
        resp = session.get(BASE_URL, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        base, html = resp.url, resp.text
        log.info(f"  Page fetched. Length: {len(html)} bytes.")
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch main page: {e}")
        base, html = BASE_URL, ""

    next_data = _extract_next_data(html)
    if next_data:
        records = extract_records_from_json(next_data)
        if records:
            log.info(f"✓ Extracted {len(records)} records from __NEXT_DATA__")
            return records

    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for script_text in scripts:
        s = script_text.strip()
        if not (s.startswith('{') or s.startswith('[')): continue
        try:
            payload = json.loads(s)
            records = extract_records_from_json(payload)
            if records:
                log.info(f"✓ Extracted {len(records)} records from embedded HTML JSON")
                return records
        except json.JSONDecodeError: pass

    log.info("  No embedded data found. Discovering API endpoints...")
    endpoints = discover_endpoints(base, html)
    log.info(f"  {len(endpoints)} candidate endpoint(s):")
    for url in endpoints: log.info(f"    • {url}")

    for url in endpoints:
        data = try_graphql(url) if 'graphql' in url.lower() else try_rest(url)
        if data: return data

    raise RuntimeError(
        "No endpoint returned data. Check logs above for "
        "'🔥 INTROSPECTION SUCCESS' to see which queries the server exposed."
    )

# ── Processing ─────────────────────────────────────────────────────────
def extract_records(payload):
    if isinstance(payload, dict) and '_csv' in payload:
        return pd.read_csv(StringIO(payload['_csv'])).to_dict('records')
    if isinstance(payload, list):
        return payload

    data = payload.get('data', payload) if isinstance(payload, dict) else payload
    # Common container keys
    for key in ['measurements', 'results', 'data', 'items', 'records',
                'sites', 'samples', 'samplingEvents', 'pathogens',
                'metrics', 'timeseries', 'sitesData',
                'allMeasurements', 'allSites', 'getMeasurements']:
        if isinstance(data, dict) and key in data and isinstance(data[key], list):
            recs = data[key]
            if recs and isinstance(recs[0], dict) and 'node' in recs[0]:
                recs = [e['node'] for e in recs]
            return recs

    # Fallback: find any list of dicts anywhere
    found = extract_records_from_json(data)
    return found if found else data

def pick_third_latest(records):
    if not isinstance(records, list) or not records:
        raise RuntimeError("No records to pick from.")
    if len(records) < 3:
        log.warning(f"Only got {len(records)} records, returning latest instead of third-to-latest.")
        return records[0]

    def sort_key(r):
        for k in ('sampleDate', 'sample_date', 'date', 'timestamp',
                  'sampledAt', 'createdAt', 'collectedAt'):
            if r.get(k): return str(r[k])
        return ''
    rec = sorted(records, key=sort_key, reverse=True)[2]
    log.info(f"  Third-to-latest date found: "
             f"{rec.get('sampleDate') or rec.get('sample_date') or rec.get('date', '?')}")
    return rec

def _first_non_null(d, *keys):
    for k in keys:
        v = d.get(k)
        if v is not None: return v
    return None

def to_df(record):
    loc = (record.get('location') or record.get('site')
           or record.get('properties') or record.get('siteBySiteId') or {})
    flat = {
        'sample_date':         _first_non_null(record, 'sampleDate', 'sample_date', 'date', 'sampledAt', 'collectedAt'),
        'pathogen':            _first_non_null(record, 'pathogen', 'target', 'pathogenName', 'pathogen_id'),
        'concentration':       _first_non_null(record, 'concentration', 'value', 'concentrationValue', 'result'),
        'concentration_units': _first_non_null(record, 'concentrationUnits', 'units', 'unit'),
        'pct_detectable':      _first_non_null(record, 'pctDetect', 'pct_detectable', 'percentDetectable', 'pctDetectable'),
        'rolling_average':     _first_non_null(record, 'rollingAverage', 'rolling_average', 'rollingAvg'),
        'location_id':         _first_non_null(loc, 'id', 'siteId', 'site_id'),
        'location_name':       _first_non_null(loc, 'name', 'siteName', 'site_name'),
        'region':              loc.get('region'),
        'state':               loc.get('state'),
        'fetched_at_utc':      datetime.now(timezone.utc).isoformat(),
    }
    df = pd.DataFrame([flat])
    df['sample_date'] = pd.to_datetime(df['sample_date'], errors='coerce')
    return df

def save_csv(df, path):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    df.to_csv(path, index=False)
    log.info(f"✓ Saved {path}")

def maybe_email(path):
    if not EMAIL_ENABLED: return
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
    payload = fetch_data()
    records = extract_records(payload)
    record  = pick_third_latest(records)
    df      = to_df(record)
    save_csv(df, CSV_PATH)
    maybe_email(CSV_PATH)
