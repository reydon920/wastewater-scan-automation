#!/usr/bin/env python3
"""
WastewaterScan Data Exporter
=============================
Fetches infectious-disease measurements from data.wastewaterscan.org.
Strategy:
  1. Configured WWS_API_URL override
  2. __NEXT_DATA__ embedded in the page HTML
  3. Inline <script> JSON
  4. Discover API endpoints from JS bundles → introspect → multi-query
  5. Same-domain REST/GraphQL fallback paths
Picks the third-to-latest record, writes CSV, optionally emails.
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
CSV_PATH      = os.path.join(
    OUT_DIR, f"wastewater_{datetime.now(timezone.utc):%Y%m%d}.csv"
)

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
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://data.wastewaterscan.org",
    "Referer": "https://data.wastewaterscan.org/",
})

# ── Introspection & Queries ────────────────────────────────────────────
INTROSPECTION_QUERY = """{ __schema { queryType { fields { name } } } }"""

GRAPHQL_QUERIES = [
    """query { measurements(limit: 100, orderBy: { sampleDate: DESC }) { sampleDate pathogen concentration concentrationUnits pctDetect rollingAverage location { id name region state } } }""",
    """query { measurements(limit: 100, order_by: {sample_date: desc}) { sample_date pathogen concentration concentration_units pct_detect rolling_average location { id name region state } } }""",
    """query { measurements(first: 100) { edges { node { sampleDate pathogen concentration pctDetect location { id name region state } } } } }""",
    """query { sites { id name state region measurements(limit: 10, orderBy: { sampleDate: DESC }) { sampleDate pathogen concentration pctDetect } } }""",
    """query { measurements(limit: 100) { sampleDate pathogen concentration location { id name region state } } }""",
]

# ── Embedded-data extraction ───────────────────────────────────────────
def _looks_like_records(obj_list):
    """Broad check: does this list contain dictionary objects (potential records)?"""
    if not isinstance(obj_list, list) or len(obj_list) == 0:
        return False
    # A list of dicts with at least 3 keys is likely a data record structure
    if isinstance(obj_list[0], dict) and len(obj_list[0].keys()) >= 3:
        return True
    return False

def extract_records_from_json(payload):
    """Recursively search a JSON object for measurement-record lists."""
    def _find(obj, depth=0):
        if depth > 10:
            return None
        if isinstance(obj, list) and _looks_like_records(obj):
            return obj
        # GeoJSON features → flatten properties
        if (isinstance(obj, list) and len(obj) > 0
                and isinstance(obj[0], dict)
                and obj[0].get("type") == "Feature"
                and "properties" in obj[0]):
            props = [f.get("properties", {}) for f in obj if isinstance(f.get("properties"), dict)]
            if props and _looks_like_records(props):
                return props
        if isinstance(obj, dict):
            for v in obj.values():
                r = _find(v, depth + 1)
                if r is not None:
                    return r
        return None
    return _find(payload)


# ── __NEXT_DATA__ ──────────────────────────────────────────────────────
def _extract_next_data(html):
    m = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None


# ── URL harvesting from JS / HTML ──────────────────────────────────────
def _harvest_urls(text, base, out):
    out.update(re.findall(r'["\'`](https?://[^"\'`]*graphql[^"\'`]*)["\'`]', text, re.I))
    out.update(re.findall(r'["\'`](https?://[^"\'`]*/api/[^"\'`]*)["\'`]', text, re.I))
    for p in re.findall(r'["\'`](/(?:api|graphql)/[^"\'`]*)["\'`]', text):
        out.add(urljoin(base, p))
    for p in re.findall(r'fetch\s*\(\s*["\'`]([^"\'`]+)["\'`]', text):
        out.add(p if p.startswith("http") else urljoin(base, p))
    out.update(re.findall(r'uri\s*:\s*["\'`](https?://[^"\'`]+)["\'`]', text))


# ── Endpoint discovery ─────────────────────────────────────────────────
def discover_endpoints(base_url, html):
    candidates = set()
    _harvest_urls(html, base_url, candidates)

    ext_scripts = re.findall(r'<script[^>]+src="([^"]+)"', html)
    for src in ext_scripts[:15]:
        if not src.endswith('.js'):
            continue
        url = urljoin(base_url, src)
        log.info(f"  Inspecting JS bundle: {url}")
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200:
                _harvest_urls(r.text, base_url, candidates)
        except requests.exceptions.RequestException:
            pass

    # Strict same-domain educated guesses ONLY
    for path in ['/graphql', '/api/graphql', '/v1/graphql', '/api/data', '/api/measurements', '/data.json']:
        candidates.add(urljoin(base_url, path))

    gql = sorted(c for c in candidates if 'graphql' in c.lower())
    rest = sorted(c for c in candidates if c not in gql)
    return gql + rest


def try_graphql(url):
    log.info(f"  Trying GraphQL: {url}")
    try:
        r = session.post(url, json={"query": INTROSPECTION_QUERY}, headers={"Content-Type": "application/json"}, timeout=10)
        if r.status_code == 403:
            log.info("    ✗ 403 Forbidden (API is blocking automated requests)")
            return None
        if r.status_code == 200 and "data" in r.json():
            log.info("    ✓ Introspection successful")
    except requests.exceptions.ConnectionError:
        log.info("    ✗ DNS/Connection failed (skipping)")
        return None
    except Exception:
        pass

    for i, query in enumerate(GRAPHQL_QUERIES):
        try:
            r = session.post(url, json={"query": query}, headers={"Content-Type": "application/json"}, timeout=15)
            if r.status_code == 200:
                body = r.json()
                if body.get("data"):
                    log.info(f"    ✓ Query {i+1} succeeded!")
                    return body
        except Exception:
            continue
    return None


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
        log.info("    ✗ DNS/Connection failed (skipping)")
    except Exception:
        pass
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
        log.info(f"  Page fetched successfully. Length: {len(html)} bytes.")
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch main page: {e}")
        base, html = BASE_URL, ""

    # 1. __NEXT_DATA__
    next_data = _extract_next_data(html)
    if next_data:
        records = extract_records_from_json(next_data)
        if records:
            log.info(f"✓ Extracted {len(records)} records from __NEXT_DATA__")
            return records

    # 2. Generic script JSON
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for script_text in scripts:
        if not script_text.strip().startswith('{') and not script_text.strip().startswith('['):
            continue
        try:
            payload = json.loads(script_text)
            records = extract_records_from_json(payload)
            if records:
                log.info(f"✓ Extracted {len(records)} records from embedded HTML JSON")
                return records
        except json.JSONDecodeError:
            pass

    # 3. Discover & probe endpoints
    log.info("  No embedded data found. Discovering API endpoints...")
    endpoints = discover_endpoints(base, html)

    log.info(f"  {len(endpoints)} candidate endpoint(s) to try:")
    for url in endpoints:
        log.info(f"    • {url}")

    for url in endpoints:
        data = try_graphql(url) if 'graphql' in url.lower() else try_rest(url)
        if data: return data

    raise RuntimeError(
        "No endpoint returned data. Open data.wastewaterscan.org in your browser, "
        "open DevTools (F12) → Network tab → filter 'graphql' or 'api', find the real "
        "endpoint URL, then set it as the WWS_API_URL environment variable in GitHub Actions."
    )


# ── Processing ─────────────────────────────────────────────────────────
def extract_records(payload):
    if isinstance(payload, dict) and '_csv' in payload:
        return pd.read_csv(StringIO(payload['_csv'])).to_dict('records')
    if isinstance(payload, list):
        return payload
        
    data = payload.get('data', payload) if isinstance(payload, dict) else payload
    for key in ('measurements', 'results', 'data', 'items', 'records', 'sites'):
        if isinstance(data, dict) and key in data and isinstance(data[key], list):
            recs = data[key]
            if recs and isinstance(recs[0], dict) and 'node' in recs[0]:
                recs = [e['node'] for e in recs]
            return recs
    return data


def pick_third_latest(records):
    if len(records) < 3:
        raise ValueError(f"Need ≥3 records, got {len(records)}")

    def sort_key(r):
        for k in ('sampleDate', 'sample_date', 'date', 'timestamp', 'createdAt'):
            if r.get(k):
                return str(r[k])
        return ''

    rec = sorted(records, key=sort_key, reverse=True)[2]
    log.info(f"  Third-to-latest date found: {rec.get('sampleDate') or rec.get('date', '?')}")
    return rec


def to_df(record):
    loc = record.get('location') or record.get('site') or record.get('properties') or {}
    flat = {
        'sample_date':         record.get('sampleDate') or record.get('sample_date') or record.get('date'),
        'pathogen':            record.get('pathogen') or record.get('target') or record.get('pathogenName'),
        'concentration':       record.get('concentration') or record.get('value'),
        'concentration_units': record.get('concentrationUnits') or record.get('units'),
        'pct_detectable':      record.get('pctDetect') or record.get('pct_detectable') or record.get('percentDetectable'),
        'rolling_average':     record.get('rollingAverage') or record.get('rolling_average'),
        'location_id':         loc.get('id') or loc.get('siteId'),
        'location_name':       loc.get('name') or loc.get('siteName'),
        'region':              loc.get('region'),
        'state':               loc.get('state'),
        'fetched_at_utc':      datetime.now(timezone.utc).isoformat(),
    }
    df = pd.DataFrame([flat])
    df['sample_date'] = pd.to_datetime(df['sample_date'], errors='coerce')
    return df


# ── Output ─────────────────────────────────────────────────────────────
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
        msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=os.path.basename(path))
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
    log.info(f"✓ Emailed to {EMAIL_TO}")


# ── Main ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    payload = fetch_data()
    records = extract_records(payload)
    record  = pick_third_latest(records)
    df      = to_df(record)
    save_csv(df, CSV_PATH)
    maybe_email(CSV_PATH)
