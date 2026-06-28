#!/usr/bin/env python3
"""
WastewaterScan Data Exporter
=============================
Fetches infectious-disease measurements from data.wastewaterscan.org.
Strategy:
  1. Configured WWS_API_URL override
  2. __NEXT_DATA__ embedded in the page HTML
  3. RSC flight-data chunks  (Next.js 13+ App Router)
  4. Inline <script> JSON
  5. Discover API endpoints from JS bundles → introspect → multi-query
  6. REST fallback paths
Picks the third-to-latest record, writes CSV, optionally emails.
"""

import os
import re
import json
import logging
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
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
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json,text/csv,*/*",
})

# ── Introspection ──────────────────────────────────────────────────────
INTROSPECTION_QUERY = """{
  __schema {
    queryType { fields { name } }
  }
}"""

# ── Multiple GraphQL query shapes to try ────────────────────────────────
GRAPHQL_QUERIES = [
    # 1 – Keystone / Pothos  (camelCase, orderBy object)
    """query { measurements(limit: 100, orderBy: { sampleDate: DESC }) {
      sampleDate pathogen concentration concentrationUnits pctDetect rollingAverage
      location { id name region state }
    }}""",
    # 2 – Hasura  (snake_case, order_by)
    """query { measurements(limit: 100, order_by: {sample_date: desc}) {
      sample_date pathogen concentration concentration_units pct_detect rolling_average
      location { id name region state }
    }}""",
    # 3 – Relay-style edges
    """query { measurements(first: 100) { edges { node {
      sampleDate pathogen concentration concentrationUnits pctDetect rollingAverage
      location { id name region state }
    }}}}}""",
    # 4 – Enum-style sort
    """query { measurements(limit: 100, orderBy: SAMPLE_DATE_DESC) {
      sampleDate pathogen concentration pctDetect
      location { id name region state }
    }}""",
    # 5 – "sites" root with nested measurements
    """query { sites { id name state region
      measurements(limit: 10, orderBy: { sampleDate: DESC }) {
        sampleDate pathogen concentration pctDetect
      }
    }}""",
    # 6 – No sort at all
    """query { measurements(limit: 100) {
      sampleDate pathogen concentration concentrationUnits pctDetect rollingAverage
      location { id name region state }
    }}""",
    # 7 – verb-style "getMeasurements"
    """query { getMeasurements(limit: 100) {
      sampleDate pathogen concentration pctDetect
      location { id name region state }
    }}""",
    # 8 – generic "data" root
    """query { data(limit: 100) {
      sampleDate pathogen concentration location { name state }
    }}""",
    # 9 – "allMeasurements" (common in some frameworks)
    """query { allMeasurements(first: 100) {
      edges { node {
        sampleDate pathogen concentration pctDetect
        location { id name region state }
      }}
    }}""",
    # 10 – "measurement" singular + connection args
    """query { measurement(limit: 100, order_by: {sample_date: desc}) {
      sample_date pathogen concentration
      site { id name state region }
    }}""",
]

GRAPHQL_PATHS = [
    "/api/graphql", "/graphql", "/v1/graphql",
    "/api/v1/graphql", "/api/graphql/query",
    "/gql", "/api/gql", "/query", "/api/query",
]

REST_PATHS = [
    "/api/data", "/api/measurements", "/api/v1/measurements",
    "/api/v1/data", "/api/sites", "/api/export",
    "/data.csv", "/api/csv",
]


# ── Embedded-data extraction ───────────────────────────────────────────
def _looks_like_records(obj_list):
    """Quick check: does this list look like wastewater measurement records?"""
    if not obj_list or not isinstance(obj_list[0], dict):
        return False
    keys = set()
    for item in obj_list[:5]:
        if isinstance(item, dict):
            keys.update(item.keys())
    return any(k in keys for k in (
        "sampleDate", "sample_date", "date", "pathogen",
        "pctDetect", "concentration", "timestamp",
    ))


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
            props = [f.get("properties", {}) for f in obj
                     if isinstance(f.get("properties"), dict)]
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
    """Pull the JSON blob out of  <script id="__NEXT_DATA__"> ... </script>."""
    m = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL,
    )
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None


# ── RSC flight data ────────────────────────────────────────────────────
def _extract_rsc_data(html):
    """Best-effort extraction from Next.js 13+ RSC streaming chunks."""
    chunks = []
    for m in re.finditer(
        r'self\.__next_f\.push\(\[\s*\d+\s*,\s*"(.*?)"\s*\]\)',
        html, re.DOTALL,
    ):
        raw = m.group(1)
        raw = (raw.replace("\\n", "\n")
                  .replace('\\"', '"')
                  .replace("\\\\", "\\"))
        chunks.append(raw)
    if not chunks:
        return None
    combined = "\n".join(chunks)
    # RSC lines look like  key:value  where value can be a JSON blob
    for line in combined.split("\n"):
        _, _, val = line.partition(":")
        val = val.strip()
        if val and val[0] in ("[", "{"):
            try:
                data = json.loads(val)
                records = extract_records_from_json(data)
                if records:
                    return records
            except (json.JSONDecodeError, ValueError):
                pass
    # Fallback: scan for large JSON arrays anywhere
    for m in re.finditer(r'\[[\s\S]{50,}?\]', combined):
        try:
            data = json.loads(m.group())
            records = extract_records_from_json(data)
            if records:
                return records
        except (json.JSONDecodeError, ValueError):
            pass
    return None


# ── URL harvesting from JS / HTML ──────────────────────────────────────
def _harvest_urls(text, base, out):
    # GraphQL absolute URLs
    out.update(re.findall(
        r'["\'`](https?://[^"\'`]*graphql[^"\'`]*)["\'`]', text, re.I))
    # /api/ absolute URLs
    out.update(re.findall(
        r'["\'`](https?://[^"\'`]*/api/[^"\'`]*)["\'`]', text, re.I))
    # Relative /api/ or /graphql/
    for p in re.findall(r'["\'`](/(?:api|graphql)/[^"\'`]*)["\'`]', text):
        out.add(urljoin(base, p))
    # fetch("…")
    for p in re.findall(r'fetch\s*\(\s*["\'`]([^"\'`]+)["\'`]', text):
        out.add(p if p.startswith("http") else urljoin(base, p))
    # uri: "…"  (Apollo)
    out.update(re.findall(r'uri\s*:\s*["\'`](https?://[^"\'`]+)["\'`]', text))
    # url / endpoint containing graphql|api
    out.update(re.findall(
        r'''(?:url|endpoint)\s*:\s*["']([^"']+(?:graphql|api)[^"']*)["']''',
        text, re.I))
    # NEXT_PUBLIC_* env vars
    out.update(re.findall(
        r'NEXT_PUBLIC_[A-Z_]*(?:API|URL|ENDPOINT|GRAPHQL)[A-Z_]*'
        r'\s*[:=]\s*["\'`]([^"\'`]+)["\'`]', text))
    # tRPC paths
    out.update(re.findall(r'["\'`](/api/trpc/[^"\'`]*)["\'`]', text))
    # Same-domain absolute URLs
    parsed = urlparse(base)
    domain = parsed.netloc
    out.update(re.findall(
        rf'["\'`](https?://{re.escape(domain)}/[^"\'`]*(?:api|graphql|query|data|measure)[^"\'`]*)["\'`]',
        text, re.I))


# ── Endpoint discovery ─────────────────────────────────────────────────
def discover_endpoints(base_url, html):
    candidates = set()

    # 1  Harvest from the page HTML itself
    _harvest_urls(html, base_url, candidates)

    # 2  External JS / MJS bundles
    ext_scripts = re.findall(r'<script[^>]+src="([^"]+)"', html)
    for src in ext_scripts[:20]:  # Scan up to 20 bundles
        if not src.endswith('.js') and not src.endswith('.mjs'):
            continue
        url = urljoin(base_url, src)
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200:
                _harvest_urls(r.text, base_url, candidates)
        except requests.RequestException:
            pass

    # 3  Next.js data routes
    m = re.search(r'"buildId"\s*:\s*"([^"]+)"', html)
    if m:
        build_id = m.group(1)
        candidates.add(urljoin(base_url, f"/_next/data/{build_id}/index.json"))
        candidates.add(urljoin(base_url, f"/_next/data/{build_id}/en.json"))

    # 4  Static educated guesses
    for p in GRAPHQL_PATHS + REST_PATHS:
        candidates.add(urljoin(base_url, p))

    # Sort: prefer graphql, then api, then others
    gql = sorted(c for c in candidates if 'graphql' in c.lower() or 'gql' in c.lower())
    rest = sorted(c for c in candidates if c not in gql)
    return gql + rest


def try_graphql(url):
    # First, introspect to verify it's actually a GraphQL endpoint
    try:
        r = session.post(url, json={"query": INTROSPECTION_QUERY}, 
                         headers={"Content-Type": "application/json"}, timeout=10)
        if r.status_code != 200 or "data" not in r.json():
            return None
    except Exception:
        return None

    # If introspection passes, blast it with all query shapes
    for query in GRAPHQL_QUERIES:
        try:
            r = session.post(url, json={"query": query},
                             headers={"Content-Type": "application/json"}, timeout=15)
            if r.status_code == 200:
                body = r.json()
                if body.get("data"):
                    log.info(f"  ✓ GraphQL hit on: {url}")
                    return body
                if body.get("errors"):
                    # Errors mean the schema is there but field names are wrong—keep trying
                    continue
        except Exception:
            continue
    return None


def try_rest(url):
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            ct = r.headers.get('content-type', '')
            if 'json' in ct:
                data = r.json()
                if extract_records_from_json(data):
                    log.info(f"  ✓ REST JSON hit: {url}")
                    return data
            if 'csv' in ct or (r.text and r.text[:1].isdigit()):
                log.info(f"  ✓ REST CSV hit: {url}")
                return {"_csv": r.text}
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
    resp = session.get(BASE_URL, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    base, html = resp.url, resp.text

    # 1. __NEXT_DATA__
    next_data = _extract_next_data(html)
    if next_data:
        records = extract_records_from_json(next_data)
        if records:
            log.info(f"✓ Extracted {len(records)} records from __NEXT_DATA__")
            return records

    # 2. RSC Flight Data
    rsc_records = _extract_rsc_data(html)
    if rsc_records:
        log.info(f"✓ Extracted {len(rsc_records)} records from RSC data")
        return rsc_records

    # 3. Generic script JSON
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

    # 4. Discover & probe endpoints
    log.info("  No embedded data found. Discovering API endpoints from JS bundles...")
    endpoints = discover_endpoints(base, html)

    log.info(f"  {len(endpoints)} candidate endpoint(s) found:")
    for url in endpoints:
        log.info(f"    • {url}")

    for url in endpoints:
        data = try_graphql(url) if 'graphql' in url.lower() or 'gql' in url.lower() else try_rest(url)
        if data: return data

    raise RuntimeError(
        "No endpoint returned data. Open data.wastewaterscan.org in a browser, "
        "use DevTools → Network → filter 'graphql' or 'api', find the real "
        "endpoint URL, then set the WWS_API_URL env var in GitHub Actions."
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
