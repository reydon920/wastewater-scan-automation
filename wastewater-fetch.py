#!/usr/bin/env python3
"""
WastewaterScan Data Exporter - Aggressive Endpoint Discovery Version
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

# ── Embedded-data extraction ───────────────────────────────────────────
def extract_records_from_json(payload):
    def _find(obj, depth=0):
        if depth > 15: return None
        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
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
    # Aggressively grab absolute URLs that look like APIs
    for u in re.findall(r'["\'`](https?://[^"\'`]+)["\'`]', text, re.I):
        if any(k in u.lower() for k in ['api', 'graphql', 'data', 'wastewater', 'verily', '.json', 'measurement', 'download']):
            out.add(u)
            
    # Grab relative paths
    for p in re.findall(r'["\'`](/[^"\'`]+)["\'`]', text):
        if any(k in p.lower() for k in ['api', 'graphql', 'data', 'v1', 'measurement', 'sample', 'download']):
            out.add(urljoin(base, p))
            
    # Grab all fetch() targets
    for p in re.findall(r'fetch\s*\(\s*["\'`]([^"\'`]+)["\'`]', text):
        out.add(p if p.startswith("http") else urljoin(base, p))

def discover_endpoints(base_url, html):
    candidates = set()
    _harvest_urls(html, base_url, candidates)
    
    ext_scripts = re.findall(r'<script[^>]+src="([^"]+)"', html)
    log.info(f"  Found {len(ext_scripts)} script tags. Analyzing JS chunks...")
    for src in ext_scripts:
        if not src.endswith('.js'): continue
        url = urljoin(base_url, src)
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200:
                _harvest_urls(r.text, base_url, candidates)
        except requests.exceptions.RequestException: pass

    # Check for Next.js _next/data route
    build_id_match = re.search(r'"buildId":"([^"]+)"', html)
    if build_id_match:
        build_id = build_id_match.group(1)
        candidates.add(urljoin(base_url, f"/_next/data/{build_id}/index.json"))

    # Fallback common paths
    fallback_paths = [
        '/api/graphql', '/graphql', '/api/data', '/data.json', 
        '/api/measurements', '/api/sites', '/api/v1/measurements', 
        '/api/measurements.json', '/api/getData', '/api/getMeasurements',
        '/api/wastewater', '/api/export', '/api/download', 
        '/api/measurements/all', '/api/v1/data'
    ]
    for path in fallback_paths:
        candidates.add(urljoin(base_url, path))
        
    fallback_domains = [
        'https://api.wastewaterscan.org/graphql',
        'https://api.wastewaterscan.org/data',
        'https://api.wastewaterscan.org/v1/measurements'
    ]
    for domain in fallback_domains:
        candidates.add(domain)

    gql = sorted(c for c in candidates if 'graphql' in c.lower())
    rest = sorted(c for c in candidates if c not in gql)
    return gql + rest

# ── Endpoint Testers ───────────────────────────────────────────────────
INTROSPECTION_QUERY = """{ __schema { queryType { fields { name } } } }"""

def try_graphql(url):
    log.info(f"  Trying GraphQL: {url}")
    try:
        r = session.post(url, json={"query": INTROSPECTION_QUERY}, headers={"Content-Type": "application/json"}, timeout=15)
        if r.status_code == 200:
            body = r.json()
            if body.get("data") and body["data"].get("__schema"):
                log.info("    🔥 INTROSPECTION SUCCESS! Querying available fields...")
                schema_data = body["data"]["__schema"]
                fields = schema_data.get("queryType", {}).get("fields", [])
                
                for field in fields:
                    name = field["name"]
                    if name.startswith("__"): continue
                    
                    # Try querying the field with basic arguments
                    queries = [
                        f"""query {{ {name} {{ id }} }}""",
                        f"""query {{ {name}(limit: 1) {{ id }} }}""",
                        f"""query {{ {name}(first: 1) {{ id }} }}""",
                    ]
                    for sq in queries:
                        try:
                            r2 = session.post(url, json={"query": sq}, headers={"Content-Type": "application/json"}, timeout=15)
                            if r2.status_code == 200:
                                body2 = r2.json()
                                if body2.get("data") and body2["data"].get(name) is not None:
                                    val = body2["data"][name]
                                    if val:
                                        log.info(f"    ✓✓✓ FOUND VALID ROOT: '{name}'")
                                        # Fetch deep data dynamically
                                        deep_q = f"""query {{ {name}(limit: 10000) {{ sampleDate sample_date date pathogen target concentration value location {{ name state }} site {{ name state }} }} }}"""
                                        r3 = session.post(url, json={"query": deep_q}, headers={"Content-Type": "application/json"}, timeout=20)
                                        if r3.status_code == 200:
                                            body3 = r3.json()
                                            if body3.get("data") and body3["data"].get(name):
                                                return body3
                                        return body2
                        except Exception: pass
            elif body.get("errors"):
                log.info(f"    Introspection disabled: {body['errors'][0].get('message')}")
        else:
            log.info(f"    ✗ HTTP {r.status_code}")
    except Exception as e:
        log.info(f"    ✗ Connection failed: {e}")
    return None

def try_rest(url):
    log.info(f"  Trying REST: {url}")
    try:
        # Try GET first
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            ct = r.headers.get('content-type', '')
            if 'json' in ct:
                data = r.json()
                if extract_records_from_json(data):
                    log.info("    ✓ REST JSON hit! (GET)")
                    return data
            if 'csv' in ct or (r.text and r.text[:1].isdigit()):
                log.info("    ✓ REST CSV hit! (GET)")
                return {"_csv": r.text}
                
        # Try POST (Next.js API routes often require POST)
        r_post = session.post(url, json={}, timeout=15)
        if r_post.status_code == 200:
            ct = r_post.headers.get('content-type', '')
            if 'json' in ct:
                data = r_post.json()
                if extract_records_from_json(data):
                    log.info("    ✓ REST JSON hit! (POST)")
                    return data
            if 'csv' in ct or (r_post.text and r_post.text[:1].isdigit()):
                log.info("    ✓ REST CSV hit! (POST)")
                return {"_csv": r_post.text}
                
    except Exception as e:
        log.info(f"    ✗ Connection failed: {e}")
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

    # 1. Check Next.js embedded data
    next_data = _extract_next_data(html)
    if next_data:
        records = extract_records_from_json(next_data)
        if records:
            log.info(f"✓ Extracted {len(records)} records from __NEXT_DATA__")
            return records

    # 2. Check generic script tags
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

    # 3. Discover endpoints from JS bundles
    log.info("  No embedded data found. Discovering API endpoints from JS bundles...")
    endpoints = discover_endpoints(base, html)
    log.info(f"  {len(endpoints)} candidate endpoint(s) to try:")
    for url in endpoints: log.info(f"    • {url}")

    for url in endpoints:
        data = try_graphql(url) if 'graphql' in url.lower() else try_rest(url)
        if data: return data

    raise RuntimeError(
        "No endpoint returned data. Check logs above. "
        "You may need to manually inspect the network tab on data.wastewaterscan.org "
        "to find the exact API URL and set the WWS_API_URL environment variable."
    )

# ── Processing ─────────────────────────────────────────────────────────
def extract_records(payload):
    if isinstance(payload, dict) and '_csv' in payload:
        return pd.read_csv(StringIO(payload['_csv'])).to_dict('records')
    if isinstance(payload, list): return payload
        
    data = payload.get('data', payload) if isinstance(payload, dict) else payload
    for key in ['measurements', 'results', 'data', 'items', 'records', 'sites', 'samples', 'samplingEvents', 'pathogens', 'metrics', 'timeseries', 'sitesData', 'allMeasurements', 'allSites', 'getMeasurements']:
        if isinstance(data, dict) and key in data and isinstance(data[key], list):
            recs = data[key]
            if recs and isinstance(recs[0], dict) and 'node' in recs[0]:
                recs = [e['node'] for e in recs]
            return recs
            
    found = extract_records_from_json(data)
    return found if found else data

def pick_third_latest(records):
    if not isinstance(records, list) or not records:
        raise RuntimeError("No records to pick from.")
    if len(records) < 3:
        log.warning(f"Only got {len(records)} records, returning latest instead of third-to-latest.")
        return records[0]

    def sort_key(r):
        for k in ('sampleDate', 'sample_date', 'date', 'timestamp', 'createdAt', 'sampledAt', 'collectedAt'):
            if r.get(k): return str(r[k])
        return ''

    rec = sorted(records, key=sort_key, reverse=True)[2]
    log.info(f"  Third-to-latest date found: {rec.get('sampleDate') or rec.get('date', '?')}")
    return rec

def to_df(records):
    if not isinstance(records, list):
        records = [records]
        
    flat_list = []
    for record in records:
        loc = record.get('location') or record.get('site') or record.get('properties') or record.get('siteBySiteId') or {}
        flat = {
            'sample_date':         record.get('sampleDate') or record.get('sample_date') or record.get('date') or record.get('sampledAt') or record.get('collectedAt'),
            'pathogen':            record.get('pathogen') or record.get('target') or record.get('pathogenName'),
            'concentration':       record.get('concentration') or record.get('value') or record.get('concentrationValue') or record.get('result'),
            'concentration_units': record.get('concentrationUnits') or record.get('units') or record.get('unit'),
            'pct_detectable':      record.get('pctDetect') or record.get('pct_detectable') or record.get('percentDetectable') or record.get('pctDetectable'),
            'rolling_average':     record.get('rollingAverage') or record.get('rolling_average') or record.get('rollingAvg'),
            'location_id':         loc.get('id') or loc.get('siteId') or loc.get('site_id'),
            'location_name':       loc.get('name') or loc.get('siteName') or loc.get('site_name'),
            'region':              loc.get('region'),
            'state':               loc.get('state'),
            'fetched_at_utc':      datetime.now(timezone.utc).isoformat(),
        }
        flat_list.append(flat)
        
    df = pd.DataFrame(flat_list)
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
        msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=os.path.basename(path))
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
    log.info(f"✓ Emailed to {EMAIL_TO}")

if __name__ == "__main__":
    payload = fetch_data()
    records = extract_records(payload)
    
    # Removed: record = pick_third_latest(records)
    
    df      = to_df(records)  # Now processes ALL records
    save_csv(df, CSV_PATH)
    maybe_email(CSV_PATH)
