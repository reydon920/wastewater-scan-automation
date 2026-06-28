#!/usr/bin/env python3
"""
WastewaterScan Data Exporter
=============================
Intelligently fetches infectious-disease measurements from data.wastewaterscan.org.
It first checks for data embedded directly in the page HTML (Next.js __NEXT_DATA__ 
or GeoJSON). If none is found, it inspects JavaScript bundles to discover the API. 
It then extracts the third-to-latest record and saves it as a CSV.
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
KNOWN_API_URL = os.getenv("WWS_API_URL", "")          # manual override
OUT_DIR       = os.getenv("WWS_OUT_DIR", "output")
CSV_PATH = os.path.join(
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/json,text/csv,*/*",
})


# ── Embedded Data Extraction ───────────────────────────────────────────
def extract_records_from_json(payload):
    """Recursively search a JSON object for lists of data records."""
    def _find(obj, d=0):
        if d > 8:
            return None
        if isinstance(obj, list):
            if len(obj) > 0 and isinstance(obj[0], dict):
                keys = set()
                for item in obj[:5]:
                    if isinstance(item, dict):
                        keys.update(item.keys())
                
                # Look for Wastewaterscan specific keys
                if any(k in keys for k in ('sampleDate', 'sample_date', 'pathogen', 'pctDetect', 'concentration')):
                    return obj
                
                # Look for GeoJSON features (common in mapping dashboards)
                if obj[0].get("type") == "Feature" and "properties" in obj[0]:
                    features = []
                    for f in obj:
                        props = f.get("properties", {})
                        if isinstance(props, dict):
                            features.append(props)
                    if features:
                        fkeys = set()
                        for f in features[:5]: 
                            fkeys.update(f.keys())
                        if any(k in fkeys for k in ('sampleDate', 'sample_date', 'pathogen', 'pctDetect', 'concentration')):
                            return features
                            
        if isinstance(obj, dict):
            for v in obj.values():
                r = _find(v, d + 1)
                if r:
                    return r
        return None

    return _find(payload)


# ── Endpoint Discovery ─────────────────────────────────────────────────
def _harvest_urls(text, base, out):
    """Extract API-looking URLs from a chunk of JS."""
    out.update(re.findall(r'["\'`](https?://[^"\'`]*graphql[^"\'`]*)["\'`]', text))
    out.update(re.findall(r'["\'`](https?://[^"\'`]*/api/[^"\'`]*)["\'`]', text))
    for p in re.findall(r'["\'`](/(?:api|graphql)/[^"\'`]*)["\'`]', text):
        out.add(urljoin(base, p))
    for p in re.findall(r'fetch\s*\(\s*["\'`]([^"\'`]+)["\'`]', text):
        out.add(p if p.startswith('http') else urljoin(base, p))
    out.update(re.findall(r'uri\s*:\s*["\'`](https?://[^"\'`]+)["\'`]', text))


def try_graphql(url):
    query = """
    query {
      measurements(limit: 100, orderBy: { sampleDate: DESC }) {
        sampleDate location { id name region state }
        pathogen concentration concentrationUnits pctDetect rollingAverage
      }
    }
    """
    try:
        r = session.post(url, json={"query": query}, headers={"Content-Type": "application/json"}, timeout=15)
        if r.status_code == 200 and "data" in r.json():
            log.info(f"  ✓ GraphQL OK: {url}")
            return r.json()
    except Exception:
        pass
    return None


def try_rest(url):
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            ct = r.headers.get('content-type', '')
            if 'json' in ct:
                log.info(f"  ✓ REST JSON: {url}")
                return r.json()
            if 'csv' in ct or (r.text and r.text[:1].isdigit()):
                log.info(f"  ✓ REST CSV: {url}")
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

    # 1. Try to find data directly embedded in the HTML
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for script_text in scripts:
        if not script_text.strip().startswith('{') and not script_text.strip().startswith('['):
            continue
        try:
            payload = json.loads(script_text)
            records = extract_records_from_json(payload)
            if records:
                log.info(f"✓ Extracted {len(records)} records directly from embedded HTML JSON!")
                return records
        except json.JSONDecodeError:
            pass

    # 2. Fallback: Discover API endpoints from JS bundles
    log.info("  No embedded data found. Discovering API endpoints from JS bundles...")
    candidates = set()
    _harvest_urls(html, base, candidates)
    
    ext_scripts = re.findall(r'<script[^>]+src="([^"]+)"', html)
    for src in ext_scripts[:15]:
        if not src.endswith('.js'): continue
        url = urljoin(base, src)
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                _harvest_urls(r.text, base, candidates)
        except requests.RequestException:
            pass

    # Static educated guesses
    for path in ['/graphql', '/api/graphql', '/api/data', '/api/measurements']:
        candidates.add(urljoin(base, path))

    gql = sorted(c for c in candidates if 'graphql' in c.lower())
    rest = sorted(c for c in candidates if c not in gql)
    ordered = gql + rest

    log.info(f"  {len(ordered)} candidate endpoint(s) found:")
    for c in ordered: log.info(f"    • {c}")

    for url in ordered:
        data = try_graphql(url) if 'graphql' in url.lower() else try_rest(url)
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
