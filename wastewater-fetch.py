#!/usr/bin/env python3
"""
WastewaterScan Data Exporter
=============================
Dynamically discovers the API endpoint used by data.wastewaterscan.org,
fetches infectious-disease measurements, extracts the third-to-latest
record, and saves as CSV.

If WWS_API_URL is set, that URL is used directly (bypassing discovery).
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
    "User-Agent": "Mozilla/5.0 (compatible; wws-export/1.0)",
    "Accept": "text/html,application/json,text/csv,*/*",
})


# ── Endpoint Discovery ─────────────────────────────────────────────────
def discover_endpoints():
    """
    Fetch the main page, inspect its JS bundles, and return a ranked
    set of candidate API endpoint URLs.
    """
    log.info(f"→ Fetching {BASE_URL}")
    resp = session.get(BASE_URL, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    base, html = resp.url, resp.text
    log.info(f"  Final URL: {base}  ({len(html):,} bytes)")

    candidates = set()

    # 1 · __NEXT_DATA__ (Next.js embeds JSON in a script tag)
    nd = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
    )
    if nd:
        try:
            data = json.loads(nd.group(1))
            for url in re.findall(r'https?://[^\s"\'\\]+', json.dumps(data)):
                if any(k in url.lower() for k in ('api', 'graphql', 'data')):
                    candidates.add(url.split('\\')[0])
            log.info(f"  __NEXT_DATA__: {len(candidates)} URL(s) so far")
        except json.JSONDecodeError:
            pass

    # 2 · Inline scripts (may contain fetch / API calls directly)
    for script in re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL):
        _harvest_urls(script, base, candidates)

    # 3 · External JS bundles
    scripts = re.findall(r'<script[^>]+src="([^"]+)"', html)
    log.info(f"  {len(scripts)} external script(s) found")
    for src in scripts[:20]:                       # cap at 20 bundles
        if not src.endswith('.js'):
            continue
        url = urljoin(base, src)
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                _harvest_urls(r.text, base, candidates)
        except requests.RequestException:
            pass

    # 4 · Static fallbacks
    for path in ['/graphql', '/api/graphql', '/api/data',
                 '/api/measurements', '/api/v1/data',
                 '/api/v1/measurements', '/data.csv',
                 '/api/data.csv']:
        candidates.add(urljoin(base, path))
    candidates.add("https://api.wastewaterscan.org/graphql")
    candidates.add("https://api.wastewaterscan.org/data")

    # Deduplicate & sort (GraphQL endpoints first)
    gql = sorted(c for c in candidates if 'graphql' in c.lower())
    rest = sorted(c for c in candidates if c not in gql)
    ordered = gql + rest

    log.info(f"  {len(ordered)} candidate endpoint(s):")
    for c in ordered:
        log.info(f"    • {c}")
    return ordered


def _harvest_urls(text, base, out):
    """Extract API-looking URLs from a chunk of JS."""
    # Absolute GraphQL / API URLs
    out.update(re.findall(r'["\'`](https?://[^"\'`]*graphql[^"\'`]*)["\'`]', text))
    out.update(re.findall(r'["\'`](https?://[^"\'`]*/api/[^"\'`]*)["\'`]', text))
    # Relative API / graphql paths
    for p in re.findall(r'["\'`](/(?:api|graphql)/[^"\'`]*)["\'`]', text):
        out.add(urljoin(base, p))
    # fetch("...") calls
    for p in re.findall(r'fetch\s*\(\s*["\'`]([^"\'`]+)["\'`]', text):
        out.add(p if p.startswith('http') else urljoin(base, p))
    # Apollo / urql uri: "..."
    out.update(re.findall(r'uri\s*:\s*["\'`](https?://[^"\'`]+)["\'`]', text))
    # axios.get("...") / axios.post("...")
    for p in re.findall(r'axios\.\w+\s*\(\s*["\'`]([^"\'`]+)["\'`]', text):
        out.add(p if p.startswith('http') else urljoin(base, p))


# ── Data Fetching ──────────────────────────────────────────────────────
GQL_QUERY = """
query {
  measurements(limit: 100, orderBy: { sampleDate: DESC }) {
    sampleDate
    location { id name region state }
    pathogen
    concentration
    concentrationUnits
    pctDetect
    rollingAverage
  }
}
"""

def try_graphql(url):
    try:
        r = session.post(
            url, json={"query": GQL_QUERY},
            headers={"Content-Type": "application/json"}, timeout=15,
        )
        if r.status_code == 200:
            body = r.json()
            if "data" in body:
                log.info(f"  ✓ GraphQL OK: {url}")
                return body
            if "errors" in body:
                log.info(f"  · GraphQL errors at {url}: {str(body['errors'])[:120]}")
        else:
            log.info(f"  · {r.status_code} at {url}")
    except Exception as e:
        log.info(f"  · Error at {url}: {e}")
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
        else:
            log.info(f"  · {r.status_code} at {url}")
    except Exception as e:
        log.info(f"  · Error at {url}: {e}")
    return None


def fetch_data():
    """Try known URL, then discover, then try each candidate."""
    # Manual override
    if KNOWN_API_URL:
        log.info(f"Using WWS_API_URL = {KNOWN_API_URL}")
        data = (try_graphql(KNOWN_API_URL) if 'graphql' in KNOWN_API_URL.lower()
                else try_rest(KNOWN_API_URL))
        if data:
            return data
        raise RuntimeError(f"Configured WWS_API_URL failed: {KNOWN_API_URL}")

    # Discovery
    candidates = discover_endpoints()
    for url in candidates:
        data = try_graphql(url) if 'graphql' in url.lower() else try_rest(url)
        if data:
            return data

    raise RuntimeError(
        "No endpoint returned data. Open data.wastewaterscan.org in a browser, "
        "use DevTools → Network → filter 'graphql' or 'api', find the real "
        "endpoint URL, then set the WWS_API_URL env var."
    )


# ── Processing ─────────────────────────────────────────────────────────
def extract_records(payload):
    """Normalise various response shapes into a list of dict records."""
    # CSV shortcut
    if isinstance(payload, dict) and '_csv' in payload:
        return pd.read_csv(StringIO(payload['_csv'])).to_dict('records')

    data = payload.get('data', payload) if isinstance(payload, dict) else payload

    # Known keys
    for key in ('measurements', 'results', 'data', 'items', 'records', 'sites'):
        if isinstance(data, dict) and key in data and isinstance(data[key], list):
            recs = data[key]
            if recs and isinstance(recs[0], dict) and 'node' in recs[0]:
                recs = [e['node'] for e in recs]     # Relay edges
            return recs

    # Already a list
    if isinstance(data, list):
        return data

    # Recursive search (max depth 5)
    def _find(obj, d=0):
        if d > 5:
            return None
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            return obj
        if isinstance(obj, dict):
            for v in obj.values():
                r = _find(v, d + 1)
                if r:
                    return r
        return None

    found = _find(data)
    if found:
        return found
    raise ValueError(f"Cannot extract records from payload (keys: "
                     f"{list(payload.keys()) if isinstance(payload, dict) else type(payload)})")


def pick_third_latest(records):
    if len(records) < 3:
        raise ValueError(f"Need ≥3 records, got {len(records)}")

    def sort_key(r):
        for k in ('sampleDate', 'sample_date', 'date', 'timestamp', 'createdAt'):
            if r.get(k):
                return str(r[k])
        return ''

    rec = sorted(records, key=sort_key, reverse=True)[2]
    log.info(f"  Third-to-latest: {rec.get('sampleDate') or rec.get('date', '?')}")
    return rec


def to_df(record):
    loc = record.get('location') or record.get('site') or {}
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
    if not EMAIL_ENABLED:
        return
    msg = EmailMessage()
    msg["Subject"] = f"WastewaterScan export — {os.path.basename(path)}"
    msg["From"], msg["To"] = SMTP_USER, EMAIL_TO
    msg.set_content("Weekly wastewater data export attached.")
    with open(path, "rb") as f:
        msg.add_attachment(
            f.read(), maintype="text", subtype="csv",
            filename=os.path.basename(path),
        )
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
    log.info(f"✓ Emailed to {EMAIL_TO}")


# ── Main ───────────────────────────────────────────────────────────────
def main():
    payload = fetch_data()
    records = extract_records(payload)
    log.info(f"Extracted {len(records)} record(s)")
    record  = pick_third_latest(records)
    df      = to_df(record)
    save_csv(df, CSV_PATH)
    maybe_email(CSV_PATH)


if __name__ == "__main__":
    main()
