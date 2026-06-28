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
    for u in re.findall(r'["\'`](https?://[^"\'`]+)["\'`]', text, re.I):
        if any(k in u.lower() for k in ['api', 'graphql', 'data', 'wastewater', 'verily', '.json', 'measurement', 'download']):
            out.add(u)
    for p in re.findall(r'["\'`](/[^"\'`]+)["\'`]', text):
        if any(k in p.lower() for k in ['api', 'graphql', 'data', 'v1', 'measurement', 'sample', 'download']):
            out.add(urljoin(base, p))
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

    build_id_match = re.search(r'"buildId":"([^"]+)"', html)
    if build_id_match:
        build_id = build_id_match.group(1)
        candidates.add(urljoin(base_url, f"/_next/data/{build_id}/index.json"))

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
        'https://api.wastewaterscan.org/v1/measurements',
        'https://data.wastewaterscan.org/api/data',
        'https://data.wastewaterscan.org/api/measurements'
    ]
    for domain in fallback_domains:
        candidates.add(domain)

    gql = sorted(c for c in candidates if 'graphql' in c.lower())
    rest = sorted(c for c in candidates if c not in gql)
    return gql + rest

# ── Endpoint Testers ───────────────────────────────────────────────────
INTROSPECTION_QUERY = """{ __schema { queryType { fields { name type { name kind ofType { name kind ofType { name kind } } } } } } }"""

def get_graphql_fields(url, type_name, depth=0):
    if depth > 1: return ""
    q = """{ __type(name: "%s") { fields { name type { name kind ofType { name kind ofType { name kind } } } } } }""" % type_name
    try:
        r = session.post(url, json={"query": q}, headers={"Content-Type": "application/json"}, timeout=15)
        if r.status_code == 200:
            body = r.json()
            if body.get("data") and body["data"].get("__type"):
                fields_str = []
                for f in body["data"]["__type"]["fields"]:
                    fname = f["name"]
                    if fname.startswith("__"): continue
                    
                    # Block geographic fields that break CSVs and cause clumped data
                    if fname.lower() in ['geometry', 'geography', 'coordinates', 'geojson', 'geom', 'the_geom', 'wkb_geometry', 'bounds']:
                        continue
                        
                    ftype = f["type"]
                    while ftype.get("ofType"):
                        ftype = ftype["ofType"]
                    fkind = ftype.get("kind")
                    ftypename = ftype.get("name")
                    
                    if fkind == "SCALAR":
                        fields_str.append(fname)
                    elif fkind == "OBJECT" and depth < 1:
                        nested = get_graphql_fields(url, ftypename, depth + 1)
                        if nested:
                            fields_str.append(f"{fname} {{ {nested} }}")
                return " ".join(fields_str)
    except Exception: pass
    return ""

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
                
                def field_priority(f):
                    name = f["name"].lower()
                    if any(k in name for k in ['measurement', 'sample', 'metric', 'timeseries', 'data']): return 0
                    return 1
                fields.sort(key=field_priority)
                
                for field in fields:
                    name = field["name"]
                    if name.startswith("__"): continue
                    type_info = field.get("type", {})
                    type_name = type_info.get("name")
                    while type_info.get("ofType"):
                        type_info = type_info["ofType"]
                        if type_info.get("name"): type_name = type_info["name"]
                    if not type_name: continue
                        
                    query_fields_str = get_graphql_fields(url, type_name)
                    if not query_fields_str: continue
                        
                    queries = [
                        f"""query {{ {name}(limit: 10000) {{ {query_fields_str} }} }}""",
                        f"""query {{ {name}(first: 10000) {{ {query_fields_str} }} }}""",
                        f"""query {{ {name} {{ {query_fields_str} }} }}""",
                    ]
                    for q in queries:
                        try:
                            r_data = session.post(url, json={"query": q}, headers={"Content-Type": "application/json"}, timeout=30)
                            if r_data.status_code == 200:
                                body_data = r_data.json()
                                if body_data.get("errors"): continue
                                if body_data.get("data") and body_data.get("data").get(name):
                                    val = body_data["data"][name]
                                    if isinstance(val, (list, dict)):
                                        log.info(f"    ✓✓✓ FOUND VALID ROOT: '{name}'")
                                        return body_data
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
def is_measurement_data(data):
    records = extract_records(data)
    if not isinstance(records, list) or not records:
        return False
    for r in records[:5]:
        if not isinstance(r, dict): continue
        has_date = any('date' in k.lower() or 'time' in k.lower() for k in r)
        has_value = any(v in k.lower() for k in r for v in ['concentration', 'value', 'result', 'average', 'detect', 'pct', 'pathogen', 'target'])
        if has_date and has_value:
            return True
    return False

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

    next_data = _extract_next_data(html)
    if next_data:
        if is_measurement_data(next_data):
            log.info(f"✓ Extracted measurement records from __NEXT_DATA__")
            return next_data

    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for script_text in scripts:
        s = script_text.strip()
        if not (s.startswith('{') or s.startswith('[')): continue
        try:
            payload = json.loads(s)
            if is_measurement_data(payload):
                log.info(f"✓ Extracted measurement records from embedded HTML JSON")
                return payload
        except json.JSONDecodeError: pass

    log.info("  No embedded measurement data found. Discovering API endpoints from JS bundles...")
    endpoints = discover_endpoints(base, html)
    log.info(f"  {len(endpoints)} candidate endpoint(s) to try:")
    for url in endpoints: log.info(f"    • {url}")

    best_data = None
    for url in endpoints:
        data = try_graphql(url) if 'graphql' in url.lower() else try_rest(url)
        if data:
            if is_measurement_data(data):
                log.info("✓ Found valid measurement data!")
                return data
            else:
                if best_data is None: best_data = data
                log.info("  ✗ Data found, but missing measurement fields. Continuing search...")

    if best_data:
        log.warning("⚠️ Could not find a measurements endpoint. Returning sites/locations data instead.")
        return best_data
        
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
    
    # Deep search for a list that looks like measurements, unnesting if necessary
    def _find_all_measurements(obj, depth=0, parent_data=None):
        if depth > 10: return []
        
        found_measurements = []
        
        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
            sample = obj[0]
            has_date = any('date' in k.lower() or 'time' in k.lower() for k in sample)
            has_value = any(v in k.lower() for k in sample for v in ['concentration', 'value', 'result', 'average', 'detect', 'pct', 'pathogen', 'target'])
            if has_date and has_value:
                # Enrich with parent data (e.g., attach site name to measurement)
                if parent_data:
                    for item in obj:
                        for pk, pv in parent_data.items():
                            if isinstance(pv, (str, int, float, bool)):
                                item.setdefault(f"site_{pk}", pv)
                return obj
                
        if isinstance(obj, dict):
            current_parent = {k: v for k, v in obj.items() if isinstance(v, (str, int, float, bool))}
            if parent_data:
                current_parent = {**parent_data, **current_parent}
                
            for v in obj.values():
                res = _find_all_measurements(v, depth + 1, current_parent)
                if res:
                    found_measurements.extend(res)
                    
        return found_measurements

    measurements = _find_all_measurements(data)
    if measurements:
        return measurements

    # Fallback to original extraction if no measurement-like list is found
    for key in ['measurements', 'results', 'data', 'items', 'records', 'samples', 'samplingEvents', 'pathogens', 'metrics', 'timeseries', 'sitesData', 'allMeasurements', 'allSites', 'getMeasurements', 'sites']:
        if isinstance(data, dict) and key in data:
            val = data[key]
            if isinstance(val, list):
                if val and isinstance(val[0], dict) and 'node' in val[0]:
                    return [e['node'] for e in val]
                return val
            elif isinstance(val, dict) and 'edges' in val and isinstance(val['edges'], list):
                edges = val['edges']
                if edges and isinstance(edges[0], dict) and 'node' in edges[0]:
                    return [e['node'] for e in edges]
                return edges
                
    found = extract_records_from_json(data)
    return found if found else data

def to_df(records):
    if not isinstance(records, list):
        records = [records]
        
    flat_list = []
    ignore_keys = ['location', 'site', 'properties', 'siteBySiteId', 'geometry', 'coordinates', 'geography', 'geom', 'the_geom', 'wkb_geometry', 'bounds']
    
    for record in records:
        loc = record.get('location') or record.get('site') or record.get('properties') or record.get('siteBySiteId') or {}
        flat = {
            'sample_date':         record.get('sampleDate') or record.get('sample_date') or record.get('date') or record.get('sampledAt') or record.get('collectedAt'),
            'pathogen':            record.get('pathogen') or record.get('target') or record.get('pathogenName'),
            'concentration':       record.get('concentration') or record.get('value') or record.get('concentrationValue') or record.get('result'),
            'concentration_units': record.get('concentrationUnits') or record.get('units') or record.get('unit'),
            'pct_detectable':      record.get('pctDetect') or record.get('pct_detectable') or record.get('percentDetectable') or record.get('pctDetectable'),
            'rolling_average':     record.get('rollingAverage') or record.get('rolling_average') or record.get('rollingAvg'),
            'location_id':         loc.get('id') or loc.get('siteId') or loc.get('site_id') or record.get('location_id') or record.get('site_id'),
            'location_name':       loc.get('name') or loc.get('siteName') or loc.get('site_name') or record.get('location_name') or record.get('site_name'),
            'region':              loc.get('region') or record.get('region') or record.get('site_region'),
            'state':               loc.get('state') or record.get('state') or record.get('site_state'),
            'fetched_at_utc':      datetime.now(timezone.utc).isoformat(),
        }
        
        # Dynamically capture ALL other fields so we don't lose missing data
        for k, v in record.items():
            if k in ignore_keys: continue
            if isinstance(v, (str, int, float, bool)):
                if k not in flat or flat[k] is None:
                    flat[k] = v
            elif isinstance(v, dict):
                for nk, nv in v.items():
                    if nk in ignore_keys: continue
                    if isinstance(nv, (str, int, float, bool)):
                        flat_key = f"{k}_{nk}"
                        if flat_key not in flat or flat[flat_key] is None:
                            flat[flat_key] = nv
                            
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
    df      = to_df(records)
    save_csv(df, CSV_PATH)
    maybe_email(CSV_PATH)
