#!/usr/bin/env python3
"""
WastewaterScan Data Exporter - Dynamic Discovery & Measurement Extraction

- Dynamically discovers the GraphQL/REST API endpoint.
- Uses hardcoded Hasura-style queries if introspection is disabled.
- Decomposes timestamps into separate year/month/day/hour/minute/second columns.
- Strips GeoJSON cruft (geometry, type=Feature, coordinates).
"""

import os
import re
import json
import logging
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin
from io import StringIO

import requests
import pandas as pd

# ── Config ──────────────────────────────────────────────────────────────
BASE_URL      = os.getenv("WWS_BASE_URL", "https://data.wastewaterscan.org")
OUT_DIR       = os.getenv("WWS_OUT_DIR", "output")
CSV_PATH      = os.path.join(OUT_DIR, f"wastewater_{datetime.now(timezone.utc):%Y%m%d}.csv")
LOOKBACK_DAYS = int(os.getenv("WWS_LOOKBACK_DAYS", "14"))

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
    "Origin": BASE_URL,
    "Referer": BASE_URL + "/",
})

# ── GeoJSON fields to strip ─────────────────────────────────────────────
GEO_BLOCKLIST = {
    'geometry', 'geography', 'coordinates', 'geojson', 'geom', 
    'the_geom', 'wkb_geometry', 'bounds', 'type', 'features', 'crs'
}

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
        
    gql = sorted(c for c in candidates if 'graphql' in c.lower())
    rest = sorted(c for c in candidates if c not in gql)
    return gql + rest

# ── GraphQL Helpers ─────────────────────────────────────────────────────
INTROSPECTION_QUERY = """{ __schema { queryType { fields { name args { name type { name kind ofType { name kind ofType { name kind } } } } type { name kind ofType { name kind ofType { name kind } } } } } } }"""

def unwrap_type(type_ref):
    if not type_ref: return None, None
    while type_ref.get("ofType"):
        type_ref = type_ref["ofType"]
    return type_ref.get("name"), type_ref.get("kind")

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
                    if fname.startswith("__") or fname.lower() in GEO_BLOCKLIST: continue
                    ftype = f["type"]
                    while ftype.get("ofType"): ftype = ftype["ofType"]
                    fkind, ftypename = ftype.get("kind"), ftype.get("name")
                    if fkind == "SCALAR":
                        fields_str.append(fname)
                    elif fkind == "OBJECT" and depth < 1:
                        nested = get_graphql_fields(url, ftypename, depth + 1)
                        if nested: fields_str.append(f"{fname} {{ {nested} }}")
                return " ".join(fields_str)
    except: pass
    return ""

def introspect_input_type(url, type_name):
    q = """{ __type(name: "%s") { name kind inputFields { name type { name kind ofType { name kind ofType { name kind } } } } } }""" % type_name
    try:
        r = session.post(url, json={"query": q}, headers={"Content-Type": "application/json"}, timeout=15)
        if r.status_code == 200:
            body = r.json()
            if body.get("data") and body["data"].get("__type"):
                return body["data"]["__type"]
    except: pass
    return None

# ── Endpoint Testers ───────────────────────────────────────────────────
def try_graphql(url):
    log.info(f"  Trying GraphQL: {url}")
    try:
        r = session.post(url, json={"query": INTROSPECTION_QUERY}, headers={"Content-Type": "application/json"}, timeout=20)
        if r.status_code == 200:
            body = r.json()
            if body.get("data") and body["data"].get("__schema"):
                log.info("    🔥 INTROSPECTION SUCCESS! Analyzing schema...")
                schema_data = body["data"]["__schema"]
                fields = schema_data.get("queryType", {}).get("fields", [])
                
                sites_q = None
                measurements_q = None
                
                for f in fields:
                    name = f["name"]
                    ret_type, _ = unwrap_type(f.get("type"))
                    if name in ['sites', 'allSites', 'getSites']:
                        sites_q = {"name": name, "ret_type": ret_type, "args": f.get("args", [])}
                    if name in ['measurements', 'allMeasurements', 'getMeasurements']:
                        measurements_q = {"name": name, "ret_type": ret_type, "args": f.get("args", [])}
                        
                if not measurements_q:
                    log.info("    No measurements query found in schema.")
                    return None
                    
                log.info(f"    Found measurements query: {measurements_q['name']}")
                
                # 1. Fetch Sites
                site_ids = []
                if sites_q:
                    log.info(f"    Fetching sites via {sites_q['name']}...")
                    site_fields = get_graphql_fields(url, sites_q["ret_type"])
                    if not site_fields: site_fields = "id siteId name state region"
                    
                    # FALLBACK: Hardcoded queries when introspection is disabled
                queries = [
                    # Modern Verily/WWS schema often requires 'samples' instead of 'measurements'
                    """
                    query {
                      samples(first: 50000) {
                        sample_date: date
                        pathogen: target
                        concentration: value
                        pct_detectable: pctDetect
                        location_id: siteId
                        site { name state region }
                      }
                    }
                    """,
                    # Standard measurement query without the strict 'limit' argument
                    """
                    query {
                      measurements {
                        date
                        pathogen
                        concentration
                        concentrationUnits
                        pctDetectable
                        rollingAverage
                        siteId
                        site { id name state region }
                      }
                    }
                    """,
                    # Your original fallback, kept as a last resort
                    """
                    query {
                      measurements(limit: 100000) {
                        sample_date
                        pathogen
                        concentration
                        concentration_units
                        pct_detectable
                        rolling_average
                        site_id
                        site { id name state region }
                      }
                    }
                    """
                ]
                    for q in queries:
                        r_sites = session.post(url, json={"query": q}, timeout=30)
                        if r_sites.status_code == 200:
                            body_sites = r_sites.json()
                            if not body_sites.get("errors") and body_sites.get("data", {}).get(sites_q['name']):
                                val = body_sites["data"][sites_q['name']]
                                if isinstance(val, dict) and "edges" in val:
                                    val = [e["node"] for e in val["edges"]]
                                if isinstance(val, list):
                                    for s in val:
                                        sid = str(s.get("id") or s.get("siteId") or "")
                                        if sid: site_ids.append(sid)
                                    if site_ids:
                                        log.info(f"      ✓ Found {len(site_ids)} sites")
                                        break
                
                # 2. Fetch Measurements
                log.info(f"    Fetching measurements...")
                measurement_fields = get_graphql_fields(url, measurements_q["ret_type"])
                if not measurement_fields:
                    measurement_fields = "siteId date pathogen concentration concentrationUnits pctDetectable rollingAverage site { id name state region }"
                    
                end_date = datetime.now(timezone.utc)
                start_date = end_date - timedelta(days=LOOKBACK_DAYS)
                start_str, end_str = start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
                
                arg_map = {}
                for a in measurements_q["args"]:
                    t_name, t_kind = unwrap_type(a["type"])
                    arg_map[a["name"]] = {"type_name": t_name, "kind": t_kind}
                    
                # Strategy A: filter input argument
                if "filter" in arg_map:
                    filter_type = arg_map["filter"]["type_name"]
                    ft = introspect_input_type(url, filter_type)
                    filter_fields = {}
                    if ft and ft.get("inputFields"):
                        for fi in ft["inputFields"]:
                            fn, fk = unwrap_type(fi["type"])
                            filter_fields[fi["name"]] = {"type_name": fn, "kind": fk}
                            
                    def _build_filter(ids):
                        f = {}
                        if "siteIds" in filter_fields: f["siteIds"] = ids
                        if "siteId" in filter_fields and len(ids)==1: f["siteId"] = ids[0]
                        if "startDate" in filter_fields: f["startDate"] = start_str
                        if "endDate" in filter_fields: f["endDate"] = end_str
                        if "start" in filter_fields: f["start"] = start_str
                        if "end" in filter_fields: f["end"] = end_str
                        return f
                        
                    query = f"""query($filter: {filter_type}!) {{ {measurements_q['name']}(filter: $filter) {{ {measurement_fields} }} }}"""
                    
                    if site_ids:
                        log.info(f"      Attempting bulk query ({len(site_ids)} sites)...")
                        data = session.post(url, json={"query": query, "variables": {"filter": _build_filter(site_ids)}}, timeout=60).json()
                        if not data.get("errors") and data.get("data", {}).get(measurements_q['name']):
                            val = data["data"][measurements_q['name']]
                            if isinstance(val, dict) and "edges" in val: val = [e["node"] for e in val["edges"]]
                            if isinstance(val, list) and val:
                                log.info(f"      ✓✓✓ Got {len(val)} measurements (bulk)")
                                return data["data"]
                                
                    log.info("      Bulk failed or no sites. Trying batches of 20...")
                    all_meas = []
                    for i in range(0, len(site_ids), 20):
                        batch = site_ids[i:i+20]
                        r_data = session.post(url, json={"query": query, "variables": {"filter": _build_filter(batch)}}, timeout=60)
                        if r_data.status_code == 200:
                            body_data = r_data.json()
                            if not body_data.get("errors") and body_data.get("data", {}).get(measurements_q['name']):
                                val = body_data["data"][measurements_q['name']]
                                if isinstance(val, dict) and "edges" in val: val = [e["node"] for e in val["edges"]]
                                if isinstance(val, list): all_meas.extend(val)
                    if all_meas:
                        log.info(f"      ✓✓✓ Got {len(all_meas)} measurements (batches)")
                        return {measurements_q['name']: all_meas}
                        
                # Strategy B: individual args
                if any(k in arg_map for k in ("siteIds", "startDate", "limit", "first")):
                    var_defs, arg_strs, variables = [], [], {}
                    if "siteIds" in arg_map:
                        var_defs.append("$siteIds: [ID!]"); arg_strs.append("siteIds: $siteIds"); variables["siteIds"] = site_ids
                    if "startDate" in arg_map:
                        var_defs.append("$startDate: String"); arg_strs.append("startDate: $startDate"); variables["startDate"] = start_str
                    if "endDate" in arg_map:
                        var_defs.append("$endDate: String"); arg_strs.append("endDate: $endDate"); variables["endDate"] = end_str
                    if "limit" in arg_map:
                        var_defs.append("$limit: Int"); arg_strs.append("limit: $limit"); variables["limit"] = 100000
                    if "first" in arg_map:
                        var_defs.append("$first: Int"); arg_strs.append("first: $first"); variables["first"] = 100000
                        
                    query = f"query({', '.join(var_defs)}) {{ {measurements_q['name']}({', '.join(arg_strs)}) {{ {measurement_fields} }} }}"
                    r_data = session.post(url, json={"query": query, "variables": variables}, timeout=60)
                    if r_data.status_code == 200:
                        body_data = r_data.json()
                        if not body_data.get("errors") and body_data.get("data", {}).get(measurements_q['name']):
                            val = body_data["data"][measurements_q['name']]
                            if isinstance(val, dict) and "edges" in val: val = [e["node"] for e in val["edges"]]
                            if isinstance(val, list) and val:
                                log.info(f"      ✓✓✓ Got {len(val)} measurements (individual args)")
                                return body_data["data"]
                                
                # Strategy C: no args
                query = f"query {{ {measurements_q['name']} {{ {measurement_fields} }} }}"
                r_data = session.post(url, json={"query": query}, timeout=60)
                if r_data.status_code == 200:
                    body_data = r_data.json()
                    if not body_data.get("errors") and body_data.get("data", {}).get(measurements_q['name']):
                        val = body_data["data"][measurements_q['name']]
                        if isinstance(val, dict) and "edges" in val: val = [e["node"] for e in val["edges"]]
                        if isinstance(val, list) and val:
                            log.info(f"      ✓✓✓ Got {len(val)} measurements (no args)")
                            return body_data["data"]
                            
            elif body.get("errors"):
                log.info(f"    Introspection disabled: {body['errors'][0].get('message')}")
                log.info("    Trying hardcoded Hasura-style measurements queries...")
                
                # FALLBACK: Hardcoded queries when introspection is disabled
                queries = [
                    """
                    query {
                      measurements(limit: 100000) {
                        date
                        pathogen
                        concentration
                        concentrationUnits
                        pctDetectable
                        rollingAverage
                        siteId
                        site { id name state region }
                      }
                    }
                    """,
                    """
                    query {
                      measurements(limit: 100000) {
                        sample_date
                        pathogen
                        concentration
                        concentration_units
                        pct_detectable
                        rolling_average
                        site_id
                        site { id name state region }
                      }
                    }
                    """
                ]
                for q in queries:
                    r_meas = session.post(url, json={"query": q}, timeout=30)
                    if r_meas.status_code == 200:
                        body_meas = r_meas.json()
                        if not body_meas.get("errors") and body_meas.get("data", {}).get("measurements"):
                            val = body_meas["data"]["measurements"]
                            if isinstance(val, list) and val:
                                log.info(f"    ✓✓✓ FOUND MEASUREMENTS (hardcoded)! Got {len(val)} records.")
                                return body_meas["data"]
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
    except Exception as e:
        log.info(f"    ✗ Connection failed: {e}")
    return None

# ── Main Fetch Logic ───────────────────────────────────────────────────
def is_measurement_data(data):
    records = extract_records(data)
    if not isinstance(records, list) or not records: return False
    for r in records[:5]:
        if not isinstance(r, dict): continue
        has_date = any('date' in k.lower() or 'time' in k.lower() for k in r)
        has_value = any(v in k.lower() for k in r for v in ['concentration', 'value', 'result', 'average', 'detect', 'pct', 'pathogen', 'target'])
        if has_date and has_value: return True
    return False

def fetch_data():
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
    if next_data and is_measurement_data(next_data):
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
        
    raise RuntimeError("No endpoint returned data.")

# ── Processing ─────────────────────────────────────────────────────────
def extract_records(payload):
    if isinstance(payload, dict) and '_csv' in payload:
        return pd.read_csv(StringIO(payload['_csv'])).to_dict('records')
    if isinstance(payload, list): return payload
        
    data = payload.get('data', payload) if isinstance(payload, dict) else payload
    
    def _find_all_measurements(obj, depth=0, parent_data=None):
        if depth > 10: return []
        found = []
        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
            sample = obj[0]
            has_date = any('date' in k.lower() or 'time' in k.lower() for k in sample)
            has_value = any(v in k.lower() for k in sample for v in ['concentration', 'value', 'result', 'average', 'detect', 'pct', 'pathogen', 'target'])
            if has_date and has_value:
                if parent_data:
                    for item in obj:
                        for pk, pv in parent_data.items():
                            if isinstance(pv, (str, int, float, bool)):
                                item.setdefault(f"site_{pk}", pv)
                return obj
        if isinstance(obj, dict):
            current_parent = {k: v for k, v in obj.items() if isinstance(v, (str, int, float, bool))}
            if parent_data: current_parent = {**parent_data, **current_parent}
            for v in obj.values():
                res = _find_all_measurements(v, depth + 1, current_parent)
                if res: found.extend(res)
        return found

    measurements = _find_all_measurements(data)
    if measurements: return measurements

    for key in ['measurements', 'results', 'data', 'items', 'records', 'samples', 'sites']:
        if isinstance(data, dict) and key in data:
            val = data[key]
            if isinstance(val, list):
                if val and isinstance(val[0], dict) and 'node' in val[0]:
                    return [e['node'] for e in val]
                return val
            elif isinstance(val, dict) and 'edges' in val:
                edges = val['edges']
                if edges and isinstance(edges[0], dict) and 'node' in edges[0]:
                    return [e['node'] for e in edges]
                return edges
                
    found = extract_records_from_json(data)
    return found if found else data

def to_df(records):
    if not isinstance(records, list): records = [records]
    flat_list = []
    ignore_keys = ['location', 'site', 'properties', 'siteBySiteId'] + list(GEO_BLOCKLIST)
    
    for record in records:
        loc = record.get('location') or record.get('site') or record.get('properties') or record.get('siteBySiteId') or {}
        if not isinstance(loc, dict): loc = {}
        
        site_fields = {}
        for k, v in record.items():
            if k.startswith("site_") and isinstance(v, (str, int, float, bool)):
                site_fields[k[5:]] = v
        merged_loc = {**site_fields, **loc}
        
        raw_date = record.get('sampleDate') or record.get('sample_date') or record.get('date') or record.get('sampledAt') or record.get('collectedAt') or record.get('site_date')
        now = datetime.now(timezone.utc)
        
        flat = {
            'sample_date': raw_date,
            'pathogen': record.get('pathogen') or record.get('target') or record.get('pathogenName'),
            'concentration': record.get('concentration') or record.get('value') or record.get('concentrationValue') or record.get('result'),
            'concentration_units': record.get('concentrationUnits') or record.get('units') or record.get('unit') or record.get('concentration_units'),
            'pct_detectable': record.get('pctDetect') or record.get('pct_detectable') or record.get('percentDetectable') or record.get('pctDetectable'),
            'rolling_average': record.get('rollingAverage') or record.get('rolling_average') or record.get('rollingAvg'),
            'location_id': merged_loc.get('id') or merged_loc.get('siteId') or merged_loc.get('site_id') or record.get('location_id') or record.get('site_id') or record.get('siteId'),
            'location_name': merged_loc.get('name') or merged_loc.get('siteName') or merged_loc.get('site_name') or record.get('location_name') or record.get('site_name') or record.get('siteName'),
            'region': merged_loc.get('region') or record.get('region') or record.get('site_region'),
            'state': merged_loc.get('state') or record.get('state') or record.get('site_state'),
            
            'fetched_at_utc': now.isoformat(),
            'fetched_year': now.year, 'fetched_month': now.month, 'fetched_day': now.day,
            'fetched_hour': now.hour, 'fetched_minute': now.minute, 'fetched_second': now.second,
            'fetched_timezone': 'UTC', 'fetched_date': now.strftime("%Y-%m-%d"), 'fetched_time': now.strftime("%H:%M:%S"),
        }
        
        if raw_date:
            try:
                parsed = pd.to_datetime(raw_date, errors="coerce")
                if pd.notna(parsed):
                    flat['sample_year'] = parsed.year
                    flat['sample_month'] = parsed.month
                    flat['sample_day'] = parsed.day
                    flat['sample_weekday'] = parsed.day_name()
                else: flat.update(sample_year=None, sample_month=None, sample_day=None, sample_weekday=None)
            except: flat.update(sample_year=None, sample_month=None, sample_day=None, sample_weekday=None)
        else: flat.update(sample_year=None, sample_month=None, sample_day=None, sample_weekday=None)
            
        mapped_keys = set(flat.keys())
        for k, v in record.items():
            if k in ignore_keys or k in mapped_keys or k.startswith("site_"): continue
            if isinstance(v, (str, int, float, bool)): flat[k] = v
            elif isinstance(v, dict):
                for nk, nv in v.items():
                    if nk in ignore_keys: continue
                    if isinstance(nv, (str, int, float, bool)):
                        key = f"{k}_{nk}"
                        if key not in flat: flat[key] = nv
                        
        flat_list.append(flat)
        
    df = pd.DataFrame(flat_list)
    df['sample_date'] = pd.to_datetime(df['sample_date'], errors='coerce')
    if 'sample_date' in df.columns and df['sample_date'].notna().any():
        df = df.sort_values('sample_date', ascending=False).reset_index(drop=True)
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
    with open(path, "rb") as f: msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=os.path.basename(path))
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
