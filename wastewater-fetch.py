#!/usr/bin/env python3
"""
WastewaterScan Data Exporter
=============================
Fetches infectious-disease measurements from data.wastewaterscan.org.
Strategy:
  1  Configured WWS_API_URL override
  2  __NEXT_DATA__ embedded in the page HTML
  3  Next.js /_next/data/<buildId>/... JSON routes
  4  RSC flight-data chunks  (Next.js 13+ App Router)
  5  Inline <script> JSON
  6  Discover API endpoints from JS bundles → introspect → multi-query
  7  REST fallback paths
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
    "/api/graphql",
    "/graphql",
    "/v1/graphql",
    "/api/v1/graphql",
    "/api/graphql/query",
    "/gql",
    "/api/gql",
    "/query",
    "/api/query",
]

REST_PATHS = [
    "/api/data",
    "/api/measurements",
    "/api/v1/measurements",
    "/api/v1/data",
    "/api/sites",
    "/api/export",
    "/data.csv",
    "/api/csv",
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
    # Same-domain absolute URLs (cast wider net)
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
    ext_scripts = re.findall(r'<script[^
