#!/usr/bin/env python3
"""
Fetch infectious-disease tracking data from data.wastewaterscan.org's
GraphQL backend, extract the third-to-latest location data point,
format with Pandas, and save as CSV. Optionally email it.
"""

import os
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone

import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# The public dashboard at data.wastewaterscan.org is a Next.js app that
# talks to a GraphQL backend. The endpoint + query below reflect the
# publicly observable network calls; if the upstream contract changes you
# only need to adjust GRAPHQL_URL and the QUERY string.
GRAPHQL_URL = os.getenv(
    "WWS_GRAPHQL_URL",
    "https://data.wastewaterscan.org/api/graphql",
)

QUERY = """
query Measurements {
  measurements(limit: 100, orderBy: { sampleDate: DESC }) {
    sampleDate
    location {
      id
      name
      region
      state
    }
    pathogen
    concentration
    concentrationUnits
    pctDetect
    rollingAverage
  }
}
"""

OUT_DIR = os.getenv("WWS_OUT_DIR", "output")
CSV_PATH = os.path.join(
    OUT_DIR,
    f"wastewater_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv",
)

# Email (optional). Set WWS_EMAIL=1 to enable.
EMAIL_ENABLED = os.getenv("WWS_EMAIL", "0") == "1"
SMTP_HOST = os.getenv("WWS_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("WWS_SMTP_PORT", "465"))
SMTP_USER = os.getenv("WWS_SMTP_USER", "")
SMTP_PASS = os.getenv("WWS_SMTP_PASS", "")
EMAIL_TO  = os.getenv("WWS_EMAIL_TO", "")


# ---------------------------------------------------------------------------
# Fetch + transform
# ---------------------------------------------------------------------------
def fetch_graphql():
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "wws-export/1.0",
    }
    resp = requests.post(
        GRAPHQL_URL,
        json={"query": QUERY, "variables": {}},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def pick_third_to_latest(payload):
    """
    Return the third-to-latest measurement record.

    The response is expected to be a list ordered by sampleDate DESC.
    Index 0 = latest, 1 = second-to-latest, 2 = third-to-latest.
    """
    data = payload.get("data") or payload
    measurements = data.get("measurements") or []

    # Flatten Relay-style edges if present
    if measurements and isinstance(measurements[0], dict) and "node" in measurements[0]:
        measurements = [m["node"] for m in measurements]

    if len(measurements) < 3:
        raise ValueError(f"Need >=3 measurements, got {len(measurements)}")

    return measurements[2]


def to_dataframe(record):
    loc = record.get("location") or {}
    flat = {
        "sample_date":        record.get("sampleDate"),
        "pathogen":           record.get("pathogen"),
        "concentration":      record.get("concentration"),
        "concentration_units":record.get("concentrationUnits"),
        "pct_detectable":     record.get("pctDetect"),
        "rolling_average":    record.get("rollingAverage"),
        "location_id":        loc.get("id"),
        "location_name":      loc.get("name"),
        "region":             loc.get("region"),
        "state":              loc.get("state"),
        "fetched_at_utc":     datetime.now(timezone.utc).isoformat(),
    }
    df = pd.DataFrame([flat])
    df["sample_date"] = pd.to_datetime(df["sample_date"], errors="coerce")
    return df


def save_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Wrote {len(df)} row(s) -> {path}")


def maybe_email(path):
    if not EMAIL_ENABLED:
        return
    msg = EmailMessage()
    msg["Subject"] = f"WastewaterScan export — {os.path.basename(path)}"
    msg["From"]    = SMTP_USER
    msg["To"]      = EMAIL_TO
    msg.set_content("See attached CSV for this week's wastewater data export.")
    with open(path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=os.path.basename(path),
        )
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
    print(f"Emailed {path} -> {EMAIL_TO}")


def main():
    payload = fetch_graphql()
    record  = pick_third_to_latest(payload)
    df      = to_dataframe(record)
    save_csv(df, CSV_PATH)
    maybe_email(CSV_PATH)


if __name__ == "__main__":
    main()
