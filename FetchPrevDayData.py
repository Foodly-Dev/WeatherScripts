#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch past 24h hourly weather from Google Weather API and append any new rows.
- Default city: Tbilisi (override via LOCATIONS_JSON env)
- Default output: data/weather_history_hourly_north.csv (override via DATA_PATH env)
- API key must be provided in GOOGLE_WEATHER_API_SECRET
"""
from __future__ import annotations

import os
import csv
import json
import time
import datetime as dt
from typing import Dict, List, Tuple, Any
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

ENDPOINT = "https://weather.googleapis.com/v1/history/hours:lookup"
TIMEZONE = os.getenv("TIMEZONE", "Asia/Tbilisi")

# ---- Configuration via env (safe defaults) ----------------------------------
API_KEY = os.getenv("GOOGLE_WEATHER_API_SECRET", "")
DATA_PATH = Path(os.getenv("DATA_PATH", "data/weather_history_hourly_north.csv"))

# LOCATIONS_JSON may be like: {"tbilisi":[41.795923,44.806403],"batumi":[41.643414,41.6399]}
# If absent, we default to Tbilisi only.
_default_locations: Dict[str, Tuple[float, float]] = {
    "tbilisi": (41.795923, 44.806403),
}
try:
    LOCATIONS: Dict[str, Tuple[float, float]] = (
        json.loads(os.getenv("LOCATIONS_JSON", "")) if os.getenv("LOCATIONS_JSON") else _default_locations
    )
except Exception:
    LOCATIONS = _default_locations

DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---- Helpers ----------------------------------------------------------------
def _retry_get(url: str, *, params: Dict[str, Any], timeout: int = 30, retries: int = 3, backoff: float = 1.5):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff ** attempt)
    if last_err:
        raise last_err

def fetch_history(lat: float, lon: float, hours: int = 24, page_size: int = 24) -> List[Dict[str, Any]]:
    params = {
        "key": API_KEY,
        "location.latitude": lat,
        "location.longitude": lon,
        "unitsSystem": "METRIC",
        "languageCode": "en",
        "hours": hours,
        "pageSize": page_size,
    }
    r = _retry_get(ENDPOINT, params=params)
    return r.json().get("historyHours", []) or []

def mm_from_qpf(qpf: Dict[str, Any] | None) -> float | None:
    if not qpf:
        return None
    qty = qpf.get("quantity")
    unit = qpf.get("unit")
    if qty is None:
        return None
    v = float(qty)
    return round(v * (25.4 if unit == "INCHES" else 1.0), 2)

def wind_ms(wind: Dict[str, Any] | None) -> float | None:
    if not wind or not wind.get("speed"):
        return None
    val = wind["speed"].get("value")
    unit = wind["speed"].get("unit")
    if val is None:
        return None
    v = float(val)
    return round((v * 0.44704) if unit == "MILES_PER_HOUR" else (v / 3.6), 2)

def to_local_iso(ts_utc: str, tz: str = TIMEZONE) -> str:
    dt_utc = dt.datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    return dt_utc.astimezone(ZoneInfo(tz)).isoformat(timespec="seconds")

def flatten(city: str, h: Dict[str, Any]) -> Dict[str, Any]:
    interval = h.get("interval", {})
    start_utc = interval.get("startTime")
    end_utc = interval.get("endTime")
    precip = (h.get("precipitation") or {})
    wind = (h.get("wind") or {})
    temp = (h.get("temperature") or {})
    cond = (h.get("weatherCondition") or {})
    cond_desc = (cond.get("description") or {}).get("text")

    return {
        "city": city,
        "interval_start_utc": start_utc,
        "interval_end_utc": end_utc,
        "local_time": to_local_iso(start_utc) if start_utc else "",
        "temperature_c": temp.get("degrees"),
        "relative_humidity_pct": h.get("relativeHumidity"),
        "cloud_cover_pct": h.get("cloudCover"),
        "precip_mm_per_hr": mm_from_qpf(precip.get("qpf")),
        "precip_probability_pct": (precip.get("probability") or {}).get("percent"),
        "precip_type": (precip.get("probability") or {}).get("type"),
        "wind_speed_m_s": wind_ms(wind),
        "wind_dir_deg": (wind.get("direction") or {}).get("degrees"),
        "is_daytime": h.get("isDaytime"),
        "condition_type": cond.get("type"),
        "condition_text": cond_desc,
    }

def read_existing() -> List[Dict[str, Any]]:
    if not DATA_PATH.exists():
        return []
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_all(rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(DATA_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def last_existing_ts(rows: List[Dict[str, Any]], city: str) -> str | None:
    latest = None
    for r in rows:
        if r.get("city") == city and r.get("interval_start_utc"):
            ts = r["interval_start_utc"]
            if (latest is None) or (ts > latest):
                latest = ts
    return latest

# ---- Main -------------------------------------------------------------------
def main() -> None:
    if not API_KEY:
        raise SystemExit("Missing GOOGLE_WEATHER_API_SECRET environment variable.")

    # 1) Fetch & flatten
    new_rows: List[Dict[str, Any]] = []
    for city, (lat, lon) in LOCATIONS.items():
        hours = fetch_history(lat, lon, hours=24, page_size=24)
        for h in hours:
            new_rows.append(flatten(city, h))

    # Ensure deterministic order
    new_rows.sort(key=lambda r: (r["city"], r.get("interval_start_utc") or ""))

    # 2) Read existing and prepare de-duplication
    existing = read_existing()
    key = lambda r: (r.get("city", ""), r.get("interval_start_utc", ""))
    seen = {key(r) for r in existing}

    # 3) Keep only truly new rows
    appended = [r for r in new_rows if key(r) not in seen]

    # Debug summary
    print(f"Output file: {DATA_PATH}")
    for city in LOCATIONS.keys():
        newest_fetched = max((r["interval_start_utc"] for r in new_rows if r["city"] == city), default=None)
        print(f"[{city}] last_existing={last_existing_ts(existing, city)} newest_fetched={newest_fetched}")

    if not appended:
        print("Added 0 new rows. No write needed (file unchanged).")
        return

    # 4) Merge & write with stable, explicit columns
    cols = [
        "city",
        "interval_start_utc",
        "interval_end_utc",
        "local_time",
        "temperature_c",
        "relative_humidity_pct",
        "cloud_cover_pct",
        "precip_mm_per_hr",
        "precip_probability_pct",
        "precip_type",
        "wind_speed_m_s",
        "wind_dir_deg",
        "is_daytime",
        "condition_type",
        "condition_text",
    ]

    merged = (existing + appended) if existing else new_rows
    final_rows = [{c: r.get(c) for c in cols} for r in merged]
    write_all(final_rows, fieldnames=cols)

    print(f"Added {len(appended)} new rows. Total rows in file: {len(final_rows)}.")

if __name__ == "__main__":
    main()
