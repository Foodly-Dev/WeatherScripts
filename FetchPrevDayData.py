#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch past 24h hourly weather from Google Weather API for Tbilisi & Batumi,
and append any new data to data/weather_history_hourly.csv.
"""
import os, csv, requests, datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo

API_KEY    = "AIzaSyCaotSY2KFJaq_GIwMe8X3-1TNwakyVgV4"
ENDPOINT  = "https://weather.googleapis.com/v1/history/hours:lookup"
TIMEZONE  = "Asia/Tbilisi"

# City -> (lat, lon)
LOCATIONS = {
    "tbilisi": (41.7151377, 44.827096),
    "batumi":  (41.643414,  41.639900),
}

DATA_PATH = Path("data/weather_history_hourly.csv")
DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

def fetch_history(lat: float, lon: float, hours: int = 24, page_size: int = 24):
    params = {
        "key": API_KEY,
        "location.latitude": lat,
        "location.longitude": lon,
        "unitsSystem": "METRIC",      # temps °C, wind km/h, precip mm
        "languageCode": "en",
        "hours": hours,               # up to 24
        "pageSize": page_size         # up to 24 (single page)
    }
    r = requests.get(ENDPOINT, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("historyHours", [])

def mm_from_qpf(qpf: dict | None) -> float | None:
    if not qpf: 
        return None
    qty  = qpf.get("quantity")
    unit = qpf.get("unit")
    if qty is None:
        return None
    return round(float(qty) * (25.4 if unit == "INCHES" else 1.0), 2)

def wind_ms(wind: dict | None) -> float | None:
    if not wind or not wind.get("speed"):
        return None
    val  = wind["speed"].get("value")
    unit = wind["speed"].get("unit")
    if val is None:
        return None
    v = float(val)
    return round((v * 0.44704) if unit == "MILES_PER_HOUR" else (v / 3.6), 2)

def to_local_iso(ts_utc: str, tz: str = TIMEZONE) -> str:
    dt_utc = dt.datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    return dt_utc.astimezone(ZoneInfo(tz)).isoformat(timespec="seconds")

def flatten(city: str, h: dict) -> dict:
    interval   = h.get("interval", {})
    start_utc  = interval.get("startTime")
    end_utc    = interval.get("endTime")
    precip     = (h.get("precipitation") or {})
    wind       = (h.get("wind") or {})
    temp       = (h.get("temperature") or {})
    cond       = (h.get("weatherCondition") or {})
    cond_desc  = (cond.get("description") or {}).get("text")

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

def read_existing():
    if not DATA_PATH.exists():
        return []
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_all(rows: list[dict]):
    if not rows:
        return
    # Use the keys from the first row for the header.
    # This assumes all rows have the same keys.
    with open(DATA_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

def main():
    if not API_KEY:
        raise SystemExit("Missing GOOGLE_WEATHER_API_SECRET env var.")

    # 1) fetch + flatten for all cities
    new_rows = []
    for city, (lat, lon) in LOCATIONS.items():
        hours = fetch_history(lat, lon, hours=24, page_size=24)
        for h in hours:
            new_rows.append(flatten(city, h))
    # ensure chronological
    new_rows.sort(key=lambda r: (r["city"], r["interval_start_utc"] or ""))

    # 2) read existing + index for dedupe
    existing = read_existing()
    key = lambda r: (r.get("city",""), r.get("interval_start_utc",""))
    seen = { key(r) for r in existing }

    # 3) append only truly new rows
    appended = [r for r in new_rows if key(r) not in seen]
    merged = existing + appended if existing else new_rows

    # 4) write back, ensuring stable column order
    cols = [
        "city","interval_start_utc","interval_end_utc","local_time",
        "temperature_c","relative_humidity_pct","cloud_cover_pct",
        "precip_mm_per_hr","precip_probability_pct","precip_type",
        "wind_speed_m_s","wind_dir_deg","is_daytime","condition_type","condition_text"
    ]
    
    if merged:
        # Reorder columns and ensure all rows have all columns
        final_rows = [{c: r.get(c) for c in cols} for r in merged]
        write_all(final_rows)

    print(f"Added {len(appended)} new rows. Total rows in file: {len(merged)}.")

if __name__ == "__main__":
    main()
