#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, requests, datetime as dt
from zoneinfo import ZoneInfo

from pathlib import Path

# ───────── config ─────────
DATA_DIR = Path("data")    # relative to repo root
DATA_DIR.mkdir(parents=True, exist_ok=True)



# ───────── config ─────────
API_KEY   = os.getenv("GOOGLE_WEATHER_API_SECRET")
BASE_URL   = "https://weather.googleapis.com/v1/forecast/hours:lookup"
TBILISI_TZ = ZoneInfo("Asia/Tbilisi")

CITIES = [
    ("Tbilisi", 41.7151377, 44.827096),
    ("Batumi",  41.6167550, 41.6367450),
]

HOURS_TOTAL = 168      # 7 days
PAGE_SIZE   = 24       # API returns max 24 hours per page

# ───────── helpers ─────────
def fetch_hourly(lat, lon, hours=HOURS_TOTAL, page_size=PAGE_SIZE):
    """Fetch ~next `hours` of hourly forecast, paging by `page_size`."""
    all_hours = []
    page_token = None
    params_base = {
        "location.latitude": lat,
        "location.longitude": lon,
        "unitsSystem": "METRIC",
        "hours": hours,
        "pageSize": page_size,
        "key": API_KEY,
    }
    while True:
        params = dict(params_base)
        if page_token:
            params["pageToken"] = page_token
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        hours_chunk = data.get("forecastHours", [])
        all_hours.extend(hours_chunk)

        page_token = data.get("nextPageToken")
        if not page_token or len(hours_chunk) == 0:
            break

        # Safety stop if API over-returns for any reason
        if len(all_hours) >= hours:
            break

    # Sort by interval start (UTC)
    def start_utc(h):
        return (h.get("interval", {}) or {}).get("startTime", "")
    all_hours.sort(key=start_utc)
    return all_hours[:hours]

def _parse_display_datetime(dt_obj):
    """Return (ISO local string, offset_minutes) from Weather API DateTime."""
    if not dt_obj:
        return None, None
    y = dt_obj.get("year"); m = dt_obj.get("month"); d = dt_obj.get("day")
    hh = dt_obj.get("hours", 0); mm = dt_obj.get("minutes", 0); ss = dt_obj.get("seconds", 0)
    tzinfo = None; offset_min = None

    # Prefer fixed UTC offset if provided (e.g., "14400s")
    utc_off = dt_obj.get("utcOffset")
    if utc_off:
        try:
            sec = float(str(utc_off).rstrip("s"))
            tzinfo = dt.timezone(dt.timedelta(seconds=sec))
            offset_min = int(round(sec / 60))
        except Exception:
            pass

    # Or use an IANA zone if present
    tz = (dt_obj.get("timeZone") or {}).get("id")
    if tz:
        try:
            tzinfo = ZoneInfo(tz)
        except Exception:
            pass

    if None in (y, m, d, hh, mm):
        return None, offset_min
    local = dt.datetime(y, m, d, hh, mm, ss or 0, tzinfo=tzinfo)
    if offset_min is None and local.tzinfo and local.utcoffset() is not None:
        offset_min = int(local.utcoffset().total_seconds() // 60)
    return local.isoformat(), offset_min

def row_from_hour(h):
    def g(obj, *keys, default=None):
        x = obj
        for k in keys:
            if not isinstance(x, dict): return default
            x = x.get(k)
        return default if x is None else x

    # Local civil time
    display_iso, offset_min = _parse_display_datetime(h.get("displayDateTime"))

    # Wind speed → m/s
    wind_val  = g(h, "wind", "speed", "value")
    wind_unit = g(h, "wind", "speed", "unit")
    wind_ms = None
    if wind_val is not None:
        if wind_unit == "KILOMETERS_PER_HOUR":
            wind_ms = wind_val / 3.6
        elif wind_unit == "MILES_PER_HOUR":
            wind_ms = wind_val * 0.44704
        else:
            wind_ms = wind_val  # unknown unit, keep as-is

    # QPF → mm
    qpf_qty  = g(h, "precipitation", "qpf", "quantity")
    qpf_unit = g(h, "precipitation", "qpf", "unit")
    if qpf_qty is None:
        precip_mm = 0.0  # API may omit when zero
    else:
        precip_mm = qpf_qty * 25.4 if qpf_unit == "INCHES" else qpf_qty  # MILLIMETERS else inches

    return {
        "interval_start_utc": g(h, "interval", "startTime"),
        "display_local":      display_iso,
        "utc_offset_min":     offset_min,
        "temp_c":             g(h, "temperature", "value"),
        "feels_like_c":       g(h, "feelsLikeTemperature", "value"),
        "rel_humidity_pct":   g(h, "relativeHumidity"),
        "wind_speed_ms":      wind_ms,
        "wind_dir_deg":       g(h, "wind", "direction", "degrees"),
        "precip_mm":          precip_mm,
        "precip_prob_pct":    g(h, "precipitation", "probability", "percent", default=0),
        "precip_type":        g(h, "precipitation", "probability", "type"),
        "condition":          g(h, "weatherCondition", "description", "text") or g(h, "weatherCondition", "type"),
    }


def save_csv(city_name, rows):
    # fixed name, not dated
    fname = DATA_DIR / f"WeekPrediction_{city_name.lower()}.csv"

    headers = [
        "interval_start_utc","display_local","utc_offset_min",
        "temp_c","feels_like_c","rel_humidity_pct",
        "wind_speed_ms","wind_dir_deg",
        "precip_mm","precip_prob_pct","precip_type","condition"
    ]

    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    print(f"Saved {len(rows):4d} rows → {fname}")


# ───────── main ─────────
def main():
    for name, lat, lon in CITIES:
        hours = fetch_hourly(lat, lon)
        rows  = [row_from_hour(h) for h in hours]
        save_csv(name, rows)

if __name__ == "__main__":
    main()
