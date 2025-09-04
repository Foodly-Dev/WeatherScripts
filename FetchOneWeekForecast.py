#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, requests, datetime as dt, json, re
from zoneinfo import ZoneInfo
from pathlib import Path

# ── Sheets deps
import gspread
from google.oauth2.service_account import Credentials

# ───────── config ─────────
DATA_DIR = Path("data")    # relative to repo root
DATA_DIR.mkdir(parents=True, exist_ok=True)

API_KEY   = os.getenv("GOOGLE_WEATHER_API_SECRET")
if not API_KEY:
    raise ValueError("API key not found. Make sure GOOGLE_WEATHER_API_SECRET is set in your repository secrets.")
BASE_URL   = "https://weather.googleapis.com/v1/forecast/hours:lookup"
TBILISI_TZ = ZoneInfo("Asia/Tbilisi")

CITIES = [
    ("Tbilisi", 41.7151377, 44.827096),
    ("Batumi",  41.6167550, 41.6367450),
]

HOURS_TOTAL = 168      # 7 days
PAGE_SIZE   = 24       # API returns max 24 hours per page

# ───────── Sheets config (Option A) ─────────
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_URL_OR_ID = os.getenv("GOOGLE_SHEETS_URL")  # full URL or bare ID
TAB_GID  = os.getenv("GOOGLE_SHEETS_TAB_GID")                 # prefer this for exact tab
TAB_NAME = os.getenv("GOOGLE_SHEETS_TAB_NAME")                # optional fallback (auto-create)
SA_JSON  = os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON")

SHEET_HEADERS = [
    "interval_start_utc","display_local","utc_offset_min",
    "temp_c","feels_like_c","rel_humidity_pct",
    "wind_speed_ms","wind_dir_deg",
    "precip_mm","precip_prob_pct","precip_type","condition"
]

# ───────── Sheets helpers ─────────
def _parse_spreadsheet_id(url_or_id: str) -> str:
    if not url_or_id:
        raise ValueError("GOOGLE_SHEETS_URL is not set.")
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_id)
    return m.group(1) if m else url_or_id.strip()

def _gspread_client():
    if not SA_JSON:
        raise ValueError("Service Account JSON not found in GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON.")
    creds = Credentials.from_service_account_info(json.loads(SA_JSON), scopes=SHEETS_SCOPES)
    return gspread.authorize(creds)

def _open_spreadsheet():
    ssid = _parse_spreadsheet_id(SPREADSHEET_URL_OR_ID)
    return _gspread_client().open_by_key(ssid)

def _get_or_create_by_name(sh, title):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=HOURS_TOTAL + 10, cols=len(SHEET_HEADERS) + 2)

def _get_by_gid(sh, gid_int: int):
    if hasattr(sh, "get_worksheet_by_id"):
        return sh.get_worksheet_by_id(gid_int)
    for ws in sh.worksheets():
        if getattr(ws, "id", None) == gid_int:
            return ws
    raise gspread.WorksheetNotFound(f"No worksheet with gid={gid_int}")

def resolve_target_worksheet(default_title: str):
    """Priority: TAB_GID → TAB_NAME → default_title (created if missing)."""
    sh = _open_spreadsheet()
    if TAB_GID:
        return _get_by_gid(sh, int(TAB_GID))
    if TAB_NAME:
        return _get_or_create_by_name(sh, TAB_NAME)
    return _get_or_create_by_name(sh, default_title)

# ───────── weather helpers (your originals) ─────────
def fetch_hourly(lat, lon, hours=HOURS_TOTAL, page_size=PAGE_SIZE):
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

        if len(all_hours) >= hours:
            break

    def start_utc(h):
        return (h.get("interval", {}) or {}).get("startTime", "")
    all_hours.sort(key=start_utc)
    return all_hours[:hours]

def _parse_display_datetime(dt_obj):
    if not dt_obj:
        return None, None
    y = dt_obj.get("year"); m = dt_obj.get("month"); d = dt_obj.get("day")
    hh = dt_obj.get("hours", 0); mm = dt_obj.get("minutes", 0); ss = dt_obj.get("seconds", 0)
    tzinfo = None; offset_min = None

    utc_off = dt_obj.get("utcOffset")
    if utc_off:
        try:
            sec = float(str(utc_off).rstrip("s"))
            tzinfo = dt.timezone(dt.timedelta(seconds=sec))
            offset_min = int(round(sec / 60))
        except Exception:
            pass

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

    display_iso, offset_min = _parse_display_datetime(h.get("displayDateTime"))

    wind_val  = g(h, "wind", "speed", "value")
    wind_unit = g(h, "wind", "speed", "unit")
    wind_ms = None
    if wind_val is not None:
        if wind_unit == "KILOMETERS_PER_HOUR":
            wind_ms = wind_val / 3.6
        elif wind_unit == "MILES_PER_HOUR":
            wind_ms = wind_val * 0.44704
        else:
            wind_ms = wind_val

    qpf_qty  = g(h, "precipitation", "qpf", "quantity")
    qpf_unit = g(h, "precipitation", "qpf", "unit")
    if qpf_qty is None:
        precip_mm = 0.0
    else:
        precip_mm = qpf_qty * 25.4 if qpf_unit == "INCHES" else qpf_qty

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
    fname = DATA_DIR / f"WeekPrediction_{city_name.lower()}.csv"
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SHEET_HEADERS)
        w.writeheader()
        w.writerows(rows)
    print(f"Saved {len(rows):4d} rows → {fname}")

def upload_to_sheets(city_name, rows):
    ws = resolve_target_worksheet(default_title=city_name)  # TAB_GID wins; else TAB_NAME; else per-city
    values = [[r.get(h) for h in SHEET_HEADERS] for r in rows]
    ws.clear()
    ws.update("A1", [SHEET_HEADERS] + values)
    try:
        ws.freeze(rows=1)
    except Exception:
        pass
    print(f"Uploaded {len(values):4d} rows → Google Sheet tab '{ws.title}' (gid={getattr(ws,'id','?')})")

# ───────── main ─────────
def main():
    for name, lat, lon in CITIES:
        hours = fetch_hourly(lat, lon)
        rows  = [row_from_hour(h) for h in hours]
        save_csv(name, rows)          # keep artifacts if you want
        upload_to_sheets(name, rows)  # push to Sheets

if __name__ == "__main__":
    main()
