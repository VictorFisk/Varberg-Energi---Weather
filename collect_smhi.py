#!/usr/bin/env python3
"""
collect_smhi.py — Varberg Energi SMHI Data Collector
=====================================================
Fetches weather observations and forecasts from SMHI Open Data API
and stores them in a local SQLite database.

Usage:
    python3 collect_smhi.py            # collect latest data
    python3 collect_smhi.py --backfill # backfill last 30 days
    python3 collect_smhi.py --export   # export latest week as JSON for HTML report

Schedule with cron (every hour):
    0 * * * * /usr/bin/python3 /path/to/collect_smhi.py >> /path/to/collect.log 2>&1

SMHI Open Data API docs: https://opendata.smhi.se/apidocs/
"""

import sqlite3
import requests
import json
import math
import sys
import logging
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────────────────────────────

# Varberg coordinates
LAT  = 57.106
LON  = 12.254

# Nearest SMHI observation station for Varberg: Getterön (ID 72450)
# Find yours at: https://opendata.smhi.se/apidocs/metobs/index.html
STATION_ID = 72450

# SQLite database path (same folder as this script)
DB_PATH = Path(__file__).parent / "varberg_weather.db"

# JSON export path (read by the HTML report)
EXPORT_PATH = Path(__file__).parent / "weather_data.json"

# SMHI API base URLs
OBS_BASE      = "https://opendata-download-metobs.smhi.se/api/version/latest"
FORECAST_BASE = "https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2"

# SMHI parameter IDs for observations
# Full list: https://opendata.smhi.se/apidocs/metobs/parameters.html
SMHI_PARAMS = {
    "temp":     1,   # Air temperature (°C), hourly mean
    "wind":     4,   # Wind speed (m/s), hourly mean
    "gust":     21,  # Wind gust (m/s)
    "precip":   5,   # Precipitation (mm), hourly sum
    "humidity": 6,   # Relative humidity (%), hourly mean
    "pressure": 9,   # Air pressure (hPa)
    "solar":    118, # Global radiation (W/m²), hourly mean
}

# Heat index reference temperature (standard Swedish district heating)
HDD_REF = 17.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("smhi_collector")

# ── DATABASE SETUP ────────────────────────────────────────────────────────────

def init_db(db_path: Path) -> sqlite3.Connection:
    """Create tables if they don't exist."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.executescript("""
    PRAGMA journal_mode = WAL;
    PRAGMA foreign_keys = ON;

    -- Hourly observations from SMHI station
    CREATE TABLE IF NOT EXISTS observations (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ts          TEXT NOT NULL,          -- ISO-8601 UTC  e.g. 2026-03-06T14:00:00Z
        date        TEXT NOT NULL,          -- YYYY-MM-DD (local Swedish time)
        hour        INTEGER NOT NULL,       -- 0-23
        temp_c      REAL,
        wind_ms     REAL,
        gust_ms     REAL,
        precip_mm   REAL,
        humidity_pct REAL,
        pressure_hpa REAL,
        solar_wm2   REAL,
        UNIQUE(ts)
    );

    -- Daily aggregates (computed from observations)
    CREATE TABLE IF NOT EXISTS daily (
        date            TEXT PRIMARY KEY,   -- YYYY-MM-DD
        week            INTEGER,            -- ISO week number
        year            INTEGER,
        temp_mean       REAL,
        temp_min        REAL,
        temp_max        REAL,
        wind_mean       REAL,
        wind_max        REAL,
        precip_sum      REAL,
        humidity_mean   REAL,
        solar_mean      REAL,
        solar_max       REAL,
        hdd             REAL,               -- Heating Degree Days (HDD_REF - temp_mean, min 0)
        wind_addition   REAL,               -- wind contribution to heat index
        solar_reduction REAL,               -- solar reduction from heat index
        heat_index      REAL,               -- composite heat demand index (0-100)
        frost_day       INTEGER,            -- 1 if temp_min < 0
        updated_at      TEXT
    );

    -- Weekly aggregates
    CREATE TABLE IF NOT EXISTS weekly (
        year            INTEGER,
        week            INTEGER,
        date_from       TEXT,
        date_to         TEXT,
        temp_mean       REAL,
        temp_min        REAL,
        temp_max        REAL,
        wind_mean       REAL,
        wind_max        REAL,
        precip_sum      REAL,
        humidity_mean   REAL,
        solar_mean      REAL,
        hdd_sum         REAL,
        heat_index_mean REAL,
        frost_days      INTEGER,
        solar_reduction_mean REAL,
        updated_at      TEXT,
        PRIMARY KEY (year, week)
    );

    -- 10-day forecast from SMHI PMP3g
    CREATE TABLE IF NOT EXISTS forecast (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        fetched_at  TEXT NOT NULL,
        valid_time  TEXT NOT NULL,          -- ISO-8601 UTC
        date        TEXT NOT NULL,
        hour        INTEGER NOT NULL,
        temp_c      REAL,
        wind_ms     REAL,
        gust_ms     REAL,
        precip_mm   REAL,
        humidity_pct REAL,
        symbol      INTEGER,               -- SMHI weather symbol (1-27)
        solar_wm2   REAL,                  -- estimated from symbol + time of year
        UNIQUE(fetched_at, valid_time)
    );

    -- Collector run log
    CREATE TABLE IF NOT EXISTS run_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        run_at      TEXT NOT NULL,
        mode        TEXT,
        obs_rows    INTEGER DEFAULT 0,
        fcst_rows   INTEGER DEFAULT 0,
        daily_rows  INTEGER DEFAULT 0,
        errors      TEXT
    );
    """)
    con.commit()
    return con


# ── SMHI OBSERVATION FETCHER ──────────────────────────────────────────────────

def fetch_obs_parameter(param_id: int, station_id: int) -> list[dict]:
    """
    Fetch latest hourly observations for one parameter from SMHI metobs API.
    Returns list of {ts, value} dicts.
    """
    url = (f"{OBS_BASE}/parameter/{param_id}/station/{station_id}"
           f"/period/latest-months/data.json")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        rows = []
        for val in data.get("value", []):
            try:
                # SMHI returns epoch ms in 'date' field
                ts_ms = val["date"]
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                v = float(val["value"]) if val["value"] not in ("", None) else None
                rows.append({"ts": ts, "value": v})
            except (KeyError, ValueError, TypeError):
                continue
        return rows
    except Exception as e:
        log.warning(f"  Param {param_id} fetch failed: {e}")
        return []


def fetch_all_observations(con: sqlite3.Connection, station_id: int) -> int:
    """Fetch all configured parameters and merge into observations table."""
    log.info(f"Fetching observations from station {station_id}...")

    # Collect raw data per parameter
    raw: dict[str, dict] = {}  # ts_str -> {param: value}

    for name, param_id in SMHI_PARAMS.items():
        log.info(f"  → {name} (param {param_id})")
        rows = fetch_obs_parameter(param_id, station_id)
        for row in rows:
            ts_str = row["ts"].strftime("%Y-%m-%dT%H:00:00Z")
            if ts_str not in raw:
                raw[ts_str] = {}
            raw[ts_str][name] = row["value"]

    if not raw:
        log.warning("No observation data fetched.")
        return 0

    # Insert/update into DB
    cur = con.cursor()
    inserted = 0
    for ts_str, vals in sorted(raw.items()):
        ts_dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=timezone.utc)
        # Convert to Swedish local time (UTC+1 winter, UTC+2 summer — approximate)
        local_dt = ts_dt + timedelta(hours=1)
        date_str = local_dt.strftime("%Y-%m-%d")
        hour = local_dt.hour

        try:
            cur.execute("""
                INSERT INTO observations
                    (ts, date, hour, temp_c, wind_ms, gust_ms, precip_mm,
                     humidity_pct, pressure_hpa, solar_wm2)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(ts) DO UPDATE SET
                    temp_c       = excluded.temp_c,
                    wind_ms      = excluded.wind_ms,
                    gust_ms      = excluded.gust_ms,
                    precip_mm    = excluded.precip_mm,
                    humidity_pct = excluded.humidity_pct,
                    pressure_hpa = excluded.pressure_hpa,
                    solar_wm2    = excluded.solar_wm2
            """, (
                ts_str, date_str, hour,
                vals.get("temp"),
                vals.get("wind"),
                vals.get("gust"),
                vals.get("precip"),
                vals.get("humidity"),
                vals.get("pressure"),
                vals.get("solar"),
            ))
            if cur.rowcount:
                inserted += 1
        except sqlite3.Error as e:
            log.warning(f"  DB insert error for {ts_str}: {e}")

    con.commit()
    log.info(f"  ✓ {inserted} observation rows upserted")
    return inserted


# ── SMHI FORECAST FETCHER ─────────────────────────────────────────────────────

def estimate_solar(hour: int, symbol: int, day_of_year: int, lat: float) -> float:
    """
    Estimate solar irradiance (W/m²) from weather symbol + time of day + season.
    SMHI doesn't provide solar directly in PMP3g forecast.
    """
    # Solar elevation angle factor (simplified)
    solar_noon = 12
    hour_angle = abs(hour - solar_noon)
    if hour_angle >= 6:
        return 0.0  # Before sunrise / after sunset (rough approximation)

    # Seasonal factor: max irradiance at summer solstice
    seasonal = math.cos(math.radians((day_of_year - 172) * 360 / 365)) * 0.5 + 0.5
    # Latitude factor
    lat_factor = math.cos(math.radians(lat - 23.5 * math.cos(math.radians((day_of_year - 172) * 360 / 365))))

    # Base clear-sky irradiance
    time_factor = math.cos(math.radians(hour_angle * 15))
    base = max(0, 900 * seasonal * lat_factor * time_factor)

    # Cloud cover factor from SMHI weather symbol
    # 1-2 = clear, 3-6 = partly cloudy, 7-10 = overcast, 11+ = precip
    if symbol <= 2:
        cloud_factor = 0.95
    elif symbol <= 6:
        cloud_factor = 0.55
    elif symbol <= 10:
        cloud_factor = 0.20
    else:
        cloud_factor = 0.10

    return round(base * cloud_factor, 1)


def fetch_forecast(con: sqlite3.Connection) -> int:
    """Fetch 10-day point forecast from SMHI PMP3g."""
    log.info(f"Fetching forecast for lat={LAT}, lon={LON}...")
    url = f"{FORECAST_BASE}/geotype/point/lon/{LON}/lat/{LAT}/data.json"
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")

    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning(f"  Forecast fetch failed: {e}")
        return 0

    cur = con.cursor()
    inserted = 0

    for ts_entry in data.get("timeSeries", []):
        valid_time_str = ts_entry["validTime"]  # e.g. "2026-03-06T14:00:00Z"
        try:
            valid_dt = datetime.strptime(valid_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        local_dt = valid_dt + timedelta(hours=1)
        date_str = local_dt.strftime("%Y-%m-%d")
        hour = local_dt.hour
        doy = local_dt.timetuple().tm_yday

        # Parse parameters
        params = {p["name"]: p["values"][0] for p in ts_entry.get("parameters", []) if p.get("values")}

        temp    = params.get("t")
        wind    = params.get("ws")
        gust    = params.get("gust")
        precip  = params.get("pmin", params.get("pmean"))
        humidity = params.get("r")
        symbol  = int(params.get("Wsymb2", 1))
        solar   = estimate_solar(hour, symbol, doy, LAT)

        try:
            cur.execute("""
                INSERT INTO forecast
                    (fetched_at, valid_time, date, hour, temp_c, wind_ms, gust_ms,
                     precip_mm, humidity_pct, symbol, solar_wm2)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(fetched_at, valid_time) DO NOTHING
            """, (fetched_at, valid_time_str, date_str, hour,
                  temp, wind, gust, precip, humidity, symbol, solar))
            if cur.rowcount:
                inserted += 1
        except sqlite3.Error as e:
            log.warning(f"  Forecast DB error {valid_time_str}: {e}")

    con.commit()
    log.info(f"  ✓ {inserted} forecast hours inserted")
    return inserted


# ── DAILY AGGREGATION ─────────────────────────────────────────────────────────

def compute_heat_index(temp_mean: float, wind_mean: float, solar_mean: float) -> tuple[float, float, float]:
    """
    Compute heat demand index components.
    Returns (wind_addition, solar_reduction, heat_index).
    """
    # Temperature component: HDD-based, scaled to 0-88
    hdd = max(0.0, HDD_REF - temp_mean)
    temp_component = min(88.0, hdd * 4.8)

    # Wind addition: +0 at 0 m/s, +12 at 10+ m/s
    wind_addition = min(12.0, wind_mean * 1.2)

    # Solar reduction: irradiance * 0.025, max 12
    solar_reduction = min(12.0, solar_mean * 0.025)

    heat_index = max(0.0, min(100.0, temp_component + wind_addition - solar_reduction))
    return round(wind_addition, 2), round(solar_reduction, 2), round(heat_index, 1)


def aggregate_daily(con: sqlite3.Connection, dates: list[str] | None = None) -> int:
    """Compute daily aggregates from hourly observations."""
    cur = con.cursor()

    if dates is None:
        # Aggregate all dates that have observations but no daily record,
        # or where observations were updated recently
        cur.execute("""
            SELECT DISTINCT date FROM observations
            WHERE date NOT IN (SELECT date FROM daily)
               OR date >= date('now', '-3 days')
            ORDER BY date
        """)
        dates = [r["date"] for r in cur.fetchall()]

    log.info(f"Aggregating {len(dates)} day(s)...")
    updated = 0

    for d in dates:
        cur.execute("""
            SELECT
                AVG(temp_c)       as temp_mean,
                MIN(temp_c)       as temp_min,
                MAX(temp_c)       as temp_max,
                AVG(wind_ms)      as wind_mean,
                MAX(wind_ms)      as wind_max,
                SUM(precip_mm)    as precip_sum,
                AVG(humidity_pct) as humidity_mean,
                AVG(solar_wm2)    as solar_mean,
                MAX(solar_wm2)    as solar_max
            FROM observations
            WHERE date = ? AND temp_c IS NOT NULL
        """, (d,))
        row = cur.fetchone()
        if not row or row["temp_mean"] is None:
            continue

        temp_mean    = round(row["temp_mean"], 1)
        temp_min     = round(row["temp_min"], 1)
        temp_max     = round(row["temp_max"], 1)
        wind_mean    = round(row["wind_mean"] or 0, 1)
        wind_max     = round(row["wind_max"] or 0, 1)
        precip_sum   = round(row["precip_sum"] or 0, 1)
        humidity_mean = round(row["humidity_mean"] or 0, 1)
        solar_mean   = round(row["solar_mean"] or 0, 1)
        solar_max    = round(row["solar_max"] or 0, 1)
        hdd          = round(max(0.0, HDD_REF - temp_mean), 2)
        frost_day    = 1 if temp_min < 0 else 0

        wind_add, sol_red, heat_idx = compute_heat_index(temp_mean, wind_mean, solar_mean)

        dt = datetime.strptime(d, "%Y-%m-%d")
        iso_cal = dt.isocalendar()

        cur.execute("""
            INSERT INTO daily
                (date, week, year, temp_mean, temp_min, temp_max,
                 wind_mean, wind_max, precip_sum, humidity_mean,
                 solar_mean, solar_max, hdd, wind_addition, solar_reduction,
                 heat_index, frost_day, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
                temp_mean=excluded.temp_mean, temp_min=excluded.temp_min,
                temp_max=excluded.temp_max, wind_mean=excluded.wind_mean,
                wind_max=excluded.wind_max, precip_sum=excluded.precip_sum,
                humidity_mean=excluded.humidity_mean, solar_mean=excluded.solar_mean,
                solar_max=excluded.solar_max, hdd=excluded.hdd,
                wind_addition=excluded.wind_addition,
                solar_reduction=excluded.solar_reduction,
                heat_index=excluded.heat_index, frost_day=excluded.frost_day,
                updated_at=excluded.updated_at
        """, (
            d, iso_cal.week, iso_cal.year,
            temp_mean, temp_min, temp_max,
            wind_mean, wind_max, precip_sum, humidity_mean,
            solar_mean, solar_max, hdd,
            wind_add, sol_red, heat_idx, frost_day,
            datetime.now(timezone.utc).isoformat()
        ))
        updated += 1

    con.commit()
    log.info(f"  ✓ {updated} daily rows upserted")

    # Also update weekly aggregates
    aggregate_weekly(con)
    return updated


def aggregate_weekly(con: sqlite3.Connection) -> int:
    """Compute weekly aggregates from daily table."""
    cur = con.cursor()
    cur.execute("""
        SELECT year, week,
            MIN(date) as date_from, MAX(date) as date_to,
            ROUND(AVG(temp_mean),1)       as temp_mean,
            ROUND(MIN(temp_min),1)        as temp_min,
            ROUND(MAX(temp_max),1)        as temp_max,
            ROUND(AVG(wind_mean),1)       as wind_mean,
            ROUND(MAX(wind_max),1)        as wind_max,
            ROUND(SUM(precip_sum),1)      as precip_sum,
            ROUND(AVG(humidity_mean),1)   as humidity_mean,
            ROUND(AVG(solar_mean),1)      as solar_mean,
            ROUND(SUM(hdd),1)             as hdd_sum,
            ROUND(AVG(heat_index),1)      as heat_index_mean,
            SUM(frost_day)                as frost_days,
            ROUND(AVG(solar_reduction),2) as solar_reduction_mean
        FROM daily
        GROUP BY year, week
    """)
    rows = cur.fetchall()
    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        cur.execute("""
            INSERT INTO weekly
                (year, week, date_from, date_to, temp_mean, temp_min, temp_max,
                 wind_mean, wind_max, precip_sum, humidity_mean, solar_mean,
                 hdd_sum, heat_index_mean, frost_days, solar_reduction_mean, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(year,week) DO UPDATE SET
                date_from=excluded.date_from, date_to=excluded.date_to,
                temp_mean=excluded.temp_mean, temp_min=excluded.temp_min,
                temp_max=excluded.temp_max, wind_mean=excluded.wind_mean,
                wind_max=excluded.wind_max, precip_sum=excluded.precip_sum,
                humidity_mean=excluded.humidity_mean, solar_mean=excluded.solar_mean,
                hdd_sum=excluded.hdd_sum, heat_index_mean=excluded.heat_index_mean,
                frost_days=excluded.frost_days,
                solar_reduction_mean=excluded.solar_reduction_mean,
                updated_at=excluded.updated_at
        """, (
            row["year"], row["week"], row["date_from"], row["date_to"],
            row["temp_mean"], row["temp_min"], row["temp_max"],
            row["wind_mean"], row["wind_max"], row["precip_sum"],
            row["humidity_mean"], row["solar_mean"], row["hdd_sum"],
            row["heat_index_mean"], row["frost_days"],
            row["solar_reduction_mean"], now
        ))
    con.commit()
    return len(rows)


# ── JSON EXPORT FOR HTML REPORT ───────────────────────────────────────────────

def export_json(con: sqlite3.Connection, export_path: Path):
    """
    Export the data the HTML report needs as a JSON file.
    Includes: current conditions, 7-day forecast, last 8 weeks history.
    """
    cur = con.cursor()

    # Latest observation (current conditions)
    cur.execute("""
        SELECT * FROM observations
        WHERE temp_c IS NOT NULL
        ORDER BY ts DESC LIMIT 1
    """)
    latest_obs = dict(cur.fetchone() or {})

    # Today's daily summary
    today_str = date.today().isoformat()
    cur.execute("SELECT * FROM daily WHERE date = ?", (today_str,))
    today_daily = dict(cur.fetchone() or {})

    # Last 8 weeks of weekly data
    cur.execute("""
        SELECT * FROM weekly
        ORDER BY year DESC, week DESC
        LIMIT 104
    """)
    history_weeks = [dict(r) for r in cur.fetchall()]

    # Daily data for last 30 days (for chart popups)
    cur.execute("""
        SELECT * FROM daily
        WHERE date >= date('now', '-730 days')
        ORDER BY date ASC
    """)
    history_daily = [dict(r) for r in cur.fetchall()]

    # Forecast: next 7 days, daily aggregates
    cur.execute("""
        SELECT
            date,
            ROUND(AVG(temp_c),1)        as temp_mean,
            ROUND(MIN(temp_c),1)        as temp_min,
            ROUND(MAX(temp_c),1)        as temp_max,
            ROUND(AVG(wind_ms),1)       as wind_mean,
            ROUND(MAX(gust_ms),1)       as wind_max,
            ROUND(SUM(precip_mm),1)     as precip_sum,
            ROUND(AVG(humidity_pct),1)  as humidity_mean,
            ROUND(AVG(solar_wm2),1)     as solar_mean,
            MIN(symbol)                 as symbol_min
        FROM forecast
        WHERE fetched_at = (SELECT MAX(fetched_at) FROM forecast)
          AND date >= date('now')
          AND date <= date('now', '+7 days')
        GROUP BY date
        ORDER BY date ASC
    """)
    forecast_daily = [dict(r) for r in cur.fetchall()]

    # Hourly forecast for next 48 hours (for the hour modal)
    cur.execute("""
        SELECT * FROM forecast
        WHERE fetched_at = (SELECT MAX(fetched_at) FROM forecast)
          AND valid_time >= datetime('now')
          AND valid_time <= datetime('now', '+48 hours')
        ORDER BY valid_time ASC
    """)
    forecast_hourly = [dict(r) for r in cur.fetchall()]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "station_id": STATION_ID,
        "lat": LAT,
        "lon": LON,
        "latest_observation": latest_obs,
        "today": today_daily,
        "forecast_daily": forecast_daily,
        "forecast_hourly": forecast_hourly,
        "history_weekly": history_weeks,
        "history_daily": history_daily,
    }

    with open(export_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"  ✓ Exported JSON → {export_path}")
    log.info(f"    {len(forecast_daily)} forecast days, {len(history_weeks)} history weeks")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    mode = "collect"
    if "--backfill" in sys.argv:
        mode = "backfill"
    elif "--export" in sys.argv:
        mode = "export-only"

    log.info(f"=== SMHI Collector starting (mode={mode}) ===")
    log.info(f"    DB: {DB_PATH}")

    con = init_db(DB_PATH)
    errors = []
    obs_rows = fcst_rows = daily_rows = 0

    try:
        if mode == "export-only":
            export_json(con, EXPORT_PATH)
        else:
            # Always fetch fresh observations + forecast
            obs_rows  = fetch_all_observations(con, STATION_ID)
            fcst_rows = fetch_forecast(con)
            daily_rows = aggregate_daily(con)
            export_json(con, EXPORT_PATH)

    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
        errors.append(str(e))

    # Log this run
    cur = con.cursor()
    cur.execute("""
        INSERT INTO run_log (run_at, mode, obs_rows, fcst_rows, daily_rows, errors)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        datetime.now(timezone.utc).isoformat(),
        mode, obs_rows, fcst_rows, daily_rows,
        "; ".join(errors) if errors else None
    ))
    con.commit()
    con.close()

    log.info(f"=== Done. obs={obs_rows} fcst={fcst_rows} daily={daily_rows} ===")


if __name__ == "__main__":
    main()
