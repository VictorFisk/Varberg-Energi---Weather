#!/usr/bin/env python3
"""
backfill_smhi.py — Varberg Energi Historical Data Backfill
===========================================================
Fetches up to 2 years of corrected historical observations from
SMHI metobs API and loads them into the local SQLite database.

Uses the 'corrected-archive' period which provides quality-controlled
data — ideal for accurate year-on-year comparisons.

Usage:
    python3 backfill_smhi.py              # fetch all available archive data
    python3 backfill_smhi.py --year 2024  # fetch only a specific year
    python3 backfill_smhi.py --dry-run    # show what would be fetched, no DB writes

Run once manually. After that, collect_smhi.py handles ongoing collection.

SMHI corrected-archive docs:
  https://opendata.smhi.se/apidocs/metobs/period.html
"""

import sqlite3
import requests
import sys
import time
import logging
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

# ── CONFIG (must match collect_smhi.py) ──────────────────────────────────────

STATION_ID = 72450  # Getterön, Varberg
DB_PATH    = Path(__file__).parent / "varberg_weather.db"
EXPORT_PATH = Path(__file__).parent / "weather_data.json"
HDD_REF    = 17.0

OBS_BASE = "https://opendata-download-metobs.smhi.se/api/version/latest"

# SMHI parameter IDs — same as collect_smhi.py
SMHI_PARAMS = {
    "temp":     1,    # Air temperature (°C)
    "wind":     4,    # Wind speed (m/s)
    "gust":     21,   # Wind gust (m/s)
    "precip":   5,    # Precipitation (mm)
    "humidity": 6,    # Relative humidity (%)
    "pressure": 9,    # Air pressure (hPa)
    "solar":    118,  # Global radiation (W/m²)
}

# SMHI archive periods to try (in order of preference)
# corrected-archive = quality-checked, goes back ~2 years
# latest-year = raw, current year only (fallback)
ARCHIVE_PERIODS = ["corrected-archive", "latest-year", "latest-months"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("smhi_backfill")


# ── DATABASE ──────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    if not DB_PATH.exists():
        log.error(f"Database not found: {DB_PATH}")
        log.error("Run collect_smhi.py first to initialise the database.")
        sys.exit(1)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def get_existing_dates(con: sqlite3.Connection) -> set[str]:
    """Return set of dates that already have at least 18 observation hours."""
    cur = con.cursor()
    cur.execute("""
        SELECT date FROM observations
        GROUP BY date
        HAVING COUNT(*) >= 18
    """)
    return {r["date"] for r in cur.fetchall()}


# ── SMHI ARCHIVE FETCHER ──────────────────────────────────────────────────────

def fetch_archive_parameter(param_id: int, station_id: int) -> tuple[list[dict], str]:
    """
    Fetch full archive for one parameter, trying each period in order.
    Returns (rows, period_used).
    Each row: {"ts": datetime (UTC), "value": float|None}
    """
    for period in ARCHIVE_PERIODS:
        url = (f"{OBS_BASE}/parameter/{param_id}/station/{station_id}"
               f"/period/{period}/data.json")
        log.info(f"    Trying {period}...")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 404:
                log.info(f"    → 404, trying next period")
                continue
            r.raise_for_status()
            data = r.json()
            rows = []
            for val in data.get("value", []):
                try:
                    ts_ms = val["date"]
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    v = float(val["value"]) if val["value"] not in ("", None, "Saknas") else None
                    rows.append({"ts": ts, "value": v})
                except (KeyError, ValueError, TypeError):
                    continue
            if rows:
                log.info(f"    → {len(rows)} rows from {period}")
                return rows, period
        except requests.exceptions.Timeout:
            log.warning(f"    Timeout on {period}, trying next")
            continue
        except Exception as e:
            log.warning(f"    Error on {period}: {e}")
            continue

    return [], "none"


def filter_by_year(rows: list[dict], year: int | None) -> list[dict]:
    if year is None:
        return rows
    return [r for r in rows if r["ts"].year == year]


# ── MERGE AND INSERT ──────────────────────────────────────────────────────────

def insert_observations(con: sqlite3.Connection, raw: dict, dry_run: bool) -> int:
    """Insert merged observations into DB. Returns count of new/updated rows."""
    if dry_run:
        log.info(f"  [dry-run] Would insert/update {len(raw)} observation hours")
        return len(raw)

    cur = con.cursor()
    inserted = 0

    for ts_str, vals in sorted(raw.items()):
        ts_dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=timezone.utc)
        # Swedish local time (UTC+1 winter, +2 summer — simplified to +1)
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
                    temp_c        = COALESCE(excluded.temp_c,        temp_c),
                    wind_ms       = COALESCE(excluded.wind_ms,       wind_ms),
                    gust_ms       = COALESCE(excluded.gust_ms,       gust_ms),
                    precip_mm     = COALESCE(excluded.precip_mm,     precip_mm),
                    humidity_pct  = COALESCE(excluded.humidity_pct,  humidity_pct),
                    pressure_hpa  = COALESCE(excluded.pressure_hpa,  pressure_hpa),
                    solar_wm2     = COALESCE(excluded.solar_wm2,     solar_wm2)
                    -- Note: observations have no source tag; source is tracked on daily level
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
            log.warning(f"  DB error at {ts_str}: {e}")

    con.commit()
    return inserted


# ── DAILY AGGREGATION (copied from collect_smhi.py) ───────────────────────────

def compute_heat_index(temp_mean, wind_mean, solar_mean):
    hdd = max(0.0, HDD_REF - temp_mean)
    temp_component = min(88.0, hdd * 4.8)
    wind_addition = min(12.0, wind_mean * 1.2)
    solar_reduction = min(12.0, solar_mean * 0.025)
    heat_index = max(0.0, min(100.0, temp_component + wind_addition - solar_reduction))
    return round(wind_addition, 2), round(solar_reduction, 2), round(heat_index, 1)


def aggregate_all_daily(con: sqlite3.Connection, dry_run: bool) -> int:
    """Recompute daily aggregates for all dates that have observations."""
    if dry_run:
        cur = con.cursor()
        cur.execute("SELECT COUNT(DISTINCT date) as n FROM observations")
        n = cur.fetchone()["n"]
        log.info(f"  [dry-run] Would aggregate {n} days")
        return n

    cur = con.cursor()
    cur.execute("SELECT DISTINCT date FROM observations ORDER BY date")
    dates = [r["date"] for r in cur.fetchall()]
    log.info(f"  Aggregating {len(dates)} days...")

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

        temp_mean     = round(row["temp_mean"], 1)
        temp_min      = round(row["temp_min"], 1)
        temp_max      = round(row["temp_max"], 1)
        wind_mean     = round(row["wind_mean"] or 0, 1)
        wind_max      = round(row["wind_max"] or 0, 1)
        precip_sum    = round(row["precip_sum"] or 0, 1)
        humidity_mean = round(row["humidity_mean"] or 0, 1)
        solar_mean    = round(row["solar_mean"] or 0, 1)
        solar_max     = round(row["solar_max"] or 0, 1)
        hdd           = round(max(0.0, HDD_REF - temp_mean), 2)
        frost_day     = 1 if temp_min < 0 else 0
        wind_add, sol_red, heat_idx = compute_heat_index(temp_mean, wind_mean, solar_mean)

        dt = datetime.strptime(d, "%Y-%m-%d")
        iso_cal = dt.isocalendar()

        cur.execute("""
            INSERT INTO daily
                (date, week, year, temp_mean, temp_min, temp_max,
                 wind_mean, wind_max, precip_sum, humidity_mean,
                 solar_mean, solar_max, hdd, wind_addition, solar_reduction,
                 heat_index, frost_day, updated_at, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'backfill')
            ON CONFLICT(date) DO UPDATE SET
                temp_mean=excluded.temp_mean, temp_min=excluded.temp_min,
                temp_max=excluded.temp_max, wind_mean=excluded.wind_mean,
                wind_max=excluded.wind_max, precip_sum=excluded.precip_sum,
                humidity_mean=excluded.humidity_mean, solar_mean=excluded.solar_mean,
                solar_max=excluded.solar_max, hdd=excluded.hdd,
                wind_addition=excluded.wind_addition,
                solar_reduction=excluded.solar_reduction,
                heat_index=excluded.heat_index, frost_day=excluded.frost_day,
                updated_at=excluded.updated_at,
                source=CASE WHEN daily.source='live' THEN 'live' ELSE 'backfill' END
                -- Backfill never downgrades a live row; live data takes priority
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

    # Weekly rollup
    cur.execute("""
        INSERT OR REPLACE INTO weekly
            (year, week, date_from, date_to, temp_mean, temp_min, temp_max,
             wind_mean, wind_max, precip_sum, humidity_mean, solar_mean,
             hdd_sum, heat_index_mean, frost_days, solar_reduction_mean, updated_at)
        SELECT
            year, week,
            MIN(date), MAX(date),
            ROUND(AVG(temp_mean),1), ROUND(MIN(temp_min),1), ROUND(MAX(temp_max),1),
            ROUND(AVG(wind_mean),1), ROUND(MAX(wind_max),1),
            ROUND(SUM(precip_sum),1), ROUND(AVG(humidity_mean),1),
            ROUND(AVG(solar_mean),1), ROUND(SUM(hdd),1),
            ROUND(AVG(heat_index),1), SUM(frost_day),
            ROUND(AVG(solar_reduction),2),
            datetime('now')
        FROM daily
        GROUP BY year, week
    """)
    con.commit()
    log.info(f"  ✓ {updated} daily rows, weekly rollup done")
    return updated


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    dry_run     = "--dry-run" in sys.argv
    year_filter = None
    if "--year" in sys.argv:
        idx = sys.argv.index("--year")
        try:
            year_filter = int(sys.argv[idx + 1])
        except (IndexError, ValueError):
            log.error("Usage: --year YYYY  (e.g. --year 2024)")
            sys.exit(1)

    log.info("=" * 60)
    log.info("Varberg Energi — SMHI Historical Backfill")
    log.info(f"Station: {STATION_ID}  |  DB: {DB_PATH}")
    log.info(f"Year filter: {year_filter or 'all available'}  |  Dry run: {dry_run}")
    log.info("=" * 60)

    con = get_db()
    existing = get_existing_dates(con)
    log.info(f"Existing complete days in DB: {len(existing)}")
    if existing:
        dates_sorted = sorted(existing)
        log.info(f"  Range: {dates_sorted[0]} → {dates_sorted[-1]}")

    # ── Fetch each parameter ──────────────────────────────────────────────────
    raw: dict[str, dict] = {}  # ts_str -> {param: value}
    param_stats = {}

    for name, param_id in SMHI_PARAMS.items():
        log.info(f"\nFetching {name} (param {param_id})...")
        rows, period_used = fetch_archive_parameter(param_id, STATION_ID)

        if year_filter:
            rows = filter_by_year(rows, year_filter)

        param_stats[name] = {"rows": len(rows), "period": period_used}

        for row in rows:
            ts_str = row["ts"].strftime("%Y-%m-%dT%H:00:00Z")
            if ts_str not in raw:
                raw[ts_str] = {}
            raw[ts_str][name] = row["value"]

        # Be polite to SMHI API
        time.sleep(0.5)

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("\n── Fetch summary ──────────────────────────────────────────")
    for name, stats in param_stats.items():
        log.info(f"  {name:<12} {stats['rows']:>6} rows  [{stats['period']}]")

    if not raw:
        log.error("No data fetched. Check station ID and network connection.")
        sys.exit(1)

    # Date range of fetched data
    all_dates = set()
    for ts_str in raw:
        ts_dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=timezone.utc)
        all_dates.add((ts_dt + timedelta(hours=1)).strftime("%Y-%m-%d"))

    new_dates = all_dates - existing
    log.info(f"\nTotal hours fetched: {len(raw)}")
    log.info(f"Date range: {min(all_dates)} → {max(all_dates)}")
    log.info(f"Total days: {len(all_dates)}  |  New days: {len(new_dates)}  |  Already in DB: {len(all_dates - new_dates)}")

    # ── Insert ────────────────────────────────────────────────────────────────
    log.info("\n── Inserting into database ────────────────────────────────")
    inserted = insert_observations(con, raw, dry_run)
    log.info(f"  ✓ {inserted} observation rows inserted/updated")

    # ── Aggregate ─────────────────────────────────────────────────────────────
    log.info("\n── Computing daily & weekly aggregates ────────────────────")
    daily_rows = aggregate_all_daily(con, dry_run)

    # ── Export JSON ───────────────────────────────────────────────────────────
    if not dry_run:
        log.info("\n── Exporting weather_data.json ────────────────────────────")
        try:
            # Import and call the export function from collect_smhi
            import importlib.util, sys as _sys
            spec = importlib.util.spec_from_file_location(
                "collect_smhi", Path(__file__).parent / "collect_smhi.py")
            csm = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(csm)
            csm.export_json(con, EXPORT_PATH)
        except Exception as e:
            log.warning(f"  Could not export JSON: {e}")
            log.warning("  Run: python3 collect_smhi.py --export")

    con.close()

    log.info("\n" + "=" * 60)
    if dry_run:
        log.info("DRY RUN complete — no data was written.")
        log.info("Remove --dry-run to perform the actual backfill.")
    else:
        log.info("Backfill complete!")
        log.info(f"  Observation hours: {inserted}")
        log.info(f"  Daily aggregates:  {daily_rows}")
        log.info(f"  JSON export:       {EXPORT_PATH}")
        log.info("\nNext: run collect_smhi.py for ongoing hourly collection.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
