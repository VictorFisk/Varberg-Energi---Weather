#!/usr/bin/env python3
"""
query_db.py — Varberg Energi Weather Database Query Tool
=========================================================
Handy queries for inspecting the collected data.

Usage:
    python3 query_db.py summary          # latest conditions + this week
    python3 query_db.py weekly           # last 8 weeks
    python3 query_db.py daily [N]        # last N days (default 14)
    python3 query_db.py hourly [date]    # hourly obs for date (default today)
    python3 query_db.py forecast         # upcoming forecast
    python3 query_db.py runs             # recent collector runs
    python3 query_db.py check            # data quality check
"""

import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "varberg_weather.db"

def con():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def fmt(v, unit="", decimals=1):
    if v is None: return "—"
    if isinstance(v, float): return f"{v:.{decimals}f}{unit}"
    return f"{v}{unit}"

def hr():
    print("─" * 70)

def cmd_summary():
    with con() as db:
        cur = db.cursor()
        # Latest obs
        cur.execute("SELECT * FROM observations WHERE temp_c IS NOT NULL ORDER BY ts DESC LIMIT 1")
        obs = cur.fetchone()
        # Today daily
        cur.execute("SELECT * FROM daily WHERE date = ?", (date.today().isoformat(),))
        day = cur.fetchone()
        # This week
        iso = date.today().isocalendar()
        cur.execute("SELECT * FROM weekly WHERE year=? AND week=?", (iso.year, iso.week))
        wk = cur.fetchone()

    print("\n=== LATEST OBSERVATION ===")
    if obs:
        print(f"  Time:        {obs['ts']}")
        print(f"  Temperature: {fmt(obs['temp_c'], '°C')}")
        print(f"  Wind:        {fmt(obs['wind_ms'], ' m/s')}  gusts {fmt(obs['gust_ms'], ' m/s')}")
        print(f"  Precipitation: {fmt(obs['precip_mm'], ' mm')}")
        print(f"  Humidity:    {fmt(obs['humidity_pct'], '%', 0)}")
        print(f"  Solar:       {fmt(obs['solar_wm2'], ' W/m²', 0)}")

    print("\n=== TODAY ===")
    if day:
        print(f"  Date:        {day['date']}")
        print(f"  Temp mean:   {fmt(day['temp_mean'], '°C')}  [{fmt(day['temp_min'], '°C')} – {fmt(day['temp_max'], '°C')}]")
        print(f"  Wind:        {fmt(day['wind_mean'], ' m/s')}  max {fmt(day['wind_max'], ' m/s')}")
        print(f"  Solar:       {fmt(day['solar_mean'], ' W/m²', 0)}")
        print(f"  HDD:         {fmt(day['hdd'])}")
        print(f"  Heat index:  {fmt(day['heat_index'], ' idx')}")
        print(f"  Solar reduc: {fmt(day['solar_reduction'], ' idx')}")

    print("\n=== THIS WEEK ===")
    if wk:
        print(f"  Week v.{wk['week']} {wk['date_from']} – {wk['date_to']}")
        print(f"  Temp mean:   {fmt(wk['temp_mean'], '°C')}  [{fmt(wk['temp_min'], '°C')} – {fmt(wk['temp_max'], '°C')}]")
        print(f"  Heat index:  {fmt(wk['heat_index_mean'], ' idx')}")
        print(f"  Solar mean:  {fmt(wk['solar_mean'], ' W/m²', 0)}")
        print(f"  HDD sum:     {fmt(wk['hdd_sum'])}")
        print(f"  Frost days:  {wk['frost_days']}")
    print()

def cmd_weekly():
    with con() as db:
        cur = db.cursor()
        cur.execute("""
            SELECT * FROM weekly ORDER BY year DESC, week DESC LIMIT 12
        """)
        rows = cur.fetchall()

    hr()
    print(f"{'Week':<8} {'Period':<22} {'Temp':>6} {'Min':>5} {'Max':>5} "
          f"{'Wind':>6} {'Prec':>6} {'Solar':>7} {'HDD':>6} {'Idx':>5} {'Frost':>5}")
    hr()
    for r in rows:
        print(f"v.{r['week']:<6} {r['date_from']} – {r['date_to'][-5:]}  "
              f"{fmt(r['temp_mean'], '°'):>6} {fmt(r['temp_min'], '°'):>5} {fmt(r['temp_max'], '°'):>5} "
              f"{fmt(r['wind_mean'], ''):>5}ms {fmt(r['precip_sum'], 'mm'):>6} "
              f"{fmt(r['solar_mean'], '', 0):>5}W/m² "
              f"{fmt(r['hdd_sum'], ''):>6} {fmt(r['heat_index_mean'], ''):>5} "
              f"{r['frost_days'] or 0:>4}d")
    hr()

def cmd_daily(n=14):
    with con() as db:
        cur = db.cursor()
        cur.execute("""
            SELECT * FROM daily ORDER BY date DESC LIMIT ?
        """, (int(n),))
        rows = cur.fetchall()

    hr()
    print(f"{'Date':<12} {'Temp':>6} {'Min':>5} {'Max':>5} {'Wind':>6} "
          f"{'Prec':>6} {'Solar':>7} {'HDD':>5} {'Idx':>5} Fr")
    hr()
    for r in rows:
        fr = "❄" if r['frost_day'] else " "
        print(f"{r['date']:<12} {fmt(r['temp_mean'], '°'):>6} {fmt(r['temp_min'], '°'):>5} "
              f"{fmt(r['temp_max'], '°'):>5} {fmt(r['wind_mean'], ''):>5}ms "
              f"{fmt(r['precip_sum'], 'mm'):>6} {fmt(r['solar_mean'], '', 0):>5}W/m² "
              f"{fmt(r['hdd'], ''):>5} {fmt(r['heat_index'], ''):>5} {fr}")
    hr()

def cmd_hourly(day=None):
    day = day or date.today().isoformat()
    with con() as db:
        cur = db.cursor()
        cur.execute("""
            SELECT * FROM observations WHERE date = ? ORDER BY hour ASC
        """, (day,))
        rows = cur.fetchall()

    print(f"\nHourly observations for {day}:")
    hr()
    print(f"{'Hour':>5} {'Temp':>6} {'Wind':>6} {'Gust':>6} {'Prec':>6} {'Hum':>5} {'Solar':>7}")
    hr()
    for r in rows:
        print(f"{r['hour']:>5}  {fmt(r['temp_c'], '°'):>6} {fmt(r['wind_ms'], ''):>5}ms "
              f"{fmt(r['gust_ms'], ''):>5}ms {fmt(r['precip_mm'], 'mm'):>6} "
              f"{fmt(r['humidity_pct'], '%', 0):>5} {fmt(r['solar_wm2'], '', 0):>5}W/m²")
    hr()

def cmd_forecast():
    with con() as db:
        cur = db.cursor()
        cur.execute("""
            SELECT date, hour, temp_c, wind_ms, gust_ms, precip_mm, humidity_pct, solar_wm2, symbol
            FROM forecast
            WHERE fetched_at = (SELECT MAX(fetched_at) FROM forecast)
              AND valid_time >= datetime('now')
            ORDER BY valid_time ASC
            LIMIT 72
        """)
        rows = cur.fetchall()

    print(f"\nForecast (next 72h):")
    hr()
    print(f"{'Date':<12} {'Hr':>3} {'Temp':>6} {'Wind':>6} {'Prec':>6} {'Hum':>5} {'Solar':>7} Sym")
    hr()
    for r in rows:
        print(f"{r['date']:<12} {r['hour']:>3}  {fmt(r['temp_c'], '°'):>6} {fmt(r['wind_ms'], ''):>5}ms "
              f"{fmt(r['precip_mm'], 'mm'):>6} {fmt(r['humidity_pct'], '%', 0):>5} "
              f"{fmt(r['solar_wm2'], '', 0):>5}W/m² {r['symbol']:>3}")
    hr()

def cmd_runs():
    with con() as db:
        cur = db.cursor()
        cur.execute("SELECT * FROM run_log ORDER BY id DESC LIMIT 20")
        rows = cur.fetchall()

    print(f"\nCollector run log (last 20):")
    hr()
    for r in rows:
        err = f" ⚠ {r['errors']}" if r['errors'] else ""
        print(f"  {r['run_at'][:19]}  [{r['mode']:<12}]  "
              f"obs={r['obs_rows']:>4}  fcst={r['fcst_rows']:>4}  daily={r['daily_rows']:>3}{err}")
    hr()

def cmd_check():
    with con() as db:
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) as n, MIN(date) as first, MAX(date) as last FROM observations")
        r = cur.fetchone()
        print(f"\nObservations: {r['n']} hours  ({r['first']} → {r['last']})")

        cur.execute("SELECT COUNT(*) as n, MIN(date) as first, MAX(date) as last FROM daily")
        r = cur.fetchone()
        print(f"Daily rows:   {r['n']} days  ({r['first']} → {r['last']})")

        cur.execute("SELECT COUNT(*) as n FROM weekly")
        r = cur.fetchone()
        print(f"Weekly rows:  {r['n']} weeks")

        cur.execute("SELECT COUNT(*) as n, MAX(fetched_at) as last FROM forecast")
        r = cur.fetchone()
        print(f"Forecast hrs: {r['n']}  (last fetch: {r['last']})")

        # Check for gaps in daily data
        cur.execute("""
            SELECT date FROM daily WHERE date >= date('now', '-30 days')
            ORDER BY date
        """)
        have = {r['date'] for r in cur.fetchall()}
        today = date.today()
        missing = []
        for i in range(30):
            d = (today - __import__('datetime').timedelta(days=i)).isoformat()
            if d not in have:
                missing.append(d)
        if missing:
            print(f"\n⚠ Missing daily data for: {', '.join(missing[:10])}")
        else:
            print("✓ No gaps in last 30 days of daily data")
    print()

if __name__ == "__main__":
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("Run: python3 collect_smhi.py")
        sys.exit(1)

    cmd = sys.argv[1] if len(sys.argv) > 1 else "summary"
    arg = sys.argv[2] if len(sys.argv) > 2 else None

    dispatch = {
        "summary":  cmd_summary,
        "weekly":   cmd_weekly,
        "daily":    lambda: cmd_daily(arg or 14),
        "hourly":   lambda: cmd_hourly(arg),
        "forecast": cmd_forecast,
        "runs":     cmd_runs,
        "check":    cmd_check,
    }
    fn = dispatch.get(cmd)
    if fn:
        fn()
    else:
        print(__doc__)
