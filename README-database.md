# Varberg Energi — SMHI Weather Database

Automatic weather data collection from SMHI Open Data API into a local SQLite database, with JSON export for the HTML weather report.

---

## Files

| File | Purpose |
|------|---------|
| `collect_smhi.py` | Main collector — fetches from SMHI, stores to DB, exports JSON |
| `query_db.py` | CLI tool for inspecting the database |
| `setup.py` | One-time setup wizard |
| `varberg_weather.db` | SQLite database (created on first run) |
| `weather_data.json` | JSON export read by the HTML report |
| `collect.log` | Collector run log (created by cron) |

---

## Quick Start

```bash
# 1. Run setup (installs deps, inits DB, first collection, sets up cron)
python3 setup.py

# 2. Check collected data
python3 query_db.py summary

# 3. View weekly history
python3 query_db.py weekly
```

---

## Manual Usage

```bash
# Collect latest data + export JSON
python3 collect_smhi.py

# Export JSON only (no new API calls)
python3 collect_smhi.py --export

# Backfill (re-fetches more historical data)
python3 collect_smhi.py --backfill
```

---

## Database Schema

### `observations` — hourly raw data from SMHI station
| Column | Type | Description |
|--------|------|-------------|
| `ts` | TEXT | ISO-8601 UTC timestamp |
| `date` | TEXT | YYYY-MM-DD (Swedish local time) |
| `hour` | INT | 0–23 |
| `temp_c` | REAL | Air temperature (°C) |
| `wind_ms` | REAL | Wind speed (m/s) |
| `gust_ms` | REAL | Wind gust (m/s) |
| `precip_mm` | REAL | Precipitation (mm) |
| `humidity_pct` | REAL | Relative humidity (%) |
| `pressure_hpa` | REAL | Air pressure (hPa) |
| `solar_wm2` | REAL | Global solar irradiance (W/m²) |

### `daily` — computed daily aggregates
| Column | Type | Description |
|--------|------|-------------|
| `date` | TEXT | YYYY-MM-DD |
| `week` | INT | ISO week number |
| `temp_mean/min/max` | REAL | Temperature range |
| `wind_mean/max` | REAL | Wind statistics |
| `precip_sum` | REAL | Total precipitation |
| `solar_mean/max` | REAL | Solar irradiance |
| `hdd` | REAL | Heating Degree Days (ref 17°C) |
| `wind_addition` | REAL | Wind contribution to heat index |
| `solar_reduction` | REAL | Solar reduction from heat index |
| `heat_index` | REAL | Composite heat demand index (0–100) |
| `frost_day` | INT | 1 if min temp < 0°C |

### `weekly` — weekly aggregates
Aggregated from `daily` — temperature, wind, precipitation, solar, HDD sum, heat index mean, frost day count.

### `forecast` — SMHI PMP3g 10-day forecast
Hourly forecast values. Solar irradiance is estimated from the SMHI weather symbol + time of day + season (SMHI doesn't provide solar in forecasts directly).

### `run_log` — collector audit trail
Timestamp, mode, rows collected, any errors.

---

## Heat Index Formula

```
wind_addition   = min(12, wind_mean_ms × 1.2)
solar_reduction = min(12, solar_mean_wm2 × 0.025)
hdd             = max(0, 17 - temp_mean)
temp_component  = min(88, hdd × 4.8)

heat_index = temp_component + wind_addition - solar_reduction
```

Scale: 0 = no heating needed, 100 = maximum seasonal load (~−15°C, no sun, strong wind).

---

## SMHI Station

**Getterön (ID 72450)** — the primary synoptic weather station near Varberg.

- Coordinates: 57.07°N, 12.17°E
- Parameters collected: temperature (param 1), wind (4), gusts (21), precipitation (5), humidity (6), pressure (9), global solar radiation (118)

To change station: edit `STATION_ID` in `collect_smhi.py`.  
Find stations: https://opendata.smhi.se/apidocs/metobs/index.html

---

## Cron Schedule

The collector is designed to run **every hour**:

```cron
# Varberg Energi SMHI collector — every hour
0 * * * * /usr/bin/python3 /path/to/collect_smhi.py >> /path/to/collect.log 2>&1
```

For a **daily report generation** (e.g. at 06:00):
```cron
0 6 * * * /usr/bin/python3 /path/to/collect_smhi.py >> /path/to/collect.log 2>&1
```

---

## JSON Export Format

`weather_data.json` is written after every successful collection. The HTML report reads this file.

```json
{
  "generated_at": "2026-03-06T14:00:00+00:00",
  "station_id": 72450,
  "lat": 57.106,
  "lon": 12.254,
  "latest_observation": { "ts": "...", "temp_c": 2.1, ... },
  "today": { "date": "2026-03-06", "heat_index": 55.0, ... },
  "forecast_daily": [ { "date": "2026-03-06", "temp_mean": 1.2, ... }, ... ],
  "forecast_hourly": [ { "valid_time": "...", "temp_c": 1.5, ... }, ... ],
  "history_weekly": [ { "year": 2026, "week": 10, ... }, ... ],
  "history_daily": [ { "date": "2026-02-05", ... }, ... ]
}
```

---

## Querying the Database

```bash
python3 query_db.py summary           # Latest obs + today + this week
python3 query_db.py weekly            # Last 12 weeks table
python3 query_db.py daily 30          # Last 30 days table
python3 query_db.py hourly 2026-03-06 # Hourly data for a specific date
python3 query_db.py forecast          # Next 72h forecast
python3 query_db.py runs              # Collector run history
python3 query_db.py check             # Data quality check + gap detection
```

Or query directly:
```bash
sqlite3 varberg_weather.db "SELECT date, temp_mean, heat_index FROM daily ORDER BY date DESC LIMIT 10"
```

---

## SMHI Open Data Links

- Observations API: https://opendata.smhi.se/apidocs/metobs/
- Forecast API (PMP3g): https://opendata.smhi.se/apidocs/metfcst/
- Parameter list: https://opendata.smhi.se/apidocs/metobs/parameters.html
- Station search: https://opendata.smhi.se/apidocs/metobs/stations.html

No API key required — SMHI Open Data is freely available.
