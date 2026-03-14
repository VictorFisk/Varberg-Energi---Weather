#!/usr/bin/env python3
"""
send_weekly_email.py — Varberg Energi Weekly Weather Email
===========================================================
Builds a rich HTML email with:
  - 7-day forecast table
  - Heat demand index trend (text sparkline)
  - Frost warnings
  - Comparison vs last week

Reads from varberg_weather.db and sends via Gmail SMTP.

Environment variables (set as GitHub Secrets):
  GMAIL_USER      your.address@gmail.com
  GMAIL_APP_PASS  16-char app password from Google Account settings
  RECIPIENT       recipient@example.com  (can be same as GMAIL_USER)
  PAGES_URL       https://VictorFisk.github.io/your-repo/ (optional)
  OVERRIDE_TO     override recipient for test runs (optional)
"""

import os
import sqlite3
import smtplib
import logging
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

DB_PATH = Path(__file__).parent / "varberg_weather.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("weekly_email")

# ── SMHI weather symbol → Swedish label ──────────────────────────────────────

SYMBOL_LABELS = {
    1:"Klart", 2:"Nästan klart", 3:"Halvklart", 4:"Halvklart",
    5:"Mulet", 6:"Mulet", 7:"Dimma",
    8:"Lätt regnskur", 9:"Regnskur", 10:"Kraftig regnskur", 11:"Åska",
    12:"Lätt snöblandad", 13:"Snöblandad", 14:"Kraftig snöblandad",
    15:"Lätt snöby", 16:"Snöby", 17:"Kraftig snöby",
    18:"Lätt regn", 19:"Regn", 20:"Kraftigt regn", 21:"Åska",
    22:"Lätt snöblandat", 23:"Snöblandat", 24:"Kraftigt snöblandat",
    25:"Lätt snöfall", 26:"Snöfall", 27:"Kraftigt snöfall",
}

def symbol_emoji(sym):
    if sym in (1, 2):      return "☀️"
    if sym in (3, 4):      return "🌤️"
    if sym in (5, 6, 7):   return "☁️"
    if sym in (8, 9, 10):  return "🌧️"
    if sym == 11:           return "⛈️"
    if sym in (12,13,14,22,23,24): return "🌨️"
    if sym in (15,16,17,25,26,27): return "❄️"
    if sym in (18,19,20):  return "🌧️"
    return "🌡️"

# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_temp(v):
    if v is None: return "—"
    n = float(v)
    return f"+{n:.1f}°" if n >= 0 else f"{n:.1f}°"

def fmt_num(v, decimals=1):
    return "—" if v is None else f"{float(v):.{decimals}f}"

def swedish_day(date_str):
    days = ["Mån","Tis","Ons","Tor","Fre","Lör","Sön"]
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return days[d.weekday()]

def short_date(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d")
    months = ["jan","feb","mar","apr","maj","jun",
              "jul","aug","sep","okt","nov","dec"]
    return f"{d.day} {months[d.month-1]}"

def temp_color(v):
    """Return inline color for a temperature value."""
    if v is None: return "#666"
    if float(v) <= -3: return "#C53030"
    if float(v) < 0:   return "#0057A8"
    if float(v) < 5:   return "#555"
    return "#C94A2B"

def idx_color(v):
    if v is None: return "#666"
    n = float(v)
    if n >= 80: return "#C53030"
    if n >= 60: return "#C94A2B"
    if n >= 40: return "#D97B10"
    return "#3A9E6E"

def sparkline(values, width=120, height=28):
    """Build a tiny inline SVG bar sparkline."""
    if not values:
        return ""
    max_v = max(values) or 1
    bar_w = width / len(values)
    bars = ""
    for i, v in enumerate(values):
        h = max(2, int((v / max_v) * height))
        x = i * bar_w + 1
        y = height - h
        color = idx_color(v)
        bars += f'<rect x="{x:.1f}" y="{y}" width="{bar_w-2:.1f}" height="{h}" rx="2" fill="{color}"/>'
    return f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">{bars}</svg>'

# ── Data fetching ─────────────────────────────────────────────────────────────

def get_data():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    today = date.today().isoformat()
    iso = date.today().isocalendar()

    # 7-day forecast (daily aggregates)
    cur.execute("""
        SELECT
            date,
            ROUND(AVG(temp_c),1)       as temp_mean,
            ROUND(MIN(temp_c),1)       as temp_min,
            ROUND(MAX(temp_c),1)       as temp_max,
            ROUND(AVG(wind_ms),1)      as wind_mean,
            ROUND(MAX(gust_ms),1)      as wind_max,
            ROUND(SUM(precip_mm),1)    as precip_sum,
            ROUND(AVG(humidity_pct),1) as humidity_mean,
            ROUND(AVG(solar_wm2),1)    as solar_mean,
            MIN(symbol)                as symbol
        FROM forecast
        WHERE fetched_at = (SELECT MAX(fetched_at) FROM forecast)
          AND date >= ?
          AND date <= date(?, '+7 days')
        GROUP BY date
        ORDER BY date ASC
        LIMIT 7
    """, (today, today))
    forecast = [dict(r) for r in cur.fetchall()]

    # This week's daily data (from observations, for heat index trend)
    cur.execute("""
        SELECT date, heat_index, temp_mean, temp_min, frost_day, solar_reduction
        FROM daily
        WHERE date >= date(?, '-7 days')
        ORDER BY date ASC
    """, (today,))
    recent_daily = [dict(r) for r in cur.fetchall()]

    # This week aggregate
    cur.execute("""
        SELECT * FROM weekly WHERE year=? AND week=?
    """, (iso.year, iso.week))
    this_week = dict(cur.fetchone() or {})

    # Previous week aggregate
    prev_date = (date.today() - timedelta(weeks=1))
    prev_iso = prev_date.isocalendar()
    cur.execute("""
        SELECT * FROM weekly WHERE year=? AND week=?
    """, (prev_iso.year, prev_iso.week))
    prev_week = dict(cur.fetchone() or {})

    # Latest observation
    cur.execute("""
        SELECT * FROM observations WHERE temp_c IS NOT NULL
        ORDER BY ts DESC LIMIT 1
    """)
    latest = dict(cur.fetchone() or {})

    con.close()
    return forecast, recent_daily, this_week, prev_week, latest, iso


# ── Email HTML builder ────────────────────────────────────────────────────────

def build_html(forecast, recent_daily, this_week, prev_week, latest, iso, pages_url):

    pages_link = f'<a href="{pages_url}" style="color:#007A6E">Öppna fullständig rapport →</a>' if pages_url else ""

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    week_num = iso.week
    year = iso.year

    # ── Frost warnings ────────────────────────────────────────────────────────
    frost_days = [d for d in forecast if d.get("temp_min") is not None and float(d["temp_min"]) < 0]
    severe_frost = [d for d in frost_days if float(d["temp_min"]) < -4]

    frost_banner = ""
    if severe_frost:
        days_str = ", ".join(f"{swedish_day(d['date'])} {short_date(d['date'])} ({fmt_temp(d['temp_min'])})" for d in severe_frost)
        frost_banner = f"""
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#FFF0F0;border-left:4px solid #C53030;border-radius:6px;margin-bottom:20px;">
            <tr><td style="padding:14px 18px;">
              <div style="font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#C53030;margin-bottom:4px;">❄️ STARK FROSTVARNING</div>
              <div style="font-size:14px;color:#1A2025;">Kraftig frost förväntas: <strong>{days_str}</strong>. Justera framledningstemperaturen i förväg.</div>
            </td></tr>
          </table>
        </td></tr>"""
    elif frost_days:
        days_str = ", ".join(f"{swedish_day(d['date'])} ({fmt_temp(d['temp_min'])})" for d in frost_days)
        frost_banner = f"""
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#EEF4FF;border-left:4px solid #0057A8;border-radius:6px;margin-bottom:20px;">
            <tr><td style="padding:14px 18px;">
              <div style="font-size:11px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#0057A8;margin-bottom:4px;">❄ FROSTVARNING</div>
              <div style="font-size:14px;color:#1A2025;">Frostnätter förväntas: <strong>{days_str}</strong>.</div>
            </td></tr>
          </table>
        </td></tr>"""

    # ── vs last week comparison ───────────────────────────────────────────────
    def diff_row(label, current, previous, unit="", higher_is_bad=True):
        if not current or not previous:
            return f"<tr><td style='padding:6px 0;color:#666;border-bottom:1px solid #EEE;'>{label}</td><td style='padding:6px 0;text-align:right;border-bottom:1px solid #EEE;'>—</td><td style='padding:6px 0;text-align:right;border-bottom:1px solid #EEE;'></td></tr>"
        diff = float(current) - float(previous)
        if abs(diff) < 0.05:
            arrow, color = "≈", "#888"
        elif diff > 0:
            arrow = f"▲ +{diff:.1f}{unit}"
            color = "#C53030" if higher_is_bad else "#3A9E6E"
        else:
            arrow = f"▼ {diff:.1f}{unit}"
            color = "#3A9E6E" if higher_is_bad else "#C53030"
        return f"""<tr>
          <td style="padding:7px 0;color:#444;border-bottom:1px solid #F0F0F0;font-size:13px;">{label}</td>
          <td style="padding:7px 0;text-align:right;border-bottom:1px solid #F0F0F0;font-weight:600;font-size:13px;">{fmt_num(current)}{unit}</td>
          <td style="padding:7px 0;text-align:right;border-bottom:1px solid #F0F0F0;color:{color};font-weight:600;font-size:13px;">{arrow}</td>
        </tr>"""

    comparison_rows = (
        diff_row("Medeltemperatur", this_week.get("temp_mean"), prev_week.get("temp_mean"), "°", higher_is_bad=False) +
        diff_row("Lägsta temperatur", this_week.get("temp_min"), prev_week.get("temp_min"), "°", higher_is_bad=False) +
        diff_row("Frostdygn", this_week.get("frost_days"), prev_week.get("frost_days"), " st", higher_is_bad=True) +
        diff_row("Total nederbörd", this_week.get("precip_sum"), prev_week.get("precip_sum"), " mm", higher_is_bad=True) +
        diff_row("Värmebehov-index", this_week.get("heat_index_mean"), prev_week.get("heat_index_mean"), "", higher_is_bad=True) +
        diff_row("HDD-summa", this_week.get("hdd_sum"), prev_week.get("hdd_sum"), "", higher_is_bad=True)
    )

    # ── Heat index sparkline ──────────────────────────────────────────────────
    hi_values = [d.get("heat_index") or 0 for d in recent_daily]
    spark_svg = sparkline(hi_values, width=180, height=36)
    hi_dates  = " · ".join(swedish_day(d["date"])[:2] for d in recent_daily)

    # ── 7-day forecast table rows ─────────────────────────────────────────────
    forecast_rows = ""
    for i, day in enumerate(forecast):
        sym = day.get("symbol") or 1
        bg = "#F0F9F7" if i % 2 == 0 else "#FFFFFF"
        frost_style = f"color:{temp_color(day.get('temp_min'))};font-weight:700;" if day.get("temp_min") and float(day["temp_min"]) < 0 else ""

        # Compute heat index for forecast day
        temp_mean = float(day.get("temp_mean") or 0)
        wind_mean = float(day.get("wind_mean") or 0)
        solar_mean = float(day.get("solar_mean") or 0)
        hdd = max(0, 17 - temp_mean)
        wind_add = min(12, wind_mean * 1.2)
        sol_red = min(12, solar_mean * 0.025)
        hi = round(min(100, max(0, hdd * 4.8 + wind_add - sol_red)))
        hi_c = idx_color(hi)

        # Mini index bar (50px wide)
        bar_w = round(hi * 50 / 100)
        mini_bar = f'<table cellpadding="0" cellspacing="0"><tr><td style="background:#E8E8E8;border-radius:3px;width:50px;height:7px;"><div style="background:{hi_c};width:{bar_w}px;height:7px;border-radius:3px;"></div></td><td style="padding-left:5px;font-size:11px;color:{hi_c};font-weight:700;">{hi}</td></tr></table>'

        forecast_rows += f"""
        <tr style="background:{bg};">
          <td style="padding:9px 12px;font-weight:700;white-space:nowrap;">{symbol_emoji(sym)} {swedish_day(day['date'])}<br><span style="font-weight:400;font-size:11px;color:#888;">{short_date(day['date'])}</span></td>
          <td style="padding:9px 12px;text-align:center;">
            <span style="color:{temp_color(day.get('temp_max'))};font-weight:700;">{fmt_temp(day.get('temp_max'))}</span><br>
            <span style="{frost_style}font-size:12px;">{fmt_temp(day.get('temp_min'))}</span>
          </td>
          <td style="padding:9px 12px;text-align:center;font-size:13px;">{fmt_num(day.get('wind_mean'),0)}/{fmt_num(day.get('wind_max'),0)}<br><span style="font-size:11px;color:#888;">m/s</span></td>
          <td style="padding:9px 12px;text-align:center;font-size:13px;color:#D97B10;">{fmt_num(day.get('solar_mean'),0)}<br><span style="font-size:11px;color:#888;">W/m²</span></td>
          <td style="padding:9px 12px;">{mini_bar}</td>
        </tr>"""

    # ── Assemble full HTML ────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="sv">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Väderleksrapport V.{week_num} · Varberg Energi</title>
</head>
<body style="margin:0;padding:0;background:#F0F4F3;font-family:'Segoe UI',Arial,sans-serif;color:#1A2025;">

<table width="100%" cellpadding="0" cellspacing="0" style="background:#F0F4F3;padding:24px 12px;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

  <!-- HEADER -->
  <tr><td style="background:#005F56;border-radius:10px 10px 0 0;padding:22px 28px;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td>
          <div style="font-size:11px;color:rgba(255,255,255,0.7);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:4px;">Väderleksrapport · Fjärrvärmedrift</div>
          <div style="font-size:22px;font-weight:700;color:#FFFFFF;line-height:1.2;">Varberg — Vecka {week_num}, {year}</div>
          <div style="font-size:12px;color:rgba(255,255,255,0.65);margin-top:4px;">Genererad {now_str}</div>
        </td>
        <td align="right" style="vertical-align:top;">
          <div style="background:rgba(255,255,255,0.15);border-radius:8px;padding:8px 14px;text-align:center;">
            <div style="font-size:10px;color:rgba(255,255,255,0.7);text-transform:uppercase;letter-spacing:0.08em;">Nu</div>
            <div style="font-size:24px;font-weight:700;color:#FFFFFF;line-height:1.1;">{fmt_temp(latest.get('temp_c'))}</div>
            <div style="font-size:11px;color:rgba(255,255,255,0.65);">{fmt_num(latest.get('wind_ms'),0)} m/s · {fmt_num(latest.get('humidity_pct'),0)}%</div>
          </div>
        </td>
      </tr>
    </table>
  </td></tr>

  <!-- BODY -->
  <tr><td style="background:#FFFFFF;padding:24px 28px;border-radius:0 0 10px 10px;">
  <table width="100%" cellpadding="0" cellspacing="0">

    <!-- FROST WARNINGS -->
    {frost_banner}

    <!-- FORECAST TABLE -->
    <tr><td>
      <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#007A6E;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #E6F4F2;">📅 7-DYGNSPROGNOS</div>
      <table width="100%" cellpadding="0" cellspacing="0" style="border-radius:8px;overflow:hidden;border:1px solid #E0ECEB;">
        <thead>
          <tr style="background:#E6F4F2;">
            <th style="padding:8px 12px;text-align:left;font-size:10px;color:#005F56;text-transform:uppercase;letter-spacing:0.07em;font-weight:700;">Dag</th>
            <th style="padding:8px 12px;text-align:center;font-size:10px;color:#005F56;text-transform:uppercase;letter-spacing:0.07em;font-weight:700;">Temp</th>
            <th style="padding:8px 12px;text-align:center;font-size:10px;color:#005F56;text-transform:uppercase;letter-spacing:0.07em;font-weight:700;">Vind</th>
            <th style="padding:8px 12px;text-align:center;font-size:10px;color:#005F56;text-transform:uppercase;letter-spacing:0.07em;font-weight:700;">Sol</th>
            <th style="padding:8px 12px;font-size:10px;color:#005F56;text-transform:uppercase;letter-spacing:0.07em;font-weight:700;">Värmeidx</th>
          </tr>
        </thead>
        <tbody>
          {forecast_rows}
        </tbody>
      </table>
      <div style="font-size:11px;color:#888;margin-top:6px;">Värmeidx = HDD-komponent + vindtillägg − solreduktion (0–100)</div>
    </td></tr>

    <tr><td style="height:24px;"></td></tr>

    <!-- HEAT INDEX TREND -->
    <tr><td>
      <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#007A6E;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #E6F4F2;">📊 VÄRMEBEHOV-INDEX · SENASTE 7 DAGARNA</div>
      <table width="100%" cellpadding="0" cellspacing="0" style="background:#F8FAFA;border-radius:8px;padding:16px;border:1px solid #E0ECEB;">
        <tr>
          <td style="padding:16px;">
            {spark_svg}
            <div style="font-size:10px;color:#888;margin-top:4px;letter-spacing:0.05em;">{hi_dates}</div>
          </td>
          <td style="padding:16px;vertical-align:top;min-width:120px;">
            <div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:4px;">Veckosnitt</div>
            <div style="font-size:28px;font-weight:700;color:{idx_color(this_week.get('heat_index_mean'))};line-height:1;">{fmt_num(this_week.get('heat_index_mean'),0)}</div>
            <div style="font-size:11px;color:#888;margin-top:2px;">av 100</div>
            <div style="font-size:11px;color:#888;margin-top:8px;">Frostdygn: <strong style="color:#0057A8;">{this_week.get('frost_days') or 0}</strong></div>
            <div style="font-size:11px;color:#888;">HDD-summa: <strong>{fmt_num(this_week.get('hdd_sum'),0)}</strong></div>
          </td>
        </tr>
      </table>
    </td></tr>

    <tr><td style="height:24px;"></td></tr>

    <!-- COMPARISON VS LAST WEEK -->
    <tr><td>
      <div style="font-size:11px;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#007A6E;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid #E6F4F2;">📈 JÄMFÖRELSE MED FÖRRA VECKAN (V.{(week_num-1) if week_num > 1 else 52})</div>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <th style="text-align:left;font-size:10px;color:#888;text-transform:uppercase;letter-spacing:0.07em;padding-bottom:6px;"></th>
          <th style="text-align:right;font-size:10px;color:#888;text-transform:uppercase;letter-spacing:0.07em;padding-bottom:6px;">Denna vecka</th>
          <th style="text-align:right;font-size:10px;color:#888;text-transform:uppercase;letter-spacing:0.07em;padding-bottom:6px;">Förändring</th>
        </tr>
        {comparison_rows}
      </table>
    </td></tr>

    <tr><td style="height:28px;"></td></tr>

    <!-- FOOTER LINK -->
    {"<tr><td style='text-align:center;padding:16px;background:#F0F9F7;border-radius:8px;'>" + pages_link + "</td></tr>" if pages_url else ""}

  </table>
  </td></tr>

  <!-- EMAIL FOOTER -->
  <tr><td style="padding:16px 28px;text-align:center;">
    <div style="font-size:11px;color:#888;">Varberg Energi Fjärrvärme · Automatisk rapport · Data från SMHI Open Data</div>
    <div style="font-size:11px;color:#AAA;margin-top:2px;">Du får detta mail för att du prenumererar på veckorapporten.</div>
  </td></tr>

</table>
</td></tr>
</table>

</body>
</html>"""
    return html


# ── Send email ────────────────────────────────────────────────────────────────

def send_email(html: str, subject: str):
    gmail_user  = os.environ.get("GMAIL_USER", "")
    app_pass    = os.environ.get("GMAIL_APP_PASS", "")
    pages_url   = os.environ.get("PAGES_URL", "")

    # Support comma-separated recipients in the RECIPIENT_EMAIL secret
    raw = os.environ.get("OVERRIDE_TO") or os.environ.get("RECIPIENT", gmail_user)
    recipients = [r.strip() for r in raw.split(",") if r.strip()]

    if not gmail_user or not app_pass:
        raise ValueError("GMAIL_USER and GMAIL_APP_PASS must be set as GitHub Secrets.")
    if not recipients:
        raise ValueError("No recipients configured.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Varberg Energi Väder <{gmail_user}>"
    msg["To"]      = ", ".join(recipients)  # shows all recipients in email header

    # Plain-text fallback
    plain = f"Varberg Energi Väderleksrapport\n{subject}\n\nÖppna HTML-versionen för fullständig rapport.\n{pages_url}"
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html,  "html",  "utf-8"))

    log.info(f"Sending to {recipients} via {gmail_user}...")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, app_pass)
        server.sendmail(gmail_user, recipients, msg.as_bytes())  # list delivers to all
    log.info(f"✓ Email sent successfully to {len(recipients)} recipient(s)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("Building weekly weather email...")

    try:
        forecast, recent_daily, this_week, prev_week, latest, iso = get_data()
    except FileNotFoundError as e:
        log.error(str(e))
        log.error("Run collect_smhi.py first.")
        raise

    pages_url = os.environ.get("PAGES_URL", "")
    html = build_html(forecast, recent_daily, this_week, prev_week, latest, iso, pages_url)

    week_num = iso.week
    year = iso.year
    subject = f"☀️ Varberg Fjärrvärme · Veckoprognos V.{week_num} {year}"

    send_email(html, subject)


if __name__ == "__main__":
    main()
