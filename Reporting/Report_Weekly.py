# =============================================================================
#  Synovia Fusion – EPOS  |  Weekly Report
# -----------------------------------------------------------------------------
#  Module:        Fusion EPOS
#  Script Name:   Report_Weekly.py
#
#  Version:       1.1.0
#  Release Date:  2026-03-30
#
#  Author:        Synovia Digital
#
# -----------------------------------------------------------------------------
#  Description:
#  ------------
#  Sends the weekly summary email covering the Mon–Sun week just completed.
#  Run every Sunday at 08:00 via Windows Task Scheduler — no arguments needed.
#
#    python Report_Weekly.py
#
#  Sections:
#   • KPI tiles  — 8 tiles across two rows
#   • Daily breakdown — files, orders, lines, value per day
#   • Top 10 customers by lines
#   • STO files received this week (Duracell_EPOS / STO_Received)
#   • Performance by Sales Rep
#   • Breakdown by Product Category
#   • Breakdown by Order Type
#   • Error files (any status not in the known-valid list)
#
# -----------------------------------------------------------------------------
#  Task Scheduler:
#   Action  : python "D:\Applications\Fusion_EPOS\...\Report_Weekly.py"
#   Trigger : Weekly, Sunday at 08:00
# =============================================================================

import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyodbc

sys.path.insert(0, str(Path(__file__).parent))
from reporting_shared import (
    CONFIG_FILE,
    load_config, get_db_connection, logo_base64,
    tile_row, html_wrapper, send_email, alert_ok,
    pct_bar, fmt_val,
    fetch_rep_cat_ot, build_rep_section, build_category_section,
    build_ordertype_section, build_error_section, SQL_ERRORS,
    utc_now, dublin_now, utc_to_dublin_full,
)

LOG_FILE = Path(r"D:\FusionHub\Logs\Fusion_EPOS_EmailReporter.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
log = logging.getLogger(__name__)


# =============================================================================
#  SQL
# =============================================================================

SQL_SUMMARY = """
SELECT
    COUNT(DISTINCT Source_Filename)                                           AS Files_Total,
    COUNT(*)                                                                  AS Rows_Total,
    SUM(CASE WHEN Order_Classification = 'Standard_Order' THEN 1 ELSE 0 END) AS Rows_Standard,
    SUM(CASE WHEN Order_Classification = 'Duracell_EPOS'  THEN 1 ELSE 0 END) AS Rows_Duracell,
    SUM(CASE WHEN Fusion_Status = 'Loaded_to_Dynamics'    THEN 1 ELSE 0 END) AS Rows_Loaded,
    SUM(CASE WHEN Fusion_Status = 'Processed_by_D365'     THEN 1 ELSE 0 END) AS Rows_D365,
    SUM(CASE WHEN Fusion_Status = 'STO_Received'          THEN 1 ELSE 0 END) AS Rows_STO,
    COUNT(DISTINCT CustomerCode)                                              AS Unique_Customers,
    COUNT(DISTINCT OrderId)                                                   AS Unique_Orders
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
"""

SQL_DAILY = """
SELECT
    CAST(Load_Timestamp AS DATE)            AS Day,
    COUNT(DISTINCT Source_Filename)         AS Files,
    COUNT(DISTINCT OrderId)                 AS Orders,
    COUNT(*)                                AS Lines,
    SUM(CASE WHEN Order_Classification = 'Standard_Order' THEN 1 ELSE 0 END) AS Standard_Lines,
    SUM(CASE WHEN Order_Classification = 'Duracell_EPOS'  THEN 1 ELSE 0 END) AS Duracell_Lines,
    SUM(Price * Quantity)                   AS Day_Value
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
GROUP BY CAST(Load_Timestamp AS DATE)
ORDER BY 1
"""

SQL_TOP_CUSTOMERS = """
SELECT TOP 10
    CustomerCode,
    MAX(CustomerName)       AS CustomerName,
    COUNT(DISTINCT OrderId) AS Orders,
    COUNT(*)                AS Lines,
    SUM(Price * Quantity)   AS Order_Value
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
GROUP BY CustomerCode
ORDER BY COUNT(*) DESC
"""

SQL_STO = """
SELECT
    Source_Filename,
    MAX(CustomerName)               AS CustomerName,
    COUNT(DISTINCT OrderId)         AS Orders,
    COUNT(*)                        AS Lines,
    MIN(Load_Timestamp)             AS Received
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
  AND Fusion_Status        = 'STO_Received'
  AND Order_Classification = 'Duracell_EPOS'
GROUP BY Source_Filename
ORDER BY MIN(Load_Timestamp)
"""


# =============================================================================
#  REPORT
# =============================================================================

def run(conn: pyodbc.Connection, cfg) -> None:
    now_utc        = utc_now()
    dublin_dt, tz_label = dublin_now()
    today          = now_utc.date()
    days_since_sun = (today.weekday() + 1) % 7
    week_end       = today - timedelta(days=days_since_sun)       # last Sunday
    week_start     = week_end - timedelta(days=6)                 # previous Monday
    ws_dt          = datetime(week_start.year, week_start.month, week_start.day)
    we_dt          = datetime(week_end.year,   week_end.month,   week_end.day, 23, 59, 59)

    logo = logo_base64()
    cur  = conn.cursor()
    p    = (ws_dt, we_dt)

    cur.execute(SQL_SUMMARY, p)
    s = cur.fetchone()
    files_total   = s.Files_Total      or 0
    rows_total    = s.Rows_Total       or 0
    rows_standard = s.Rows_Standard    or 0
    rows_duracell = s.Rows_Duracell    or 0
    rows_loaded   = s.Rows_Loaded      or 0
    rows_d365     = s.Rows_D365        or 0
    rows_sto      = s.Rows_STO         or 0
    unique_cust   = s.Unique_Customers or 0
    unique_orders = s.Unique_Orders    or 0

    cur.execute(SQL_DAILY,         p); daily    = cur.fetchall()
    cur.execute(SQL_TOP_CUSTOMERS, p); top_cust = cur.fetchall()
    cur.execute(SQL_STO,           p); sto_rows = cur.fetchall()
    cur.execute(SQL_ERRORS,        p); errors   = cur.fetchall()

    rep_rows, cat_rows, ot_rows = fetch_rep_cat_ot(cur, p)

    loaded_pct = int(rows_loaded / rows_total * 100) if rows_total else 0
    d365_pct   = int(rows_d365   / rows_total * 100) if rows_total else 0

    # ── Tiles ──
    tiles = (
        tile_row([
            {"value": str(files_total),   "label": "Files",       "colour": "#1E88E5",
             "sub": "This Week"},
            {"value": str(unique_orders), "label": "Orders",      "colour": "#1E88E5"},
            {"value": f"{rows_total:,}",  "label": "Order Lines", "colour": "#1E88E5"},
            {"value": str(unique_cust),   "label": "Customers",   "colour": "#00897B"},
        ])
        + tile_row([
            {"value": f"{rows_loaded:,}", "label": "Loaded → D365",    "colour": "#43A047",
             "value_colour": "#2E7D32",   "sub": f"{loaded_pct}% of lines"},
            {"value": f"{rows_d365:,}",  "label": "Processed by D365", "colour": "#43A047",
             "value_colour": "#2E7D32",   "sub": f"{d365_pct}% of lines"},
            {"value": str(len(sto_rows)),"label": "STO Files",          "colour": "#8E24AA"},
            {"value": str(len(errors)),  "label": "Error Files",
             "colour": "#E53935" if errors else "#43A047",
             "value_colour": "#C62828" if errors else "#2E7D32"},
        ])
    )

    # ── Daily breakdown ──
    daily_tr = ""
    for d in daily:
        lines     = int(d.Lines or 0)
        day_label = d.Day.strftime('%A %d %b') if hasattr(d.Day, 'strftime') else str(d.Day)
        daily_tr += f"""
        <tr>
          <td><strong>{day_label}</strong></td>
          <td class="num">{d.Files}</td>
          <td class="num">{d.Orders}</td>
          <td class="num">{lines:,}</td>
          <td class="num">{int(d.Standard_Lines or 0):,}</td>
          <td class="num">{int(d.Duracell_Lines or 0):,}</td>
          <td class="num">{fmt_val(d.Day_Value)}</td>
          <td style="padding:8px 14px;min-width:90px">{pct_bar(lines, rows_total)}</td>
        </tr>"""

    daily_table = f"""
<div class="section-head"><h2>Daily Breakdown</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>Day</th><th class="num">Files</th><th class="num">Orders</th>
        <th class="num">Lines</th><th class="num">Standard</th>
        <th class="num">Duracell</th><th class="num">Value</th><th>Volume</th>
      </tr>
    </thead>
    <tbody>
      {daily_tr if daily_tr else
       '<tr><td colspan="8" style="text-align:center;color:#9CA3AF;padding:20px">'
       'No data for this week</td></tr>'}
    </tbody>
  </table>
</div>"""

    # ── Top customers ──
    cust_tr = ""
    for i, c in enumerate(top_cust, 1):
        cust_tr += f"""
        <tr>
          <td style="color:#9CA3AF;font-weight:700">#{i}</td>
          <td><strong>{c.CustomerCode}</strong><br>
              <span style="color:#9CA3AF;font-size:10px">{c.CustomerName}</span></td>
          <td class="num">{c.Orders}</td>
          <td class="num">{int(c.Lines):,}</td>
          <td class="num">{fmt_val(c.Order_Value)}</td>
        </tr>"""

    cust_table = f"""
<div class="section-head"><h2>Top Customers by Lines</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr><th>#</th><th>Customer</th>
          <th class="num">Orders</th><th class="num">Lines</th><th class="num">Value</th></tr>
    </thead>
    <tbody>
      {cust_tr if cust_tr else
       '<tr><td colspan="5" style="text-align:center;color:#9CA3AF;padding:20px">No data</td></tr>'}
    </tbody>
  </table>
</div>"""

    # ── STO files ──
    if sto_rows:
        sto_tr = ""
        for f in sto_rows:
            ts = utc_to_dublin_full(f.Received)
            sto_tr += f"""
            <tr>
              <td><code style="font-size:11px">{f.Source_Filename}</code></td>
              <td>{f.CustomerName or "—"}</td>
              <td class="num">{int(f.Orders)}</td>
              <td class="num">{int(f.Lines):,}</td>
              <td class="num">{ts}</td>
            </tr>"""
        sto_section = f"""
<div class="section-head"><h2>Stock Transfer Order (STO) Files — Received This Week</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr><th>File</th><th>Customer</th>
          <th class="num">Orders</th><th class="num">Lines</th>
          <th class="num">Received</th></tr>
    </thead>
    <tbody>{sto_tr}</tbody>
  </table>
</div>"""
    else:
        sto_section = ""

    body = (
        tiles
        + daily_table
        + cust_table
        + sto_section
        + build_rep_section(rep_rows)
        + build_category_section(cat_rows)
        + build_ordertype_section(ot_rows)
        + build_error_section(errors)
        + '<div style="height:20px"></div>'
    )

    subject  = f"Fusion EPOS | Weekly Report | W/E {week_end.strftime('%d %b %Y')}"
    subtitle = (
        f"Weekly Summary · {week_start.strftime('%d %b')} – "
        f"{week_end.strftime('%d %b %Y')}  ·  "
        f"Run {dublin_dt.strftime('%H:%M')} {tz_label}  "
        f"({now_utc.strftime('%H:%M UTC')})"
    )
    send_email(cfg, subject, html_wrapper(logo, "Weekly Report", subtitle, body))


# =============================================================================
#  ENTRY POINT  — no CLI arguments
# =============================================================================

def main() -> None:
    now_utc = utc_now()
    dublin_dt, tz_label = dublin_now()
    log.info("=" * 70)
    log.info("  Synovia Fusion – EPOS Weekly Report  v1.1.0")
    log.info("  Started : %s  (%s %s)", now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"), dublin_dt.strftime("%H:%M"), tz_label)
    log.info("=" * 70)

    cfg  = load_config(CONFIG_FILE)
    conn = get_db_connection(cfg)
    try:
        run(conn, cfg)
    finally:
        conn.close()

    log.info("  Finished.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
