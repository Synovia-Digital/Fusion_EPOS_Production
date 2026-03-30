# =============================================================================
#  Synovia Fusion – EPOS  |  End of Day Report
# -----------------------------------------------------------------------------
#  Module:        Fusion EPOS
#  Script Name:   Report_End_of_Day.py
#
#  Version:       1.0.0
#  Release Date:  2026-03-30
#
#  Author:        Synovia Digital
#
# -----------------------------------------------------------------------------
#  Description:
#  ------------
#  Sends the end-of-day summary email for today's EPOS activity.
#  Run daily at 18:00 via Windows Task Scheduler — no arguments needed.
#
#    python Report_End_of_Day.py
#
#  Sections:
#   • KPI tiles  — 8 tiles across two rows
#   • Delivery schedule — per-order table with estimated delivery dates
#       Files before 14:00  → Day+3 working days
#       Files from  14:00   → Day+4 working days
#   • Files by hour — volume breakdown
#   • Performance by Sales Rep
#   • Breakdown by Product Category
#   • Breakdown by Order Type
#   • Error files (if any)
#
# -----------------------------------------------------------------------------
#  Task Scheduler:
#   Action  : python "D:\Applications\Fusion_EPOS\...\Report_End_of_Day.py"
#   Trigger : Daily at 18:00
# =============================================================================

import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pyodbc

sys.path.insert(0, str(Path(__file__).parent))
from reporting_shared import (
    CONFIG_FILE, CUTOFF_HOUR,
    load_config, get_db_connection, logo_base64, next_working_day,
    tile_row, html_wrapper, send_email,
    status_badge, class_badge, pct_bar, fmt_val,
    delivery_note_html, delivery_date_str,
    dublin_now, utc_now, utc_to_dublin, utc_to_dublin_full,
    fetch_rep_cat_ot, build_rep_section, build_category_section,
    build_ordertype_section, build_error_section, SQL_ERRORS,
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
    SUM(CASE WHEN Fusion_Status = 'Filtered'              THEN 1 ELSE 0 END) AS Rows_Filtered_Remaining,
    SUM(CASE WHEN Fusion_Status = 'STO_Received'          THEN 1 ELSE 0 END) AS Rows_STO,
    COUNT(DISTINCT CustomerCode)                                              AS Unique_Customers,
    COUNT(DISTINCT OrderId)                                                   AS Unique_Orders
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_ORDERS = """
SELECT
    Source_Filename,
    OrderId,
    CustomerCode,
    CustomerName,
    Order_Classification,
    Fusion_Status,
    COUNT(*)              AS Line_Count,
    SUM(Price * Quantity) AS Order_Value,
    MIN(Load_Timestamp)   AS Load_Time
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY Source_Filename, OrderId, CustomerCode, CustomerName,
         Order_Classification, Fusion_Status
ORDER BY MIN(Load_Timestamp)
"""

SQL_BY_HOUR = """
SELECT
    DATEPART(HOUR, Load_Timestamp)  AS Hour_UTC,
    COUNT(DISTINCT Source_Filename) AS Files,
    COUNT(*)                        AS Rows
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY DATEPART(HOUR, Load_Timestamp)
ORDER BY 1
"""


# =============================================================================
#  REPORT
# =============================================================================

def run(conn: pyodbc.Connection, cfg) -> None:
    now_utc          = utc_now()
    dublin_dt, tz_label = dublin_now()
    today            = now_utc.date()
    logo  = logo_base64()
    cur   = conn.cursor()

    day_start = datetime(now_utc.year, now_utc.month, now_utc.day)
    day_end   = datetime(now_utc.year, now_utc.month, now_utc.day, 23, 59, 59)
    p = (day_start, day_end)

    cur.execute(SQL_SUMMARY)
    row = cur.fetchone()
    files_total    = row.Files_Total             or 0
    rows_total     = row.Rows_Total              or 0
    rows_standard  = row.Rows_Standard           or 0
    rows_duracell  = row.Rows_Duracell           or 0
    rows_loaded    = row.Rows_Loaded             or 0
    rows_remaining = row.Rows_Filtered_Remaining or 0
    rows_sto       = row.Rows_STO                or 0
    unique_cust    = row.Unique_Customers        or 0
    unique_orders  = row.Unique_Orders           or 0

    cur.execute(SQL_ORDERS);  orders = cur.fetchall()
    cur.execute(SQL_BY_HOUR); hourly = cur.fetchall()
    cur.execute(SQL_ERRORS, p); errors = cur.fetchall()

    rep_rows, cat_rows, ot_rows = fetch_rep_cat_ot(cur, p)

    # ── Tiles ──
    tiles = (
        tile_row([
            {"value": str(files_total),   "label": "Files",       "colour": "#1E88E5"},
            {"value": str(unique_orders), "label": "Orders",      "colour": "#1E88E5"},
            {"value": f"{rows_total:,}",  "label": "Order Lines", "colour": "#1E88E5"},
            {"value": str(unique_cust),   "label": "Customers",   "colour": "#00897B"},
        ])
        + tile_row([
            {"value": f"{rows_loaded:,}",   "label": "Loaded → D365",   "colour": "#43A047",
             "value_colour": "#2E7D32"},
            {"value": f"{rows_standard:,}", "label": "Standard Lines",  "colour": "#1E88E5"},
            {"value": f"{rows_sto:,}",      "label": "STO Received",    "colour": "#8E24AA"},
            {"value": str(rows_remaining),  "label": "Pending Filtered",
             "colour": "#FB8C00" if rows_remaining else "#43A047",
             "value_colour": "#E65100" if rows_remaining else "#2E7D32"},
        ])
    )

    # ── Order delivery table ──
    order_tr = ""
    for o in orders:
        val = fmt_val(o.Order_Value)
        ts  = utc_to_dublin(o.Load_Time)
        order_tr += f"""
        <tr>
          <td><code style="font-size:11px">{o.OrderId}</code></td>
          <td>{o.CustomerCode}<br>
              <span style="color:#9CA3AF;font-size:10px">{o.CustomerName}</span></td>
          <td>{class_badge(o.Order_Classification)}</td>
          <td>{status_badge(o.Fusion_Status)}</td>
          <td class="num">{o.Line_Count}</td>
          <td class="num">{val}</td>
          <td class="num">{ts}</td>
          <td class="num"><strong>{delivery_date_str(o.Load_Time)}</strong></td>
        </tr>"""

    orders_table = f"""
<div class="section-head"><h2>Order Delivery Schedule</h2></div>
{delivery_note_html(today)}
<div class="table-wrap" style="margin-top:10px">
  <table class="data-table">
    <thead>
      <tr>
        <th>Order ID</th><th>Customer</th><th>Type</th><th>Status</th>
        <th class="num">Lines</th><th class="num">Value</th>
        <th class="num">Loaded</th><th class="num">Est. Delivery</th>
      </tr>
    </thead>
    <tbody>
      {order_tr if order_tr else
       '<tr><td colspan="8" style="text-align:center;color:#9CA3AF;padding:20px">'
       'No orders today</td></tr>'}
    </tbody>
  </table>
</div>"""

    # ── Hourly activity ──
    hourly_tr = ""
    for h in hourly:
        hourly_tr += f"""
        <tr>
          <td>{h.Hour_UTC:02d}:00 UTC</td>
          <td class="num">{h.Files}</td>
          <td class="num">{int(h.Rows):,}</td>
          <td style="padding:8px 14px;min-width:120px">{pct_bar(int(h.Rows), rows_total)}</td>
        </tr>"""

    hourly_table = f"""
<div class="section-head"><h2>Files by Hour</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr><th>Hour (UTC+0/+1)</th><th class="num">Files</th>
          <th class="num">Lines</th><th>Volume</th></tr>
    </thead>
    <tbody>
      {hourly_tr if hourly_tr else
       '<tr><td colspan="4" style="text-align:center;color:#9CA3AF;padding:20px">No data</td></tr>'}
    </tbody>
  </table>
</div>"""

    body = (
        tiles
        + orders_table
        + hourly_table
        + build_rep_section(rep_rows)
        + build_category_section(cat_rows)
        + build_ordertype_section(ot_rows)
        + build_error_section(errors)
        + '<div style="height:20px"></div>'
    )

    dublin_date_str = dublin_dt.strftime("%A %d %B %Y")
    dublin_time_str = f"{dublin_dt.strftime('%H:%M')} {tz_label}"
    utc_str = now_utc.strftime("%H:%M UTC")
    subject  = f"Fusion EPOS | End of Day | {dublin_dt.strftime('%d %b %Y')}"
    subtitle = f"End-of-Day Summary · {dublin_date_str}  ·  {dublin_time_str}  ({utc_str})"
    send_email(cfg, subject, html_wrapper(logo, "End of Day Report", subtitle, body))


# =============================================================================
#  ENTRY POINT  — no CLI arguments
# =============================================================================

def main() -> None:
    now = datetime.now(timezone.utc)
    log.info("=" * 70)
    dublin_dt, tz_label = dublin_now()
    log.info("  Synovia Fusion – EPOS End of Day Report  v1.1.0")
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
