# =============================================================================
#  Synovia Fusion – EPOS  |  Day Update Report
# -----------------------------------------------------------------------------
#  Module:        Fusion EPOS
#  Script Name:   Report_Day_Update.py
#
#  Version:       1.0.0
#  Release Date:  2026-03-30
#
#  Author:        Synovia Digital
#
# -----------------------------------------------------------------------------
#  Description:
#  ------------
#  Sends a day-to-date operational snapshot email covering everything
#  received and processed today (midnight UTC to now).
#
#  Intended to run hourly via Windows Task Scheduler — no arguments needed.
#  Simply execute:
#
#    python Report_Day_Update.py
#
#  Sections:
#   • KPI tiles  — files, lines, loaded to D365, Duracell/STO
#   • Alert banner — warns if any Standard Order files are stuck in Filtered
#   • Recent file activity — last 10 files processed today
#   • Performance by Sales Rep
#   • Breakdown by Product Category
#   • Breakdown by Order Type
#
# -----------------------------------------------------------------------------
#  Task Scheduler:
#   Action  : python "D:\Applications\Fusion_EPOS\...\Report_Day_Update.py"
#   Trigger : Every 1 hour, starting 06:00
# =============================================================================

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyodbc

# All shared helpers, SQL, HTML builders live in reporting_shared.py
# which must be in the same directory as this script.
sys.path.insert(0, str(Path(__file__).parent))
from reporting_shared import (
    CONFIG_FILE, load_config, get_db_connection, logo_base64,
    tile_row, html_wrapper, send_email, alert_ok, alert_warn,
    status_badge, class_badge, pct_bar, fmt_val,
    fetch_rep_cat_ot, build_rep_section, build_category_section,
    build_ordertype_section, build_error_section, SQL_ERRORS,
    dublin_now, utc_now, utc_to_dublin,
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
    SUM(CASE WHEN Fusion_Status = 'Filtered'              THEN 1 ELSE 0 END) AS Rows_Filtered,
    COUNT(DISTINCT CASE WHEN Order_Classification = 'Standard_Order'
                        THEN Source_Filename END)                             AS Files_Standard,
    COUNT(DISTINCT CASE WHEN Order_Classification = 'Duracell_EPOS'
                        THEN Source_Filename END)                             AS Files_Duracell,
    COUNT(DISTINCT CustomerCode)                                              AS Unique_Customers
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_STUCK = """
SELECT Source_Filename, COUNT(*) AS Row_Count, MIN(Load_Timestamp) AS First_Loaded
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
  AND Fusion_Status        = 'Filtered'
  AND Order_Classification = 'Standard_Order'
GROUP BY Source_Filename
ORDER BY First_Loaded
"""

SQL_RECENT = """
SELECT TOP 10
    Source_Filename,
    Order_Classification,
    Fusion_Status,
    COUNT(*)            AS Rows,
    MAX(Load_Timestamp) AS Last_Seen
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY Source_Filename, Order_Classification, Fusion_Status
ORDER BY MAX(Load_Timestamp) DESC
"""


# =============================================================================
#  REPORT
# =============================================================================

def run(conn: pyodbc.Connection, cfg) -> None:
    now_utc          = utc_now()
    dublin_dt, tz_label = dublin_now()
    logo             = logo_base64()
    cur              = conn.cursor()

    day_start = datetime(now_utc.year, now_utc.month, now_utc.day)
    day_end   = datetime(now_utc.year, now_utc.month, now_utc.day, 23, 59, 59)
    p = (day_start, day_end)

    cur.execute(SQL_SUMMARY)
    row = cur.fetchone()
    files_total    = row.Files_Total     or 0
    rows_total     = row.Rows_Total      or 0
    rows_loaded    = row.Rows_Loaded     or 0
    rows_duracell  = row.Rows_Duracell   or 0
    files_standard = row.Files_Standard  or 0
    files_duracell = row.Files_Duracell  or 0
    unique_cust    = row.Unique_Customers or 0

    cur.execute(SQL_STUCK);      stuck  = cur.fetchall()
    cur.execute(SQL_RECENT);     recent = cur.fetchall()
    cur.execute(SQL_ERRORS, p);  errors = cur.fetchall()

    rep_rows, cat_rows, ot_rows = fetch_rep_cat_ot(cur, p)

    dublin_time_display = f"{dublin_dt.strftime('%H:%M')} {tz_label}"
    utc_time_display    = now_utc.strftime("%H:%M UTC")

    # ── Tiles ──
    tiles = tile_row([
        {"value": str(files_total),    "label": "Files Today",    "colour": "#1E88E5"},
        {"value": f"{rows_total:,}",   "label": "Order Lines",    "colour": "#1E88E5"},
        {"value": f"{rows_loaded:,}",  "label": "Loaded to D365", "colour": "#43A047",
         "value_colour": "#2E7D32",    "sub": f"{files_standard} file(s)"},
        {"value": str(unique_cust),    "label": "Customers",      "colour": "#00897B"},
    ]) + tile_row([
        {"value": f"{rows_duracell:,}", "label": "Duracell Lines", "colour": "#8E24AA"},
        {"value": str(files_duracell),  "label": "Duracell Files", "colour": "#8E24AA"},
        {"value": str(len(stuck)),      "label": "Stuck Filtered",
         "colour": "#FB8C00" if stuck else "#43A047",
         "value_colour": "#E65100" if stuck else "#2E7D32"},
        {"value": str(len(errors)),     "label": "Errors",
         "colour": "#E53935" if errors else "#43A047",
         "value_colour": "#C62828" if errors else "#2E7D32"},
    ])

    # ── Alert ──
    if stuck:
        stuck_list = ", ".join(r.Source_Filename for r in stuck)
        banner = alert_warn(
            f"{len(stuck)} Standard Order file(s) remain in <strong>Filtered</strong> "
            f"status — FTP upload may have failed.<br>Files: {stuck_list}"
        )
    else:
        banner = alert_ok("All Standard Order files successfully uploaded to Dynamics 365.")

    # ── Recent files ──
    recent_tr = ""
    for r in recent:
        ts = utc_to_dublin(r.Last_Seen)
        recent_tr += f"""
        <tr>
          <td><code style="font-size:11px">{r.Source_Filename}</code></td>
          <td>{class_badge(r.Order_Classification)}</td>
          <td>{status_badge(r.Fusion_Status)}</td>
          <td class="num">{int(r.Rows)}</td>
          <td class="num">{ts}</td>
        </tr>"""

    recent_table = f"""
<div class="section-head"><h2>Recent File Activity</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr><th>File</th><th>Classification</th><th>Status</th>
          <th class="num">Lines</th><th class="num">Last Seen</th></tr>
    </thead>
    <tbody>
      {recent_tr if recent_tr else
       '<tr><td colspan="5" style="text-align:center;color:#9CA3AF;padding:20px">'
       'No files processed today</td></tr>'}
    </tbody>
  </table>
</div>"""

    body = (
        tiles + banner + recent_table
        + build_rep_section(rep_rows)
        + build_category_section(cat_rows)
        + build_ordertype_section(ot_rows)
        + build_error_section(errors)
        + '<div style="height:20px"></div>'
    )

    subject  = (
        f"Fusion EPOS | Day Update | {dublin_dt.strftime('%d %b %Y')} "
        f"as at {dublin_time_display}"
    )
    subtitle = (
        f"Day-to-Date · {dublin_dt.strftime('%A %d %B %Y')} · "
        f"{dublin_time_display}  ({utc_time_display})"
    )
    send_email(cfg, subject, html_wrapper(logo, "Day Update", subtitle, body))


# =============================================================================
#  ENTRY POINT  — no CLI arguments
# =============================================================================

def main() -> None:
    now_utc = utc_now()
    dublin_dt, tz_label = dublin_now()
    log.info("=" * 70)
    log.info("  Synovia Fusion – EPOS Day Update Report  v1.1.0")
    log.info("  Started : %s  (%s %s)",
             now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
             dublin_dt.strftime("%H:%M"), tz_label)
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
