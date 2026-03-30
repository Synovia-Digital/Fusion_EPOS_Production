"""
Synovia Fusion – EPOS Consignment Ready Queue Report
=====================================================
Finds ALL files with READY orders in CTL.EPOS_Header going back
LOOKBACK_WEEKS weeks and produces a summary report email.

Summary per file (no line-level detail):
  - File ID, File Name, Year, Week
  - READY store orders (header count)
  - Total units across READY lines
  - Total sales value across READY lines

Use this to see what is queued and ready to send before running
the consignment staging process.

Secrets:
  ENV FUSION_EPOS_BOOTSTRAP_PASSWORD → INI password= → RuntimeError
  ENV FUSION_EPOS_SMTP_PASSWORD      → INI smtp_password= → RuntimeError
"""

from __future__ import annotations

import datetime as dt
import logging
import smtplib
import sys
import traceback
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import sys
import os

# reporting_shared.py lives in the sibling Reporting\ folder
_REPORTING_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "Reporting"
)
if _REPORTING_DIR not in sys.path:
    sys.path.insert(0, _REPORTING_DIR)

import pandas as pd

from _fusion_shared import (
    connect_autocommit,
    resolve_smtp_password,
    ensure_dir,
    fmt_int,
    fmt_money,
    html_escape,
    LOG_DIR,
    OUTPUT_ROOT,
    SMTP_SERVER,
    SMTP_PORT,
    SENDER_EMAIL,
)
from reporting_shared import (
    html_wrapper,
    tile_row,
    alert_ok,
    alert_warn,
    logo_base64,
    dublin_time_str,
)

# =============================================================================
# CONFIG
# =============================================================================
REGION          = "ROI"
SUPPLIER_NUMBER = 3380
LOOKBACK_WEEKS  = 8          # how many weeks back to search for READY files

SEND_EMAIL = True

RECIPIENTS: list[str] = [
    "aidan.harrington@synoviadigital.com",
    "sarah.kelly@primeline.ie",
]

OUTDIR = Path(OUTPUT_ROOT) / "Consignment"

# =============================================================================
# LOGGING
# =============================================================================
ensure_dir(LOG_DIR)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(LOG_DIR) / "Process_Consignment_Orders.log", encoding="utf-8"
        ),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# DB QUERY
# =============================================================================

def get_ready_queue(conn, lookback_weeks: int) -> pd.DataFrame:
    """
    Returns one row per file that has at least one READY CTL header.
    Columns: File_ID, FileName, Year, Week, Ready_Orders,
             Total_Units, Total_Sales, Earliest_Created, Latest_Created
    """
    return pd.read_sql("""
        SELECT
              f.File_ID
            , f.FileName
            , f.[Year]
            , f.[Week]
            , COUNT(DISTINCT h.EPOS_Fusion_Key)   AS Ready_Orders
            , SUM(l.Units_Sold)                    AS Total_Units
            , SUM(l.Sales_Value_Inc_VAT)           AS Total_Sales
            , MIN(h.Created_UTC)                   AS Earliest_Created
            , MAX(h.Created_UTC)                   AS Latest_Created
        FROM RAW.EPOS_File f
        JOIN CTL.EPOS_Header h
          ON  h.Source_File_ID = f.File_ID
        JOIN CTL.EPOS_Line_Item l
          ON  l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
         AND  l.Integration_Status = 'READY'
        WHERE h.Integration_Status = 'READY'
          AND f.Region          = ?
          AND f.Supplier_Number = ?
          AND f.[Year] >= YEAR(DATEADD(WEEK, -?, SYSUTCDATETIME())) % 100
        GROUP BY
              f.File_ID
            , f.FileName
            , f.[Year]
            , f.[Week]
        ORDER BY
              f.[Year]  DESC
            , f.[Week]  DESC
            , f.File_ID DESC;
    """, conn, params=[REGION, int(SUPPLIER_NUMBER), int(lookback_weeks)])


# =============================================================================
# HTML REPORT
# =============================================================================

def render_report(df: pd.DataFrame, generated_at: str) -> str:
    n_files  = len(df)
    n_orders = int(df["Ready_Orders"].sum()) if not df.empty else 0
    n_units  = int(df["Total_Units"].sum())  if not df.empty else 0
    n_sales  = float(df["Total_Sales"].sum()) if not df.empty else 0.0

    # ── KPI tiles ──────────────────────────────────────────────────────────
    tiles = tile_row([
        {"value": fmt_int(n_files),
         "label": "Files Ready",
         "colour": "#1E88E5" if n_files else "#9CA3AF"},
        {"value": fmt_int(n_orders),
         "label": "Store Orders",
         "colour": "#0D47A1"},
        {"value": fmt_int(n_units),
         "label": "Total Units",
         "colour": "#00897B"},
        {"value": f"€{n_sales:,.0f}",
         "label": "Total Sales Value",
         "colour": "#43A047"},
    ])

    # ── Status banner ──────────────────────────────────────────────────────
    if n_files == 0:
        banner = alert_ok("No files currently READY — queue is clear.")
    else:
        banner = alert_warn(
            f"{n_files} file(s) with READY orders going back up to "
            f"{LOOKBACK_WEEKS} weeks. Review and stage when confirmed."
        )

    # ── File table ─────────────────────────────────────────────────────────
    if df.empty:
        table_html = '<p style="padding:0 20px;font-size:12px;color:#9CA3AF">No READY files found.</p>'
    else:
        rows = ""
        for _, r in df.iterrows():
            rows += f"""
            <tr>
              <td class="num">{int(r.File_ID)}</td>
              <td>{html_escape(str(r.FileName or ""))}</td>
              <td class="num">{int(r.Year)}</td>
              <td class="num">{int(r.Week):02d}</td>
              <td class="num">{fmt_int(r.Ready_Orders)}</td>
              <td class="num">{fmt_int(r.Total_Units)}</td>
              <td class="num">€{float(r.Total_Sales or 0):,.2f}</td>
              <td class="num" style="font-size:11px;color:#9CA3AF">
                  {str(r.Latest_Created)[:16] if r.Latest_Created else "—"}
              </td>
            </tr>"""

        table_html = f"""
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th class="num">File ID</th>
        <th>File Name</th>
        <th class="num">Year</th>
        <th class="num">Week</th>
        <th class="num">Store Orders</th>
        <th class="num">Total Units</th>
        <th class="num">Total Sales</th>
        <th class="num">Last Updated (UTC)</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""

    body = f"""
{tiles}
<div style="padding:8px 20px 0">{banner}</div>
<div class="section-head"><h2>READY File Queue — {REGION} / Supplier {SUPPLIER_NUMBER}</h2></div>
{table_html}
<div style="padding:12px 20px;font-size:11px;color:#9CA3AF">
  Generated: {generated_at} &nbsp;·&nbsp; Lookback: {LOOKBACK_WEEKS} weeks
</div>
"""

    return html_wrapper(
        logo_src=logo_base64(),
        title="EPOS Consignment Ready Queue",
        subtitle=f"Region {REGION} | Supplier {SUPPLIER_NUMBER}",
        body_html=body,
    )


# =============================================================================
# EMAIL
# =============================================================================

def send_report(subject: str, html_body: str) -> bool:
    if not RECIPIENTS:
        log.warning("No recipients configured — skipping email.")
        return False
    smtp_pw = resolve_smtp_password()
    msg = MIMEMultipart("alternative")
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = ", ".join(RECIPIENTS)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP(host=SMTP_SERVER, port=SMTP_PORT, timeout=30) as srv:
            srv.ehlo()
            if srv.has_extn("starttls"):
                srv.starttls()
                srv.ehlo()
            if SENDER_EMAIL and smtp_pw:
                srv.login(SENDER_EMAIL, smtp_pw)
            srv.send_message(msg)
        log.info("Email sent ✓  →  %s", RECIPIENTS)
        return True
    except Exception as e:
        log.warning("Email failed: %s", e)
        log.debug(traceback.format_exc())
        return False


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    ensure_dir(str(OUTDIR))
    ts           = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    generated_at = dublin_time_str("%Y-%m-%d %H:%M")

    log.info("Consignment Ready Queue  Region=%s  Supplier=%s  Lookback=%d weeks",
             REGION, SUPPLIER_NUMBER, LOOKBACK_WEEKS)

    with connect_autocommit("Fusion_EPOS_ConsignmentQueue") as conn:
        df = get_ready_queue(conn, LOOKBACK_WEEKS)

    n_files  = len(df)
    n_orders = int(df["Ready_Orders"].sum()) if not df.empty else 0
    log.info("Found %d READY file(s) with %d total store orders", n_files, n_orders)

    if not df.empty:
        for _, r in df.iterrows():
            log.info("  File %s | Y%s W%02d | %s orders | %s",
                     r.File_ID, int(r.Year), int(r.Week),
                     fmt_int(r.Ready_Orders), r.FileName)

    html = render_report(df, generated_at)

    # Save HTML
    html_path = OUTDIR / f"consignment_queue_{ts}.html"
    html_path.write_text(html, encoding="utf-8")
    log.info("Wrote: %s", html_path)

    # Email
    subject = (
        f"Fusion EPOS Consignment Queue — "
        f"{n_files} file(s) READY | {n_orders} store orders | "
        f"{REGION}/{SUPPLIER_NUMBER}"
    )
    if SEND_EMAIL:
        send_report(subject, html)
    else:
        log.info("Email disabled (SEND_EMAIL=False).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
