# =============================================================================
#  Synovia Fusion – EPOS Email Reporter
# -----------------------------------------------------------------------------
#  Module:        Fusion EPOS
#  Script Name:   Reporting_Email.py
#
#  Version:       1.1.0
#  Release Date:  2026-03-29
#
#  Author:        Synovia Digital
#
# -----------------------------------------------------------------------------
#  Description:
#  ------------
#  Companion reporting script for the Fusion EPOS WASP Order Processor.
#  Queries Raw.WASP_Orders and sends branded HTML email reports via the
#  Nexus SMTP relay configured in Fusion_EPOS_Production.txt.
#
#  Three report modes (controlled by --mode argument):
#
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │  Mode 1 – HOURLY  (--mode hourly)                                  │
#   │  Mode 2 – EOD     (--mode eod)                                     │
#   │  Mode 3 – WEEKLY  (--mode weekly)                                  │
#   └─────────────────────────────────────────────────────────────────────┘
#
#  All three modes include:
#   • KPI tile summary
#   • Breakdown by Sales Rep (UserCode / FirstName)
#   • Breakdown by Product Category
#   • Breakdown by OrderType
#
#  Usage:
#   python Reporting_Email.py --mode hourly
#   python Reporting_Email.py --mode eod
#   python Reporting_Email.py --mode weekly
#
# -----------------------------------------------------------------------------
#  Dependencies:
#  -------------
#   Python  >= 3.10
#   pyodbc  >= 4.0
#
#  Config file:
#   D:\Configuration\Fusion_EPOS_Production.txt
#
#  Fusion logo:
#   D:\Configuration\fusionlogo.jpg
#
# -----------------------------------------------------------------------------
#  Change History:
#  ---------------
#  v1.3.0  (2026-03-30)
#    • SQL updated to query STO_Received status (aligns with
#        Order Processor v2.1.0 which writes STO_Received on insert)
#    • STO_Received added to error exclusion list
#
#  v1.2.0  (2026-03-29)
#    • Cutover/STO files excluded from Error Files section
#    • Dedicated STO Files section added to weekly report
#    • STO tile added to weekly KPI row
#
#  v1.1.0  (2026-03-29)
#    • Added Rep summary section (UserCode, FirstName, orders, lines, value)
#    • Added Product Category breakdown
#    • Added OrderType breakdown
#
#  v1.0.1  (2026-03-29)
#    • Config file path corrected to Fusion_EPOS_Production.txt
#
#  v1.0.0  (2026-03-29)
#    • Initial release
# =============================================================================

import argparse
import base64
import configparser
import logging
import smtplib
import sys
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pyodbc


# =============================================================================
#  LOGGING
# =============================================================================

LOG_FILE = Path(r"D:\FusionHub\Logs\Fusion_EPOS_EmailReporter.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
#  CONSTANTS
# =============================================================================

CONFIG_FILE = r"D:\Configuration\Fusion_EPOS_Production.txt"
LOGO_FILE   = Path(r"D:\Configuration\fusionlogo.jpg")
RECIPIENTS  = ["aidan.harrington@synoviadigital.com"]
CUTOFF_HOUR = 14   # Files before 14:00 → Day+3 delivery; at/after → Day+4


# =============================================================================
#  CONFIG + HELPERS
# =============================================================================

def load_config(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def _clean(value: str) -> str:
    return value.strip().strip('"')


def get_db_connection(cfg: configparser.ConfigParser) -> pyodbc.Connection:
    db = cfg["database"]
    conn_str = (
        f"DRIVER={_clean(db['driver'])};"
        f"SERVER={_clean(db['server'])};"
        f"DATABASE={_clean(db['database'])};"
        f"UID={_clean(db['user'])};"
        f"PWD={_clean(db['password'])};"
        f"Encrypt={_clean(db['encrypt'])};"
        f"TrustServerCertificate={_clean(db['trust_server_certificate'])};"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def logo_base64() -> str:
    if LOGO_FILE.exists():
        with open(LOGO_FILE, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf-8")
        return f"data:image/jpeg;base64,{data}"
    return ""


def next_working_day(d: date, offset_days: int) -> date:
    current = d
    added   = 0
    while added < offset_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


# =============================================================================
#  BADGE + FORMAT HELPERS
# =============================================================================

def status_badge(s: str) -> str:
    if s == "Loaded_to_Dynamics":
        return '<span class="badge badge-green">Loaded</span>'
    if s == "Filtered":
        return '<span class="badge badge-amber">Filtered</span>'
    return f'<span class="badge badge-blue">{s}</span>'


def class_badge(c: str) -> str:
    if c == "Standard_Order":
        return '<span class="badge badge-blue">Standard</span>'
    if c == "Duracell_EPOS":
        return '<span class="badge badge-purple">Duracell</span>'
    return c


def fmt_val(v) -> str:
    if v is None:
        return "—"
    try:
        return f"€{float(v):,.2f}"
    except (TypeError, ValueError):
        return "—"


def pct_bar(part: int, total: int, colour: str = "#1E88E5") -> str:
    pct = min(100, int((part / max(1, total)) * 100))
    return (
        f'<div style="background:#E5E7EB;border-radius:3px;height:6px;width:100%">'
        f'<div style="background:{colour};height:6px;border-radius:3px;width:{pct}%"></div>'
        f'</div>'
    )


def tile_row(tiles: list[dict]) -> str:
    """
    Render a row of KPI tiles using an HTML <table> — the only layout that
    works reliably across Outlook, Office 365 webmail, and Gmail.

    Each tile dict:
      value       : str   – large number/text displayed
      label       : str   – small uppercase label
      sub         : str   – optional small sub-text
      colour      : str   – top-bar hex colour
      value_colour: str   – optional hex for the value text
    """
    col_w = f"{int(100 / len(tiles))}%"
    cells = ""
    for t in tiles:
        bar_colour   = t.get("colour", "#1E88E5")
        value_colour = t.get("value_colour", "#0D1B2A")
        sub_html     = (
            f'<div style="font-size:11px;color:#9CA3AF;margin-top:4px">{t["sub"]}</div>'
            if t.get("sub") else ""
        )
        cells += f"""
      <td width="{col_w}" style="padding:4px">
        <table width="100%" cellpadding="0" cellspacing="0" border="0"
               style="background:#ffffff;border-radius:10px;
                      box-shadow:0 1px 4px rgba(0,0,0,.07);
                      font-family:'Montserrat','Segoe UI',Arial,sans-serif">
          <tr>
            <td style="height:3px;background:{bar_colour};
                       border-radius:10px 10px 0 0;font-size:0;line-height:0">&nbsp;</td>
          </tr>
          <tr>
            <td style="padding:18px 12px 18px;text-align:center">
              <div style="font-size:30px;font-weight:800;color:{value_colour};
                          line-height:1;margin-bottom:5px">{t["value"]}</div>
              <div style="font-size:10px;font-weight:700;color:#6B7280;
                          text-transform:uppercase;letter-spacing:.7px">{t["label"]}</div>
              {sub_html}
            </td>
          </tr>
        </table>
      </td>"""

    return f"""
<table width="100%" cellpadding="0" cellspacing="0" border="0"
       style="padding:20px 20px 0">
  <tr>{cells}</tr>
</table>"""


# =============================================================================
#  EMAIL SENDER
# =============================================================================

def send_email(cfg: configparser.ConfigParser, subject: str, html_body: str) -> None:
    em_cfg = cfg["Nexus_Email"]
    host   = _clean(em_cfg["smtp_host"])
    port   = int(_clean(em_cfg["smtp_port"]))
    user   = _clean(em_cfg["smtp_user"])
    pwd    = _clean(em_cfg["smtp_password"])

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = user
    msg["To"]      = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    log.info("  Sending  →  %s  via  %s:%s", RECIPIENTS, host, port)
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(user, pwd)
        smtp.sendmail(user, RECIPIENTS, msg.as_bytes())
    log.info("  Email sent  ✓")


# =============================================================================
#  HTML SHELL
# =============================================================================

def html_wrapper(logo_src: str, title: str, subtitle: str, body_html: str) -> str:
    logo_tag = (
        f"<img src='{logo_src}' alt='Fusion' style='height:44px;width:auto;display:block'>"
        if logo_src
        else '<span style="font-size:22px;font-weight:800;color:#ffffff;letter-spacing:-0.5px">FUSION</span>'
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  body{{margin:0;padding:0;background:#F0F2F5;font-family:'Montserrat','Segoe UI',Arial,sans-serif;-webkit-font-smoothing:antialiased}}

  /* Section headings */
  .section-head{{padding:22px 20px 8px}}
  .section-head h2{{font-size:12px;font-weight:700;color:#1B3A5C;text-transform:uppercase;
                    letter-spacing:1px;border-left:3px solid #1E88E5;padding-left:10px;margin:0}}

  /* Data tables */
  .table-wrap{{padding:0 20px}}
  .data-table{{width:100%;border-collapse:collapse;background:#ffffff;border-radius:10px;
               overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.07);font-size:12px}}
  .data-table thead tr{{background:#1B3A5C;color:#ffffff}}
  .data-table thead th{{padding:11px 14px;font-weight:600;font-size:11px;text-transform:uppercase;
                         letter-spacing:.5px;text-align:left}}
  .data-table thead th.num{{text-align:right}}
  .data-table tbody tr{{border-bottom:1px solid #F3F4F6}}
  .data-table tbody tr:last-child{{border-bottom:none}}
  .data-table tbody tr:nth-child(even){{background:#FAFAFA}}
  .data-table tbody td{{padding:10px 14px;color:#374151;vertical-align:middle;
                        font-family:'Montserrat','Segoe UI',Arial,sans-serif}}
  .data-table tbody td.num{{text-align:right;font-weight:600}}

  /* Badges */
  .badge{{display:inline-block;padding:2px 9px;border-radius:20px;font-size:10px;
          font-weight:700;text-transform:uppercase;letter-spacing:.4px}}
  .badge-green{{background:#E8F5E9;color:#2E7D32}}
  .badge-amber{{background:#FFF3E0;color:#E65100}}
  .badge-blue{{background:#E3F2FD;color:#1565C0}}
  .badge-red{{background:#FFEBEE;color:#C62828}}
  .badge-purple{{background:#F3E5F5;color:#6A1B9A}}
</style>
</head>
<body>
<table width="100%" cellpadding="0" cellspacing="0" border="0"
       style="background:#F0F2F5;min-width:600px">
<tr><td align="center">
<table width="760" cellpadding="0" cellspacing="0" border="0"
       style="max-width:760px;width:100%">

  <!-- ══ HEADER ══ -->
  <tr>
    <td style="background:linear-gradient(135deg,#0D1B2A 0%,#1B3A5C 60%,#1E5F99 100%);
               padding:28px 28px 0;border-radius:0 0 6px 6px">
      <table width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td style="vertical-align:middle">{logo_tag}</td>
          <td style="vertical-align:middle;text-align:right">
            <div style="font-size:19px;font-weight:800;color:#ffffff;line-height:1.2;
                        font-family:'Montserrat','Segoe UI',Arial,sans-serif">{title}</div>
            <div style="font-size:11px;font-weight:600;color:#7EB8E8;margin-top:4px;
                        text-transform:uppercase;letter-spacing:.7px;
                        font-family:'Montserrat','Segoe UI',Arial,sans-serif">{subtitle}</div>
          </td>
        </tr>
        <tr>
          <td colspan="2" height="20">&nbsp;</td>
        </tr>
        <tr>
          <td colspan="2" height="3"
              style="background:linear-gradient(90deg,#1E88E5,#42A5F5,rgba(255,255,255,0));
                     font-size:0;line-height:0">&nbsp;</td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ══ BODY ══ -->
  <tr><td>{body_html}</td></tr>

  <!-- ══ FOOTER ══ -->
  <tr>
    <td style="padding:20px 20px 28px;text-align:center;font-size:10px;color:#9CA3AF;
               line-height:1.7;font-family:'Montserrat','Segoe UI',Arial,sans-serif">
      <strong style="color:#6B7280">Synovia Fusion – EPOS Module</strong><br>
      Automated report &nbsp;·&nbsp; Reporting_Email.py &nbsp;·&nbsp;
      Do not reply &nbsp;·&nbsp; nexus@synoviaintegration.com
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# =============================================================================
#  SHARED SECTION BUILDERS
# =============================================================================

def build_rep_section(rows) -> str:
    """
    Rep performance table.
    Expects rows with: UserCode, FirstName, Orders, Files, Lines, Order_Value
    """
    if not rows:
        return ""

    total_lines = sum(int(r.Lines or 0) for r in rows)
    tr = ""
    for i, r in enumerate(rows, 1):
        lines = int(r.Lines or 0)
        tr += f"""
        <tr>
          <td style="color:#9CA3AF;font-weight:700;width:32px">{i}</td>
          <td>
            <strong>{r.FirstName or "—"}</strong>
            <span style="color:#9CA3AF;font-size:10px;margin-left:6px">{r.UserCode}</span>
          </td>
          <td class="num">{int(r.Orders or 0)}</td>
          <td class="num">{int(r.Files  or 0)}</td>
          <td class="num">{lines:,}</td>
          <td class="num">{fmt_val(r.Order_Value)}</td>
          <td style="padding:8px 14px;min-width:90px">{pct_bar(lines, total_lines, '#1E88E5')}</td>
        </tr>"""

    return f"""
<div class="section-head"><h2>Performance by Sales Rep</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>#</th>
        <th>Rep</th>
        <th class="num">Orders</th>
        <th class="num">Files</th>
        <th class="num">Lines</th>
        <th class="num">Order Value</th>
        <th>Share</th>
      </tr>
    </thead>
    <tbody>{tr}</tbody>
  </table>
</div>"""


def build_category_section(rows) -> str:
    """
    Product Category breakdown.
    Expects rows with: ProductCategory, Lines, Order_Value, Orders
    """
    if not rows:
        return ""

    total_lines = sum(int(r.Lines or 0) for r in rows)
    colours = ["#1E88E5","#43A047","#FB8C00","#8E24AA","#00897B",
               "#E53935","#3949AB","#00ACC1","#7CB342","#F4511E"]
    tr = ""
    for i, r in enumerate(rows):
        lines = int(r.Lines or 0)
        colour = colours[i % len(colours)]
        cat = r.ProductCategory or "Uncategorised"
        tr += f"""
        <tr>
          <td>
            <span style="display:inline-block;width:10px;height:10px;border-radius:50%;
                         background:{colour};margin-right:6px;vertical-align:middle"></span>
            {cat}
          </td>
          <td class="num">{int(r.Orders or 0)}</td>
          <td class="num">{lines:,}</td>
          <td class="num">{fmt_val(r.Order_Value)}</td>
          <td style="padding:8px 14px;min-width:100px">{pct_bar(lines, total_lines, colour)}</td>
        </tr>"""

    return f"""
<div class="section-head"><h2>Breakdown by Product Category</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>Category</th>
        <th class="num">Orders</th>
        <th class="num">Lines</th>
        <th class="num">Value</th>
        <th>Share</th>
      </tr>
    </thead>
    <tbody>{tr}</tbody>
  </table>
</div>"""


def build_ordertype_section(rows) -> str:
    """
    OrderType breakdown.
    Expects rows with: OrderType, Orders, Lines, Order_Value
    """
    if not rows:
        return ""

    total_lines = sum(int(r.Lines or 0) for r in rows)
    tr = ""
    for r in rows:
        lines = int(r.Lines or 0)
        ot = r.OrderType or "—"
        tr += f"""
        <tr>
          <td><strong>{ot}</strong></td>
          <td class="num">{int(r.Orders or 0)}</td>
          <td class="num">{lines:,}</td>
          <td class="num">{fmt_val(r.Order_Value)}</td>
          <td style="padding:8px 14px;min-width:100px">{pct_bar(lines, total_lines, '#00897B')}</td>
        </tr>"""

    return f"""
<div class="section-head"><h2>Breakdown by Order Type</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>Order Type</th>
        <th class="num">Orders</th>
        <th class="num">Lines</th>
        <th class="num">Value</th>
        <th>Share</th>
      </tr>
    </thead>
    <tbody>{tr}</tbody>
  </table>
</div>"""


# =============================================================================
#  SHARED SQL – REP / CATEGORY / ORDERTYPE
#  Parameterised with a date range:  WHERE Load_Timestamp >= ? AND < ?
# =============================================================================

SQL_REP = """
SELECT
    UserCode,
    MAX(FirstName)                  AS FirstName,
    COUNT(DISTINCT OrderId)         AS Orders,
    COUNT(DISTINCT Source_Filename) AS Files,
    COUNT(*)                        AS Lines,
    SUM(Price * Quantity)           AS Order_Value
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
GROUP BY UserCode
ORDER BY COUNT(*) DESC
"""

SQL_CATEGORY = """
SELECT
    ProductCategory,
    COUNT(DISTINCT OrderId)  AS Orders,
    COUNT(*)                 AS Lines,
    SUM(Price * Quantity)    AS Order_Value
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
GROUP BY ProductCategory
ORDER BY COUNT(*) DESC
"""

SQL_ORDERTYPE = """
SELECT
    OrderType,
    COUNT(DISTINCT OrderId)  AS Orders,
    COUNT(*)                 AS Lines,
    SUM(Price * Quantity)    AS Order_Value
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
GROUP BY OrderType
ORDER BY COUNT(*) DESC
"""


def fetch_rep_cat_ot(cur, p):
    """Fetch rep, category and ordertype rows for a given date-range param tuple."""
    cur.execute(SQL_REP,       p); rep_rows  = cur.fetchall()
    cur.execute(SQL_CATEGORY,  p); cat_rows  = cur.fetchall()
    cur.execute(SQL_ORDERTYPE, p); ot_rows   = cur.fetchall()
    return rep_rows, cat_rows, ot_rows


# =============================================================================
#  MODE 1 – HOURLY
# =============================================================================

SQL_HOURLY_SUMMARY = """
SELECT
    COUNT(DISTINCT Source_Filename)                                          AS Files_Total,
    COUNT(*)                                                                 AS Rows_Total,
    SUM(CASE WHEN Order_Classification='Standard_Order' THEN 1 ELSE 0 END)  AS Rows_Standard,
    SUM(CASE WHEN Order_Classification='Duracell_EPOS'  THEN 1 ELSE 0 END)  AS Rows_Duracell,
    SUM(CASE WHEN Fusion_Status='Loaded_to_Dynamics'    THEN 1 ELSE 0 END)  AS Rows_Loaded,
    SUM(CASE WHEN Fusion_Status='Filtered'              THEN 1 ELSE 0 END)  AS Rows_Filtered,
    COUNT(DISTINCT CASE WHEN Order_Classification='Standard_Order'
                        THEN Source_Filename END)                            AS Files_Standard,
    COUNT(DISTINCT CASE WHEN Order_Classification='Duracell_EPOS'
                        THEN Source_Filename END)                            AS Files_Duracell
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_HOURLY_STUCK = """
SELECT Source_Filename, Order_Classification, COUNT(*) AS Row_Count,
       MIN(Load_Timestamp) AS First_Loaded
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
  AND Fusion_Status = 'Filtered'
  AND Order_Classification = 'Standard_Order'
GROUP BY Source_Filename, Order_Classification
ORDER BY First_Loaded
"""

SQL_HOURLY_RECENT = """
SELECT TOP 10
    Source_Filename,
    Order_Classification,
    Fusion_Status,
    COUNT(*)              AS Rows,
    MAX(Load_Timestamp)   AS Last_Seen
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY Source_Filename, Order_Classification, Fusion_Status
ORDER BY MAX(Load_Timestamp) DESC
"""


def run_hourly(conn: pyodbc.Connection, cfg: configparser.ConfigParser) -> None:
    now  = datetime.now(timezone.utc)
    logo = logo_base64()
    cur  = conn.cursor()

    # Date-range params for shared queries (today midnight → now)
    day_start = datetime(now.year, now.month, now.day)
    day_end   = datetime(now.year, now.month, now.day, 23, 59, 59)
    p = (day_start, day_end)

    cur.execute(SQL_HOURLY_SUMMARY)
    row            = cur.fetchone()
    files_total    = row.Files_Total    or 0
    rows_total     = row.Rows_Total     or 0
    rows_loaded    = row.Rows_Loaded    or 0
    rows_duracell  = row.Rows_Duracell  or 0
    files_standard = row.Files_Standard or 0
    files_duracell = row.Files_Duracell or 0

    cur.execute(SQL_HOURLY_STUCK);  stuck_rows = cur.fetchall()
    cur.execute(SQL_HOURLY_RECENT); recent     = cur.fetchall()

    rep_rows, cat_rows, ot_rows = fetch_rep_cat_ot(cur, p)

    # ── Tiles ──
    tiles = tile_row([
        {"value": str(files_total),   "label": "Files Today",    "colour": "#1E88E5"},
        {"value": f"{rows_total:,}",  "label": "Order Lines",    "colour": "#1E88E5"},
        {"value": f"{rows_loaded:,}", "label": "Loaded to D365", "colour": "#43A047",
         "value_colour": "#2E7D32",   "sub": f"{files_standard} file(s)"},
        {"value": str(rows_duracell), "label": "Duracell / TO",  "colour": "#8E24AA",
         "sub": f"{files_duracell} file(s)"},
    ])

    # ── Alert ──
    if stuck_rows:
        stuck_list = ", ".join(r.Source_Filename for r in stuck_rows)
        alert = (
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            f'<tr><td style="padding:12px 20px 0">'
            f'<table width="100%" cellpadding="14" cellspacing="0" border="0" '
            f'style="background:#FFF8E1;border-radius:8px;border-left:4px solid #FFC107;'
            f'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;font-size:12px;'
            f'font-weight:600;color:#795548">'
            f'<tr><td>⚠️ &nbsp;{len(stuck_rows)} Standard Order file(s) remain in '
            f'<strong>Filtered</strong> status — FTP upload may have failed.'
            f'<br>Files: {stuck_list}</td></tr></table>'
            f'</td></tr></table>'
        )
    else:
        alert = (
            '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            '<tr><td style="padding:12px 20px 0">'
            '<table width="100%" cellpadding="14" cellspacing="0" border="0" '
            'style="background:#E8F5E9;border-radius:8px;border-left:4px solid #43A047;'
            'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;font-size:12px;'
            'font-weight:600;color:#2E7D32">'
            '<tr><td>✔ &nbsp;All Standard Order files successfully uploaded to Dynamics 365.</td></tr>'
            '</table></td></tr></table>'
        )

    # ── Recent files table ──
    recent_tr = ""
    for r in recent:
        ts = r.Last_Seen.strftime("%H:%M:%S") if r.Last_Seen else "—"
        recent_tr += f"""
        <tr>
          <td><code style="font-size:11px">{r.Source_Filename}</code></td>
          <td>{class_badge(r.Order_Classification)}</td>
          <td>{status_badge(r.Fusion_Status)}</td>
          <td class="num">{int(r.Rows)}</td>
          <td class="num">{ts}</td>
        </tr>"""

    recent_table = f"""
<div class="section-head"><h2>Recent File Activity (Today)</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>File</th><th>Classification</th><th>Status</th>
        <th class="num">Lines</th><th class="num">Last Seen</th>
      </tr>
    </thead>
    <tbody>
      {recent_tr if recent_tr else
       '<tr><td colspan="5" style="text-align:center;color:#9CA3AF;padding:20px">No files processed today</td></tr>'}
    </tbody>
  </table>
</div>"""

    body = (
        tiles + alert + recent_table
        + build_rep_section(rep_rows)
        + build_category_section(cat_rows)
        + build_ordertype_section(ot_rows)
        + '<div style="height:20px"></div>'
    )

    subject  = f"Fusion EPOS | Hourly Update | {now.strftime('%d %b %Y %H:%M')} UTC"
    subtitle = f"Day-to-Date · As at {now.strftime('%H:%M UTC')}"
    send_email(cfg, subject, html_wrapper(logo, "Hourly Operations Update", subtitle, body))


# =============================================================================
#  MODE 2 – END OF DAY
# =============================================================================

SQL_EOD_SUMMARY = """
SELECT
    COUNT(DISTINCT Source_Filename)                                          AS Files_Total,
    COUNT(*)                                                                 AS Rows_Total,
    SUM(CASE WHEN Order_Classification='Standard_Order' THEN 1 ELSE 0 END)  AS Rows_Standard,
    SUM(CASE WHEN Order_Classification='Duracell_EPOS'  THEN 1 ELSE 0 END)  AS Rows_Duracell,
    SUM(CASE WHEN Fusion_Status='Loaded_to_Dynamics'    THEN 1 ELSE 0 END)  AS Rows_Loaded,
    SUM(CASE WHEN Fusion_Status='Filtered'              THEN 1 ELSE 0 END)  AS Rows_Filtered_Remaining,
    COUNT(DISTINCT CustomerCode)                                             AS Unique_Customers,
    COUNT(DISTINCT OrderId)                                                  AS Unique_Orders
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_EOD_ORDERS = """
SELECT
    Source_Filename,
    OrderId,
    CustomerCode,
    CustomerName,
    Order_Classification,
    Fusion_Status,
    COUNT(*)                AS Line_Count,
    SUM(Price * Quantity)   AS Order_Value,
    MIN(Load_Timestamp)     AS Load_Time
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY Source_Filename, OrderId, CustomerCode, CustomerName,
         Order_Classification, Fusion_Status
ORDER BY MIN(Load_Timestamp)
"""

SQL_EOD_BY_HOUR = """
SELECT
    DATEPART(HOUR, Load_Timestamp)  AS Hour_UTC,
    COUNT(DISTINCT Source_Filename) AS Files,
    COUNT(*)                        AS Rows
FROM Raw.WASP_Orders
WHERE CAST(Load_Timestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY DATEPART(HOUR, Load_Timestamp)
ORDER BY 1
"""


def run_eod(conn: pyodbc.Connection, cfg: configparser.ConfigParser) -> None:
    today = date.today()
    now   = datetime.now(timezone.utc)
    logo  = logo_base64()
    cur   = conn.cursor()

    day_start = datetime(today.year, today.month, today.day)
    day_end   = datetime(today.year, today.month, today.day, 23, 59, 59)
    p = (day_start, day_end)

    cur.execute(SQL_EOD_SUMMARY)
    row = cur.fetchone()
    files_total    = row.Files_Total             or 0
    rows_total     = row.Rows_Total              or 0
    rows_standard  = row.Rows_Standard           or 0
    rows_duracell  = row.Rows_Duracell           or 0
    rows_loaded    = row.Rows_Loaded             or 0
    rows_remaining = row.Rows_Filtered_Remaining or 0
    unique_cust    = row.Unique_Customers        or 0
    unique_orders  = row.Unique_Orders           or 0

    cur.execute(SQL_EOD_ORDERS);  orders = cur.fetchall()
    cur.execute(SQL_EOD_BY_HOUR); hourly = cur.fetchall()

    rep_rows, cat_rows, ot_rows = fetch_rep_cat_ot(cur, p)

    # ── Tiles row 1 ──
    tiles = (
        tile_row([
            {"value": str(files_total),    "label": "Files",        "colour": "#1E88E5"},
            {"value": str(unique_orders),  "label": "Orders",       "colour": "#1E88E5"},
            {"value": f"{rows_total:,}",   "label": "Order Lines",  "colour": "#1E88E5"},
            {"value": str(unique_cust),    "label": "Customers",    "colour": "#00897B"},
        ])
        + tile_row([
            {"value": f"{rows_loaded:,}",   "label": "Loaded → D365",   "colour": "#43A047", "value_colour": "#2E7D32"},
            {"value": f"{rows_duracell:,}", "label": "Duracell / TO",   "colour": "#8E24AA"},
            {"value": f"{rows_standard:,}", "label": "Standard Lines",  "colour": "#1E88E5"},
            {"value": str(rows_remaining),  "label": "Pending Filtered",
             "colour": "#FB8C00" if rows_remaining else "#43A047",
             "value_colour": "#E65100" if rows_remaining else "#2E7D32"},
        ])
    )

    # ── Delivery schedule ──
    early_del = next_working_day(today, 3)
    late_del  = next_working_day(today, 4)

    delivery_note = (
        '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        '<tr><td style="padding:10px 20px 0">'
        '<table width="100%" cellpadding="12" cellspacing="0" border="0" '
        'style="background:#EFF6FF;border-radius:8px;border-left:4px solid #1E88E5;'
        'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;font-size:11.5px;'
        'color:#1B3A5C;font-weight:500;line-height:1.7">'
        f'<tr><td><strong>Delivery Day Rule (Cut-off {CUTOFF_HOUR:02d}:00)</strong><br>'
        f'Files received <strong>before {CUTOFF_HOUR:02d}:00</strong> → '
        f'<strong>Day+3 delivery : {early_del.strftime("%A %d %b %Y")}</strong><br>'
        f'Files received <strong>at or after {CUTOFF_HOUR:02d}:00</strong> → '
        f'<strong>Day+4 delivery : {late_del.strftime("%A %d %b %Y")}</strong>'
        '</td></tr></table></td></tr></table>'
    )

    def delivery_date_str(load_ts) -> str:
        if load_ts is None:
            return "—"
        off = 3 if load_ts.hour < CUTOFF_HOUR else 4
        return next_working_day(load_ts.date(), off).strftime("%a %d %b")

    order_tr = ""
    for o in orders:
        val = fmt_val(o.Order_Value)
        ts  = o.Load_Time.strftime("%H:%M") if o.Load_Time else "—"
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
{delivery_note}
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
       '<tr><td colspan="8" style="text-align:center;color:#9CA3AF;padding:20px">No orders today</td></tr>'}
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
      <tr>
        <th>Hour</th><th class="num">Files</th>
        <th class="num">Lines</th><th>Volume</th>
      </tr>
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
        + '<div style="height:20px"></div>'
    )

    subject  = f"Fusion EPOS | End of Day Report | {today.strftime('%d %b %Y')}"
    subtitle = f"End-of-Day Summary · {today.strftime('%A %d %B %Y')}"
    send_email(cfg, subject, html_wrapper(logo, "End of Day Report", subtitle, body))


# =============================================================================
#  MODE 3 – WEEKLY
# =============================================================================

SQL_WEEKLY_SUMMARY = """
SELECT
    COUNT(DISTINCT Source_Filename)                                          AS Files_Total,
    COUNT(*)                                                                 AS Rows_Total,
    SUM(CASE WHEN Order_Classification='Standard_Order' THEN 1 ELSE 0 END)  AS Rows_Standard,
    SUM(CASE WHEN Order_Classification='Duracell_EPOS'  THEN 1 ELSE 0 END)  AS Rows_Duracell,
    SUM(CASE WHEN Fusion_Status='Loaded_to_Dynamics'    THEN 1 ELSE 0 END)  AS Rows_Loaded,
    COUNT(DISTINCT CustomerCode)                                             AS Unique_Customers,
    COUNT(DISTINCT OrderId)                                                  AS Unique_Orders
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
"""

SQL_WEEKLY_DAILY = """
SELECT
    CAST(Load_Timestamp AS DATE)            AS Day,
    COUNT(DISTINCT Source_Filename)         AS Files,
    COUNT(DISTINCT OrderId)                 AS Orders,
    COUNT(*)                                AS Lines,
    SUM(CASE WHEN Order_Classification='Standard_Order' THEN 1 ELSE 0 END) AS Standard_Lines,
    SUM(CASE WHEN Order_Classification='Duracell_EPOS'  THEN 1 ELSE 0 END) AS Duracell_Lines,
    SUM(Price * Quantity)                   AS Day_Value
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
GROUP BY CAST(Load_Timestamp AS DATE)
ORDER BY 1
"""

SQL_WEEKLY_TOP_CUSTOMERS = """
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

SQL_WEEKLY_ERRORS = """
SELECT
    Source_Filename,
    Order_Classification,
    Fusion_Status,
    COUNT(*)            AS Rows,
    MIN(Load_Timestamp) AS First_Seen
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
  AND Fusion_Status NOT IN ('Loaded_to_Dynamics', 'Filtered', 'STO_Received', 'Cutover')
GROUP BY Source_Filename, Order_Classification, Fusion_Status
ORDER BY MIN(Load_Timestamp)
"""

SQL_WEEKLY_STO = """
SELECT
    Source_Filename,
    Order_Classification,
    Fusion_Status,
    COUNT(*)                        AS Rows,
    COUNT(DISTINCT OrderId)         AS Orders,
    MAX(CustomerName)               AS CustomerName,
    MIN(Load_Timestamp)             AS First_Seen
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
  AND Fusion_Status = 'STO_Received'
  AND Order_Classification = 'Duracell_EPOS'
GROUP BY Source_Filename, Order_Classification, Fusion_Status
ORDER BY MIN(Load_Timestamp)
"""


def run_weekly(conn: pyodbc.Connection, cfg: configparser.ConfigParser) -> None:
    today            = date.today()
    days_since_sun   = (today.weekday() + 1) % 7
    week_end         = today - timedelta(days=days_since_sun)
    week_start       = week_end - timedelta(days=6)
    week_start_dt    = datetime(week_start.year, week_start.month, week_start.day)
    week_end_dt      = datetime(week_end.year,   week_end.month,   week_end.day, 23, 59, 59)

    logo = logo_base64()
    cur  = conn.cursor()
    p    = (week_start_dt, week_end_dt)

    cur.execute(SQL_WEEKLY_SUMMARY, p)
    s = cur.fetchone()
    files_total    = s.Files_Total    or 0
    rows_total     = s.Rows_Total     or 0
    rows_standard  = s.Rows_Standard  or 0
    rows_duracell  = s.Rows_Duracell  or 0
    rows_loaded    = s.Rows_Loaded    or 0
    unique_cust    = s.Unique_Customers or 0
    unique_orders  = s.Unique_Orders  or 0

    cur.execute(SQL_WEEKLY_DAILY,         p); daily     = cur.fetchall()
    cur.execute(SQL_WEEKLY_TOP_CUSTOMERS, p); top_cust  = cur.fetchall()
    cur.execute(SQL_WEEKLY_ERRORS,        p); errors    = cur.fetchall()
    cur.execute(SQL_WEEKLY_STO,           p); sto_files = cur.fetchall()

    rep_rows, cat_rows, ot_rows = fetch_rep_cat_ot(cur, p)

    loaded_pct = int(rows_loaded / rows_total * 100) if rows_total else 0

    # ── Tiles ──
    tiles = (
        tile_row([
            {"value": str(files_total),   "label": "Files",       "colour": "#1E88E5", "sub": "This Week"},
            {"value": str(unique_orders), "label": "Orders",      "colour": "#1E88E5"},
            {"value": f"{rows_total:,}",  "label": "Order Lines", "colour": "#1E88E5"},
            {"value": str(unique_cust),   "label": "Customers",   "colour": "#00897B"},
        ])
        + tile_row([
            {"value": f"{rows_loaded:,}",   "label": "Loaded → D365",  "colour": "#43A047",
             "value_colour": "#2E7D32",      "sub": f"{loaded_pct}% of lines"},
            {"value": f"{rows_duracell:,}", "label": "Duracell / TO",  "colour": "#8E24AA"},
            {"value": str(len(sto_files)),  "label": "STO Files",      "colour": "#FB8C00"},
            {"value": str(len(errors)),     "label": "Error Files",
             "colour": "#E53935" if errors else "#43A047",
             "value_colour": "#C62828" if errors else "#2E7D32"},
        ])
    )

    # ── Daily breakdown ──
    daily_tr = ""
    for d in daily:
        lines = int(d.Lines or 0)
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
       '<tr><td colspan="8" style="text-align:center;color:#9CA3AF;padding:20px">No data for this week</td></tr>'}
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
      <tr>
        <th>#</th><th>Customer</th>
        <th class="num">Orders</th><th class="num">Lines</th><th class="num">Value</th>
      </tr>
    </thead>
    <tbody>
      {cust_tr if cust_tr else
       '<tr><td colspan="5" style="text-align:center;color:#9CA3AF;padding:20px">No data</td></tr>'}
    </tbody>
  </table>
</div>"""

    # ── Errors ──
    if errors:
        err_tr = ""
        for e in errors:
            ts = e.First_Seen.strftime("%d %b %H:%M") if e.First_Seen else "—"
            err_tr += f"""
            <tr>
              <td><code style="font-size:11px">{e.Source_Filename}</code></td>
              <td>{e.Order_Classification}</td>
              <td><span class="badge badge-red">{e.Fusion_Status}</span></td>
              <td class="num">{int(e.Rows)}</td>
              <td class="num">{ts}</td>
            </tr>"""
        error_section = f"""
<div class="section-head"><h2>⚠ Error Files</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>File</th><th>Classification</th><th>Status</th>
        <th class="num">Rows</th><th class="num">First Seen</th>
      </tr>
    </thead>
    <tbody>{err_tr}</tbody>
  </table>
</div>"""
    else:
        error_section = (
            '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            '<tr><td style="padding:12px 20px 0">'
            '<table width="100%" cellpadding="14" cellspacing="0" border="0" '
            'style="background:#E8F5E9;border-radius:8px;border-left:4px solid #43A047;'
            'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;font-size:12px;'
            'font-weight:600;color:#2E7D32">'
            '<tr><td>✔ &nbsp;No error files found this week — all orders processed cleanly.</td></tr>'
            '</table></td></tr></table>'
        )

    # ── STO / Cutover files section ──
    if sto_files:
        sto_tr = ""
        for f in sto_files:
            ts = f.First_Seen.strftime("%d %b %H:%M") if f.First_Seen else "—"
            sto_tr += f"""
            <tr>
              <td><code style="font-size:11px">{f.Source_Filename}</code></td>
              <td>{f.CustomerName or "—"}</td>
              <td><span class="badge badge-purple">Duracell EPOS</span></td>
              <td><span class="badge badge-amber">STO Received</span></td>
              <td class="num">{int(f.Orders)}</td>
              <td class="num">{int(f.Rows)}</td>
              <td class="num">{ts}</td>
            </tr>"""
        sto_section = f"""
<div class="section-head"><h2>Stock Transfer Order (STO) Files — Received</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>File</th><th>Customer</th><th>Classification</th><th>Status</th>
        <th class="num">Orders</th><th class="num">Lines</th><th class="num">Received</th>
      </tr>
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
        + error_section
        + '<div style="height:20px"></div>'
    )

    subject  = f"Fusion EPOS | Weekly Report | W/E {week_end.strftime('%d %b %Y')}"
    subtitle = f"Weekly Summary · {week_start.strftime('%d %b')} – {week_end.strftime('%d %b %Y')}"
    send_email(cfg, subject, html_wrapper(logo, "Weekly Report", subtitle, body))


# =============================================================================
#  ENTRY POINT
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Synovia Fusion EPOS Email Reporter")
    parser.add_argument(
        "--mode", required=True, choices=["hourly", "eod", "weekly"],
        help="hourly | eod | weekly",
    )
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    log.info("=" * 70)
    log.info("  Synovia Fusion – EPOS Email Reporter  v1.3.0")
    log.info("  Mode    :  %s", args.mode.upper())
    log.info("  Started :  %s", now.strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("=" * 70)

    cfg  = load_config(CONFIG_FILE)
    conn = get_db_connection(cfg)

    try:
        if args.mode == "hourly":
            run_hourly(conn, cfg)
        elif args.mode == "eod":
            run_eod(conn, cfg)
        elif args.mode == "weekly":
            run_weekly(conn, cfg)
    finally:
        conn.close()

    log.info("  Reporter finished.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
