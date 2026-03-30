# =============================================================================
#  Synovia Fusion – EPOS Reporting  |  Shared Library
# -----------------------------------------------------------------------------
#  Module:        Fusion EPOS
#  Script Name:   reporting_shared.py
#
#  Version:       1.0.0
#  Release Date:  2026-03-30
#
#  Author:        Synovia Digital
#
# -----------------------------------------------------------------------------
#  Description:
#  ------------
#  Shared constants, helpers, HTML builder, and SQL used by all three
#  Fusion EPOS email reporter scripts:
#
#    Report_Day_Update.py   – Hourly day-to-date snapshot
#    Report_End_of_Day.py   – End-of-day summary with delivery schedule
#    Report_Weekly.py       – Weekly summary (run Sunday)
#
#  Import pattern in each report script:
#    from reporting_shared import *
#
# =============================================================================

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
#  LOGGING  (shared — each script gets its own log entry via __name__)
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

CONFIG_FILE = r"\\pl-az-int-prd\D_Drive\Configuration\Fusion_EPOS_Production.ini"
LOGO_FILE   = Path(r"\\pl-az-int-prd\D_Drive\Configuration\fusionlogo.jpg")

RECIPIENTS  = [
    "aidan.harrington@synoviadigital.com",
    "jarlath.jennings@jaymark.primeline.ie",
    "david.preston@primeline.ie",
    "owen.mccarthy@primeline.ie",
]

CUTOFF_HOUR = 14   # Files before 14:00 → Day+3 delivery; at/after → Day+4

# All known valid Fusion_Status values — anything not in this set is an error
VALID_STATUSES = (
    "Filtered",
    "Loaded_to_Dynamics",
    "STO_Received",
    "Cutover",
    "Processed_by_D365",
)

# Dublin timezone offset helpers
# Ireland observes GMT (UTC+0) in winter and IST/BST (UTC+1) in summer.
# DST runs last Sunday of March → last Sunday of October (same schedule as UK).
_UTC = timezone.utc


def _last_sunday(year: int, month: int) -> date:
    """Return the last Sunday of the given month/year."""
    # Start from the last day of the month and step back to Sunday
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = last_day.weekday() + 1  # days back to last Sunday (Mon=0, Sun=6)
    if offset == 7:
        offset = 0
    return last_day - timedelta(days=offset)


def dublin_now() -> datetime:
    """
    Return current datetime in Dublin local time (GMT/IST).
    BST (UTC+1) applies from last Sunday of March to last Sunday of October.
    No third-party library required.
    """
    utc_now   = datetime.now(_UTC)
    year      = utc_now.year
    bst_start = datetime(_last_sunday(year, 3).year,
                         _last_sunday(year, 3).month,
                         _last_sunday(year, 3).day,
                         1, 0, 0, tzinfo=_UTC)   # clocks go forward 01:00 UTC
    bst_end   = datetime(_last_sunday(year, 10).year,
                         _last_sunday(year, 10).month,
                         _last_sunday(year, 10).day,
                         1, 0, 0, tzinfo=_UTC)   # clocks go back  01:00 UTC
    if bst_start <= utc_now < bst_end:
        offset = timedelta(hours=1)
        tz_label = "IST"
    else:
        offset = timedelta(hours=0)
        tz_label = "GMT"
    local_dt = utc_now + offset
    # Return a naive datetime tagged with tz_label via a custom attribute trick —
    # callers use dublin_now() for display; tz_label exposed as module constant.
    return local_dt.replace(tzinfo=None), tz_label


def dublin_time_str(fmt: str = "%H:%M") -> str:
    """Return current Dublin time formatted as a string."""
    dt, label = dublin_now()
    return f"{dt.strftime(fmt)} {label}"


def utc_now() -> datetime:
    return datetime.now(_UTC)


def utc_to_dublin(dt) -> str:
    """
    Convert a naive UTC datetime (as stored in the DB) to Dublin local time
    string.  Returns "—" for None.  Format: HH:MM IST / HH:MM GMT
    """
    if dt is None:
        return "—"
    # Treat the naive DB datetime as UTC
    aware = dt.replace(tzinfo=_UTC)
    year  = aware.year
    bst_start = datetime(_last_sunday(year, 3).year,
                         _last_sunday(year, 3).month,
                         _last_sunday(year, 3).day,
                         1, 0, 0, tzinfo=_UTC)
    bst_end   = datetime(_last_sunday(year, 10).year,
                         _last_sunday(year, 10).month,
                         _last_sunday(year, 10).day,
                         1, 0, 0, tzinfo=_UTC)
    if bst_start <= aware < bst_end:
        local = aware + timedelta(hours=1)
        label = "IST"
    else:
        local = aware
        label = "GMT"
    return f"{local.strftime('%H:%M')} {label}"


def utc_to_dublin_full(dt) -> str:
    """
    Full date+time conversion for timestamps needing date context (e.g. error tables).
    Returns "dd Mon HH:MM IST" or "—".
    """
    if dt is None:
        return "—"
    aware = dt.replace(tzinfo=_UTC)
    year  = aware.year
    bst_start = datetime(_last_sunday(year, 3).year,
                         _last_sunday(year, 3).month,
                         _last_sunday(year, 3).day,
                         1, 0, 0, tzinfo=_UTC)
    bst_end   = datetime(_last_sunday(year, 10).year,
                         _last_sunday(year, 10).month,
                         _last_sunday(year, 10).day,
                         1, 0, 0, tzinfo=_UTC)
    if bst_start <= aware < bst_end:
        local = aware + timedelta(hours=1)
        label = "IST"
    else:
        local = aware
        label = "GMT"
    return f"{local.strftime('%d %b %H:%M')} {label}"


# =============================================================================
#  CONFIG + DB HELPERS
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


def next_working_day(d: date, offset_days: int) -> date:
    """Add offset_days working days (Mon–Fri) to date d."""
    current = d
    added   = 0
    while added < offset_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


# =============================================================================
#  FORMAT HELPERS
# =============================================================================

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


def status_badge(s: str) -> str:
    mapping = {
        "Loaded_to_Dynamics":  "badge-green",
        "Filtered":            "badge-amber",
        "STO_Received":        "badge-purple",
        "Cutover":             "badge-amber",
        "Processed_by_D365":   "badge-green",
    }
    css = mapping.get(s, "badge-blue")
    return f'<span class="badge {css}">{s}</span>'


def class_badge(c: str) -> str:
    if c == "Standard_Order":
        return '<span class="badge badge-blue">Standard</span>'
    if c == "Duracell_EPOS":
        return '<span class="badge badge-purple">Duracell</span>'
    return c


# =============================================================================
#  TILE ROW  (email-safe table-based layout)
# =============================================================================

def tile_row(tiles: list[dict]) -> str:
    """
    Render a row of KPI tiles using nested <table> elements.
    Works across Outlook, O365 webmail, and Gmail.

    Each tile dict keys:
      value        : str  – large metric displayed
      label        : str  – small uppercase label
      sub          : str  – optional sub-text below label
      colour       : str  – hex for the top colour bar
      value_colour : str  – optional hex for the value text
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
            <td style="padding:18px 12px;text-align:center">
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
#  ALERT BANNERS
# =============================================================================

def alert_ok(message: str) -> str:
    return (
        '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        '<tr><td style="padding:12px 20px 0">'
        '<table width="100%" cellpadding="14" cellspacing="0" border="0" '
        'style="background:#E8F5E9;border-radius:8px;border-left:4px solid #43A047;'
        'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;'
        f'font-size:12px;font-weight:600;color:#2E7D32">'
        f'<tr><td>✔ &nbsp;{message}</td></tr>'
        '</table></td></tr></table>'
    )


def alert_warn(message: str) -> str:
    return (
        '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        '<tr><td style="padding:12px 20px 0">'
        '<table width="100%" cellpadding="14" cellspacing="0" border="0" '
        'style="background:#FFF8E1;border-radius:8px;border-left:4px solid #FFC107;'
        'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;'
        f'font-size:12px;font-weight:600;color:#795548">'
        f'<tr><td>⚠️ &nbsp;{message}</td></tr>'
        '</table></td></tr></table>'
    )


def delivery_note_html(today: date) -> str:
    early_del = next_working_day(today, 3)
    late_del  = next_working_day(today, 4)
    return (
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


# =============================================================================
#  HTML SHELL
# =============================================================================

def html_wrapper(logo_src: str, title: str, subtitle: str, body_html: str) -> str:
    logo_tag = (
        f"<img src='{logo_src}' alt='Fusion' style='height:44px;width:auto;display:block'>"
        if logo_src
        else '<span style="font-size:22px;font-weight:800;color:#ffffff;'
             'letter-spacing:-0.5px">FUSION</span>'
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap"
      rel="stylesheet">
<style>
  body{{margin:0;padding:0;background:#F0F2F5;
       font-family:'Montserrat','Segoe UI',Arial,sans-serif;
       -webkit-font-smoothing:antialiased}}
  .section-head{{padding:22px 20px 8px}}
  .section-head h2{{font-size:12px;font-weight:700;color:#1B3A5C;text-transform:uppercase;
                    letter-spacing:1px;border-left:3px solid #1E88E5;
                    padding-left:10px;margin:0}}
  .table-wrap{{padding:0 20px}}
  .data-table{{width:100%;border-collapse:collapse;background:#ffffff;border-radius:10px;
               overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.07);font-size:12px}}
  .data-table thead tr{{background:#1B3A5C;color:#ffffff}}
  .data-table thead th{{padding:11px 14px;font-weight:600;font-size:11px;
                         text-transform:uppercase;letter-spacing:.5px;text-align:left}}
  .data-table thead th.num{{text-align:right}}
  .data-table tbody tr{{border-bottom:1px solid #F3F4F6}}
  .data-table tbody tr:last-child{{border-bottom:none}}
  .data-table tbody tr:nth-child(even){{background:#FAFAFA}}
  .data-table tbody td{{padding:10px 14px;color:#374151;vertical-align:middle;
                        font-family:'Montserrat','Segoe UI',Arial,sans-serif}}
  .data-table tbody td.num{{text-align:right;font-weight:600}}
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

  <!-- HEADER -->
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
        <tr><td colspan="2" height="20">&nbsp;</td></tr>
        <tr>
          <td colspan="2" height="3"
              style="background:linear-gradient(90deg,#1E88E5,#42A5F5,rgba(255,255,255,0));
                     font-size:0;line-height:0">&nbsp;</td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- BODY -->
  <tr><td>{body_html}</td></tr>

  <!-- FOOTER -->
  <tr>
    <td style="padding:20px 20px 28px;text-align:center;font-size:10px;color:#9CA3AF;
               line-height:1.7;font-family:'Montserrat','Segoe UI',Arial,sans-serif">
      <strong style="color:#6B7280">Synovia Fusion – EPOS Module</strong><br>
      Automated report &nbsp;·&nbsp; Do not reply
      &nbsp;·&nbsp; nexus@synoviaintegration.com
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


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


def logo_base64() -> str:
    if LOGO_FILE.exists():
        with open(LOGO_FILE, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf-8")
        return f"data:image/jpeg;base64,{data}"
    return ""


# =============================================================================
#  SHARED SECTION BUILDERS
# =============================================================================

def build_rep_section(rows) -> str:
    if not rows:
        return ""
    total_lines = sum(int(r.Lines or 0) for r in rows)
    tr = ""
    for i, r in enumerate(rows, 1):
        lines = int(r.Lines or 0)
        tr += f"""
        <tr>
          <td style="color:#9CA3AF;font-weight:700;width:28px">{i}</td>
          <td><strong>{r.FirstName or "—"}</strong>
              <span style="color:#9CA3AF;font-size:10px;margin-left:6px">{r.UserCode}</span></td>
          <td class="num">{int(r.Orders or 0)}</td>
          <td class="num">{int(r.Files  or 0)}</td>
          <td class="num">{lines:,}</td>
          <td class="num">{fmt_val(r.Order_Value)}</td>
          <td style="padding:8px 14px;min-width:90px">{pct_bar(lines, total_lines)}</td>
        </tr>"""
    return f"""
<div class="section-head"><h2>Performance by Sales Rep</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr><th>#</th><th>Rep</th><th class="num">Orders</th><th class="num">Files</th>
          <th class="num">Lines</th><th class="num">Value</th><th>Share</th></tr>
    </thead>
    <tbody>{tr}</tbody>
  </table>
</div>"""


def build_category_section(rows) -> str:
    if not rows:
        return ""
    total_lines = sum(int(r.Lines or 0) for r in rows)
    colours = ["#1E88E5","#43A047","#FB8C00","#8E24AA","#00897B",
               "#E53935","#3949AB","#00ACC1","#7CB342","#F4511E"]
    tr = ""
    for i, r in enumerate(rows):
        lines  = int(r.Lines or 0)
        colour = colours[i % len(colours)]
        cat    = r.ProductCategory or "Uncategorised"
        tr += f"""
        <tr>
          <td><span style="display:inline-block;width:10px;height:10px;border-radius:50%;
                           background:{colour};margin-right:6px;vertical-align:middle">
              </span>{cat}</td>
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
      <tr><th>Category</th><th class="num">Orders</th>
          <th class="num">Lines</th><th class="num">Value</th><th>Share</th></tr>
    </thead>
    <tbody>{tr}</tbody>
  </table>
</div>"""


def build_ordertype_section(rows) -> str:
    if not rows:
        return ""
    total_lines = sum(int(r.Lines or 0) for r in rows)
    tr = ""
    for r in rows:
        lines = int(r.Lines or 0)
        tr += f"""
        <tr>
          <td><strong>{r.OrderType or "—"}</strong></td>
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
      <tr><th>Order Type</th><th class="num">Orders</th>
          <th class="num">Lines</th><th class="num">Value</th><th>Share</th></tr>
    </thead>
    <tbody>{tr}</tbody>
  </table>
</div>"""


# =============================================================================
#  SHARED SQL
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

VALID_STATUS_LIST = "', '".join(VALID_STATUSES)
SQL_ERRORS = f"""
SELECT
    Source_Filename,
    Order_Classification,
    Fusion_Status,
    COUNT(*)            AS Rows,
    MIN(Load_Timestamp) AS First_Seen
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
  AND Fusion_Status NOT IN ('{VALID_STATUS_LIST}')
GROUP BY Source_Filename, Order_Classification, Fusion_Status
ORDER BY MIN(Load_Timestamp)
"""


def fetch_rep_cat_ot(cur, p: tuple) -> tuple:
    cur.execute(SQL_REP,      p); rep_rows = cur.fetchall()
    cur.execute(SQL_CATEGORY, p); cat_rows = cur.fetchall()
    cur.execute(SQL_ORDERTYPE,p); ot_rows  = cur.fetchall()
    return rep_rows, cat_rows, ot_rows


def build_error_section(errors) -> str:
    if errors:
        err_tr = ""
        for e in errors:
            ts = utc_to_dublin_full(e.First_Seen)
            err_tr += f"""
            <tr>
              <td><code style="font-size:11px">{e.Source_Filename}</code></td>
              <td>{e.Order_Classification}</td>
              <td><span class="badge badge-red">{e.Fusion_Status}</span></td>
              <td class="num">{int(e.Rows)}</td>
              <td class="num">{ts}</td>
            </tr>"""
        return f"""
<div class="section-head"><h2>⚠ Error Files</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr><th>File</th><th>Classification</th><th>Status</th>
          <th class="num">Rows</th><th class="num">First Seen</th></tr>
    </thead>
    <tbody>{err_tr}</tbody>
  </table>
</div>"""
    else:
        return alert_ok("No error files found — all orders processed cleanly.")
