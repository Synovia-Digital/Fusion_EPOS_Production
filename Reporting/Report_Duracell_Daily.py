# =============================================================================
#  Synovia Fusion – EPOS  |  Duracell Daily Transfer Order Report
# -----------------------------------------------------------------------------
#  Module:        Fusion EPOS
#  Script Name:   Report_Duracell_Daily.py
#
#  Version:       3.0.0
#  Release Date:  2026-03-31
#
#  Author:        Synovia Digital
#
# -----------------------------------------------------------------------------
#  Description:
#  ------------
#  Daily Duracell STO snapshot.  Two clearly separated batches:
#
#  ┌─────────────────────────────────────────────────────────────────────┐
#  │  CLOSED BATCH  (prev cutoff → today 14:00 local)                   │
#  │                                                                     │
#  │  Orders that have passed the transmission window.  Transfer Orders  │
#  │  should be raised in D365 and sent to SAP.  Report shows D365       │
#  │  document number, SAP SO number, SAP status and confirmed           │
#  │  RequestedReceiptDate as the agreed delivery date.                  │
#  │                                                                     │
#  │  Any order in this section NOT yet confirmed by SAP is flagged.     │
#  └─────────────────────────────────────────────────────────────────────┘
#  ┌─────────────────────────────────────────────────────────────────────┐
#  │  OPEN BATCH  (today 14:00 local → tomorrow 14:00 local)            │
#  │                                                                     │
#  │  Orders received after today's cutoff.  Awaiting the next          │
#  │  transmission window.  No D365/SAP numbers yet.  Delivery date =   │
#  │  today + 4 working days (Day+4).                                    │
#  └─────────────────────────────────────────────────────────────────────┘
#
#  All times displayed in Irish local time (IST/GMT).
#  Run daily at 15:30 via Windows Task Scheduler.
#  No arguments needed:
#
#    python Report_Duracell_Daily.py
#
# -----------------------------------------------------------------------------
#  Task Scheduler:
#   Action  : python "D:\Applications\Fusion_EPOS\...\Report_Duracell_Daily.py"
#   Trigger : Daily at 15:30
# =============================================================================

import logging
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pyodbc

sys.path.insert(0, str(Path(__file__).parent))
from reporting_shared import (
    CONFIG_FILE, load_config, get_db_connection, logo_base64,
    tile_row, html_wrapper, alert_ok, alert_warn,
    next_working_day, utc_to_dublin, utc_to_dublin_full,
    dublin_now, utc_now, CUTOFF_HOUR,
    fmt_val,
)

LOG_FILE = Path(r"D:\FusionHub\Logs\Fusion_EPOS_EmailReporter.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
log = logging.getLogger(__name__)

DURACELL_RECIPIENTS = ["aidan.harrington@synoviadigital.com"]


# =============================================================================
#  WINDOW CALCULATOR
# =============================================================================

def get_windows(now_naive: datetime):
    """
    Return three boundary datetimes (all naive UTC):

      prev_cutoff   — 14:00 UTC on the last working day before today
      today_cutoff  — 14:00 UTC today
      next_cutoff   — 14:00 UTC on the next working day after today

    These define:
      Closed batch : prev_cutoff  → today_cutoff
      Open batch   : today_cutoff → next_cutoff
    """
    today   = now_naive.date()
    weekday = today.weekday()   # Mon=0 … Sun=6

    # Previous working day (skip weekends)
    prev = today - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)

    # Next working day (skip weekends)
    nxt = today + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)

    # If today is weekend, treat Friday as today_cutoff base
    if weekday >= 5:
        days_back   = weekday - 4
        effective   = today - timedelta(days=days_back)
        prev_day    = effective - timedelta(days=1)
        while prev_day.weekday() >= 5:
            prev_day -= timedelta(days=1)
        prev_cutoff  = datetime(prev_day.year,  prev_day.month,  prev_day.day,  CUTOFF_HOUR, 0, 0)
        today_cutoff = datetime(effective.year, effective.month, effective.day, CUTOFF_HOUR, 0, 0)
        next_cutoff  = datetime(nxt.year, nxt.month, nxt.day, CUTOFF_HOUR, 0, 0)
    else:
        prev_cutoff  = datetime(prev.year,  prev.month,  prev.day,  CUTOFF_HOUR, 0, 0)
        today_cutoff = datetime(today.year, today.month, today.day, CUTOFF_HOUR, 0, 0)
        next_cutoff  = datetime(nxt.year,   nxt.month,   nxt.day,   CUTOFF_HOUR, 0, 0)

    return prev_cutoff, today_cutoff, next_cutoff


# =============================================================================
#  SQL
# =============================================================================

# Orders query — parameterised by (CUTOFF_HOUR, window_start, window_end)
SQL_ORDERS = """
SELECT
    w.OrderId,
    w.CustomerCode,
    MAX(w.CustomerName)                                   AS CustomerName,
    MAX(w.Address1)                                       AS Address1,
    MAX(w.Address2)                                       AS Address2,
    MAX(w.Address3)                                       AS Address3,
    w.Source_Filename,
    w.Fusion_Status,
    COUNT(*)                                              AS Line_Count,
    SUM(w.Price * w.Quantity)                             AS Order_Value,
    MIN(w.Load_Timestamp)                                 AS Load_Timestamp,

    -- Transfer Order Control
    MAX(tc.DynamicsDocumentNo)                            AS DynamicsDocumentNo,
    MAX(tc.MasterStatus)                                  AS MasterStatus,
    MAX(tc.SapStatus)                                     AS SapStatus,
    MAX(tc.StatusUpdatedAt)                               AS StatusUpdatedAt,
    MAX(tc.ShippingWarehouseId)                           AS ShippingWarehouse,
    MAX(tc.ReceivingWarehouseId)                          AS ReceivingWarehouse,
    MAX(CAST(tc.RequestedShippingDate AS VARCHAR(20)))    AS RequestedShippingDate,
    MAX(CAST(tc.RequestedReceiptDate  AS VARCHAR(20)))    AS RequestedReceiptDate,
    MAX(tc.LastError)                                     AS LastError,
    MAX(tc.RetryCount)                                    AS RetryCount,

    -- SAP Control
    MAX(sc.Status)                                        AS SAP_Status,
    MAX(sc.ErrorMessage)                                  AS SAP_ErrorMessage,
    MAX(sc.SentOn)                                        AS SAP_SentOn,

    -- SAP Header
    MAX(sh.SO_Number)                                     AS SAP_SO_Number,
    MAX(CAST(sh.Delivery_Date AS VARCHAR(30)))            AS SAP_Delivery_Date

FROM Raw.WASP_Orders w
LEFT JOIN EXC.TransferOrder_Control tc
       ON tc.SourceID = 'ORDER_' + CAST(w.OrderId AS VARCHAR(20))
LEFT JOIN INT.Transfer_Order_SAP_Control sc
       ON sc.TransferOrderNumber = tc.DynamicsDocumentNo
LEFT JOIN INT.Transfer_Order_SAP_Header sh
       ON sh.SO_Number = tc.DynamicsDocumentNo

WHERE w.Load_Timestamp >= ? AND w.Load_Timestamp < ?
  AND w.Order_Classification = 'Duracell_EPOS'
GROUP BY w.OrderId, w.CustomerCode, w.Source_Filename, w.Fusion_Status
ORDER BY MIN(w.Load_Timestamp)
"""

SQL_LINES = """
SELECT OrderId, ProductCode, ProductDescription,
       ProductCategory, ProductSubcategory,
       Quantity, Price, Price * Quantity AS Line_Value, FreeOfCharge
FROM Raw.WASP_Orders
WHERE Load_Timestamp >= ? AND Load_Timestamp < ?
  AND Order_Classification = 'Duracell_EPOS'
ORDER BY OrderId, ProductCategory, ProductCode
"""


# =============================================================================
#  BADGES
# =============================================================================

def mbadge(s):
    if not s:
        return '<span class="badge badge-amber">NOT RAISED</span>'
    m = {
        "SENT_DYNAMICS": ("badge-blue",  s),
        "VALIDATED":     ("badge-blue",  s),
        "SENT_SAP":      ("badge-green", s),
        "COMPLETED":     ("badge-green", s),
        "ERROR":         ("badge-red",   s),
        "FAILED":        ("badge-red",   s),
    }
    css, lbl = m.get(s.upper(), ("badge-blue", s))
    return f'<span class="badge {css}">{lbl}</span>'


def sbadge(s):
    if not s:
        return '<span class="badge badge-amber">PENDING</span>'
    if s.upper() == "SUCCESS":
        return '<span class="badge badge-green">SUCCESS</span>'
    if s.upper() in ("ERROR", "FAILED"):
        return '<span class="badge badge-red">ERROR</span>'
    return f'<span class="badge badge-blue">{s}</span>'


def fbadge(s):
    m = {
        "STO_Received":       ("badge-purple", "STO Received"),
        "Cutover":            ("badge-amber",  "Cutover"),
        "Filtered":           ("badge-amber",  "Pending"),
        "Loaded_to_Dynamics": ("badge-green",  "Loaded"),
    }
    css, lbl = m.get(s or "", ("badge-blue", s or "—"))
    return f'<span class="badge {css}">{lbl}</span>'


def ok_or_warn(value, ok_css="badge-green", warn_css="badge-red"):
    """Green badge if truthy, red if not."""
    return ok_css if value else warn_css


# =============================================================================
#  EMAIL
# =============================================================================

def send_duracell_email(cfg, subject: str, html_body: str) -> None:
    def _c(v): return v.strip().strip('"')
    em   = cfg["Nexus_Email"]
    host = _c(em["smtp_host"])
    port = int(_c(em["smtp_port"]))
    user = _c(em["smtp_user"])
    pwd  = _c(em["smtp_password"])
    msg  = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = user
    msg["To"]      = ", ".join(DURACELL_RECIPIENTS)
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    log.info("  Duracell email → %s", DURACELL_RECIPIENTS)
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.ehlo(); smtp.starttls(); smtp.login(user, pwd)
        smtp.sendmail(user, DURACELL_RECIPIENTS, msg.as_bytes())
    log.info("  Email sent ✓")


# =============================================================================
#  CLOSED BATCH TABLE
#  Orders that have passed the cutoff — TOs should exist in D365 + SAP
# =============================================================================

def closed_batch_table(orders, lines_by_order, delivery_date_label: str) -> str:
    if not orders:
        return """
<div class="section-head"><h2>Closed Batch — No Orders</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <tbody>
      <tr><td colspan="8" style="text-align:center;color:#9CA3AF;padding:24px">
        No Duracell orders in this batch window.
      </td></tr>
    </tbody>
  </table>
</div>"""

    rows_html = ""
    for o in orders:
        load_local  = utc_to_dublin(o.Load_Timestamp)
        val         = fmt_val(o.Order_Value)
        to_num      = o.DynamicsDocumentNo or "—"
        sap_so      = o.SAP_SO_Number      or "—"

        # Agreed delivery date: prefer SAP Delivery_Date, then RequestedReceiptDate
        raw_del = o.SAP_Delivery_Date or o.RequestedReceiptDate or ""
        if raw_del:
            # Parse various formats: "Apr  1 2026 12:00AM" / "2026-03-25" / "2026-04-01T00:00:00"
            agreed_del = "—"
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%b %d %Y %I:%M%p",
                        "%b  %d %Y %I:%M%p"):
                try:
                    agreed_del = datetime.strptime(raw_del.strip(), fmt).strftime("%a %d %b %Y")
                    break
                except ValueError:
                    continue
        else:
            agreed_del = "—"

        # Flag rows where TO is not confirmed
        is_error   = bool(o.LastError or (o.SAP_Status and o.SAP_Status.upper() == "ERROR"))
        not_raised = not o.DynamicsDocumentNo
        row_bg     = ('style="background:#FFF5F5"' if is_error
                      else 'style="background:#FFFDE7"' if not_raised else "")

        rows_html += f"""
        <tr {row_bg}>
          <td>
            <strong style="font-size:12px">{o.CustomerCode}</strong><br>
            <span style="color:#6B7280;font-size:10px">{o.CustomerName or ""}</span><br>
            <span style="color:#9CA3AF;font-size:10px">{o.Address1 or ""}{(", "+o.Address2) if o.Address2 else ""}</span>
          </td>
          <td>
            <code style="font-size:10px;color:#374151">{o.Source_Filename}</code><br>
            <span style="color:#9CA3AF;font-size:10px">Order {o.OrderId}</span>
          </td>
          <td class="num">{o.Line_Count}</td>
          <td class="num">{val}</td>
          <td>
            <code style="font-size:11px;color:#1565C0;font-weight:700">{to_num}</code><br>
            {mbadge(o.MasterStatus)}
          </td>
          <td>
            <code style="font-size:11px;color:#2E7D32;font-weight:700">{sap_so}</code><br>
            {sbadge(o.SAP_Status)}
            {"<br><span style='color:#C62828;font-size:10px'>"+str(o.SAP_ErrorMessage)+"</span>"
             if o.SAP_ErrorMessage else ""}
          </td>
          <td class="num" style="font-size:11px;color:#6B7280">{load_local}</td>
          <td class="num">
            <strong style="color:#1B3A5C;font-size:11px">{agreed_del}</strong>
          </td>
        </tr>"""

        # Product lines
        for ln in lines_by_order.get(o.OrderId, []):
            foc = (' <span class="badge badge-amber" style="font-size:9px">FOC</span>'
                   if ln.FreeOfCharge == 1 else "")
            rows_html += f"""
        <tr style="background:#F9F0FF">
          <td colspan="2" style="padding:4px 14px 4px 28px;font-size:10px;color:#6B7280">
            <span style="font-family:monospace;color:#374151">{ln.ProductCode}</span>
            &nbsp;{ln.ProductDescription}{foc}
          </td>
          <td class="num" style="padding:4px 14px;font-size:10px;color:#374151">{int(ln.Quantity):g}</td>
          <td class="num" style="padding:4px 14px;font-size:10px;color:#374151">{fmt_val(ln.Line_Value)}</td>
          <td colspan="4" style="padding:4px 14px;font-size:10px;color:#9CA3AF">
            {ln.ProductCategory or ""}{(" / "+ln.ProductSubcategory) if ln.ProductSubcategory else ""}
          </td>
        </tr>"""

    grp_lines = sum(o.Line_Count for o in orders)
    grp_val   = fmt_val(sum(float(o.Order_Value or 0) for o in orders))

    return f"""
<div class="section-head"><h2>Closed Batch — Transmitted to D365 &amp; SAP</h2></div>
<table width="100%" cellpadding="0" cellspacing="0" border="0">
  <tr><td style="padding:0 20px 8px">
    <table width="100%" cellpadding="9" cellspacing="0" border="0"
           style="background:#1565C0;border-radius:8px;
                  font-family:'Montserrat','Segoe UI',Arial,sans-serif;
                  font-size:11px;font-weight:600;color:#ffffff">
      <tr>
        <td>{len(orders)} order(s) &nbsp;·&nbsp; {grp_lines} lines &nbsp;·&nbsp; {grp_val}</td>
        <td style="text-align:right">
          Requested Delivery Date &nbsp;→&nbsp;
          <strong>{delivery_date_label}</strong>
        </td>
      </tr>
    </table>
  </td></tr>
</table>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>Customer</th>
        <th>File / Order</th>
        <th class="num">Lines</th>
        <th class="num">Value</th>
        <th>D365 Transfer Order</th>
        <th>SAP</th>
        <th class="num">Received</th>
        <th class="num">Delivery Date</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>"""


# =============================================================================
#  OPEN BATCH TABLE
#  Orders received after today's cutoff — awaiting next transmission window
# =============================================================================

def open_batch_table(orders, lines_by_order, delivery_date_label: str,
                     next_window_open: str) -> str:
    if not orders:
        return ""   # No open orders — omit section entirely

    rows_html = ""
    for o in orders:
        load_local = utc_to_dublin(o.Load_Timestamp)
        val        = fmt_val(o.Order_Value)

        rows_html += f"""
        <tr>
          <td>
            <strong style="font-size:12px">{o.CustomerCode}</strong><br>
            <span style="color:#6B7280;font-size:10px">{o.CustomerName or ""}</span><br>
            <span style="color:#9CA3AF;font-size:10px">{o.Address1 or ""}{(", "+o.Address2) if o.Address2 else ""}</span>
          </td>
          <td>
            <code style="font-size:10px;color:#374151">{o.Source_Filename}</code><br>
            <span style="color:#9CA3AF;font-size:10px">Order {o.OrderId}</span>
          </td>
          <td class="num">{o.Line_Count}</td>
          <td class="num">{val}</td>
          <td style="text-align:center">{fbadge(o.Fusion_Status)}</td>
          <td class="num" style="font-size:10px;color:#6B7280">{load_local}</td>
          <td class="num">
            <strong style="color:#1B3A5C;font-size:11px">{delivery_date_label}</strong>
          </td>
        </tr>"""

        # Product lines
        for ln in lines_by_order.get(o.OrderId, []):
            foc = (' <span class="badge badge-amber" style="font-size:9px">FOC</span>'
                   if ln.FreeOfCharge == 1 else "")
            rows_html += f"""
        <tr style="background:#EDE7F6">
          <td colspan="2" style="padding:4px 14px 4px 28px;font-size:10px;color:#6B7280">
            <span style="font-family:monospace;color:#374151">{ln.ProductCode}</span>
            &nbsp;{ln.ProductDescription}{foc}
          </td>
          <td class="num" style="padding:4px 14px;font-size:10px;color:#374151">{int(ln.Quantity):g}</td>
          <td class="num" style="padding:4px 14px;font-size:10px;color:#374151">{fmt_val(ln.Line_Value)}</td>
          <td colspan="3" style="padding:4px 14px;font-size:10px;color:#9CA3AF">
            {ln.ProductCategory or ""}{(" / "+ln.ProductSubcategory) if ln.ProductSubcategory else ""}
          </td>
        </tr>"""

    grp_lines = sum(o.Line_Count for o in orders)
    grp_val   = fmt_val(sum(float(o.Order_Value or 0) for o in orders))

    return f"""
<div class="section-head"><h2>Open Batch — Awaiting Next Transmission Window</h2></div>
<table width="100%" cellpadding="0" cellspacing="0" border="0">
  <tr><td style="padding:0 20px 8px">
    <table width="100%" cellpadding="9" cellspacing="0" border="0"
           style="background:#6A1B9A;border-radius:8px;
                  font-family:'Montserrat','Segoe UI',Arial,sans-serif;
                  font-size:11px;font-weight:600;color:#ffffff">
      <tr>
        <td>{len(orders)} order(s) &nbsp;·&nbsp; {grp_lines} lines &nbsp;·&nbsp; {grp_val}</td>
        <td style="text-align:right">
          Transmission window opens &nbsp;→&nbsp; <strong>{next_window_open}</strong>
          &nbsp;·&nbsp; Requested Delivery &nbsp;→&nbsp; <strong>{delivery_date_label}</strong>
        </td>
      </tr>
    </table>
  </td></tr>
</table>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr>
        <th>Customer</th>
        <th>File / Order</th>
        <th class="num">Lines</th>
        <th class="num">Value</th>
        <th>Fusion Status</th>
        <th class="num">Received</th>
        <th class="num">Requested Delivery</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>"""


# =============================================================================
#  REPORT
# =============================================================================

def run(conn: pyodbc.Connection, cfg) -> None:
    now_utc          = utc_now().replace(tzinfo=None)
    dublin_dt, tz_label = dublin_now()
    logo             = logo_base64()
    cur              = conn.cursor()

    prev_cutoff, today_cutoff, next_cutoff = get_windows(now_utc)

    log.info("  Prev cutoff  : %s UTC", prev_cutoff.strftime("%Y-%m-%d %H:%M"))
    log.info("  Today cutoff : %s UTC", today_cutoff.strftime("%Y-%m-%d %H:%M"))
    log.info("  Next cutoff  : %s UTC", next_cutoff.strftime("%Y-%m-%d %H:%M"))

    # Delivery dates (from today's perspective)
    today         = now_utc.date()
    closed_del_d  = next_working_day(today, 3)   # closed batch → Day+3
    open_del_d    = next_working_day(today, 4)   # open batch   → Day+4
    closed_del    = closed_del_d.strftime("%A %d %b %Y")
    open_del      = open_del_d.strftime("%A %d %b %Y")
    next_win_open = utc_to_dublin_full(next_cutoff)

    # ── Closed batch orders (prev_cutoff → today_cutoff) ──
    cur.execute(SQL_ORDERS, (prev_cutoff, today_cutoff))
    closed_orders = cur.fetchall()

    cur.execute(SQL_LINES, (prev_cutoff, today_cutoff))
    closed_lines: dict = {}
    for ln in cur.fetchall():
        closed_lines.setdefault(ln.OrderId, []).append(ln)

    # ── Open batch orders (today_cutoff → next_cutoff) ──
    cur.execute(SQL_ORDERS, (today_cutoff, next_cutoff))
    open_orders = cur.fetchall()

    cur.execute(SQL_LINES, (today_cutoff, next_cutoff))
    open_lines: dict = {}
    for ln in cur.fetchall():
        open_lines.setdefault(ln.OrderId, []).append(ln)

    # ── Summary counts ──
    total_orders  = len(closed_orders) + len(open_orders)
    total_lines   = (sum(o.Line_Count for o in closed_orders) +
                     sum(o.Line_Count for o in open_orders))
    tos_raised    = sum(1 for o in closed_orders if o.DynamicsDocumentNo)
    sap_confirmed = sum(1 for o in closed_orders
                        if o.SAP_Status and o.SAP_Status.upper() == "SUCCESS")
    errors        = sum(1 for o in closed_orders
                        if o.LastError or
                        (o.SAP_Status and o.SAP_Status.upper() == "ERROR"))
    not_raised    = sum(1 for o in closed_orders if not o.DynamicsDocumentNo)

    # ── Tiles ──
    tiles = (
        tile_row([
            {"value": str(len(closed_orders)), "label": "Closed Batch",    "colour": "#1565C0",
             "sub": f"Del: {closed_del}"},
            {"value": str(len(open_orders)),   "label": "Open Batch",      "colour": "#6A1B9A",
             "sub": f"Del: {open_del}"},
            {"value": str(total_orders),       "label": "Total Orders",    "colour": "#8E24AA"},
            {"value": f"{total_lines:,}",      "label": "Total Lines",     "colour": "#8E24AA"},
        ])
        + tile_row([
            {"value": str(tos_raised),    "label": "TOs in D365",
             "colour": "#43A047" if tos_raised == len(closed_orders) else "#FB8C00",
             "value_colour": "#2E7D32" if tos_raised == len(closed_orders) else "#E65100"},
            {"value": str(sap_confirmed), "label": "SAP Confirmed",
             "colour": "#43A047" if sap_confirmed == len(closed_orders) else "#FB8C00",
             "value_colour": "#2E7D32" if sap_confirmed == len(closed_orders) else "#E65100"},
            {"value": str(not_raised),    "label": "Not Yet Raised",
             "colour": "#FB8C00" if not_raised else "#43A047",
             "value_colour": "#E65100" if not_raised else "#2E7D32"},
            {"value": str(errors),        "label": "Errors",
             "colour": "#E53935" if errors else "#43A047",
             "value_colour": "#C62828" if errors else "#2E7D32"},
        ])
    )

    # ── Alert ──
    if errors:
        banner = alert_warn(
            f"{errors} Transfer Order(s) have errors in the closed batch — "
            "see error table below."
        )
    elif not_raised:
        banner = alert_warn(
            f"{not_raised} order(s) in the closed batch have not yet been raised "
            "as Transfer Orders. Processing may still be in progress."
        )
    else:
        banner = alert_ok(
            f"All {len(closed_orders)} closed-batch Transfer Order(s) confirmed by SAP. "
            f"Delivery: {closed_del}."
        )

    # ── Window context box ──
    window_box = (
        '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        '<tr><td style="padding:12px 20px 0">'
        '<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        '<tr>'
        # Left cell — closed batch
        '<td width="50%" style="padding:0 6px 0 0;vertical-align:top">'
        '<table width="100%" cellpadding="11" cellspacing="0" border="0" '
        'style="background:#E3F2FD;border-radius:8px;border-left:4px solid #1565C0;'
        'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;'
        'font-size:11px;color:#0D47A1;font-weight:500;line-height:1.8">'
        f'<tr><td>'
        f'<strong style="font-size:12px">📦 Closed Batch</strong><br>'
        f'Window: {utc_to_dublin_full(prev_cutoff)} → {utc_to_dublin_full(today_cutoff)}<br>'
        f'Requested Delivery: <strong>{closed_del}</strong><br>'
        f'Status: TO raised in D365, transmitted to SAP'
        f'</td></tr></table></td>'
        # Right cell — open batch
        '<td width="50%" style="padding:0 0 0 6px;vertical-align:top">'
        '<table width="100%" cellpadding="11" cellspacing="0" border="0" '
        'style="background:#F3E5F5;border-radius:8px;border-left:4px solid #6A1B9A;'
        'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;'
        'font-size:11px;color:#4A148C;font-weight:500;line-height:1.8">'
        f'<tr><td>'
        f'<strong style="font-size:12px">🕐 Open Batch</strong><br>'
        f'Window: {utc_to_dublin_full(today_cutoff)} → {next_win_open}<br>'
        f'Requested Delivery: <strong>{open_del}</strong><br>'
        f'Status: Staged — awaiting transmission'
        f'</td></tr></table></td>'
        '</tr></table></td></tr></table>'
    )

    # ── Error table ──
    err_orders = [o for o in closed_orders if
                  o.LastError or (o.SAP_Status and o.SAP_Status.upper() == "ERROR")]
    if err_orders:
        err_tr = "".join(f"""
            <tr>
              <td><code style="font-size:11px">{e.DynamicsDocumentNo or "—"}</code></td>
              <td>{e.CustomerCode}<br>
                  <span style="color:#9CA3AF;font-size:10px">{e.CustomerName or ""}</span></td>
              <td>{mbadge(e.MasterStatus)}</td>
              <td>{sbadge(e.SAP_Status)}</td>
              <td style="color:#C62828;font-size:11px">
                {e.LastError or e.SAP_ErrorMessage or "—"}
              </td>
              <td class="num">{int(e.RetryCount or 0)}</td>
            </tr>""" for e in err_orders)
        error_section = f"""
<div class="section-head"><h2>⚠ Transfer Order Errors</h2></div>
<div class="table-wrap">
  <table class="data-table">
    <thead>
      <tr><th>TO Number</th><th>Customer</th><th>D365 Status</th>
          <th>SAP Status</th><th>Error</th><th class="num">Retries</th></tr>
    </thead>
    <tbody>{err_tr}</tbody>
  </table>
</div>"""
    else:
        error_section = ""

    body = (
        tiles
        + banner
        + window_box
        + closed_batch_table(closed_orders, closed_lines, closed_del)
        + open_batch_table(open_orders, open_lines, open_del, next_win_open)
        + error_section
        + '<div style="height:20px"></div>'
    )

    dublin_disp = f"{dublin_dt.strftime('%H:%M')} {tz_label}"
    utc_disp    = now_utc.strftime("%H:%M UTC")
    subject     = (
        f"Fusion EPOS | Duracell STO | "
        f"{dublin_dt.strftime('%d %b %Y')} · {dublin_disp}"
    )
    subtitle    = (
        f"Duracell Transfer Orders · {dublin_dt.strftime('%A %d %B %Y')} · "
        f"{dublin_disp}  ({utc_disp})"
    )
    send_duracell_email(cfg, subject,
                        html_wrapper(logo, "Duracell STO Report", subtitle, body))


# =============================================================================
#  ENTRY POINT
# =============================================================================

def main() -> None:
    now_utc = utc_now().replace(tzinfo=None)
    dublin_dt, tz_label = dublin_now()
    log.info("=" * 70)
    log.info("  Synovia Fusion – EPOS Duracell STO Report  v3.0.0")
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
