# =============================================================================
#  Synovia Fusion – EPOS  |  STO Operations Report
# -----------------------------------------------------------------------------
#  Module:        Fusion EPOS — Transfer Order Pipeline
#  Script Name:   Report_STO_Operations.py
#
#  Version:       2.1.0
#  Release Date:  2026-03-31
#
#  Author:        Synovia Digital
#
# -----------------------------------------------------------------------------
#  Column naming conventions confirmed from INFORMATION_SCHEMA:
#
#  CreatedAt    : EXC.TransferOrder_Control
#                 EXC.STO_Order_Audit_Log      (LoggedAt)
#                 EXC.STO_Processing_Log       (LoggedAt)
#                 EXC.TransferOrder_InterfaceLog
#                 INT.TransferOrderHeader
#                 INT.TransferOrderLine
#                 STG.WASP_STO_Scheduled
#
#  CreatedAtUtc : INT.Transfer_Order_SAP_Header
#                 INT.Transfer_Order_SAP_Item
#                 INT.Transfer_Order_SAP_Note
#                 INT.Transfer_Order_SAP_Control
#                 INT.Transfer_Order_SAP_Request_Log
#                 INT.Transfer_Order_SAP_Response_Log
#
#  Note: INT.Transfer_Order_SAP_Header has NO CreatedAt/CreatedAtUtc
#        date filter by design — scope via EXC.TransferOrder_Control.CreatedAt
#        joined through DynamicsDocumentNo = SO_Number.
# =============================================================================

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyodbc

sys.path.insert(0, str(Path(__file__).parent))
from reporting_shared import (
    CONFIG_FILE, load_config, get_db_connection, logo_base64,
    tile_row, html_wrapper, alert_ok, alert_warn,
    status_badge, pct_bar,
    dublin_now, utc_now, utc_to_dublin, utc_to_dublin_full,
)

LOG_FILE = Path(r"D:\FusionHub\Logs\Fusion_EPOS_EmailReporter.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
log = logging.getLogger(__name__)

RECIPIENT = "aidan.harrington@synoviadigital.com"

# =============================================================================
#  SQL — STEP 1: INGEST
#  RAW.EPOS_TransferRequest  — date col: LoadTimestamp
#  LOG.EPOS_IngestionSummary — date col: LoadTimestamp
# =============================================================================

SQL_INGEST_SUMMARY = """
SELECT
    COUNT(DISTINCT r.OrderId)                                       AS Orders_Received,
    COUNT(*)                                                        AS Lines_Received,
    COUNT(DISTINCT r.CustomerCode)                                  AS Unique_Customers,
    MIN(r.LoadTimestamp)                                            AS First_Received,
    MAX(r.LoadTimestamp)                                            AS Last_Received,
    SUM(CASE WHEN r.SynProcessed = 0
              AND r.SynStaged    = 0 THEN 1 ELSE 0 END)            AS Lines_Awaiting_Schedule,
    SUM(CASE WHEN r.SynStaged    = 1 THEN 1 ELSE 0 END)            AS Lines_Staged,
    SUM(CASE WHEN r.SynProcessed = 1 THEN 1 ELSE 0 END)            AS Lines_Removed
FROM RAW.EPOS_TransferRequest r
WHERE CAST(r.LoadTimestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_INGEST_FILES = """
SELECT
    i.LoadFileName,
    i.LoadTimestamp,
    i.TotalRows,
    i.SuccessfulRows,
    i.FailedRows,
    i.Status,
    i.ErrorMessage
FROM LOG.EPOS_IngestionSummary i
WHERE CAST(i.LoadTimestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
ORDER BY i.LoadTimestamp DESC
"""

SQL_INGEST_ORDERS = """
SELECT
    r.OrderId,
    MAX(r.CustomerName)                     AS CustomerName,
    MAX(r.CustomerCode)                     AS CustomerCode,
    MAX(r.OrderDate)                        AS OrderDate,
    MAX(r.DateRequired)                     AS DateRequired,
    COUNT(*)                                AS LineCount,
    MAX(CAST(r.SynStaged    AS TINYINT))    AS SynStaged,
    MAX(CAST(r.SynProcessed AS TINYINT))    AS SynProcessed,
    MAX(r.LoadTimestamp)                    AS LoadTimestamp
FROM RAW.EPOS_TransferRequest r
WHERE CAST(r.LoadTimestamp AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY r.OrderId
ORDER BY MAX(r.LoadTimestamp) DESC
"""

# =============================================================================
#  SQL — STEP 2: SCHEDULING
#  STG.WASP_STO_Scheduled    — date col: CreatedAt
#  EXC.STO_Order_Audit_Log   — date col: LoggedAt
# =============================================================================

SQL_SCHEDULE_SUMMARY = """
SELECT
    COUNT(*)                                                            AS Total_Evaluated,
    SUM(CASE WHEN s.ScheduledDeliveryDate IS NOT NULL
              AND s.DateChanged = 0 THEN 1 ELSE 0 END)                AS Date_Confirmed,
    SUM(CASE WHEN s.ScheduledDeliveryDate IS NOT NULL
              AND s.DateChanged = 1 THEN 1 ELSE 0 END)                AS Date_Rescheduled,
    SUM(CASE WHEN s.ScheduledDeliveryDate IS NULL THEN 1 ELSE 0 END)  AS Orders_Held,
    MIN(s.ScheduledDeliveryDate)                                       AS Earliest_Delivery,
    MAX(s.ScheduledDeliveryDate)                                       AS Latest_Delivery
FROM STG.WASP_STO_Scheduled s
WHERE CAST(s.CreatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_SCHEDULE_ORDERS = """
SELECT
    s.OrderId,
    s.CustomerName,
    s.CustomerCode,
    s.OriginalDateRequired,
    s.ScheduledDeliveryDate,
    s.ScheduledShippingDate,
    s.DateChanged,
    s.ChangeReason,
    s.RuleApplied,
    s.SynProcessed,
    s.CreatedAt
FROM STG.WASP_STO_Scheduled s
WHERE CAST(s.CreatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
ORDER BY s.CreatedAt DESC
"""

SQL_AUDIT_PRODUCT_EVENTS = """
SELECT
    a.EventType,
    COUNT(DISTINCT a.OrderId)       AS AffectedOrders,
    COUNT(*)                        AS EventCount,
    COUNT(DISTINCT a.ProductCode)   AS UniqueProducts
FROM EXC.STO_Order_Audit_Log a
WHERE CAST(a.LoggedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
  AND a.EventType IN (
      'PRODUCT_APPROVED','PRODUCT_REMOVED','PRODUCT_PASSTHROUGH',
      'CUSTOMER_APPROVED','CUSTOMER_HELD','ORDER_HELD')
GROUP BY a.EventType
ORDER BY a.EventType
"""

SQL_HELD_ORDERS = """
SELECT
    a.OrderId,
    a.CustomerCode,
    a.CustomerName,
    a.EventType,
    a.ChangeReason,
    a.LoggedAt
FROM EXC.STO_Order_Audit_Log a
WHERE CAST(a.LoggedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
  AND a.EventType IN ('CUSTOMER_HELD','ORDER_HELD')
ORDER BY a.LoggedAt DESC
"""

SQL_REMOVED_PRODUCTS = """
SELECT
    a.OrderId,
    a.CustomerName,
    a.ProductCode,
    a.ProductCategory,
    a.ProductSubcategory,
    a.ProductDescription,
    a.ChangeReason,
    a.LoggedAt
FROM EXC.STO_Order_Audit_Log a
WHERE CAST(a.LoggedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
  AND a.EventType = 'PRODUCT_REMOVED'
ORDER BY a.OrderId, a.ProductCode
"""

SQL_PASSTHROUGH_PRODUCTS = """
SELECT
    a.OrderId,
    a.CustomerName,
    a.ProductCode,
    a.ProductDescription,
    a.ProductCategory,
    a.ProductSubcategory,
    a.LoggedAt
FROM EXC.STO_Order_Audit_Log a
WHERE CAST(a.LoggedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
  AND a.EventType = 'PRODUCT_PASSTHROUGH'
ORDER BY a.OrderId, a.ProductCode
"""

# =============================================================================
#  SQL — STEPS 3 & 4: CONTROL / D365
#  EXC.TransferOrder_Control — date col: CreatedAt
#  INT.TransferOrderHeader   — date col: CreatedAt
#  INT.TransferOrderLine     — date col: CreatedAt
# =============================================================================

SQL_CONTROL_SUMMARY = """
SELECT
    COUNT(*)                                                            AS Total,
    SUM(CASE WHEN c.MasterStatus = 'VALIDATED'      THEN 1 ELSE 0 END) AS Staged_Ready,
    SUM(CASE WHEN c.MasterStatus = 'SENT_DYNAMICS'  THEN 1 ELSE 0 END) AS In_D365,
    SUM(CASE WHEN c.MasterStatus = 'SENT_SAP'       THEN 1 ELSE 0 END) AS In_SAP,
    SUM(CASE WHEN c.MasterStatus = 'ERROR'          THEN 1 ELSE 0 END) AS Errored,
    SUM(CASE WHEN c.ReadyForProcessing = 0          THEN 1 ELSE 0 END) AS Not_Ready,
    COUNT(DISTINCT c.DynamicsDocumentNo)                                AS D365_TOs_Created,
    SUM(CASE WHEN c.DynamicsResponseCode = 'OK'     THEN 1 ELSE 0 END) AS D365_Confirmed
FROM EXC.TransferOrder_Control c
WHERE CAST(c.CreatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_CONTROL_ORDERS = """
SELECT
    c.TransferControlId,
    c.OrderId,
    c.CustomerCode,
    c.MasterStatus,
    c.ReadyForProcessing,
    c.ValidationMessage,
    c.DynamicsDocumentNo,
    c.DynamicsResponseCode,
    c.SapStatus,
    c.RequestedShippingDate,
    c.RequestedReceiptDate,
    c.StatusUpdatedAt,
    c.LastError,
    h.LineCount,
    h.TotalQty
FROM EXC.TransferOrder_Control c
LEFT JOIN (
    SELECT
        l.TransferControlId,
        COUNT(*)        AS LineCount,
        SUM(l.Quantity) AS TotalQty
    FROM INT.TransferOrderLine l
    GROUP BY l.TransferControlId
) h ON h.TransferControlId = c.TransferControlId
WHERE CAST(c.CreatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
ORDER BY c.TransferControlId DESC
"""

# =============================================================================
#  SQL — STEP 5: SAP STAGING
#  INT.Transfer_Order_SAP_Header  — date col: CreatedAtUtc
#  INT.Transfer_Order_SAP_Item    — date col: CreatedAtUtc
#  Scoped to today via EXC.TransferOrder_Control.CreatedAt
#  joined on DynamicsDocumentNo = SO_Number
# =============================================================================

SQL_SAP_STAGING_SUMMARY = """
SELECT
    COUNT(DISTINCT sh.TransferOrderSAPHeader_ID)                        AS Total_Staged,
    SUM(CASE WHEN sh.SynProcessed = 0 THEN 1 ELSE 0 END)               AS Awaiting_CPI,
    SUM(CASE WHEN sh.SynProcessed = 1 THEN 1 ELSE 0 END)               AS Sent_To_SAP,
    (
        SELECT COUNT(*)
        FROM INT.Transfer_Order_SAP_Item si
        WHERE si.TransferOrderSAPHeader_ID IN (
            SELECT sh2.TransferOrderSAPHeader_ID
            FROM INT.Transfer_Order_SAP_Header sh2
            INNER JOIN EXC.TransferOrder_Control c2
                ON c2.DynamicsDocumentNo = sh2.SO_Number
            WHERE CAST(c2.CreatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
        )
    )                                                                   AS Total_Items,
    MIN(sh.Delivery_Date)                                               AS Earliest_Delivery,
    MAX(sh.Delivery_Date)                                               AS Latest_Delivery
FROM INT.Transfer_Order_SAP_Header sh
INNER JOIN EXC.TransferOrder_Control c
    ON c.DynamicsDocumentNo = sh.SO_Number
WHERE CAST(c.CreatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_SAP_STAGING_ORDERS = """
SELECT
    sh.TransferOrderSAPHeader_ID,
    sh.SO_Number,
    sh.Customer_Code,
    sh.Document_Date,
    sh.Delivery_Date,
    sh.SynProcessed,
    sh.CreatedAtUtc,
    c.OrderId,
    c.MasterStatus,
    c.SapStatus,
    COUNT(si.TransferOrderSAPItem_ID)   AS ItemCount,
    SUM(si.Quantity)                    AS TotalQty
FROM INT.Transfer_Order_SAP_Header sh
INNER JOIN EXC.TransferOrder_Control c
    ON c.DynamicsDocumentNo = sh.SO_Number
LEFT JOIN INT.Transfer_Order_SAP_Item si
    ON si.TransferOrderSAPHeader_ID = sh.TransferOrderSAPHeader_ID
WHERE CAST(c.CreatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY
    sh.TransferOrderSAPHeader_ID, sh.SO_Number, sh.Customer_Code,
    sh.Document_Date, sh.Delivery_Date, sh.SynProcessed, sh.CreatedAtUtc,
    c.OrderId, c.MasterStatus, c.SapStatus
ORDER BY sh.TransferOrderSAPHeader_ID DESC
"""

# =============================================================================
#  SQL — STEP 6: SAP CPI TRANSMISSION
#  INT.Transfer_Order_SAP_Control      — date col: CreatedAtUtc, SentOn
#  INT.Transfer_Order_SAP_Request_Log  — date col: CreatedAtUtc
#  INT.Transfer_Order_SAP_Response_Log — date col: CreatedAtUtc
# =============================================================================

SQL_CPI_SUMMARY = """
SELECT
    COUNT(*)                                                            AS Total_Transmitted,
    SUM(CASE WHEN sc.Status = 'Success' THEN 1 ELSE 0 END)             AS Succeeded,
    SUM(CASE WHEN sc.Status = 'Error'   THEN 1 ELSE 0 END)             AS Failed,
    SUM(CASE WHEN sc.Status = 'Pending' THEN 1 ELSE 0 END)             AS Pending,
    MIN(sc.SentOn)                                                      AS First_Sent,
    MAX(sc.SentOn)                                                      AS Last_Sent
FROM INT.Transfer_Order_SAP_Control sc
WHERE CAST(sc.CreatedAtUtc AS DATE) = CAST(GETUTCDATE() AS DATE)
"""

SQL_CPI_ORDERS = """
SELECT
    sc.SAPControlID,
    sc.TransferOrderNumber,
    sc.CustomerCode,
    sc.Status,
    sc.ErrorMessage,
    sc.SentOn,
    rl.HTTPStatus,
    rl.ErrorMessage1,
    rq.Delivery_Date
FROM INT.Transfer_Order_SAP_Control sc
LEFT JOIN INT.Transfer_Order_SAP_Response_Log rl
    ON rl.SAPControlID = sc.SAPControlID
LEFT JOIN INT.Transfer_Order_SAP_Request_Log rq
    ON rq.SAPControlID = sc.SAPControlID
WHERE CAST(sc.CreatedAtUtc AS DATE) = CAST(GETUTCDATE() AS DATE)
ORDER BY sc.SAPControlID DESC
"""

SQL_CPI_ERRORS = """
SELECT
    sc.TransferOrderNumber,
    sc.CustomerCode,
    sc.ErrorMessage         AS ControlError,
    rl.HTTPStatus,
    rl.ErrorMessage1,
    rl.ErrorMessage2,
    rl.ErrorMessage3,
    sc.SentOn
FROM INT.Transfer_Order_SAP_Control sc
LEFT JOIN INT.Transfer_Order_SAP_Response_Log rl
    ON rl.SAPControlID = sc.SAPControlID
WHERE sc.Status = 'Error'
  AND CAST(sc.CreatedAtUtc AS DATE) = CAST(GETUTCDATE() AS DATE)
ORDER BY sc.SentOn DESC
"""

# =============================================================================
#  SQL — PROCESSING LOG
#  EXC.STO_Processing_Log — date col: LoggedAt
# =============================================================================

SQL_PROCESSING_LOG_SUMMARY = """
SELECT
    p.ProcessCode,
    p.StepCode,
    p.Status,
    COUNT(*)            AS EventCount,
    MAX(p.LoggedAt)     AS LastEvent
FROM EXC.STO_Processing_Log p
WHERE CAST(p.LoggedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
GROUP BY p.ProcessCode, p.StepCode, p.Status
ORDER BY p.ProcessCode, p.StepCode, p.Status
"""

# =============================================================================
#  HTML HELPERS
# =============================================================================

def _section(title: str, icon: str, content: str) -> str:
    return f"""
<div style="margin:0 0 28px 0">
  <div style="background:#1E293B;border-radius:8px 8px 0 0;
              padding:13px 20px;display:flex;align-items:center;gap:10px">
    <span style="font-size:20px">{icon}</span>
    <h2 style="margin:0;color:#F1F5F9;font-size:13px;font-weight:700;
               font-family:'Montserrat','Segoe UI',Arial,sans-serif;
               letter-spacing:0.6px;text-transform:uppercase">{title}</h2>
  </div>
  <div style="border:1px solid #E2E8F0;border-top:none;
              border-radius:0 0 8px 8px;overflow:hidden">
    {content}
  </div>
</div>"""


def _kpi_row(items: list) -> str:
    cells = ""
    for item in items:
        vc  = item.get("val_colour", "#1E293B")
        sub = item.get("sub", "")
        bg  = item.get("bg", "transparent")
        cells += f"""
      <td style="text-align:center;padding:18px 10px;
                 border-right:1px solid #E2E8F0;
                 vertical-align:top;background:{bg}">
        <div style="font-size:30px;font-weight:800;color:{vc};
                    font-family:'Montserrat','Segoe UI',Arial,sans-serif;
                    line-height:1">{item['value']}</div>
        <div style="font-size:10px;color:#64748B;font-weight:700;
                    margin-top:5px;text-transform:uppercase;
                    letter-spacing:0.6px">{item['label']}</div>
        {f'<div style="font-size:10px;color:#94A3B8;margin-top:3px">{sub}</div>' if sub else ''}
      </td>"""
    return f"""
  <table width="100%" cellpadding="0" cellspacing="0"
         style="border-collapse:collapse">
    <tr>{cells}</tr>
  </table>"""


def _sub_heading(text: str, colour: str = "#64748B") -> str:
    return (f'<div style="padding:14px 18px 4px">'
            f'<strong style="font-size:11px;color:{colour};'
            f'text-transform:uppercase;letter-spacing:0.5px">'
            f'{text}</strong></div>')


def _table(headers: list, rows_html: str, empty_msg: str = "No data") -> str:
    ths = "".join(
        f'<th style="background:#F8FAFC;padding:9px 14px;'
        f'text-align:{"right" if h.get("num") else "left"};'
        f'font-size:11px;color:#64748B;font-weight:700;'
        f'border-bottom:2px solid #E2E8F0;white-space:nowrap">'
        f'{h["label"]}</th>'
        for h in headers
    )
    body = rows_html or (
        f'<tr><td colspan="{len(headers)}" '
        f'style="text-align:center;color:#9CA3AF;padding:24px;'
        f'font-size:12px">{empty_msg}</td></tr>'
    )
    return f"""
  <div style="overflow-x:auto">
    <table width="100%" cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;font-size:12px;
                  font-family:'Segoe UI',Arial,sans-serif">
      <thead><tr>{ths}</tr></thead>
      <tbody>{body}</tbody>
    </table>
  </div>"""


def _td(val, align="left", mono=False, colour=None, bold=False, wrap=False):
    ws = "normal" if wrap else "nowrap"
    style = (f'padding:9px 14px;border-bottom:1px solid #F1F5F9;'
             f'text-align:{align};white-space:{ws};'
             + (f'color:{colour};'  if colour else '')
             + ('font-weight:700;' if bold   else '')
             + ('font-family:monospace;font-size:11px;' if mono else ''))
    return f'<td style="{style}">{val if val is not None else "—"}</td>'


def _pill(status):
    colours = {
        "SENT_DYNAMICS": ("#D1FAE5","#065F46"), "SENT_SAP":    ("#D1FAE5","#065F46"),
        "SAP_STAGED":    ("#EDE9FE","#5B21B6"), "VALIDATED":   ("#DBEAFE","#1E40AF"),
        "ERROR":         ("#FEE2E2","#991B1B"), "NEW":         ("#F3F4F6","#374151"),
        "SUCCESS":       ("#D1FAE5","#065F46"), "PARTIAL":     ("#FEF3C7","#92400E"),
        "FAILED":        ("#FEE2E2","#991B1B"), "HELD":        ("#FEE2E2","#991B1B"),
        "CONFIRMED":     ("#D1FAE5","#065F46"), "RESCHEDULED": ("#FEF3C7","#92400E"),
        "PASSTHROUGH":   ("#EDE9FE","#5B21B6"), "REMOVED":     ("#FEE2E2","#991B1B"),
        "APPROVED":      ("#D1FAE5","#065F46"), "PENDING":     ("#FEF3C7","#92400E"),
        "SENT":          ("#D1FAE5","#065F46"),
    }
    bg, fg = colours.get(str(status).upper(), ("#F3F4F6","#374151"))
    return (f'<span style="background:{bg};color:{fg};padding:3px 9px;'
            f'border-radius:4px;font-size:10px;font-weight:700;'
            f'white-space:nowrap">{status}</span>')


def _yn(val):
    if val in (1, True, "1"):
        return '<span style="color:#059669;font-weight:700;font-size:14px">✓</span>'
    return '<span style="color:#DC2626;font-weight:700;font-size:14px">✗</span>'


def _alert(msg: str, kind: str = "ok") -> str:
    bg  = "#F0FDF4" if kind == "ok"   else "#FFFBEB" if kind == "warn" else "#FEF2F2"
    bc  = "#86EFAC" if kind == "ok"   else "#FDE68A" if kind == "warn" else "#FECACA"
    col = "#166534" if kind == "ok"   else "#92400E" if kind == "warn" else "#991B1B"
    ico = "✅"      if kind == "ok"   else "⚠️"       if kind == "warn" else "❌"
    return (f'<div style="margin:12px 16px;padding:12px 16px;'
            f'background:{bg};border:1px solid {bc};border-radius:6px;'
            f'color:{col};font-size:12px;font-weight:500">'
            f'{ico}&nbsp; {msg}</div>')


# =============================================================================
#  SECTION BUILDERS
# =============================================================================

def build_pipeline_summary(ing, sch, ctrl, sap, cpi) -> str:
    orders_in    = ing.Orders_Received  if ing  else 0
    scheduled    = sch.Total_Evaluated  if sch  else 0
    held         = sch.Orders_Held      if sch  else 0
    in_d365      = ctrl.In_D365         if ctrl else 0
    in_sap       = ctrl.In_SAP          if ctrl else 0
    sent_cpi     = cpi.Succeeded        if cpi  else 0
    cpi_errors   = cpi.Failed           if cpi  else 0
    ctrl_errors  = ctrl.Errored         if ctrl else 0
    sap_staged   = sap.Total_Staged     if sap  else 0
    total_errors = (ctrl_errors or 0) + (cpi_errors or 0)

    def _step(num, label, value, colour, arrow=True):
        return f"""
      <td style="text-align:center;vertical-align:top;padding:8px 5px">
        <div style="background:{colour};border-radius:10px;
                    padding:13px 16px;display:inline-block;min-width:80px;
                    box-shadow:0 2px 8px rgba(0,0,0,0.12)">
          <div style="font-size:9px;color:rgba(255,255,255,0.75);font-weight:700;
                      text-transform:uppercase;letter-spacing:0.8px">Step {num}</div>
          <div style="font-size:26px;font-weight:900;color:#fff;
                      line-height:1.1;margin:5px 0">{value}</div>
          <div style="font-size:10px;color:rgba(255,255,255,0.9);
                      font-weight:600">{label}</div>
        </div>
        {"<span style='font-size:18px;color:#94A3B8;vertical-align:middle;margin:0 2px'>→</span>" if arrow else ""}
      </td>"""

    flow = f"""
  <div style="overflow-x:auto;padding:8px 0">
  <table cellpadding="0" cellspacing="0" style="margin:8px auto">
    <tr>
      {_step(1, "Received",   orders_in,  "#1E88E5")}
      {_step(2, "Scheduled",  scheduled,  "#8E24AA")}
      {_step(3, "→ D365",     in_d365,    "#0288D1")}
      {_step(4, "SAP Staged", sap_staged, "#5E35B1")}
      {_step(5, "Sent SAP",   sent_cpi,   "#2E7D32")}
      {_step(6, "Held",       held,
             "#E53935" if held else "#9E9E9E", arrow=False)}
    </tr>
  </table>
  </div>"""

    if total_errors:
        banner = _alert(
            f"{total_errors} order(s) encountered errors today — "
            f"D365 errors: {ctrl_errors}  |  SAP CPI errors: {cpi_errors}. "
            f"Check sections below.", "error")
    elif held:
        banner = _alert(
            f"{held} order(s) held during scheduling — "
            f"see Held Orders section below.", "warn")
    else:
        banner = _alert(
            "All orders processed cleanly — no errors or holds today.", "ok")

    content = flow + '<div style="padding:0 8px 8px">' + banner + "</div>"
    return _section("Today's Pipeline — At a Glance", "🔭", content)


def build_ingest_section(summary, files, orders) -> str:
    if not summary:
        return _section("Step 1 — Inbound Orders Received", "📥",
                        '<p style="padding:16px;color:#9CA3AF">No inbound data today.</p>')
    s = summary[0]
    kpis = _kpi_row([
        {"value": str(s.Orders_Received or 0),
         "label": "Orders Received",        "val_colour": "#1E88E5"},
        {"value": f"{s.Lines_Received or 0:,}",
         "label": "Product Lines",          "val_colour": "#1E88E5"},
        {"value": str(s.Unique_Customers or 0),
         "label": "Unique Stores",          "val_colour": "#00897B"},
        {"value": utc_to_dublin(s.First_Received) if s.First_Received else "—",
         "label": "First Received",         "val_colour": "#64748B"},
        {"value": utc_to_dublin(s.Last_Received)  if s.Last_Received  else "—",
         "label": "Last Received",          "val_colour": "#64748B"},
        {"value": str(s.Lines_Awaiting_Schedule or 0),
         "label": "Awaiting Schedule",
         "val_colour": "#FB8C00" if (s.Lines_Awaiting_Schedule or 0) > 0 else "#43A047"},
    ])
    file_rows = ""
    for f in files:
        file_rows += "<tr>"
        file_rows += _td(f'<code style="font-size:11px">{f.LoadFileName}</code>')
        file_rows += _td(utc_to_dublin(f.LoadTimestamp))
        file_rows += _td(f.TotalRows or 0,      align="right")
        file_rows += _td(f.SuccessfulRows or 0, align="right", colour="#059669")
        file_rows += _td(f.FailedRows or 0,     align="right",
                         colour="#DC2626" if (f.FailedRows or 0) > 0 else None)
        file_rows += _td(_pill(f.Status))
        file_rows += _td(f.ErrorMessage or "—", colour="#9CA3AF", wrap=True)
        file_rows += "</tr>"
    order_rows = ""
    for o in orders:
        order_rows += "<tr>"
        order_rows += _td(o.OrderId,    mono=True)
        order_rows += _td(o.CustomerName)
        order_rows += _td(o.CustomerCode, mono=True, colour="#64748B")
        order_rows += _td(utc_to_dublin(o.OrderDate))
        order_rows += _td(str(o.DateRequired)[:10] if o.DateRequired else "—")
        order_rows += _td(o.LineCount or 0, align="right")
        order_rows += _td(_yn(o.SynStaged))
        order_rows += _td(_yn(o.SynProcessed))
        order_rows += "</tr>"
    content = (kpis
               + _sub_heading("📂  CSV Files Ingested Today")
               + _table([{"label":"Filename"},{"label":"Loaded At"},
                         {"label":"Rows","num":True},{"label":"OK","num":True},
                         {"label":"Failed","num":True},{"label":"Status"},
                         {"label":"Error"}], file_rows, "No files today")
               + _sub_heading("📋  Orders Breakdown")
               + _table([{"label":"Order ID"},{"label":"Store"},
                         {"label":"Cust Code"},{"label":"Received"},
                         {"label":"Requested Delivery"},
                         {"label":"Lines","num":True},
                         {"label":"Staged"},{"label":"Processed"}],
                        order_rows, "No orders today"))
    return _section("Step 1 — Inbound Orders Received", "📥", content)


def build_schedule_section(summary, orders, audit_events,
                           held, removed, passthrough) -> str:
    if not summary:
        return _section("Step 2 — Delivery Date Scheduling", "📅",
                        '<p style="padding:16px;color:#9CA3AF">No scheduling data today.</p>')
    s = summary[0]
    kpis = _kpi_row([
        {"value": str(s.Total_Evaluated or 0),
         "label": "Orders Evaluated",        "val_colour": "#1E88E5"},
        {"value": str(s.Date_Confirmed or 0),
         "label": "Dates Confirmed",         "val_colour": "#43A047"},
        {"value": str(s.Date_Rescheduled or 0),
         "label": "Dates Rescheduled",
         "val_colour": "#FB8C00" if (s.Date_Rescheduled or 0) else "#43A047",
         "sub": "pushed to meet lead time"},
        {"value": str(s.Orders_Held or 0),
         "label": "Orders Held",
         "val_colour": "#E53935" if (s.Orders_Held or 0) else "#43A047",
         "sub": "customer or product issue"},
        {"value": str(s.Earliest_Delivery)[:10] if s.Earliest_Delivery else "—",
         "label": "Earliest Delivery",       "val_colour": "#5E35B1"},
        {"value": str(s.Latest_Delivery)[:10]   if s.Latest_Delivery   else "—",
         "label": "Latest Delivery",         "val_colour": "#5E35B1"},
    ])
    event_icons = {
        "PRODUCT_APPROVED":"✅","PRODUCT_REMOVED":"🗑️",
        "PRODUCT_PASSTHROUGH":"🔄","CUSTOMER_APPROVED":"✅",
        "CUSTOMER_HELD":"🛑","ORDER_HELD":"🛑",
    }
    event_rows = ""
    for e in audit_events:
        event_rows += "<tr>"
        event_rows += _td(f'{event_icons.get(e.EventType,"ℹ️")} {e.EventType}')
        event_rows += _td(e.AffectedOrders, align="right")
        event_rows += _td(e.EventCount,     align="right")
        event_rows += _td(e.UniqueProducts, align="right")
        event_rows += "</tr>"
    sched_rows = ""
    for o in orders:
        sched_rows += "<tr>"
        sched_rows += _td(o.OrderId, mono=True)
        sched_rows += _td(o.CustomerName)
        sched_rows += _td(str(o.OriginalDateRequired)[:10]
                          if o.OriginalDateRequired else "—")
        sched_rows += _td(
            f'<strong style="color:#059669">'
            f'{str(o.ScheduledDeliveryDate)[:10]}</strong>'
            if o.ScheduledDeliveryDate else _pill("HELD"))
        sched_rows += _td(str(o.ScheduledShippingDate)[:10]
                          if o.ScheduledShippingDate else "—")
        sched_rows += _td(_pill("RESCHEDULED" if o.DateChanged else "CONFIRMED"))
        sched_rows += _td(o.ChangeReason or "—", colour="#64748B", wrap=True)
        sched_rows += "</tr>"
    held_html = ""
    if held:
        held_rows = ""
        for h in held:
            held_rows += "<tr>"
            held_rows += _td(h.OrderId, mono=True)
            held_rows += _td(h.CustomerName)
            held_rows += _td(h.CustomerCode, mono=True, colour="#64748B")
            held_rows += _td(_pill(h.EventType))
            held_rows += _td(h.ChangeReason or "—", colour="#9CA3AF", wrap=True)
            held_rows += _td(utc_to_dublin_full(h.LoggedAt))
            held_rows += "</tr>"
        held_html = (
            _sub_heading("🛑  Held Orders — Action Required", "#DC2626")
            + _table([{"label":"Order"},{"label":"Store"},{"label":"Code"},
                      {"label":"Hold Reason"},{"label":"Detail"},
                      {"label":"Time"}], held_rows))
    removed_html = ""
    if removed:
        rem_rows = ""
        for r in removed:
            rem_rows += "<tr>"
            rem_rows += _td(r.OrderId, mono=True)
            rem_rows += _td(r.CustomerName)
            rem_rows += _td(r.ProductCode, mono=True)
            rem_rows += _td(r.ProductDescription or "—", colour="#64748B")
            rem_rows += _td(r.ProductCategory)
            rem_rows += _td(r.ProductSubcategory)
            rem_rows += "</tr>"
        removed_html = (
            _sub_heading("🗑  Products Removed from Orders", "#DC2626")
            + _table([{"label":"Order"},{"label":"Store"},
                      {"label":"Product Code"},{"label":"Description"},
                      {"label":"Category"},{"label":"Subcategory"}], rem_rows))
    pass_html = ""
    if passthrough:
        pass_rows = ""
        for p in passthrough:
            pass_rows += "<tr>"
            pass_rows += _td(p.OrderId, mono=True)
            pass_rows += _td(p.CustomerName)
            pass_rows += _td(p.ProductCode, mono=True)
            pass_rows += _td(p.ProductDescription or "—", colour="#64748B")
            pass_rows += _td(p.ProductCategory)
            pass_rows += _td(p.ProductSubcategory)
            pass_rows += "</tr>"
        pass_html = (
            _sub_heading("🔄  FSDU / Co-Pack Passthrough", "#7C3AED")
            + _table([{"label":"Order"},{"label":"Store"},
                      {"label":"Product Code"},{"label":"Description"},
                      {"label":"Category"},{"label":"Subcategory"}], pass_rows))
    content = (kpis
               + _sub_heading("📊  Validation & Scheduling Events")
               + _table([{"label":"Event Type"},{"label":"Orders","num":True},
                         {"label":"Count","num":True},
                         {"label":"Products","num":True}],
                        event_rows, "No events today")
               + _sub_heading("📅  Scheduled Orders — SAP Delivery Dates Confirmed")
               + _table([{"label":"Order"},{"label":"Store"},
                         {"label":"Requested"},{"label":"SAP Delivery Date ★"},
                         {"label":"Ship Date"},{"label":"Outcome"},
                         {"label":"Reason"}],
                        sched_rows, "No orders scheduled today")
               + held_html + removed_html + pass_html)
    return _section("Step 2 — Delivery Date Scheduling", "📅", content)


def build_d365_section(summary, orders) -> str:
    if not summary:
        return _section("Steps 3 & 4 — Staged & Sent to Dynamics 365", "🔵",
                        '<p style="padding:16px;color:#9CA3AF">No data today.</p>')
    s = summary[0]
    kpis = _kpi_row([
        {"value": str(s.Total or 0),
         "label": "Orders in Pipeline",    "val_colour": "#1E88E5"},
        {"value": str(s.Staged_Ready or 0),
         "label": "Staged & Ready",        "val_colour": "#0288D1"},
        {"value": str(s.D365_TOs_Created or 0),
         "label": "D365 TOs Created",      "val_colour": "#43A047",
         "sub": "Transfer Order Nos assigned"},
        {"value": str(s.D365_Confirmed or 0),
         "label": "D365 Confirmed OK",     "val_colour": "#43A047"},
        {"value": str(s.In_SAP or 0),
         "label": "Progressed to SAP",     "val_colour": "#5E35B1"},
        {"value": str(s.Errored or 0),
         "label": "Errors",
         "val_colour": "#E53935" if (s.Errored or 0) else "#43A047",
         "bg": "#FEF2F2" if (s.Errored or 0) else "transparent"},
    ])
    order_rows = ""
    for o in orders:
        order_rows += "<tr>"
        order_rows += _td(o.OrderId, mono=True)
        order_rows += _td(o.CustomerCode, colour="#64748B")
        order_rows += _td(_pill(o.MasterStatus))
        order_rows += _td(_yn(o.ReadyForProcessing))
        order_rows += _td(
            f'<code style="font-size:11px;color:#059669;font-weight:700">'
            f'{o.DynamicsDocumentNo}</code>'
            if o.DynamicsDocumentNo else "—")
        order_rows += _td(o.DynamicsResponseCode or "—",
                          colour="#059669" if o.DynamicsResponseCode == "OK"
                          else "#DC2626")
        order_rows += _td(o.SapStatus or "—")
        order_rows += _td(
            f'<strong style="color:#059669">'
            f'{str(o.RequestedShippingDate)[:10]}</strong>'
            if o.RequestedShippingDate else "—")
        order_rows += _td(
            f'<strong style="color:#5E35B1">'
            f'{str(o.RequestedReceiptDate)[:10]}</strong>'
            if o.RequestedReceiptDate else "—")
        order_rows += _td(o.LineCount or 0, align="right")
        order_rows += _td(o.LastError or "—",
                          colour="#DC2626" if o.LastError else None, wrap=True)
        order_rows += "</tr>"
    error_banner = ""
    if s.Errored:
        err_ids = ", ".join(str(o.OrderId) for o in orders
                            if o.MasterStatus == "ERROR")
        error_banner = _alert(
            f"{s.Errored} order(s) in ERROR status — investigate: {err_ids}",
            "error")
    else:
        error_banner = _alert(
            "All orders successfully staged and confirmed in D365.", "ok")
    content = (kpis + error_banner
               + _sub_heading("📄  D365 Transfer Order Status")
               + _table(
                   [{"label":"Order"},{"label":"Store"},{"label":"Status"},
                    {"label":"Ready"},{"label":"D365 TO Number"},
                    {"label":"D365 Response"},{"label":"SAP Status"},
                    {"label":"Ship Date"},{"label":"SAP Delivery Date ★"},
                    {"label":"Lines","num":True},{"label":"Last Error"}],
                   order_rows, "No orders today"))
    return _section("Steps 3 & 4 — Staged & Sent to Dynamics 365", "🔵", content)


def build_sap_staging_section(summary, orders) -> str:
    if not summary:
        return _section("Step 5 — SAP Staging", "🟣",
                        '<p style="padding:16px;color:#9CA3AF">No SAP staging data today.</p>')
    s = summary[0]
    kpis = _kpi_row([
        {"value": str(s.Total_Staged or 0),
         "label": "Orders Staged for SAP",  "val_colour": "#5E35B1"},
        {"value": str(s.Total_Items or 0),
         "label": "Product Lines Staged",   "val_colour": "#5E35B1"},
        {"value": str(s.Awaiting_CPI or 0),
         "label": "Awaiting CPI Send",
         "val_colour": "#FB8C00" if (s.Awaiting_CPI or 0) else "#43A047",
         "sub": "SynProcessed = 0"},
        {"value": str(s.Sent_To_SAP or 0),
         "label": "Transmitted to SAP",     "val_colour": "#43A047",
         "sub": "SynProcessed = 1"},
        {"value": str(s.Earliest_Delivery)[:10] if s.Earliest_Delivery else "—",
         "label": "Earliest SAP Delivery",  "val_colour": "#1E88E5"},
        {"value": str(s.Latest_Delivery)[:10]   if s.Latest_Delivery   else "—",
         "label": "Latest SAP Delivery",    "val_colour": "#1E88E5"},
    ])
    order_rows = ""
    for o in orders:
        order_rows += "<tr>"
        order_rows += _td(o.SO_Number,   mono=True, bold=True)
        order_rows += _td(o.OrderId or "—", mono=True, colour="#64748B")
        order_rows += _td(o.Customer_Code)
        order_rows += _td(str(o.Document_Date)[:10] if o.Document_Date else "—")
        order_rows += _td(
            f'<strong style="color:#5E35B1">'
            f'{str(o.Delivery_Date)[:10]}</strong>'
            if o.Delivery_Date else "—")
        order_rows += _td(o.ItemCount or 0, align="right")
        order_rows += _td(f"{float(o.TotalQty or 0):.0f}", align="right")
        order_rows += _td(_pill("SENT" if o.SynProcessed else "PENDING"))
        order_rows += _td(o.SapStatus or "—")
        order_rows += "</tr>"
    pending_banner = ""
    if s.Awaiting_CPI:
        pending_banner = _alert(
            f"{s.Awaiting_CPI} order(s) staged but not yet sent to SAP CPI — "
            f"run sap_cpi_transmit.py to complete.", "warn")
    else:
        pending_banner = _alert(
            "All staged orders have been transmitted to SAP.", "ok")
    content = (kpis + pending_banner
               + _sub_heading("📦  SAP Staged Orders — Delivery Dates")
               + _table(
                   [{"label":"D365 TO No"},{"label":"Order ID"},
                    {"label":"Store"},{"label":"Document Date"},
                    {"label":"SAP Delivery Date ★"},
                    {"label":"Items","num":True},{"label":"Qty","num":True},
                    {"label":"CPI Status"},{"label":"SAP Status"}],
                   order_rows, "No SAP staging records today"))
    return _section("Step 5 — SAP Staging", "🟣", content)


def build_cpi_section(summary, orders, errors) -> str:
    if not summary:
        return _section("Step 6 — SAP CPI Transmission", "✅",
                        '<p style="padding:16px;color:#9CA3AF">No CPI data today.</p>')
    s = summary[0]
    kpis = _kpi_row([
        {"value": str(s.Total_Transmitted or 0),
         "label": "Transmissions Attempted", "val_colour": "#1E88E5"},
        {"value": str(s.Succeeded or 0),
         "label": "Successfully Sent",
         "val_colour": "#43A047",
         "bg": "#F0FDF4" if (s.Succeeded or 0) > 0 else "transparent"},
        {"value": str(s.Failed or 0),
         "label": "Failed",
         "val_colour": "#E53935" if (s.Failed or 0) else "#43A047",
         "bg": "#FEF2F2" if (s.Failed or 0) > 0 else "transparent"},
        {"value": str(s.Pending or 0),
         "label": "Pending",
         "val_colour": "#FB8C00" if (s.Pending or 0) else "#43A047"},
        {"value": utc_to_dublin(s.First_Sent) if s.First_Sent else "—",
         "label": "First Sent",              "val_colour": "#64748B"},
        {"value": utc_to_dublin(s.Last_Sent)  if s.Last_Sent  else "—",
         "label": "Last Sent",               "val_colour": "#64748B"},
    ])
    order_rows = ""
    for o in orders:
        order_rows += "<tr>"
        order_rows += _td(o.TransferOrderNumber, mono=True, bold=True)
        order_rows += _td(o.CustomerCode)
        order_rows += _td(_pill("SUCCESS" if o.Status == "Success"
                                else "ERROR" if o.Status == "Error"
                                else "PENDING"))
        order_rows += _td(str(o.HTTPStatus) if o.HTTPStatus else "—",
                          colour="#059669" if o.HTTPStatus in (200,201)
                          else "#DC2626")
        order_rows += _td(
            f'<strong style="color:#5E35B1">'
            f'{str(o.Delivery_Date)[:10]}</strong>'
            if o.Delivery_Date else "—")
        order_rows += _td(utc_to_dublin(o.SentOn) if o.SentOn else "—")
        order_rows += _td(o.ErrorMessage1 or "—", colour="#9CA3AF", wrap=True)
        order_rows += "</tr>"
    error_rows = ""
    for e in errors:
        error_rows += "<tr>"
        error_rows += _td(e.TransferOrderNumber, mono=True, bold=True,
                          colour="#DC2626")
        error_rows += _td(e.CustomerCode)
        error_rows += _td(str(e.HTTPStatus) if e.HTTPStatus else "—",
                          colour="#DC2626")
        error_rows += _td(e.ErrorMessage1 or "—", colour="#DC2626", wrap=True)
        error_rows += _td(e.ErrorMessage2 or "—", colour="#9CA3AF", wrap=True)
        error_rows += _td(utc_to_dublin(e.SentOn) if e.SentOn else "—")
        error_rows += "</tr>"
    health_banner = ""
    if s.Failed:
        health_banner = _alert(
            f"{s.Failed} transmission(s) failed — see error detail below.", "error")
    elif s.Pending:
        health_banner = _alert(
            f"{s.Pending} transmission(s) still pending.", "warn")
    else:
        health_banner = _alert(
            "All SAP CPI transmissions completed successfully today.", "ok")
    errors_html = ""
    if errors:
        errors_html = (
            _sub_heading("❌  CPI Transmission Errors — Action Required", "#DC2626")
            + _table([{"label":"TO Number"},{"label":"Store"},
                      {"label":"HTTP"},{"label":"SAP Error 1"},
                      {"label":"SAP Error 2"},{"label":"Sent At"}],
                     error_rows))
    content = (kpis + health_banner
               + _sub_heading("📡  CPI Transmission Log — SAP Delivery Dates Sent")
               + _table(
                   [{"label":"D365 TO Number"},{"label":"Store"},
                    {"label":"Status"},{"label":"HTTP"},
                    {"label":"SAP Delivery Date ★"},
                    {"label":"Sent At"},{"label":"SAP Message"}],
                   order_rows, "No CPI transmissions today")
               + errors_html)
    return _section("Step 6 — SAP CPI Transmission", "✅", content)


def build_future_steps_section() -> str:
    content = """
  <div style="padding:20px">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="padding:12px 16px;border-left:4px solid #94A3B8;
                   background:#fff;border-radius:0 6px 6px 0">
          <div style="font-size:12px;font-weight:700;color:#475569">
            Step 7 — SAP Despatch Confirmation
            <span style="color:#94A3B8;font-weight:400">(coming soon)</span>
          </div>
          <div style="font-size:11px;color:#94A3B8;margin-top:4px">
            SAP confirms goods despatched →
            INT.Transfer_Order_SAP_Header updated with DespatchDate
          </div>
        </td>
      </tr>
      <tr><td style="height:10px"></td></tr>
      <tr>
        <td style="padding:12px 16px;border-left:4px solid #94A3B8;
                   background:#fff;border-radius:0 6px 6px 0">
          <div style="font-size:12px;font-weight:700;color:#475569">
            Step 8 — Dynamics 365 Transfer Order Close
            <span style="color:#94A3B8;font-weight:400">(coming soon)</span>
          </div>
          <div style="font-size:11px;color:#94A3B8;margin-top:4px">
            On SAP despatch confirmation → closes the D365 Transfer Order,
            EXC.TransferOrder_Control MasterStatus = CLOSED
          </div>
        </td>
      </tr>
    </table>
  </div>"""
    return _section("Steps 7 & 8 — Despatch & Close (Coming Soon)", "🔜", content)


def build_processing_log_section(rows) -> str:
    if not rows:
        return ""
    log_rows = ""
    for r in rows:
        log_rows += "<tr>"
        log_rows += _td(r.ProcessCode, colour="#64748B", bold=True)
        log_rows += _td(r.StepCode,    mono=True)
        log_rows += _td(_pill(r.Status))
        log_rows += _td(r.EventCount,  align="right")
        log_rows += _td(utc_to_dublin_full(r.LastEvent))
        log_rows += "</tr>"
    return _section("Processing Log — Step Activity Summary", "📋",
                    _table([{"label":"Process"},{"label":"Step"},
                             {"label":"Status"},
                             {"label":"Events","num":True},
                             {"label":"Last Event"}], log_rows))


# =============================================================================
#  MAIN REPORT
# =============================================================================

def run(conn: pyodbc.Connection, cfg) -> None:
    now_utc             = utc_now()
    dublin_dt, tz_label = dublin_now()
    logo                = logo_base64()
    cur                 = conn.cursor()

    cur.execute(SQL_INGEST_SUMMARY);        ing_summary   = cur.fetchall()
    cur.execute(SQL_INGEST_FILES);          ing_files     = cur.fetchall()
    cur.execute(SQL_INGEST_ORDERS);         ing_orders    = cur.fetchall()
    cur.execute(SQL_SCHEDULE_SUMMARY);      sch_summary   = cur.fetchall()
    cur.execute(SQL_SCHEDULE_ORDERS);       sch_orders    = cur.fetchall()
    cur.execute(SQL_AUDIT_PRODUCT_EVENTS);  audit_events  = cur.fetchall()
    cur.execute(SQL_HELD_ORDERS);           held_orders   = cur.fetchall()
    cur.execute(SQL_REMOVED_PRODUCTS);      removed_prods = cur.fetchall()
    cur.execute(SQL_PASSTHROUGH_PRODUCTS);  pass_prods    = cur.fetchall()
    cur.execute(SQL_CONTROL_SUMMARY);       ctrl_summary  = cur.fetchall()
    cur.execute(SQL_CONTROL_ORDERS);        ctrl_orders   = cur.fetchall()
    cur.execute(SQL_SAP_STAGING_SUMMARY);   sap_summary   = cur.fetchall()
    cur.execute(SQL_SAP_STAGING_ORDERS);    sap_orders    = cur.fetchall()
    cur.execute(SQL_CPI_SUMMARY);           cpi_summary   = cur.fetchall()
    cur.execute(SQL_CPI_ORDERS);            cpi_orders    = cur.fetchall()
    cur.execute(SQL_CPI_ERRORS);            cpi_errors    = cur.fetchall()
    cur.execute(SQL_PROCESSING_LOG_SUMMARY);proc_log_rows = cur.fetchall()

    ing  = ing_summary[0]  if ing_summary  else None
    sch  = sch_summary[0]  if sch_summary  else None
    ctrl = ctrl_summary[0] if ctrl_summary else None
    sap  = sap_summary[0]  if sap_summary  else None
    cpi  = cpi_summary[0]  if cpi_summary  else None

    body = (
        build_pipeline_summary(ing, sch, ctrl, sap, cpi)
        + build_ingest_section(ing_summary, ing_files, ing_orders)
        + build_schedule_section(sch_summary, sch_orders, audit_events,
                                 held_orders, removed_prods, pass_prods)
        + build_d365_section(ctrl_summary, ctrl_orders)
        + build_sap_staging_section(sap_summary, sap_orders)
        + build_cpi_section(cpi_summary, cpi_orders, cpi_errors)
        + build_future_steps_section()
        + build_processing_log_section(proc_log_rows)
        + '<div style="height:24px"></div>'
    )

    dublin_time_display = f"{dublin_dt.strftime('%H:%M')} {tz_label}"
    utc_time_display    = now_utc.strftime("%H:%M UTC")

    subject = (
        f"Fusion EPOS | STO Pipeline | {dublin_dt.strftime('%d %b %Y')} "
        f"as at {dublin_time_display}"
    )
    subtitle = (
        f"STO Operations Report  ·  {dublin_dt.strftime('%A %d %B %Y')}  ·  "
        f"{dublin_time_display}  ({utc_time_display})"
    )

    import reporting_shared as rs
    _original = rs.RECIPIENTS[:]
    rs.RECIPIENTS.clear()
    rs.RECIPIENTS.append(RECIPIENT)
    from reporting_shared import send_email
    send_email(cfg, subject, html_wrapper(logo, "STO Operations", subtitle, body))
    rs.RECIPIENTS.clear()
    rs.RECIPIENTS.extend(_original)


# =============================================================================
#  ENTRY POINT
# =============================================================================

def main() -> None:
    now_utc = utc_now()
    dublin_dt, tz_label = dublin_now()
    log.info("=" * 70)
    log.info("  Synovia Fusion — STO Operations Report  v2.1.0")
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
