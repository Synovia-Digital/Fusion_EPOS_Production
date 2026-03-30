#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
#  Run_Stage_Transfer_Orders.py
#  Step 3 — STO Pipeline — Staging Wrapper
#
#  1. Pre-flight  — shows exactly what the SP will insert (read-only queries)
#  2. Confirmation — prompts before executing
#  3. Execute      — runs EXC.usp_Load_Transfer_Order_Request
#  4. Verify       — confirms rows landed in Control / Header / Lines
# =============================================================================

import pyodbc
import configparser
import sys
import socket
from pathlib import Path
from datetime import datetime

SCRIPT_NAME  = "Run_Stage_Transfer_Orders.py"
MACHINE_NAME = socket.gethostname()

CONFIG_SECTION = "Fusion_EPOS_Production"
CONFIG_PATH    = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"

# =========================================================
# ANSI COLOURS
# =========================================================
class C:
    RST  = "\033[0m";   BOLD = "\033[1m";  DIM  = "\033[2m"
    RED  = "\033[31m";  GRN  = "\033[32m"; YLW  = "\033[33m"
    BLU  = "\033[34m";  MAG  = "\033[35m"; CYN  = "\033[36m"
    BGRN = "\033[92m";  BYLW = "\033[93m"; BBLU = "\033[94m"
    BMAG = "\033[95m";  BCYN = "\033[96m"; BWHT = "\033[97m"
    BGRED = "\033[41m"; BGGRN = "\033[42m"; BGYLW = "\033[43m"
    BGBLU = "\033[44m"; BGMAG = "\033[45m"; BGCYN = "\033[46m"

_W = 74

def _ts():
    return datetime.now().strftime("%H:%M:%S")

def banner(text, bg=C.BGMAG, fg=C.BWHT):
    pad = max(0, _W - len(text) - 4)
    l, r = pad // 2, pad - pad // 2
    print(f"\n{bg}{fg}{C.BOLD}  {'─'*l}  {text}  {'─'*r}  {C.RST}")

def section(text):
    print(f"\n{C.BOLD}{C.BCYN}{'═'*_W}{C.RST}")
    print(f"{C.BOLD}{C.BCYN}  ◆  {text}{C.RST}")
    print(f"{C.BOLD}{C.BCYN}{'═'*_W}{C.RST}")

def divider():
    print(f"  {C.DIM}{'─'*(_W-2)}{C.RST}")

def kv(label, val, vc=C.BWHT):
    print(f"  {C.DIM}{label:<32}{C.RST}{vc}{val}{C.RST}")

def msg_ok(m):   print(f"  {C.BGRN}[{_ts()}]{C.RST}  {C.BGRN}✔{C.RST}  {m}")
def msg_info(m): print(f"  {C.BCYN}[{_ts()}]{C.RST}  {C.BCYN}ℹ{C.RST}  {m}")
def msg_warn(m): print(f"  {C.BYLW}[{_ts()}]{C.RST}  {C.BYLW}⚠{C.RST}  {m}")
def msg_err(m):  print(f"  {C.RED}[{_ts()}]{C.RST}  {C.RED}✘{C.RST}  {m}")

# =========================================================
# INI / DB
# =========================================================
def resolve_ini(path_str):
    p = Path(path_str)
    if p.is_dir():
        c = p / "Master_ini_config.ini"
        if not c.exists():
            raise FileNotFoundError(
                f"Master_ini_config.ini not found in {p}")
        return c
    if p.is_file():
        return p
    raise FileNotFoundError(f"Config path not found: {p}")

INI_PATH = resolve_ini(CONFIG_PATH)
cfg = configparser.ConfigParser()
cfg.read(INI_PATH)
db = cfg[CONFIG_SECTION]

CONN_STR = (
    f"DRIVER={{{db.get('driver')}}};"
    f"SERVER={db.get('server')};"
    f"DATABASE={db.get('database')};"
    f"UID={db.get('user')};PWD={db.get('password')};"
    f"Encrypt={db.get('encrypt','yes')};"
    f"TrustServerCertificate="
    f"{db.get('trust_server_certificate','no')};"
)

def get_conn(autocommit=True):
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = autocommit
    return conn

# =========================================================
# PRE-FLIGHT SQL
# =========================================================

# Step 1 — Orders the SP will pick up
PREFLIGHT_ORDERS_SQL = """
SELECT
    s.OrderId,
    s.CustomerCode,
    s.CustomerName,
    s.OriginalDateRequired,
    s.ScheduledDeliveryDate,        -- ★ SAP Delivery Date
    s.ScheduledShippingDate,
    s.DateChanged,
    s.ChangeReason,
    COUNT(r.EPOS_TransferRequest_ID)                     AS RAW_Lines,
    SUM(CASE WHEN r.SynProcessed = 0 THEN 1 ELSE 0 END)  AS Lines_Active,
    SUM(CASE WHEN r.SynProcessed = 1 THEN 1 ELSE 0 END)  AS Lines_Removed
FROM STG.WASP_STO_Scheduled s
INNER JOIN RAW.EPOS_TransferRequest r
    ON r.OrderId = s.OrderId
WHERE s.SynProcessed          = 0
  AND s.ScheduledDeliveryDate IS NOT NULL
  AND r.SynProcessed          = 0
  AND r.SynStaged             = 0
GROUP BY
    s.OrderId, s.CustomerCode, s.CustomerName,
    s.OriginalDateRequired, s.ScheduledDeliveryDate,
    s.ScheduledShippingDate, s.DateChanged, s.ChangeReason
ORDER BY s.OrderId;
"""

# Step 2 — Customer/warehouse mapping per order
PREFLIGHT_MAPPING_SQL = """
SELECT
    s.OrderId,
    s.CustomerCode,
    s.CustomerName,
    ds.Warehouse                     AS ReceivingWarehouseId,
    ds.Name                          AS StoreName,
    CASE
        WHEN ds.Account IS NULL THEN 0
        WHEN NULLIF(LTRIM(RTRIM(ds.Warehouse)),'') IS NULL THEN 0
        ELSE 1
    END                              AS ReadyForProcessing,
    CASE
        WHEN ds.Account IS NULL
            THEN CONCAT('NOT MAPPED — CustomerCode=', s.CustomerCode)
        WHEN NULLIF(LTRIM(RTRIM(ds.Warehouse)),'') IS NULL
            THEN CONCAT('WAREHOUSE BLANK — CustomerCode=', s.CustomerCode)
        ELSE 'OK'
    END                              AS ValidationMessage
FROM STG.WASP_STO_Scheduled s
LEFT JOIN CFG.Dynamics_Stores ds
    ON ds.Account = s.CustomerCode
WHERE s.SynProcessed          = 0
  AND s.ScheduledDeliveryDate IS NOT NULL
ORDER BY s.OrderId;
"""

# Step 3 — Headers that will be created
PREFLIGHT_HEADERS_SQL = """
SELECT
    CONCAT('ORDER_', s.OrderId)      AS SourceID,
    s.OrderId,
    s.CustomerCode,
    s.CustomerName,
    '010'                             AS ShippingWarehouseId,
    ds.Warehouse                      AS ReceivingWarehouseId,
    s.ScheduledShippingDate           AS RequestedShippingDate,
    s.ScheduledDeliveryDate           AS RequestedReceiptDate,  -- ★ SAP Delivery Date
    CASE
        WHEN ds.Account IS NOT NULL
         AND NULLIF(LTRIM(RTRIM(ds.Warehouse)),'') IS NOT NULL
        THEN 'WILL BE CREATED'
        ELSE 'WILL BE SKIPPED — Not ready'
    END                               AS HeaderAction
FROM STG.WASP_STO_Scheduled s
LEFT JOIN CFG.Dynamics_Stores ds
    ON ds.Account = s.CustomerCode
WHERE s.SynProcessed          = 0
  AND s.ScheduledDeliveryDate IS NOT NULL
ORDER BY s.OrderId;
"""

# Step 4 — Lines that will be created
PREFLIGHT_LINES_SQL = """
SELECT
    s.OrderId,
    s.CustomerName,
    r.ProductCode,
    MAX(r.ProductDescription)                              AS Description,
    SUM(TRY_CONVERT(DECIMAL(18,4), r.Quantity))            AS Quantity,
    ROW_NUMBER() OVER (
        PARTITION BY s.OrderId ORDER BY r.ProductCode
    )                                                      AS [LineNo],
    ds.Warehouse                                           AS ReceivingWarehouse
FROM STG.WASP_STO_Scheduled s
INNER JOIN RAW.EPOS_TransferRequest r
    ON r.OrderId = s.OrderId
LEFT JOIN CFG.Dynamics_Stores ds
    ON ds.Account = s.CustomerCode
WHERE s.SynProcessed          = 0
  AND s.ScheduledDeliveryDate IS NOT NULL
  AND r.SynProcessed          = 0
  AND r.SynStaged             = 0
  AND NULLIF(LTRIM(RTRIM(r.ProductCode)),'') IS NOT NULL
  AND ds.Account IS NOT NULL
  AND NULLIF(LTRIM(RTRIM(ds.Warehouse)),'') IS NOT NULL
GROUP BY s.OrderId, s.CustomerName, r.ProductCode, ds.Warehouse
ORDER BY s.OrderId, r.ProductCode;
"""

# Summary counts
PREFLIGHT_SUMMARY_SQL = """
SELECT
    COUNT(DISTINCT s.OrderId)                             AS Orders_Total,
    SUM(CASE
        WHEN ds.Account IS NOT NULL
         AND NULLIF(LTRIM(RTRIM(ds.Warehouse)),'') IS NOT NULL
        THEN 1 ELSE 0 END)                                AS Orders_Ready,
    SUM(CASE
        WHEN ds.Account IS NULL
          OR NULLIF(LTRIM(RTRIM(ds.Warehouse)),'') IS NULL
        THEN 1 ELSE 0 END)                                AS Orders_Not_Ready,
    COUNT(r.EPOS_TransferRequest_ID)                      AS Total_RAW_Lines,
    SUM(CASE WHEN r.SynProcessed = 0 THEN 1 ELSE 0 END)   AS Lines_Active,
    SUM(CASE WHEN r.SynProcessed = 1 THEN 1 ELSE 0 END)   AS Lines_Removed
FROM STG.WASP_STO_Scheduled s
INNER JOIN RAW.EPOS_TransferRequest r
    ON r.OrderId = s.OrderId
LEFT JOIN CFG.Dynamics_Stores ds
    ON ds.Account = s.CustomerCode
WHERE s.SynProcessed          = 0
  AND s.ScheduledDeliveryDate IS NOT NULL
  AND r.SynStaged             = 0;
"""

# Post-run verification
VERIFY_SQL = """
SELECT
    c.TransferControlId,
    c.OrderId,
    c.CustomerCode,
    c.MasterStatus,
    c.ReadyForProcessing,
    c.ValidationMessage,
    c.RequestedShippingDate,
    c.RequestedReceiptDate,           -- ★ SAP Delivery Date
    c.ReceivingWarehouseId,
    h.HeaderExists,
    h.LineCount,
    h.TotalQty
FROM EXC.TransferOrder_Control c
LEFT JOIN
(
    SELECT
        hdr.TransferControlId,
        1                    AS HeaderExists,
        COUNT(l.[LineNo])    AS LineCount,
        SUM(l.Quantity)      AS TotalQty
    FROM INT.TransferOrderHeader hdr
    LEFT JOIN INT.TransferOrderLine l
        ON l.TransferControlId = hdr.TransferControlId
    GROUP BY hdr.TransferControlId
) h ON h.TransferControlId = c.TransferControlId
WHERE CAST(c.CreatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
   OR CAST(c.StatusUpdatedAt AS DATE) = CAST(GETUTCDATE() AS DATE)
ORDER BY c.TransferControlId;
"""

# =========================================================
# ORDER CARD DISPLAY
# =========================================================
def order_card(order, mapping, headers, lines, idx, total):
    # Find mapping for this order
    m = next((x for x in mapping if x.OrderId == order.OrderId), None)
    ready = m and m.ReadyForProcessing == 1

    hdr_bg   = C.BGGRN if ready else C.BGYLW
    action_c = C.BGRN + C.BOLD if ready else C.BYLW
    ico      = "🚀" if ready else "⚠️ "

    print(f"\n{hdr_bg}{C.BOLD}"
          f"  {ico}  [{idx}/{total}]  "
          f"{order.OrderId:<12}  "
          f"{order.CustomerName:<35}  "
          f"({order.CustomerCode})  {C.RST}")
    divider()

    kv("Original Date Required",
       str(order.OriginalDateRequired)[:10] if order.OriginalDateRequired else "—")
    kv("Ship Date",
       str(order.ScheduledShippingDate)[:10] if order.ScheduledShippingDate else "—",
       C.BCYN)
    kv("SAP Delivery Date  ★",
       str(order.ScheduledDeliveryDate)[:10] if order.ScheduledDeliveryDate else "—",
       C.BGRN + C.BOLD if ready else C.BYLW)
    kv("Date Changed",
       "YES — " + (order.ChangeReason or "") if order.DateChanged else "No")

    if m:
        kv("Route",
           f"010  →  {m.ReceivingWarehouseId or '(not mapped)'}",
           C.BWHT if ready else C.BYLW)
        kv("Warehouse Status",
           m.ValidationMessage,
           C.BGRN if ready else C.RED)

    kv("RAW Lines",
       f"{int(order.RAW_Lines or 0)} total  │  "
       f"{int(order.Lines_Active or 0)} active  │  "
       f"{int(order.Lines_Removed or 0)} removed")

    # Header action
    h = next((x for x in headers if x.OrderId == order.OrderId), None)
    if h:
        kv("Header Action",
           h.HeaderAction, action_c)

    # Line detail (ready orders only)
    if ready:
        order_lines = [l for l in lines if l.OrderId == order.OrderId]
        if order_lines:
            print(f"\n  {C.DIM}  {'No':<5} "
                  f"{'Product Code':<22} "
                  f"{'Description':<30} "
                  f"{'Qty':>8}{C.RST}")
            print(f"  {C.DIM}  {'─'*70}{C.RST}")
            for ln in order_lines:
                print(f"  {C.DIM}  {str(ln.LineNo):<5}{C.RST}"
                      f"{C.BWHT}{ln.ProductCode:<22}{C.RST}"
                      f"{C.DIM}{(ln.Description or ''):<30}{C.RST}"
                      f"{C.BCYN}{float(ln.Quantity or 0):>8.0f}{C.RST}")
    divider()


# =========================================================
# MAIN
# =========================================================
def main():
    banner("STEP 3  —  STO STAGING  —  PRE-FLIGHT & EXECUTION")

    section("STARTUP")
    kv("Script",   SCRIPT_NAME)
    kv("Machine",  MACHINE_NAME)
    kv("INI",      str(INI_PATH))
    kv("DB",       f"{db.get('server')} / {db.get('database')}")
    kv("SP",       "EXC.usp_Load_Transfer_Order_Request")

    conn = get_conn()
    msg_ok("DB connection established")

    # ── Pre-flight queries ────────────────────────────────
    section("PRE-FLIGHT  —  ORDERS PENDING STAGING")

    orders  = conn.execute(PREFLIGHT_ORDERS_SQL).fetchall()
    mapping = conn.execute(PREFLIGHT_MAPPING_SQL).fetchall()
    headers = conn.execute(PREFLIGHT_HEADERS_SQL).fetchall()
    lines   = conn.execute(PREFLIGHT_LINES_SQL).fetchall()
    summary = conn.execute(PREFLIGHT_SUMMARY_SQL).fetchone()

    if not orders:
        msg_warn("No orders pending staging — nothing to do.")
        conn.close()
        sys.exit(0)

    # Per-order cards
    for idx, order in enumerate(orders, 1):
        order_card(order, mapping, headers, lines, idx, len(orders))

    # Summary box
    ready_count     = sum(1 for m in mapping if m.ReadyForProcessing == 1)
    not_ready_count = sum(1 for m in mapping if m.ReadyForProcessing == 0)
    total_lines     = int(summary.Lines_Active or 0)
    removed_lines   = int(summary.Lines_Removed or 0)

    print(f"\n{C.BGBLU}{C.BOLD}{C.BWHT}"
          f"  {'PRE-FLIGHT SUMMARY':^{_W-2}}  {C.RST}")
    print(f"{C.BGBLU}{C.BWHT}"
          f"  Orders Pending  : {len(orders):<6}"
          f"  Ready           : {ready_count:<6}"
          f"  Not Ready       : {not_ready_count:<6}  {C.RST}")
    print(f"{C.BGBLU}{C.BWHT}"
          f"  Active Lines    : {total_lines:<6}"
          f"  Removed Lines   : {removed_lines:<6}"
          f"  (by scheduler)         {C.RST}")

    if not ready_count:
        msg_warn("No orders are ready for staging — check warehouse mapping.")
        conn.close()
        sys.exit(0)

    # Warn on any not-ready orders
    not_ready = [m for m in mapping if m.ReadyForProcessing == 0]
    if not_ready:
        print()
        for nr in not_ready:
            msg_warn(f"  NOT READY: {nr.OrderId}  {nr.CustomerCode}  "
                     f"—  {nr.ValidationMessage}")

    # ── Confirmation ──────────────────────────────────────
    section("CONFIRMATION REQUIRED")
    print(f"\n  {C.BYLW}{C.BOLD}"
          f"{ready_count} order(s) will be staged — "
          f"{not_ready_count} will be skipped (not ready):{C.RST}\n")

    ready_orders = [
        m for m in mapping if m.ReadyForProcessing == 1
    ]
    for m in ready_orders:
        # Get scheduled dates for display
        s = next((o for o in orders if o.OrderId == m.OrderId), None)
        delivery = (str(s.ScheduledDeliveryDate)[:10]
                    if s and s.ScheduledDeliveryDate else "—")
        ship     = (str(s.ScheduledShippingDate)[:10]
                    if s and s.ScheduledShippingDate else "—")
        n_lines  = sum(1 for l in lines if l.OrderId == m.OrderId)

        print(f"    {C.BGRN}▶{C.RST}  "
              f"{C.BOLD}{m.OrderId:<12}{C.RST}  "
              f"{m.CustomerName:<35}  "
              f"Route: 010→{m.ReceivingWarehouseId}")
        print(f"         {C.DIM}Ship:{C.RST} {C.BCYN}{ship}{C.RST}     "
              f"{C.DIM}SAP Delivery Date ★:{C.RST}  "
              f"{C.BGRN}{C.BOLD}{delivery}{C.RST}     "
              f"{C.DIM}Lines: {n_lines}{C.RST}")

    print(f"\n  {C.BYLW}★  ScheduledDeliveryDate above flows as "
          f"RequestedReceiptDate → SAP CPI Delivery Date.{C.RST}\n")

    try:
        ans = input(
            f"  {C.BOLD}{C.BWHT}"
            f"Proceed with staging? [y/N] : {C.RST}"
        ).strip().lower()
    except KeyboardInterrupt:
        print()
        msg_warn("Cancelled.")
        conn.close()
        sys.exit(0)

    if ans != "y":
        msg_warn("Aborted — no changes made.")
        conn.close()
        sys.exit(0)

    # ── Execute SP ────────────────────────────────────────
    section("EXECUTING  EXC.usp_Load_Transfer_Order_Request")
    msg_info("Running stored procedure...")

    try:
        sp_conn = pyodbc.connect(CONN_STR)
        sp_conn.autocommit = True
        cursor = sp_conn.cursor()
        cursor.execute("EXEC EXC.usp_Load_Transfer_Order_Request")
        while cursor.nextset():
            pass
        sp_conn.close()
    except Exception as ex:
        msg_err(f"SP execution failed: {ex}")
        conn.close()
        sys.exit(1)

    msg_ok("SP executed successfully")

    # ── Post-run verification ─────────────────────────────
    section("POST-RUN VERIFICATION")

    verify_rows = conn.execute(VERIFY_SQL).fetchall()

    all_ok       = True
    headers_ok   = 0
    lines_ok     = 0

    for v in verify_rows:
        lc = int(v.LineCount or 0)
        tq = float(v.TotalQty or 0)

        if v.MasterStatus == "VALIDATED" and v.HeaderExists and lc > 0:
            headers_ok += 1
            lines_ok   += lc
            msg_ok(
                f"  {C.BOLD}ORDER_{v.OrderId}{C.RST}"
                f"  Cust={v.CustomerCode}"
                f"  Status={C.BGRN}{v.MasterStatus}{C.RST}"
                f"  Route=010→{v.ReceivingWarehouseId}"
                f"  Ship={str(v.RequestedShippingDate)[:10]}"
                f"  {C.BGRN}Delivery={str(v.RequestedReceiptDate)[:10]}{C.RST}"
                f"  Lines={lc}  Qty={tq:.0f}"
            )
        elif v.MasterStatus == "NEW":
            msg_warn(
                f"  ORDER_{v.OrderId}  "
                f"MasterStatus=NEW — {v.ValidationMessage or 'not ready'}"
            )
        else:
            msg_err(
                f"  ORDER_{v.OrderId}  "
                f"Unexpected state: Status={v.MasterStatus}  "
                f"Header={v.HeaderExists}  Lines={lc}"
            )
            all_ok = False

    # ── Final result ──────────────────────────────────────
    section("RUN COMPLETE")

    if all_ok and headers_ok > 0:
        print(f"\n  {C.BGGRN}{C.BOLD}"
              f"  ✅  {headers_ok} ORDER(S) STAGED SUCCESSFULLY  {C.RST}")
        print()
        kv("Orders Staged",  str(headers_ok),    C.BGRN + C.BOLD)
        kv("Lines Created",  str(lines_ok),       C.BGRN)
        kv("Not Ready",      str(not_ready_count),
           C.BYLW if not_ready_count else C.DIM)
        print(f"\n  {C.BCYN}Next step: run "
              f"{C.BOLD}d365_create_transfer_orders.py{C.RST}"
              f"{C.BCYN} to send to Dynamics 365.{C.RST}\n")
    else:
        print(f"\n  {C.BGRED}{C.BWHT}{C.BOLD}"
              f"  ❌  STAGING COMPLETED WITH ISSUES — INVESTIGATE  {C.RST}\n")

    conn.close()


if __name__ == "__main__":
    main()
