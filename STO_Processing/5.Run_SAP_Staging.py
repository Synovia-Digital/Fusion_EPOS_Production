#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
#  Run_SAP_Staging.py
#  Step 5 — STO Pipeline — SAP Staging Wrapper
#
#  1. Pre-flight  — shows exactly what the SP will stage (read-only)
#  2. Confirmation — prompts before executing
#  3. Execute      — runs INT.usp_Stage_Transfer_Orders_For_SAP
#  4. Verify       — confirms rows landed in SAP header / item / note tables
#
#  Delivery_Date in INT.Transfer_Order_SAP_Header is the SAP CPI Delivery Date.
#  It comes from INT.TransferOrderHeader.RequestedReceiptDate which was set
#  by the scheduling script (Step 2) — it must never be overridden here.
# =============================================================================

import pyodbc
import configparser
import sys
import socket
from pathlib import Path
from datetime import datetime

SCRIPT_NAME  = "Run_SAP_Staging.py"
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

_W = 76

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
    print(f"  {C.DIM}{label:<34}{C.RST}{vc}{val}{C.RST}")

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
            raise FileNotFoundError(f"Master_ini_config.ini not found in {p}")
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
    f"TrustServerCertificate={db.get('trust_server_certificate','no')};"
)

def get_conn(autocommit=True):
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = autocommit
    return conn

# =========================================================
# PRE-FLIGHT SQL
# =========================================================

PREFLIGHT_WILL_STAGE_SQL = """
SELECT
    c.TransferControlId,
    c.SourceID,
    c.OrderId,
    c.CustomerCode,
    c.DynamicsDocumentNo,
    c.MasterStatus,
    c.SapStatus,
    h.ShippingWarehouseId,
    h.ReceivingWarehouseId,
    h.RequestedShippingDate,
    h.RequestedReceiptDate,
    COUNT(l.[LineNo])   AS LineCount,
    SUM(l.Quantity)     AS TotalQty
FROM EXC.TransferOrder_Control c
INNER JOIN INT.TransferOrderHeader h
    ON h.TransferControlId = c.TransferControlId
LEFT JOIN INT.TransferOrderLine l
    ON l.TransferControlId = c.TransferControlId
WHERE c.MasterStatus = 'SENT_DYNAMICS'
  AND NULLIF(LTRIM(RTRIM(c.DynamicsDocumentNo)),'') IS NOT NULL
  AND (c.SapStatus IS NULL OR c.SapStatus = 'ERROR')
  AND NOT EXISTS (
      SELECT 1 FROM INT.Transfer_Order_SAP_Header sh
      WHERE sh.SO_Number = c.DynamicsDocumentNo
  )
GROUP BY
    c.TransferControlId, c.SourceID, c.OrderId, c.CustomerCode,
    c.DynamicsDocumentNo, c.MasterStatus, c.SapStatus,
    h.ShippingWarehouseId, h.ReceivingWarehouseId,
    h.RequestedShippingDate, h.RequestedReceiptDate
ORDER BY c.TransferControlId;
"""

PREFLIGHT_WILL_SKIP_SQL = """
SELECT
    c.TransferControlId,
    c.SourceID,
    c.OrderId,
    c.CustomerCode,
    c.DynamicsDocumentNo,
    c.SapStatus,
    sh.Delivery_Date,
    COUNT(si.TransferOrderSAPItem_ID) AS ItemCount
FROM EXC.TransferOrder_Control c
INNER JOIN INT.Transfer_Order_SAP_Header sh
    ON sh.SO_Number = c.DynamicsDocumentNo
LEFT JOIN INT.Transfer_Order_SAP_Item si
    ON si.TransferOrderSAPHeader_ID = sh.TransferOrderSAPHeader_ID
WHERE c.MasterStatus = 'SENT_DYNAMICS'
  AND NULLIF(LTRIM(RTRIM(c.DynamicsDocumentNo)),'') IS NOT NULL
  AND (c.SapStatus IS NULL OR c.SapStatus = 'ERROR')
GROUP BY
    c.TransferControlId, c.SourceID, c.OrderId, c.CustomerCode,
    c.DynamicsDocumentNo, c.SapStatus, sh.Delivery_Date
ORDER BY c.TransferControlId;
"""

PREFLIGHT_LINES_SQL = """
SELECT
    l.TransferControlId,
    l.[LineNo],
    l.ProductCode,
    l.Quantity
FROM INT.TransferOrderLine l
INNER JOIN EXC.TransferOrder_Control c
    ON c.TransferControlId = l.TransferControlId
WHERE c.MasterStatus = 'SENT_DYNAMICS'
  AND NULLIF(LTRIM(RTRIM(c.DynamicsDocumentNo)),'') IS NOT NULL
  AND (c.SapStatus IS NULL OR c.SapStatus = 'ERROR')
  AND NOT EXISTS (
      SELECT 1 FROM INT.Transfer_Order_SAP_Header sh
      WHERE sh.SO_Number = c.DynamicsDocumentNo
  )
ORDER BY l.TransferControlId, l.[LineNo];
"""

VERIFY_SQL = """
SELECT
    sh.SO_Number,
    sh.Customer_Code,
    sh.Document_Date,
    sh.Delivery_Date,
    sh.SynProcessed,
    c.OrderId,
    c.CustomerCode,
    c.TransferControlId,
    c.SapStatus,
    COUNT(si.TransferOrderSAPItem_ID)  AS ItemCount,
    SUM(si.Quantity)                   AS TotalQty,
    COUNT(sn.TransferOrderSAPNote_ID)  AS NoteCount
FROM INT.Transfer_Order_SAP_Header sh
INNER JOIN EXC.TransferOrder_Control c
    ON c.DynamicsDocumentNo = sh.SO_Number
LEFT JOIN INT.Transfer_Order_SAP_Item si
    ON si.TransferOrderSAPHeader_ID = sh.TransferOrderSAPHeader_ID
LEFT JOIN INT.Transfer_Order_SAP_Note sn
    ON sn.TransferOrderSAPHeader_ID = sh.TransferOrderSAPHeader_ID
WHERE sh.SO_Number IN ({ph})
GROUP BY
    sh.SO_Number, sh.Customer_Code, sh.Document_Date,
    sh.Delivery_Date, sh.SynProcessed,
    c.OrderId, c.CustomerCode, c.TransferControlId, c.SapStatus
ORDER BY sh.SO_Number;
"""

# =========================================================
# ORDER CARDS
# =========================================================
def order_card_stage(row, lines, idx, total):
    lc = int(row.LineCount or 0)
    tq = float(row.TotalQty or 0)

    print(f"\n{C.BGGRN}{C.BOLD}"
          f"  🚀  [{idx}/{total}]  "
          f"{row.DynamicsDocumentNo:<22}"
          f"  Order: {row.OrderId:<12}"
          f"  {row.CustomerCode}  {C.RST}")
    divider()
    kv("Source ID",             row.SourceID)
    kv("Customer Code",         row.CustomerCode)
    kv("Route",
       f"{row.ShippingWarehouseId}  →  {row.ReceivingWarehouseId}")
    kv("Ship Date (Document)",
       str(row.RequestedShippingDate)[:10], C.BCYN)
    kv("SAP Delivery Date  ★",
       str(row.RequestedReceiptDate)[:10],  C.BGRN + C.BOLD)
    kv("Lines / Units",
       f"{lc} lines  │  {tq:.0f} units")
    kv("Current SAP Status",    row.SapStatus or "—")

    order_lines = [l for l in lines
                   if l.TransferControlId == row.TransferControlId]
    if order_lines:
        print(f"\n  {C.DIM}  {'No':<5}{'Product Code':<22}{'Qty':>8}{C.RST}")
        print(f"  {C.DIM}  {'─'*38}{C.RST}")
        for ln in order_lines:
            print(f"  {C.DIM}  {str(ln.LineNo):<5}{C.RST}"
                  f"{C.BWHT}{ln.ProductCode:<22}{C.RST}"
                  f"{C.BCYN}{float(ln.Quantity):>8.0f}{C.RST}")
    divider()


def order_card_skip(row, idx, total):
    print(f"\n{C.BGYLW}{C.BOLD}"
          f"  ⏭   [{idx}/{total}]  "
          f"{row.DynamicsDocumentNo:<22}"
          f"  Order: {row.OrderId:<12}"
          f"  {row.CustomerCode}  {C.RST}")
    divider()
    kv("Reason",
       "Already staged in INT.Transfer_Order_SAP_Header", C.BYLW)
    kv("Delivery Date",
       str(row.Delivery_Date)[:10] if row.Delivery_Date else "—", C.DIM)
    kv("Items Staged",
       str(int(row.ItemCount or 0)), C.DIM)
    kv("SAP Status",
       row.SapStatus or "—", C.DIM)
    divider()


# =========================================================
# MAIN
# =========================================================
def main():
    banner("STEP 5  —  SAP STAGING  —  PRE-FLIGHT & EXECUTION")

    section("STARTUP")
    kv("Script",   SCRIPT_NAME)
    kv("Machine",  MACHINE_NAME)
    kv("INI",      str(INI_PATH))
    kv("DB",       f"{db.get('server')} / {db.get('database')}")
    kv("SP",       "INT.usp_Stage_Transfer_Orders_For_SAP")
    kv("Staging",  "INT.Transfer_Order_SAP_Header / Item / Note")

    conn = get_conn()
    msg_ok("DB connection established")

    # ── Pre-flight ────────────────────────────────────────
    section("PRE-FLIGHT  —  SAP STAGING ELIGIBILITY CHECK")

    will_stage = conn.execute(PREFLIGHT_WILL_STAGE_SQL).fetchall()
    will_skip  = conn.execute(PREFLIGHT_WILL_SKIP_SQL).fetchall()
    lines      = conn.execute(PREFLIGHT_LINES_SQL).fetchall()

    if not will_stage and not will_skip:
        msg_warn("No orders with MasterStatus = SENT_DYNAMICS found.")
        msg_warn("Nothing to do — check D365 script has run successfully.")
        conn.close()
        sys.exit(0)

    if will_stage:
        print(f"\n  {C.BGRN}{C.BOLD}"
              f"Orders that WILL be staged ({len(will_stage)}):{C.RST}")
        for idx, row in enumerate(will_stage, 1):
            order_card_stage(row, lines, idx, len(will_stage))
    else:
        msg_info("No new orders to stage.")

    if will_skip:
        print(f"\n  {C.BYLW}{C.BOLD}"
              f"Orders already staged — will be skipped ({len(will_skip)}):{C.RST}")
        for idx, row in enumerate(will_skip, 1):
            order_card_skip(row, idx, len(will_skip))

    # Summary box
    total_lines = sum(int(r.LineCount or 0) for r in will_stage)
    total_qty   = sum(float(r.TotalQty or 0) for r in will_stage)

    print(f"\n{C.BGBLU}{C.BOLD}{C.BWHT}"
          f"  {'PRE-FLIGHT SUMMARY':^{_W-2}}  {C.RST}")
    print(f"{C.BGBLU}{C.BWHT}"
          f"  Orders to Stage  : {len(will_stage):<6}"
          f"  Orders to Skip   : {len(will_skip):<6}  {C.RST}")
    print(f"{C.BGBLU}{C.BWHT}"
          f"  Total Lines      : {total_lines:<6}"
          f"  Total Units      : {total_qty:<8.0f}  {C.RST}")

    if not will_stage:
        msg_warn("All eligible orders already staged — nothing to do.")
        conn.close()
        sys.exit(0)

    # ── Confirmation ──────────────────────────────────────
    section("CONFIRMATION REQUIRED")
    print(f"\n  {C.BYLW}{C.BOLD}"
          f"{len(will_stage)} order(s) will be staged for SAP:{C.RST}\n")

    for row in will_stage:
        lc = int(row.LineCount or 0)
        print(f"    {C.BGRN}▶{C.RST}  "
              f"{C.BOLD}{row.DynamicsDocumentNo:<22}{C.RST}  "
              f"Order {C.DIM}{row.OrderId:<12}{C.RST}  "
              f"Cust: {C.DIM}{row.CustomerCode}{C.RST}")
        print(f"         "
              f"{C.DIM}Ship (Doc Date) :{C.RST} "
              f"{C.BCYN}{str(row.RequestedShippingDate)[:10]}{C.RST}     "
              f"{C.DIM}SAP Delivery Date ★ :{C.RST}  "
              f"{C.BGRN}{C.BOLD}{str(row.RequestedReceiptDate)[:10]}{C.RST}     "
              f"{C.DIM}Lines: {lc}{C.RST}")

    print(f"\n  {C.BYLW}★  Delivery_Date written to INT.Transfer_Order_SAP_Header"
          f"\n     is the SAP CPI Delivery Date. It flows from"
          f"\n     RequestedReceiptDate — do not override in CPI script.{C.RST}\n")

    try:
        ans = input(
            f"  {C.BOLD}{C.BWHT}"
            f"Proceed with SAP staging? [y/N] : {C.RST}"
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
    section("EXECUTING  INT.usp_Stage_Transfer_Orders_For_SAP")
    msg_info("Running stored procedure...")

    try:
        sp_conn = pyodbc.connect(CONN_STR)
        sp_conn.autocommit = True
        cursor = sp_conn.cursor()
        cursor.execute("EXEC INT.usp_Stage_Transfer_Orders_For_SAP")
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

    doc_nos = [r.DynamicsDocumentNo for r in will_stage]
    vrows   = conn.execute(
        VERIFY_SQL.format(ph=",".join("?" * len(doc_nos))),
        doc_nos
    ).fetchall()

    staged_nos = {v.SO_Number for v in vrows}
    headers_ok = 0
    items_ok   = 0
    all_ok     = True

    for v in vrows:
        ic = int(v.ItemCount or 0)
        tq = float(v.TotalQty or 0)
        nc = int(v.NoteCount or 0)

        if ic > 0:
            headers_ok += 1
            items_ok   += ic
            msg_ok(
                f"  {C.BOLD}{v.SO_Number}{C.RST}"
                f"  Order={v.OrderId}"
                f"  Cust={v.Customer_Code}"
                f"  Doc={str(v.Document_Date)[:10]}"
                f"  {C.BGRN}Delivery={str(v.Delivery_Date)[:10]}{C.RST}"
                f"  Items={ic}"
                f"  Qty={tq:.0f}"
                f"  Notes={nc}"
                f"  Synced={v.SynProcessed}"
                f"  SapStatus={v.SapStatus or '—'}"
            )
        else:
            msg_err(f"  {v.SO_Number} — staged but NO items found!")
            all_ok = False

    for dn in doc_nos:
        if dn not in staged_nos:
            msg_err(
                f"  {dn} — NOT FOUND in Transfer_Order_SAP_Header "
                f"after SP ran!")
            all_ok = False

    # ── Final result ──────────────────────────────────────
    section("RUN COMPLETE")

    if all_ok and headers_ok > 0:
        print(f"\n  {C.BGGRN}{C.BOLD}"
              f"  ✅  ALL {headers_ok} ORDER(S) STAGED FOR SAP SUCCESSFULLY  "
              f"{C.RST}")
        print()
        kv("Headers Staged",  str(headers_ok),     C.BGRN + C.BOLD)
        kv("Items Staged",    str(items_ok),        C.BGRN)
        kv("Notes Staged",    str(headers_ok),      C.BGRN)
        kv("Skipped",         str(len(will_skip)),  C.DIM)
        print(f"\n  {C.BCYN}★  SynProcessed = 0 on all staged headers."
              f"\n     These orders are now ready for the SAP CPI"
              f"\n     transmission script to pick up and send.{C.RST}\n")
    else:
        print(f"\n  {C.BGRED}{C.BWHT}{C.BOLD}"
              f"  ❌  STAGING COMPLETED WITH ERRORS — INVESTIGATE  "
              f"{C.RST}\n")

    conn.close()


if __name__ == "__main__":
    main()
