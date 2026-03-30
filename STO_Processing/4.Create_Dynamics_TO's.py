#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
#  d365_create_transfer_orders.py
#  Creates Transfer Orders in Dynamics 365.
#
#  Dates are sourced exclusively from INT.TransferOrderHeader which is
#  populated by EXC.usp_Load_Transfer_Order_Request from the scheduled
#  dates in STG.WASP_STO_Scheduled.
#
#  RequestedReceiptDate = SAP Delivery Date — flows through unchanged.
# =============================================================================

import json
import sys
import time
import logging
import traceback
import requests
import pyodbc
import configparser
import socket
from pathlib import Path
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

SCRIPT_NAME  = "d365_create_transfer_orders.py"
MACHINE_NAME = socket.gethostname()

# =========================================================
# INI CONFIG
# =========================================================
CONFIG_SECTION_DB   = "Fusion_EPOS_Production"
CONFIG_SECTION_D365 = "D365_Production"
CONFIG_PATH         = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"

def resolve_ini_path(path_str: str) -> Path:
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

INI_PATH = resolve_ini_path(CONFIG_PATH)
cfg = configparser.ConfigParser()
cfg.read(INI_PATH)

# =========================================================
# DB CONFIG
# =========================================================
db         = cfg[CONFIG_SECTION_DB]
DB_SERVER  = db.get("server")
DB_NAME    = db.get("database")
DB_USER    = db.get("user")
DB_PASS    = db.get("password")
DB_DRIVER  = db.get("driver")
DB_ENCRYPT = db.get("encrypt", "yes")
DB_TRUST   = db.get("trust_server_certificate", "no")

def get_db_connection():
    conn_str = (
        f"Driver={{{DB_DRIVER}}};"
        f"Server={DB_SERVER};"
        f"Database={DB_NAME};"
        f"UID={DB_USER};PWD={DB_PASS};"
        f"Encrypt={DB_ENCRYPT};TrustServerCertificate={DB_TRUST};"
    )
    return pyodbc.connect(conn_str)

# =========================================================
# D365 CONFIG
# =========================================================
d365               = cfg[CONFIG_SECTION_D365]
D365_TOKEN_URL     = d365.get("token_url")
D365_CLIENT_ID     = d365.get("client_id")
D365_CLIENT_SECRET = d365.get("client_secret")
D365_SCOPE         = d365.get("scope")
D365_ODATA_BASE    = d365.get("odata_base_url").rstrip("/")
D365_TIMEOUT       = int(d365.get("timeout", "60"))
D365_COMPANY       = d365.get("company", "").strip()

# =========================================================
# LOGGING
# =========================================================
BASE_DIR = Path(
    r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub"
    r"\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs"
)
BASE_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = BASE_DIR / "d365_create_transfer_orders.log"
logger   = logging.getLogger("d365_create_to_prod")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()

fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000,
                         backupCount=10, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(fh)

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(sh)

def log(m): logger.info(m)
def dbg(m): logger.debug(m)
def err(m): logger.error(m)

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

# ── Console helpers ───────────────────────────────────────
def banner(text, bg=C.BGMAG, fg=C.BWHT):
    pad = max(0, _W - len(text) - 4)
    l, r = pad // 2, pad - pad // 2
    print(f"\n{bg}{fg}{C.BOLD}  {'─'*l}  {text}  {'─'*r}  {C.RST}")

def section(text):
    print(f"\n{C.BOLD}{C.BCYN}{'═'*_W}{C.RST}")
    print(f"{C.BOLD}{C.BCYN}  ◆  {text}{C.RST}")
    print(f"{C.BOLD}{C.BCYN}{'═'*_W}{C.RST}")

def order_banner(idx, total, source_id, control_id, delivery_date):
    print(f"\n{C.BGBLU}{C.BWHT}{C.BOLD}"
          f"  [{idx}/{total}]  {source_id:<30}"
          f"  ControlId={control_id}"
          f"  Delivery: {delivery_date}  {C.RST}")

def divider():
    print(f"  {C.DIM}{'─'*(_W-2)}{C.RST}")

def kv(label, val, vc=C.BWHT):
    print(f"  {C.DIM}{label:<32}{C.RST}{vc}{val}{C.RST}")

def con_info(m):  print(f"  {C.BCYN}[{_ts()}]{C.RST}  {C.BCYN}ℹ{C.RST}  {m}")
def con_ok(m):    print(f"  {C.BGRN}[{_ts()}]{C.RST}  {C.BGRN}✔{C.RST}  {m}")
def con_warn(m):  print(f"  {C.BYLW}[{_ts()}]{C.RST}  {C.BYLW}⚠{C.RST}  {m}")
def con_err(m):   print(f"  {C.RED}[{_ts()}]{C.RST}  {C.RED}✘{C.RST}  {m}")

def api_req(method, url, payload):
    safe = {k: "***" if "secret" in k.lower() or "password" in k.lower()
            else v for k, v in payload.items()}
    print(f"\n  {C.DIM}┌─ API REQUEST {'─'*(_W-18)}{C.RST}")
    print(f"  {C.DIM}│{C.RST}  {C.BOLD}{method}{C.RST}  {C.BBLU}{url}{C.RST}")
    for k, v in safe.items():
        print(f"  {C.DIM}│  {k}: {v}{C.RST}")
    print(f"  {C.DIM}└{'─'*(_W-4)}{C.RST}")

def api_resp(status, body, label=""):
    tag   = f" [{label}]" if label else ""
    is_ok = status in (200, 201, 204)
    sym   = f"{C.BGRN}✔{C.RST}" if is_ok else f"{C.RED}✘{C.RST}"
    sc    = C.BGRN if is_ok else C.RED
    print(f"\n  {C.DIM}┌─ API RESPONSE{tag} {'─'*max(1,_W-18-len(tag))}{C.RST}")
    print(f"  {C.DIM}│{C.RST}  {sym}  {sc}{C.BOLD}HTTP {status}{C.RST}")
    if isinstance(body, dict):
        for k, v in list(body.items())[:12]:
            print(f"  {C.DIM}│  {k}: {v}{C.RST}")
        if len(body) > 12:
            print(f"  {C.DIM}│  ... (+{len(body)-12} more){C.RST}")
    else:
        print(f"  {C.DIM}│  {str(body)[:300]}{C.RST}")
    print(f"  {C.DIM}└{'─'*(_W-4)}{C.RST}\n")

def order_result(source_id, to_number, line_count,
                 success, duration_s, error_msg=""):
    bg  = C.BGGRN + C.BOLD if success else C.BGRED + C.BWHT + C.BOLD
    ico = "✅" if success else "❌"
    st  = "SUCCESS" if success else "FAILED"
    print(f"\n  {bg}  {ico}  {source_id}  —  {st}  {C.RST}")
    kv("TO Number",  to_number or "(none)",
       C.BGRN + C.BOLD if success else C.RED)
    kv("Lines Sent", str(line_count))
    kv("Duration",   f"{duration_s:.1f}s")
    if error_msg:
        kv("Error", error_msg[:120], C.RED)

def run_summary(total, success, failed, skipped, duration_s):
    bg = C.BGGRN + C.BOLD if not failed else C.BGRED + C.BWHT + C.BOLD
    section("RUN COMPLETE")
    kv("Total Queued", str(total))
    kv("Sent OK",      str(success), C.BGRN + C.BOLD)
    kv("Failed",       str(failed),
       C.RED + C.BOLD if failed else C.DIM)
    kv("Skipped",      str(skipped), C.DIM)
    kv("Elapsed",      f"{duration_s:.1f}s")

# =========================================================
# FILE OUTPUT HELPERS
# =========================================================
def make_output_dir(source_id: str) -> Path:
    stamp = time.strftime("%Y%m%d_%H%M%S")
    out   = BASE_DIR / f"{source_id}_{stamp}"
    (out / "lines").mkdir(parents=True, exist_ok=True)
    return out

def save_json(path: Path, name: str, data):
    fp = path / f"{name}.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    dbg(f"Saved {fp}")

# =========================================================
# HTTP
# =========================================================
def http_post_json(url, headers, payload):
    dbg(f"POST {url}")
    r = requests.post(url, headers=headers,
                      json=payload, timeout=D365_TIMEOUT)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    return r.status_code, body

# =========================================================
# SQL
# =========================================================
def sql_get_pending_controls(conn):
    sql = """
    SELECT
        c.TransferControlId,
        c.SourceID,
        c.OrderId,
        c.CustomerCode
    FROM EXC.TransferOrder_Control c
    WHERE c.ReadyForProcessing = 1
      AND c.MasterStatus IN ('VALIDATED','ERROR')
      AND (c.DynamicsDocumentNo IS NULL
           OR LTRIM(RTRIM(c.DynamicsDocumentNo)) = '')
    ORDER BY c.TransferControlId;
    """
    return conn.execute(sql).fetchall()

def sql_get_header(conn, control_id):
    sql = """
    SELECT h.SourceID, h.ShippingWarehouseId, h.ReceivingWarehouseId,
           h.RequestedShippingDate, h.RequestedReceiptDate
    FROM INT.TransferOrderHeader h
    WHERE h.TransferControlId = ?;
    """
    return conn.execute(sql, control_id).fetchone()

def sql_get_lines(conn, control_id):
    sql = """
    SELECT [LineNo], ProductCode, Quantity
    FROM INT.TransferOrderLine
    WHERE TransferControlId = ?
    ORDER BY [LineNo];
    """
    return conn.execute(sql, control_id).fetchall()

def sql_update_control(conn, control_id, master_status,
                       doc_no=None, resp_code=None, resp_msg=None,
                       resp_json=None, last_error=None,
                       sent_at=None, resp_at=None):
    sql = """
    UPDATE EXC.TransferOrder_Control
    SET MasterStatus            = ?,
        StatusUpdatedAt         = SYSUTCDATETIME(),
        DynamicsDocumentNo      = COALESCE(?, DynamicsDocumentNo),
        DynamicsResponseCode    = ?,
        DynamicsResponseMessage = ?,
        DynamicsResponseJson    = ?,
        DynamicsLastSentAt      = ?,
        DynamicsLastResponseAt  = ?,
        LastError               = ?
    WHERE TransferControlId = ?;
    """
    conn.execute(sql, master_status, doc_no, resp_code, resp_msg,
                 resp_json, sent_at, resp_at, last_error, control_id)
    conn.commit()

# =========================================================
# AUTH TOKEN
# =========================================================
def get_token(out_dir: Path) -> str:
    con_info("Requesting Azure AD token...")
    data = {
        "grant_type":    "client_credentials",
        "client_id":     D365_CLIENT_ID,
        "client_secret": D365_CLIENT_SECRET,
        "scope":         D365_SCOPE,
    }
    api_req("POST", D365_TOKEN_URL, {**data, "client_secret": "***"})
    save_json(out_dir, "auth_request",
              {"url": D365_TOKEN_URL,
               "payload": {**data, "client_secret": "***"}})

    r = requests.post(
        D365_TOKEN_URL, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=D365_TIMEOUT
    )
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}

    save_json(out_dir, "auth_response",
              {"status": r.status_code, "body": body})

    safe = {k: ("***TOKEN***" if k == "access_token" else v)
            for k, v in (body.items()
                         if isinstance(body, dict) else {}.items())}
    api_resp(r.status_code, safe, label="AUTH")

    if r.status_code != 200:
        raise RuntimeError(f"Auth failed: {body}")

    con_ok("Token acquired")
    return body["access_token"]

# =========================================================
# D365 CREATE HEADER
# =========================================================
def create_header(http_headers, out_dir, payload):
    url = f"{D365_ODATA_BASE}/TransferOrderHeaders"
    con_info("Creating Transfer Order Header in Dynamics...")
    api_req("POST", url, payload)
    save_json(out_dir, "header_request", {"url": url, "payload": payload})

    status, body = http_post_json(url, http_headers, payload)
    save_json(out_dir, "header_response",
              {"status": status, "body": body})
    api_resp(status, body, label="HEADER")

    if status not in (200, 201):
        raise RuntimeError(f"Header create failed: HTTP {status} | {body}")

    to_number = (body.get("TransferOrderNumber")
                 or body.get("transferOrderNumber")
                 or body.get("TransferOrderId"))
    con_ok(f"Header created  ▶  {C.BOLD}{C.BGRN}{to_number}{C.RST}")
    return to_number

# =========================================================
# D365 ADD LINE
# Uses scheduled dates from INT.TransferOrderHeader as base.
# Retries forward by business day if D365 rejects "not an open date".
# =========================================================
def add_line_with_retry(http_headers, out_dir, to_number, line_no,
                        item, qty,
                        base_receipt_dt: datetime,
                        base_ship_dt: datetime):
    url      = f"{D365_ODATA_BASE}/TransferOrderLinesV2"
    line_dir = out_dir / "lines"

    # Build up to 5 candidate receipt dates starting from the
    # scheduled date — weekdays only.
    attempts  = []
    candidate = base_receipt_dt
    while len(attempts) < 5:
        if candidate.weekday() < 5:
            attempts.append(candidate)
        candidate += timedelta(days=1)

    last_status, last_body = None, None

    for attempt_no, receipt_dt in enumerate(attempts, start=1):
        # Ship = 1 business day before receipt
        ship_dt = receipt_dt - timedelta(days=1)
        while ship_dt.weekday() >= 5:
            ship_dt -= timedelta(days=1)

        payload = {
            "TransferOrderNumber":   to_number,
            "LineNumber":            int(line_no),
            "ItemNumber":            item,
            "TransferQuantity":      float(qty),
            "RequestedShippingDate": ship_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "RequestedReceiptDate":  receipt_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if D365_COMPANY:
            payload["dataAreaId"] = D365_COMPANY

        if attempt_no == 1:
            con_info(
                f"  Line {C.BOLD}{line_no:>3}{C.RST}"
                f"  {C.DIM}│{C.RST}"
                f"  Item: {C.BWHT}{item:<22}{C.RST}"
                f"  Qty: {C.BCYN}{qty}{C.RST}"
                f"  Receipt: {C.BGRN}{receipt_dt.date()}{C.RST}"
                f"  Ship: {C.DIM}{ship_dt.date()}{C.RST}"
            )
        else:
            con_warn(
                f"  Line {line_no:>3}  │  Retry {attempt_no}"
                f"  → receipt shifted to "
                f"{C.BYLW}{receipt_dt.date()}{C.RST}"
            )

        api_req("POST", url, payload)
        save_json(line_dir,
                  f"line_{line_no}_attempt_{attempt_no}_request",
                  {"url": url, "payload": payload})

        last_status, last_body = http_post_json(url, http_headers, payload)

        save_json(line_dir,
                  f"line_{line_no}_attempt_{attempt_no}_response",
                  {"status": last_status, "body": last_body})
        api_resp(last_status, last_body,
                 label=f"LINE {line_no} attempt {attempt_no}")

        if last_status in (200, 201, 204):
            con_ok(
                f"  Line {C.BOLD}{line_no}{C.RST} accepted"
                f"  │  Receipt: {C.BGRN}{receipt_dt.date()}{C.RST}"
                f"  Ship: {C.DIM}{ship_dt.date()}{C.RST}"
            )
            return True, last_status, last_body

        msg = json.dumps(last_body).lower()
        if "not an open date" in msg:
            con_warn(
                f"  Line {line_no}  —  D365 rejected {receipt_dt.date()} "
                f"(not an open date)  —  trying next business day"
            )
            continue

        # Any other error — do not retry
        return False, last_status, last_body

    return False, last_status, last_body

# =========================================================
# PROCESS ONE CONTROL RECORD
# =========================================================
def process_one(conn, control_id: int, source_id: str,
                order_id: str,
                customer_code: str) -> tuple[bool, str, int]:
    out        = make_output_dir(source_id)
    start      = time.time()
    to_number  = None
    lines_sent = 0

    log(f"Processing {source_id} (ControlId={control_id})")

    header = sql_get_header(conn, control_id)
    if not header:
        raise RuntimeError(
            f"No INT.TransferOrderHeader for ControlId={control_id}")

    lines = sql_get_lines(conn, control_id)
    if not lines:
        raise RuntimeError(
            f"No INT.TransferOrderLine for ControlId={control_id}")

    # ── Order detail display ──────────────────────────────
    divider()
    kv("Order ID",             order_id)
    kv("Customer",             customer_code)
    kv("Route",
       f"{header.ShippingWarehouseId}  →  {header.ReceivingWarehouseId}")
    kv("Ship Date",
       str(header.RequestedShippingDate)[:10], C.BCYN)
    kv("SAP Delivery Date  ★",
       str(header.RequestedReceiptDate)[:10],  C.BGRN + C.BOLD)
    kv("Lines to send",        str(len(lines)))
    divider()

    # ── Build datetimes from the SCHEDULED dates ──────────
    # Sourced from STG.WASP_STO_Scheduled → INT.TransferOrderHeader.
    # RequestedReceiptDate is the SAP Delivery Date — never overridden.
    scheduled_receipt_dt = datetime(
        header.RequestedReceiptDate.year,
        header.RequestedReceiptDate.month,
        header.RequestedReceiptDate.day,
        12, 0, 0, tzinfo=timezone.utc
    )
    scheduled_ship_dt = datetime(
        header.RequestedShippingDate.year,
        header.RequestedShippingDate.month,
        header.RequestedShippingDate.day,
        12, 0, 0, tzinfo=timezone.utc
    )

    header_payload = {
        "ShippingWarehouseId":   header.ShippingWarehouseId,
        "ReceivingWarehouseId":  header.ReceivingWarehouseId,
        "RequestedShippingDate": scheduled_ship_dt.strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
        "RequestedReceiptDate":  scheduled_receipt_dt.strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
    }
    if D365_COMPANY:
        header_payload["dataAreaId"] = D365_COMPANY

    # ── Token ─────────────────────────────────────────────
    token        = get_token(out)
    http_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    sent_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sql_update_control(conn, control_id, master_status="VALIDATED",
                       sent_at=sent_at, last_error=None)

    # ── Create D365 Header ────────────────────────────────
    try:
        to_number = create_header(http_headers, out, header_payload)
    except Exception as ex:
        resp = str(ex)
        sql_update_control(
            conn, control_id, master_status="ERROR",
            resp_code="HEADER_FAIL",
            resp_msg="Header create failed",
            resp_json=resp, last_error=resp,
            resp_at=datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"))
        raise

    # ── Send Lines ────────────────────────────────────────
    con_info(f"Sending {len(lines)} line(s) for TO "
             f"{C.BOLD}{C.BGRN}{to_number}{C.RST}...")
    all_ok       = True
    last_failure = None

    for ln in lines:
        ok_flag, http_status, body = add_line_with_retry(
            http_headers, out,
            to_number=to_number,
            line_no=ln.LineNo,
            item=ln.ProductCode,
            qty=ln.Quantity,
            base_receipt_dt=scheduled_receipt_dt,
            base_ship_dt=scheduled_ship_dt
        )
        if ok_flag:
            lines_sent += 1
        else:
            all_ok       = False
            last_failure = (f"Line {ln.LineNo} failed: "
                            f"HTTP {http_status} | {body}")
            break

    resp_at  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    duration = time.time() - start

    if not all_ok:
        sql_update_control(
            conn, control_id, master_status="ERROR",
            doc_no=to_number,
            resp_code="LINE_FAIL",
            resp_msg="One or more lines failed",
            resp_json=last_failure,
            last_error=last_failure,
            resp_at=resp_at)
        order_result(source_id, to_number, lines_sent,
                     success=False, duration_s=duration,
                     error_msg=last_failure)
        raise RuntimeError(last_failure)

    # ── Success ───────────────────────────────────────────
    sql_update_control(
        conn, control_id, master_status="SENT_DYNAMICS",
        doc_no=to_number,
        resp_code="OK",
        resp_msg="Header and lines created in Dynamics",
        resp_json=json.dumps({"TransferOrderNumber": to_number}),
        last_error=None,
        resp_at=resp_at)

    order_result(source_id, to_number, lines_sent,
                 success=True, duration_s=duration)
    log(f"✔ D365 Transfer Order created: {to_number} for {source_id}")
    return True, to_number, lines_sent

# =========================================================
# MAIN
# =========================================================
def main():
    run_start = time.time()

    banner("D365 TRANSFER ORDER CREATION")
    section("STARTUP")
    kv("Script",   SCRIPT_NAME)
    kv("Machine",  MACHINE_NAME)
    kv("INI",      str(INI_PATH))
    kv("DB",       f"{DB_SERVER} / {DB_NAME}")
    kv("D365",     D365_ODATA_BASE)
    log(f"Using INI: {INI_PATH}")

    conn   = get_db_connection()
    counts = {"total": 0, "success": 0, "failed": 0, "skipped": 0}
    con_ok("DB connection established")

    try:
        section("SCANNING FOR PENDING ORDERS")
        pending         = sql_get_pending_controls(conn)
        counts["total"] = len(pending)

        if not pending:
            con_warn("No orders pending for Dynamics — nothing to do.")
            log("No pending orders.")
            run_summary(0, 0, 0, 0, time.time() - run_start)
            return

        con_ok(f"Found {C.BOLD}{counts['total']}{C.RST} order(s) to process")

        # ── Pre-flight display ────────────────────────────
        section("ORDERS QUEUED FOR D365")
        for r in pending:
            hdr = sql_get_header(conn, r.TransferControlId)
            if hdr:
                print(
                    f"  {C.BGRN}▶{C.RST}  "
                    f"{C.BOLD}{r.SourceID:<30}{C.RST}  "
                    f"Order: {C.DIM}{r.OrderId}{C.RST}  "
                    f"Cust: {C.DIM}{r.CustomerCode}{C.RST}\n"
                    f"     {C.DIM}Ship:{C.RST} "
                    f"{C.BCYN}{str(hdr.RequestedShippingDate)[:10]}{C.RST}   "
                    f"{C.DIM}SAP Delivery Date ★:{C.RST}  "
                    f"{C.BGRN}{C.BOLD}"
                    f"{str(hdr.RequestedReceiptDate)[:10]}{C.RST}"
                )

        section("PROCESSING ORDERS")

        for idx, row in enumerate(pending, start=1):
            hdr          = sql_get_header(conn, row.TransferControlId)
            delivery_str = (str(hdr.RequestedReceiptDate)[:10]
                            if hdr else "unknown")

            order_banner(idx, counts["total"], row.SourceID,
                         row.TransferControlId, delivery_str)
            try:
                process_one(conn, row.TransferControlId, row.SourceID,
                            row.OrderId, row.CustomerCode)
                counts["success"] += 1
            except Exception as ex:
                counts["failed"] += 1
                err(f"❌ Failed {row.SourceID}: {ex}")
                dbg(traceback.format_exc())
                con_err(f"Order {row.SourceID} failed — continuing")
                continue

    finally:
        conn.close()
        run_summary(
            counts["total"],
            counts["success"],
            counts["failed"],
            counts["skipped"],
            time.time() - run_start
        )


if __name__ == "__main__":
    main()
