#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
import time
import logging
import traceback
import requests
import pyodbc
import configparser
from pathlib import Path
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

# =========================================================
# INI CONFIG (PRODUCTION)
# =========================================================
CONFIG_SECTION_DB   = "Fusion_EPOS_Production"
CONFIG_SECTION_D365 = "D365_Production"
CONFIG_PATH         = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"

def resolve_ini_path(path_str: str) -> Path:
    p = Path(path_str)
    if p.is_dir():
        candidate = p / "Master_ini_config.ini"
        if not candidate.exists():
            raise FileNotFoundError(
                f"INI folder provided but Master_ini_config.ini not found: {candidate}")
        return candidate
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
BASE_DIR = Path(r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs")
BASE_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = BASE_DIR / "d365_create_transfer_orders.log"
logger   = logging.getLogger("d365_create_to_prod")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()

fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=10, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(fh)

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(sh)

def log(msg): logger.info(msg)
def dbg(msg): logger.debug(msg)
def err(msg): logger.error(msg)

# =========================================================
# CONSOLE HELPERS
# =========================================================
_W = 65

def _ts():
    return datetime.now().strftime("%H:%M:%S")

def con_section(title):
    print(f"\n{'=' * _W}")
    print(f"  {title}")
    print(f"{'=' * _W}")

def con_info(msg):  print(f"[{_ts()}]  INFO   {msg}")
def con_ok(msg):    print(f"[{_ts()}]  OK     {msg}")
def con_warn(msg):  print(f"[{_ts()}]  WARN   {msg}")
def con_error(msg): print(f"[{_ts()}]  ERROR  {msg}")
def con_skip(msg):  print(f"[{_ts()}]  SKIP   {msg}")

def con_divider():
    print(f"  {'-' * (_W - 2)}")

def con_api_request(method: str, url: str, payload: dict):
    safe = {k: ("***" if "secret" in k.lower() or "password" in k.lower() else v)
            for k, v in payload.items()}
    print(f"\n  ┌─ API REQUEST {'─' * (_W - 17)}")
    print(f"  │  {method}  {url}")
    for k, v in safe.items():
        print(f"  │  {k}: {v}")
    print(f"  └{'─' * (_W - 3)}")

def con_api_response(status: int, body: dict, label: str = ""):
    tag = f" [{label}]" if label else ""
    ok  = status in (200, 201, 204)
    sym = "✔" if ok else "✘"
    print(f"\n  ┌─ API RESPONSE{tag} {'─' * max(1, _W - 17 - len(tag))}")
    print(f"  │  {sym}  HTTP {status}")
    if isinstance(body, dict):
        for k, v in list(body.items())[:12]:
            print(f"  │  {k}: {v}")
        if len(body) > 12:
            print(f"  │  ... (+{len(body) - 12} more fields)")
    else:
        print(f"  │  {str(body)[:300]}")
    print(f"  └{'─' * (_W - 3)}\n")

def con_order_header(idx: int, total: int, source_id: str, control_id: int):
    print(f"\n{'─' * _W}")
    print(f"  ORDER {idx}/{total}  |  {source_id}  (ControlId={control_id})")
    print(f"{'─' * _W}")

def con_order_summary(source_id: str, to_number, line_count: int,
                      success: bool, duration_s: float, error: str = ""):
    status = "SUCCESS" if success else "FAILED"
    print(f"\n  ┌─ ORDER RESULT {'─' * (_W - 17)}")
    print(f"  │  Source ID    : {source_id}")
    print(f"  │  Status       : {status}")
    print(f"  │  TO Number    : {to_number or '(none)'}")
    print(f"  │  Lines Sent   : {line_count}")
    print(f"  │  Duration     : {duration_s:.1f}s")
    if error:
        print(f"  │  Error        : {error}")
    print(f"  └{'─' * (_W - 3)}")

def con_run_summary(total: int, success: int, failed: int,
                    skipped: int, duration_s: float):
    print(f"\n{'=' * _W}")
    print(f"  RUN COMPLETE")
    print(f"{'─' * _W}")
    print(f"  Total Queued   : {total}")
    print(f"  Sent OK        : {success}")
    print(f"  Failed         : {failed}")
    print(f"  Skipped        : {skipped}")
    print(f"  Elapsed        : {duration_s:.1f}s")
    print(f"{'=' * _W}\n")

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
# HTTP HELPERS
# =========================================================
def http_post_json(url, headers, payload):
    dbg(f"POST {url}")
    r = requests.post(url, headers=headers, json=payload, timeout=D365_TIMEOUT)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    return r.status_code, body

# =========================================================
# SQL HELPERS
# =========================================================
def sql_get_pending_controls(conn):
    sql = """
    SELECT c.TransferControlId, c.SourceID
    FROM EXC.TransferOrder_Control c
    WHERE c.ReadyForProcessing = 1
      AND c.MasterStatus IN ('VALIDATED', 'ERROR')
      AND (c.DynamicsDocumentNo IS NULL OR LTRIM(RTRIM(c.DynamicsDocumentNo)) = '')
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

def sql_update_control_dynamics(conn, control_id, master_status,
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

    con_api_request("POST", D365_TOKEN_URL,
                    {**data, "client_secret": "***"})

    save_json(out_dir, "auth_request",
              {"url": D365_TOKEN_URL,
               "payload": {**data, "client_secret": "***"}})

    r = requests.post(
        D365_TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=D365_TIMEOUT
    )
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}

    save_json(out_dir, "auth_response", {"status": r.status_code, "body": body})

    safe_body = {k: ("***TOKEN***" if k == "access_token" else v)
                 for k, v in (body.items() if isinstance(body, dict) else {}.items())}
    con_api_response(r.status_code, safe_body, label="AUTH")

    if r.status_code != 200:
        raise RuntimeError(f"Auth failed: {body}")

    con_ok("Token acquired OK")
    return body["access_token"]

# =========================================================
# D365 CREATE HEADER
# =========================================================
def create_header(http_headers, out_dir, payload):
    url = f"{D365_ODATA_BASE}/TransferOrderHeaders"

    con_info("Creating Transfer Order Header in Dynamics...")
    con_api_request("POST", url, payload)
    save_json(out_dir, "header_request", {"url": url, "payload": payload})

    status, body = http_post_json(url, http_headers, payload)

    save_json(out_dir, "header_response", {"status": status, "body": body})
    con_api_response(status, body, label="HEADER")

    if status not in (200, 201):
        raise RuntimeError(f"Header create failed: HTTP {status} | {body}")

    to_number = (body.get("TransferOrderNumber")
                 or body.get("transferOrderNumber")
                 or body.get("TransferOrderId"))
    con_ok(f"Header created  —  Transfer Order Number: {to_number}")
    return to_number

# =========================================================
# D365 ADD LINE
# Uses scheduled dates from INT.TransferOrderHeader as base.
# Retries by shifting forward one business day at a time if
# D365 rejects with "not an open date".
# =========================================================
def add_line_with_retry(http_headers, out_dir, to_number, line_no,
                        item, qty,
                        base_receipt_dt: datetime,
                        base_ship_dt: datetime):
    url      = f"{D365_ODATA_BASE}/TransferOrderLinesV2"
    line_dir = out_dir / "lines"

    # Build up to 5 retry attempts starting from the scheduled date.
    # Only business days (Mon–Fri) are valid receipt dates.
    attempts = []
    candidate = base_receipt_dt
    while len(attempts) < 5:
        if candidate.weekday() < 5:
            attempts.append(candidate)
        candidate = candidate + timedelta(days=1)

    for attempt_no, receipt_dt in enumerate(attempts, start=1):
        # Shipping = 1 business day before receipt
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
            con_info(f"  Line {line_no:>3}  |  Item: {item:<20}  "
                     f"Qty: {qty}  |  Receipt: {receipt_dt.date()}  "
                     f"Ship: {ship_dt.date()}")
        else:
            con_warn(f"  Line {line_no:>3}  |  Retry {attempt_no}  "
                     f"(receipt shifted to {receipt_dt.date()})")

        con_api_request("POST", url, payload)
        save_json(line_dir,
                  f"line_{line_no}_attempt_{attempt_no}_request",
                  {"url": url, "payload": payload})

        status, body = http_post_json(url, http_headers, payload)

        save_json(line_dir,
                  f"line_{line_no}_attempt_{attempt_no}_response",
                  {"status": status, "body": body})
        con_api_response(status, body,
                         label=f"LINE {line_no} attempt {attempt_no}")

        if status in (200, 201, 204):
            con_ok(f"  Line {line_no} accepted  |  "
                   f"Receipt: {receipt_dt.date()}  Ship: {ship_dt.date()}")
            return True, status, body

        msg = json.dumps(body).lower()
        if "not an open date" in msg:
            con_warn(f"  Line {line_no} — D365 rejected {receipt_dt.date()} "
                     f"(not an open date) — trying next business day")
            continue

        # Any other error — do not retry
        return False, status, body

    return False, status, body

# =========================================================
# PROCESS ONE CONTROL RECORD
# =========================================================
def process_one(conn, control_id: int, source_id: str) -> tuple[bool, str, int]:
    """Returns (success, to_number, lines_sent)."""
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

    con_info(f"Header: {header.ShippingWarehouseId} → "
             f"{header.ReceivingWarehouseId}  "
             f"|  Ship: {header.RequestedShippingDate}  "
             f"|  Receipt (SAP Delivery): {header.RequestedReceiptDate}")
    con_info(f"Lines to send: {len(lines)}")
    con_divider()

    requested_ship = header.RequestedShippingDate
    requested_recv = header.RequestedReceiptDate

    # ── Build datetime objects from the SCHEDULED dates ──
    # These come from STG.WASP_STO_Scheduled via the SP.
    # All times set to 12:00 UTC as required by D365 OData.
    scheduled_receipt_dt = datetime(
        requested_recv.year, requested_recv.month, requested_recv.day,
        12, 0, 0, tzinfo=timezone.utc
    )
    scheduled_ship_dt = datetime(
        requested_ship.year, requested_ship.month, requested_ship.day,
        12, 0, 0, tzinfo=timezone.utc
    )

    header_payload = {
        "ShippingWarehouseId":   header.ShippingWarehouseId,
        "ReceivingWarehouseId":  header.ReceivingWarehouseId,
        "RequestedShippingDate": scheduled_ship_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "RequestedReceiptDate":  scheduled_receipt_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if D365_COMPANY:
        header_payload["dataAreaId"] = D365_COMPANY

    # ── Acquire token ─────────────────────────────────────
    token        = get_token(out)
    http_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    sent_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sql_update_control_dynamics(conn, control_id, master_status="VALIDATED",
                                sent_at=sent_at, last_error=None)

    # ── Create Header ─────────────────────────────────────
    try:
        to_number = create_header(http_headers, out, header_payload)
    except Exception as ex:
        resp = str(ex)
        sql_update_control_dynamics(
            conn, control_id, master_status="ERROR",
            resp_code="HEADER_FAIL",
            resp_msg="Header create failed",
            resp_json=resp, last_error=resp,
            resp_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        raise

    # ── Send Lines ────────────────────────────────────────
    con_info(f"Sending {len(lines)} line(s) for TO {to_number}...")
    all_ok       = True
    last_failure = None

    for ln in lines:
        ok, http_status, body = add_line_with_retry(
            http_headers, out,
            to_number=to_number,
            line_no=ln.LineNo,
            item=ln.ProductCode,
            qty=ln.Quantity,
            base_receipt_dt=scheduled_receipt_dt,
            base_ship_dt=scheduled_ship_dt
        )
        if ok:
            lines_sent += 1
        else:
            all_ok       = False
            last_failure = (f"Line {ln.LineNo} failed: "
                            f"HTTP {http_status} | {body}")
            break

    resp_at  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    duration = time.time() - start

    if not all_ok:
        sql_update_control_dynamics(
            conn, control_id, master_status="ERROR",
            doc_no=to_number,
            resp_code="LINE_FAIL",
            resp_msg="One or more lines failed",
            resp_json=last_failure,
            last_error=last_failure,
            resp_at=resp_at)
        con_order_summary(source_id, to_number, lines_sent,
                          success=False, duration_s=duration,
                          error=last_failure)
        raise RuntimeError(last_failure)

    # ── Success ───────────────────────────────────────────
    sql_update_control_dynamics(
        conn, control_id, master_status="SENT_DYNAMICS",
        doc_no=to_number,
        resp_code="OK",
        resp_msg="Header and lines created in Dynamics",
        resp_json=json.dumps({"TransferOrderNumber": to_number}),
        last_error=None,
        resp_at=resp_at)

    con_order_summary(source_id, to_number, lines_sent,
                      success=True, duration_s=duration)
    log(f"✔ Created Dynamics Transfer Order: {to_number} for {source_id}")
    return True, to_number, lines_sent

# =========================================================
# MAIN
# =========================================================
def main():
    run_start = time.time()

    con_section("D365 TRANSFER ORDER CREATION  —  STARTUP")
    con_info(f"INI File  : {INI_PATH}")
    con_info(f"DB Server : {DB_SERVER}  /  {DB_NAME}")
    con_info(f"D365 Base : {D365_ODATA_BASE}")
    log(f"Using INI: {INI_PATH}")

    conn   = get_db_connection()
    counts = {"total": 0, "success": 0, "failed": 0, "skipped": 0}
    con_ok("DB connection established")

    try:
        con_section("SCANNING FOR PENDING ORDERS")
        pending         = sql_get_pending_controls(conn)
        counts["total"] = len(pending)

        if not pending:
            con_warn("No orders pending for Dynamics — nothing to do.")
            log("No pending orders ready for Dynamics.")
            con_run_summary(0, 0, 0, 0, time.time() - run_start)
            return

        con_ok(f"Found {counts['total']} order(s) to process")
        con_section("PROCESSING ORDERS")

        for idx, row in enumerate(pending, start=1):
            control_id = row.TransferControlId
            source_id  = row.SourceID
            con_order_header(idx, counts["total"], source_id, control_id)

            try:
                process_one(conn, control_id, source_id)
                counts["success"] += 1
            except Exception as ex:
                counts["failed"] += 1
                err(f"❌ Failed {source_id}: {ex}")
                dbg(traceback.format_exc())
                con_error(f"Order {source_id} failed — continuing to next order")
                continue

    finally:
        conn.close()
        con_run_summary(
            counts["total"],
            counts["success"],
            counts["failed"],
            counts["skipped"],
            time.time() - run_start
        )


if __name__ == "__main__":
    main()