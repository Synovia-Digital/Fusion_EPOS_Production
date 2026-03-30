#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sap_cpi_transmit.py  --  Production

Pipeline:
    STEP 1  EXEC INT.usp_Stage_Transfer_Orders_For_SAP
               Populates INT.Transfer_Order_SAP_Header/Item/Note
               from EXC.TransferOrder_Control + INT.TransferOrderLine

    STEP 1b UPDATE INT.Transfer_Order_SAP_Header
               Normalise Delivery_Date to ISO format (YYYY-MM-DDTHH:MM:SS)
               for all rows where SynProcessed = 0
               (SP sets the correct +2 day value but may store in a
               non-standard format such as 'Mar 27 2026 12:00AM')

    STEP 2  Scan INT.Transfer_Order_SAP_Header  (SynProcessed = 0)

    STEP 3  For each order:
               OAuth2 token  (cached for run lifetime)
               POST payload to SAP CPI endpoint
               Log request + response to INT + EXC tables
               Save JSON files to D:\\TransferOrder_SAP\\<SO_Number>\\

Source tables  (read):
    INT.Transfer_Order_SAP_Header
    INT.Transfer_Order_SAP_Item
    INT.Transfer_Order_SAP_Note

Write tables:
    INT.Transfer_Order_SAP_Control
    INT.Transfer_Order_SAP_Request_Log
    INT.Transfer_Order_SAP_Response_Log
    EXC.TransferOrder_Control        (SapStatus, SapDocumentNo, ...)
    EXC.TransferOrder_InterfaceLog   (REQUEST + RESPONSE rows)

INI section: [SAP_CPI_Production]
    Note: ini values may be wrapped in quotes -- ini_val() strips them.
"""

import json
import sys
import time
import logging
import traceback
import pyodbc
import requests
import configparser
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler

# =========================================================
# PATHS
# =========================================================
CONFIG_SECTION_DB  = "Fusion_EPOS_Production"
CONFIG_SECTION_CPI = "SAP_CPI_Production"
CONFIG_PATH        = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"

OUTPUT_DIR = Path(r"D:\TransferOrder_SAP")
LOG_DIR    = Path(
    r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub"
    r"\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs"
)

# =========================================================
# INI RESOLUTION
# =========================================================
def resolve_ini(path_str: str) -> Path:
    p = Path(path_str)
    if p.is_dir():
        for name in ("Master_ini_config.ini", "Master_ini_Config.ini",
                     "master_ini_config.ini", "Master.ini", "config.ini"):
            c = p / name
            if c.exists():
                return c
        ini_files = sorted(p.glob("*.ini"))
        if ini_files:
            return ini_files[0]
        raise FileNotFoundError(f"No .ini files found in: {p}")
    if p.is_file():
        return p
    raise FileNotFoundError(f"Config path not found: {p}")


def ini_val(section, key: str, fallback: str = "") -> str:
    """
    Read a configparser value and strip any surrounding quotes.
    Handles ini files where values are written as:
        key = "value"   or   key = 'value'
    configparser does NOT strip these automatically.
    """
    raw = section.get(key, fallback)
    if raw:
        raw = raw.strip().strip('"').strip("'").strip()
    return raw


INI_PATH = resolve_ini(CONFIG_PATH)
cfg = configparser.ConfigParser()
with open(INI_PATH, encoding="utf-8-sig") as f:
    cfg.read_file(f)

# =========================================================
# DB CONFIG
# =========================================================
db = cfg[CONFIG_SECTION_DB]

def get_conn():
    cs = (
        f"Driver={{{ini_val(db, 'driver')}}};"
        f"Server={ini_val(db, 'server')};"
        f"Database={ini_val(db, 'database')};"
        f"UID={ini_val(db, 'user')};"
        f"PWD={ini_val(db, 'password')};"
        f"Encrypt={ini_val(db, 'encrypt', 'yes')};"
        f"TrustServerCertificate={ini_val(db, 'trust_server_certificate', 'no')};"
    )
    conn = pyodbc.connect(cs)
    conn.autocommit = False
    return conn

# =========================================================
# CPI CONFIG  (ini_val strips any surrounding quotes)
# =========================================================
cpi               = cfg[CONFIG_SECTION_CPI]
CPI_TOKEN_URL     = ini_val(cpi, "token_url")
CPI_CLIENT_ID     = ini_val(cpi, "client_id")
CPI_CLIENT_SECRET = ini_val(cpi, "client_secret")
CPI_ENDPOINT      = ini_val(cpi, "cpi_endpoint")
CPI_TIMEOUT       = int(ini_val(cpi, "timeout") or "60")

# =========================================================
# LOGGING
# =========================================================
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "sap_cpi_transmit.log"
logger   = logging.getLogger("sap_cpi_transmit")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()

fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=10,
                         encoding="utf-8")
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
_W = 68

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

def con_order_banner(idx, total, so_number, header_id, customer, lines):
    pct  = int((idx - 1) / total * 30) if total else 0
    bar  = "#" * pct + "-" * (30 - pct)
    print(f"\n{'_' * _W}")
    print(f"  ORDER {idx:>3}/{total}  [{bar}]  {int((idx-1)/total*100) if total else 0:>3}%")
    print(f"  SO Number  : {so_number}  (HeaderID={header_id})")
    print(f"  Customer   : {customer}   Lines: {lines}")
    print(f"{'_' * _W}")

def con_api_request(method, url, payload):
    safe = {}
    if isinstance(payload, dict):
        safe = {k: ("***" if any(x in k.lower()
                                 for x in ("secret", "password", "token"))
                    else v)
                for k, v in payload.items()}
    print(f"\n  +-- API REQUEST {'-' * (_W - 18)}")
    print(f"  |  {method}  {url}")
    for k, v in list(safe.items())[:10]:
        print(f"  |  {k}: {v}")
    print(f"  +{'-' * (_W - 4)}")

def con_api_response(status, body, label=""):
    tag = f" [{label}]" if label else ""
    sym = "OK  " if status in (200, 201, 204) else "FAIL"
    print(f"\n  +-- API RESPONSE{tag} {'-' * max(1, _W - 19 - len(tag))}")
    print(f"  |  [{sym}]  HTTP {status}")
    if isinstance(body, dict):
        for k, v in list(body.items())[:10]:
            print(f"  |  {k}: {v}")
        if len(body) > 10:
            print(f"  |  ... (+{len(body) - 10} more fields)")
    else:
        print(f"  |  {str(body)[:300]}")
    print(f"  +{'-' * (_W - 4)}\n")

def con_order_result(so_number, http_status, sap_doc, success, duration_s,
                     error=""):
    sym = "OK  " if success else "FAIL"
    print(f"\n  +-- ORDER RESULT [{sym}] {'-' * (_W - 22)}")
    print(f"  |  SO Number    : {so_number}")
    print(f"  |  HTTP Status  : {http_status}")
    print(f"  |  SAP Doc No   : {sap_doc or '(none)'}")
    print(f"  |  Duration     : {duration_s:.1f}s")
    if error:
        print(f"  |  Error        : {error}")
    print(f"  +{'-' * (_W - 4)}")

def con_progress_line(idx, total, success, failed):
    """Single-line running tally printed after each order."""
    remaining = total - idx
    pct = int(idx / total * 100) if total else 0
    print(
        f"\n  >> Progress: {idx}/{total} ({pct}%)  "
        f"Success={success}  Failed={failed}  Remaining={remaining}"
    )

def con_run_summary(total, success, failed, skipped, duration_s):
    rate = f"{success/total*100:.1f}%" if total else "n/a"
    avg  = f"{duration_s/total:.1f}s"  if total else "n/a"
    print(f"\n{'=' * _W}")
    print(f"  RUN COMPLETE  --  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'_' * _W}")
    print(f"  Total Queued   : {total}")
    print(f"  Sent OK        : {success}  ({rate})")
    print(f"  Failed         : {failed}")
    print(f"  Skipped/Error  : {skipped}")
    print(f"  Total Elapsed  : {duration_s:.1f}s")
    print(f"  Avg per Order  : {avg}")
    print(f"  Log File       : {LOG_FILE}")
    print(f"  JSON Output    : {OUTPUT_DIR}")
    print(f"{'=' * _W}\n")


# =========================================================
# DATE HELPERS
# =========================================================
def fmt_date(val) -> str:
    """
    Normalise any date/datetime value coming back from pyodbc into the
    ISO-8601 string that SAP CPI expects: YYYY-MM-DDTHH:MM:SS

    pyodbc can return:
        - datetime object  (most columns)
        - date object      (DATE columns)
        - str              (already formatted, pass through)
        - None             (return empty string)
    """
    if val is None:
        return ""
    if isinstance(val, str):
        # Already a string -- normalise separators just in case
        # e.g. "Mar 27 2026 12:00AM" -> re-parse and reformat
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d", "%b %d %Y %I:%M%p"):
            try:
                return datetime.strptime(val.strip(), fmt).strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        return val   # give up, pass through as-is
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%dT%H:%M:%S")
    return str(val)


# =========================================================
# JSON HELPERS
# =========================================================
def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%dT%H:%M:%S")
    return obj

def to_json_str(obj) -> str:
    return json.dumps(make_json_safe(obj), indent=4)

def save_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        f.write(to_json_str(data))
    dbg(f"Saved: {path}")


# =========================================================
# SQL -- READ
# =========================================================
def sql_get_pending_headers(conn):
    """INT.Transfer_Order_SAP_Header where SynProcessed = 0."""
    return conn.execute("""
        SELECT *
        FROM INT.Transfer_Order_SAP_Header
        WHERE SynProcessed = 0
        ORDER BY TransferOrderSAPHeader_ID
    """).fetchall()

def sql_get_items(conn, header_id):
    return conn.execute("""
        SELECT *
        FROM INT.Transfer_Order_SAP_Item
        WHERE TransferOrderSAPHeader_ID = ?
        ORDER BY Line_Number
    """, header_id).fetchall()

def sql_get_note(conn, header_id):
    return conn.execute("""
        SELECT *
        FROM INT.Transfer_Order_SAP_Note
        WHERE TransferOrderSAPHeader_ID = ?
    """, header_id).fetchone()

def sql_get_exc_control_id(conn, so_number: str):
    """Match back to EXC.TransferOrder_Control via DynamicsDocumentNo or SourceID."""
    row = conn.execute("""
        SELECT TransferControlId
        FROM EXC.TransferOrder_Control
        WHERE DynamicsDocumentNo = ?
           OR SourceID           = ?
    """, so_number, so_number).fetchone()
    return row.TransferControlId if row else None

def sql_get_queue_summary(conn) -> dict:
    """
    Pre-run summary counts shown in the startup banner.
    """
    row = conn.execute("""
        SELECT
            COUNT(*)                                              AS TotalHeaders,
            SUM(CASE WHEN SynProcessed = 0 THEN 1 ELSE 0 END)   AS PendingHeaders,
            SUM(CASE WHEN SynProcessed = 1 THEN 1 ELSE 0 END)   AS AlreadyProcessed,
            (SELECT COUNT(*) FROM INT.Transfer_Order_SAP_Item)   AS TotalItems,
            (SELECT COUNT(*) FROM INT.Transfer_Order_SAP_Control
             WHERE Status = 'Success')                           AS PrevSuccess,
            (SELECT COUNT(*) FROM INT.Transfer_Order_SAP_Control
             WHERE Status = 'Error')                             AS PrevErrors
        FROM INT.Transfer_Order_SAP_Header
    """).fetchone()
    return {
        "total_headers":     row.TotalHeaders     or 0,
        "pending":           row.PendingHeaders   or 0,
        "already_processed": row.AlreadyProcessed or 0,
        "total_items":       row.TotalItems       or 0,
        "prev_success":      row.PrevSuccess      or 0,
        "prev_errors":       row.PrevErrors       or 0,
    }


# =========================================================
# SQL -- WRITE
# =========================================================
def sql_normalise_delivery_dates(conn) -> int:
    """
    STEP 1b  --  Normalise Delivery_Date format in the DB.

    The staging SP (usp_Stage_Transfer_Orders_For_SAP) already sets
    Delivery_Date to the correct +2 day value, but the column value
    can be stored in a non-standard format (e.g. 'Mar 27 2026 12:00AM')
    depending on the source data type / implicit conversion.

    This UPDATE casts whatever is in the column through CONVERT to
    guarantee it is stored as a clean datetime with no time component:
        2026-03-27T00:00:00

    Only touches pending (SynProcessed = 0) rows that have a value.
    """
    cursor = conn.execute("""
        UPDATE INT.Transfer_Order_SAP_Header
        SET Delivery_Date = CONVERT(datetime, CONVERT(date, Delivery_Date), 120)
        WHERE SynProcessed = 0
          AND Delivery_Date IS NOT NULL
    """)
    return cursor.rowcount


def sql_insert_control(conn, so_number, source_id, customer_code) -> int:
    return conn.execute("""
        INSERT INTO INT.Transfer_Order_SAP_Control
            (TransferOrderNumber, SourceID, CustomerCode, Status)
        OUTPUT INSERTED.SAPControlID
        VALUES (?, ?, ?, 'Pending')
    """, so_number, source_id, customer_code).fetchone()[0]


def sql_insert_request_log(conn, sap_ctrl_id, payload, parsed):
    conn.execute("""
        INSERT INTO INT.Transfer_Order_SAP_Request_Log
        (
            SAPControlID, RequestJSON,
            SO_Type, Document_Date, Sales_Pool, SO_Number,
            Division, SubDivision, Customer_Code, Delivery_Point,
            Customer_Reference, Delivery_Date, Delivery_Note,
            Reason_Code, Note_Language, Special_Delivery_Note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        sap_ctrl_id, to_json_str(payload),
        parsed.get("SO_Type"),
        parsed.get("Document_Date"),
        parsed.get("Sales_Pool"),
        parsed.get("SO_Number"),
        parsed.get("Division"),
        parsed.get("SubDivision"),
        parsed.get("Customer_Code"),
        parsed.get("Delivery_Point"),
        parsed.get("Customer_Reference"),
        parsed.get("Delivery_Date"),
        parsed.get("Delivery_Note"),
        parsed.get("Reason_Code"),
        parsed.get("Language"),
        parsed.get("Special_Delivery_Note"),
    )


def sql_insert_response_log(conn, sap_ctrl_id, http_status, resp_body, errors):
    conn.execute("""
        INSERT INTO INT.Transfer_Order_SAP_Response_Log
            (SAPControlID, ResponseJSON, HTTPStatus,
             ErrorMessage1, ErrorMessage2, ErrorMessage3)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        sap_ctrl_id, to_json_str(resp_body), http_status,
        errors[0], errors[1], errors[2],
    )


def sql_update_sap_control(conn, sap_ctrl_id, status, error_msg=None):
    conn.execute("""
        UPDATE INT.Transfer_Order_SAP_Control
        SET Status       = ?,
            ErrorMessage = ?,
            SentOn       = SYSUTCDATETIME()
        WHERE SAPControlID = ?
    """, status, error_msg, sap_ctrl_id)


def sql_update_exc_sap_cols(conn, transfer_ctrl_id, sap_status,
                             sap_doc_no, resp_code, resp_msg,
                             resp_json, last_error):
    conn.execute("""
        UPDATE EXC.TransferOrder_Control
        SET SapStatus          = ?,
            SapDocumentNo      = COALESCE(?, SapDocumentNo),
            SapResponseCode    = ?,
            SapResponseMessage = ?,
            SapResponseJson    = ?,
            SapLastSentAt      = SYSUTCDATETIME(),
            SapLastResponseAt  = SYSUTCDATETIME(),
            LastError          = ?,
            RetryCount         = RetryCount
                                 + CASE WHEN ? = 'ERROR' THEN 1 ELSE 0 END
        WHERE TransferControlId = ?
    """,
        sap_status, sap_doc_no, resp_code, resp_msg, resp_json,
        last_error, sap_status, transfer_ctrl_id,
    )


def sql_insert_interface_log(conn, transfer_ctrl_id, target, direction,
                              http_status, resp_code,
                              request_json, response_json, message):
    conn.execute("""
        INSERT INTO EXC.TransferOrder_InterfaceLog
            (TransferControlId, TargetSystem, Direction,
             HttpStatus, ResponseCode,
             RequestJson, ResponseJson, Message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        transfer_ctrl_id, target, direction,
        http_status, resp_code,
        request_json, response_json, message,
    )


def sql_mark_header_processed(conn, header_id):
    conn.execute("""
        UPDATE INT.Transfer_Order_SAP_Header
        SET SynProcessed = 1
        WHERE TransferOrderSAPHeader_ID = ?
    """, header_id)


# =========================================================
# PAYLOAD BUILDER
# =========================================================
def build_payload(header, items, note) -> dict:
    return {
        "Sales_Order": {
            "SO_Header_Details": {
                "SO_Type":            header.SO_Type,
                "Document_Date":      fmt_date(header.Document_Date),
                "Sales_Pool":         header.Sales_Pool,
                "SO_Number":          header.SO_Number,
                "Division":           header.Division,
                "SubDivision":        header.SubDivision or "",
                "Customer_Code":      header.Customer_Code,
                "Delivery_Point":     header.Delivery_Point or "",
                "Customer_Reference": header.Customer_Reference,
                "Delivery_Date":      fmt_date(header.Delivery_Date),  # +2bd, ISO formatted
                "Delivery_Note":      header.Delivery_Note or "",
                "Reason_Code":        header.Reason_Code or "",
                "SO_Item_Details": [
                    {
                        "Line_Number":     str(i.Line_Number),
                        "Product_Number":  i.Product_Number,
                        "Quantity":        f"{i.Quantity:.3f}",
                        "Unit_Of_Measure": i.Unit_Of_Measure,
                        "Warehouse":       i.Warehouse,
                        "Site":            i.Site,
                        "SAP_Status":      i.SAP_Status,
                        "Customer_Item":   i.Customer_Item or "",
                    }
                    for i in items
                ],
                "PO_Note_Details": {
                    "Language":              note.Language,
                    "Special_Delivery_Note": note.Special_Delivery_Note or "",
                },
            }
        }
    }


def parse_request_fields(payload: dict) -> dict:
    h = payload["Sales_Order"]["SO_Header_Details"]
    return {
        "SO_Type":               h["SO_Type"],
        "Document_Date":         h["Document_Date"],
        "Sales_Pool":            h["Sales_Pool"],
        "SO_Number":             h["SO_Number"],
        "Division":              h["Division"],
        "SubDivision":           h["SubDivision"],
        "Customer_Code":         h["Customer_Code"],
        "Delivery_Point":        h["Delivery_Point"],
        "Customer_Reference":    h["Customer_Reference"],
        "Delivery_Date":         h["Delivery_Date"],
        "Delivery_Note":         h["Delivery_Note"],
        "Reason_Code":           h["Reason_Code"],
        "Language":              h["PO_Note_Details"]["Language"],
        "Special_Delivery_Note": h["PO_Note_Details"]["Special_Delivery_Note"],
    }


def parse_response_errors(resp: dict) -> list:
    try:
        arr  = resp["Response"]["SalesOrder_Response"]
        if isinstance(arr, dict):
            arr = [arr]
        msgs = [r.get("Message") for r in arr]
        while len(msgs) < 3:
            msgs.append(None)
        return msgs[:3]
    except Exception:
        return [None, None, None]


def extract_sap_doc_no(resp: dict):
    for key in ("SalesOrderNumber", "SO_Number", "DocumentNumber",
                "salesOrderNumber", "document_number"):
        if isinstance(resp, dict) and resp.get(key):
            return str(resp[key])
    try:
        arr = resp["Response"]["SalesOrder_Response"]
        if isinstance(arr, dict):
            arr = [arr]
        return str(arr[0].get("SO_Number") or arr[0].get("Message", ""))
    except Exception:
        return None


# =========================================================
# OAUTH2 TOKEN  (cached for run lifetime)
# =========================================================
_token_cache = {"token": None, "expires_at": 0.0}

def get_cpi_token() -> str:
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 30:
        dbg("Using cached CPI token")
        con_info("Token: using cached (still valid)")
        return _token_cache["token"]

    con_info("Requesting SAP CPI OAuth2 token...")
    data = {
        "grant_type":    "client_credentials",
        "client_id":     CPI_CLIENT_ID,
        "client_secret": CPI_CLIENT_SECRET,
    }
    con_api_request("POST", CPI_TOKEN_URL, {**data, "client_secret": "***"})

    r = requests.post(
        CPI_TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=CPI_TIMEOUT,
    )
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}

    safe = {k: ("***TOKEN***" if k == "access_token" else v)
            for k, v in (body.items() if isinstance(body, dict) else {}.items())}
    con_api_response(r.status_code, safe, label="CPI AUTH")

    if r.status_code != 200:
        raise RuntimeError(f"CPI token failed: HTTP {r.status_code} | {body}")

    _token_cache["token"]      = body["access_token"]
    _token_cache["expires_at"] = now + int(body.get("expires_in", 3600))
    con_ok(f"Token acquired  (expires in {body.get('expires_in', '?')}s)")
    return _token_cache["token"]


# =========================================================
# PROCESS ONE ORDER
# =========================================================
def process_one(conn, header, items, note) -> bool:
    start     = time.time()
    header_id = header.TransferOrderSAPHeader_ID
    so_number = header.SO_Number
    cust_code = header.Customer_Code

    # ── Build payload ─────────────────────────────────────
    payload        = build_payload(header, items, note)
    parsed_payload = parse_request_fields(payload)
    payload_str    = to_json_str(payload)

    # ── Resolve EXC link ─────────────────────────────────
    exc_ctrl_id = sql_get_exc_control_id(conn, so_number)
    if exc_ctrl_id:
        con_info(f"EXC control linked  TransferControlId={exc_ctrl_id}")
    else:
        con_warn(f"No EXC.TransferOrder_Control match for {so_number}")

    # ── Insert SAP control record ─────────────────────────
    sap_ctrl_id = sql_insert_control(conn, so_number, so_number, cust_code)
    con_info(f"SAP control inserted  SAPControlID={sap_ctrl_id}")

    # ── Log request to DB ─────────────────────────────────
    sql_insert_request_log(conn, sap_ctrl_id, payload, parsed_payload)
    conn.commit()

    # ── Save request JSON ─────────────────────────────────
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    order_dir = OUTPUT_DIR / so_number
    order_dir.mkdir(parents=True, exist_ok=True)
    save_json(order_dir / f"request_{so_number}_{ts}.json", payload)

    # ── EXC interface log — REQUEST ───────────────────────
    if exc_ctrl_id:
        sql_insert_interface_log(
            conn, exc_ctrl_id,
            target="SAP", direction="REQUEST",
            http_status=None, resp_code=None,
            request_json=payload_str[:4000],
            response_json=None,
            message=f"Payload built  SO={so_number}  Lines={len(items)}",
        )
        conn.commit()

    # ── Get token ─────────────────────────────────────────
    token = get_cpi_token()
    http_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    # ── POST ──────────────────────────────────────────────
    con_info(f"POST  {CPI_ENDPOINT}")
    con_api_request("POST", CPI_ENDPOINT, payload)

    http_status = 0
    resp_body   = {}
    try:
        r = requests.post(
            CPI_ENDPOINT,
            data=payload_str,
            headers=http_headers,
            timeout=CPI_TIMEOUT,
        )
        http_status = r.status_code
        try:
            resp_body = r.json()
        except Exception:
            resp_body = {"raw_response": r.text}
    except Exception as ex:
        resp_body   = {"error": str(ex)}
        http_status = 0

    con_api_response(http_status, resp_body, label="CPI RESPONSE")

    # ── Save response JSON ────────────────────────────────
    save_json(order_dir / f"response_{so_number}_{ts}.json",
              {"HTTPStatus": http_status, "Response": resp_body})

    # ── Parse outcome ─────────────────────────────────────
    errors     = parse_response_errors(resp_body)
    sap_doc_no = extract_sap_doc_no(resp_body)
    resp_str   = to_json_str(resp_body)
    success    = http_status in (200, 201)
    error_msg  = errors[0] if not success else None

    # ── Write DB logs ─────────────────────────────────────
    sql_insert_response_log(conn, sap_ctrl_id, http_status, resp_body, errors)
    sql_update_sap_control(
        conn, sap_ctrl_id,
        status="Success" if success else "Error",
        error_msg=error_msg,
    )
    if success:
        sql_mark_header_processed(conn, header_id)

    if exc_ctrl_id:
        sql_update_exc_sap_cols(
            conn, exc_ctrl_id,
            sap_status="SENT_SAP" if success else "ERROR",
            sap_doc_no=sap_doc_no,
            resp_code=str(http_status),
            resp_msg=error_msg or "OK",
            resp_json=resp_str[:4000],
            last_error=error_msg,
        )
        sql_insert_interface_log(
            conn, exc_ctrl_id,
            target="SAP", direction="RESPONSE",
            http_status=http_status,
            resp_code=str(http_status),
            request_json=None,
            response_json=resp_str[:4000],
            message=error_msg or f"Accepted  SO={so_number}  DocNo={sap_doc_no}",
        )

    conn.commit()
    duration = time.time() - start
    con_order_result(so_number, http_status, sap_doc_no,
                     success, duration, error=error_msg or "")

    if success:
        log(f"OK  SO={so_number}  SAPDoc={sap_doc_no}  HTTP={http_status}")
    else:
        err(f"FAIL  SO={so_number}  HTTP={http_status}  Err={error_msg}")

    return success


# =========================================================
# MAIN
# =========================================================
def main():
    run_start = time.time()

    # ── Startup banner ────────────────────────────────────
    con_section("SAP CPI TRANSMISSION  --  STARTUP")
    con_info(f"INI File     : {INI_PATH}")
    con_info(f"DB Server    : {ini_val(db, 'server')} / {ini_val(db, 'database')}")
    con_info(f"Token URL    : {CPI_TOKEN_URL}")
    con_info(f"CPI Endpoint : {CPI_ENDPOINT}")
    con_info(f"Output Dir   : {OUTPUT_DIR}")
    con_info(f"Log File     : {LOG_FILE}")
    log(f"SAP CPI Transmit started. INI={INI_PATH}")

    conn = get_conn()
    con_ok("DB connection established")

    counts = {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    try:
        # ── STEP 1: Stage ─────────────────────────────────
        con_section("STEP 1  --  STAGING  (INT.usp_Stage_Transfer_Orders_For_SAP)")
        con_info("Executing staging stored procedure...")
        try:
            conn.execute("EXEC INT.usp_Stage_Transfer_Orders_For_SAP")
            conn.commit()
            con_ok("Staging SP completed OK")
            log("INT.usp_Stage_Transfer_Orders_For_SAP executed OK")
        except Exception as sp_ex:
            err(f"Staging SP failed: {sp_ex}")
            log(f"Staging SP failed: {sp_ex}")
            raise

        # ── STEP 1b: Normalise Delivery_Date format ──────────
        con_section("STEP 1b  --  NORMALISE DELIVERY DATE FORMAT")
        con_info("Normalising Delivery_Date to ISO format in INT.Transfer_Order_SAP_Header ...")
        try:
            updated_rows = sql_normalise_delivery_dates(conn)
            conn.commit()
            con_ok(f"Delivery_Date normalised to ISO datetime on {updated_rows} row(s)")
            log(f"Delivery_Date format normalised on {updated_rows} pending header(s)")
        except Exception as bd_ex:
            err(f"Delivery_Date update failed: {bd_ex}")
            log(f"Delivery_Date update failed: {bd_ex}")
            raise

        # ── STEP 2: Queue summary ─────────────────────────
        con_section("STEP 2  --  QUEUE SUMMARY")
        qs = sql_get_queue_summary(conn)
        print(f"  {'_' * (_W - 2)}")
        print(f"  {'Metric':<30}  {'Count':>8}")
        print(f"  {'_' * (_W - 2)}")
        print(f"  {'Total headers staged':<30}  {qs['total_headers']:>8}")
        print(f"  {'Pending (to send now)':<30}  {qs['pending']:>8}")
        print(f"  {'Already processed':<30}  {qs['already_processed']:>8}")
        print(f"  {'Total line items staged':<30}  {qs['total_items']:>8}")
        print(f"  {'Previous run successes':<30}  {qs['prev_success']:>8}")
        print(f"  {'Previous run errors':<30}  {qs['prev_errors']:>8}")
        print(f"  {'_' * (_W - 2)}")

        headers = conn.execute("""
            SELECT * FROM INT.Transfer_Order_SAP_Header
            WHERE SynProcessed = 0
            ORDER BY TransferOrderSAPHeader_ID
        """).fetchall()
        counts["total"] = len(headers)

        if not headers:
            con_warn("No pending headers -- nothing to transmit.")
            log("No pending SAP headers.")
            con_run_summary(0, 0, 0, 0, time.time() - run_start)
            return

        con_ok(f"Queued for transmission: {counts['total']} order(s)")

        # ── STEP 3: Transmit ──────────────────────────────
        con_section(f"STEP 3  --  TRANSMITTING  ({counts['total']} orders)")

        for idx, h in enumerate(headers, start=1):
            items = sql_get_items(conn, h.TransferOrderSAPHeader_ID)
            note  = sql_get_note(conn,  h.TransferOrderSAPHeader_ID)

            con_order_banner(idx, counts["total"],
                             h.SO_Number, h.TransferOrderSAPHeader_ID,
                             h.Customer_Code, len(items))
            try:
                if not items:
                    raise RuntimeError(
                        f"No items for header ID={h.TransferOrderSAPHeader_ID}")
                if not note:
                    raise RuntimeError(
                        f"No note for header ID={h.TransferOrderSAPHeader_ID}")

                ok = process_one(conn, h, items, note)
                counts["success" if ok else "failed"] += 1

            except Exception as ex:
                counts["failed"] += 1
                err(f"Failed SO={h.SO_Number}: {ex}")
                dbg(traceback.format_exc())
                try:
                    conn.rollback()
                    sid = sql_insert_control(
                        conn, h.SO_Number, h.SO_Number, h.Customer_Code)
                    sql_update_sap_control(conn, sid, "Error", str(ex)[:2000])
                    conn.commit()
                except Exception:
                    pass
                con_error(f"SO {h.SO_Number} failed -- continuing to next")

            # Running tally after every order
            con_progress_line(idx, counts["total"],
                              counts["success"], counts["failed"])

    finally:
        conn.close()
        con_run_summary(
            counts["total"], counts["success"],
            counts["failed"], counts["skipped"],
            time.time() - run_start,
        )
        log(f"Complete. Total={counts['total']} "
            f"Success={counts['success']} Failed={counts['failed']}")
         

if __name__ == "__main__":
    main()
