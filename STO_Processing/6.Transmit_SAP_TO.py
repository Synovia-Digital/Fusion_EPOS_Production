#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
#  sap_cpi_transmit.py  —  Production
#  Step 6 — STO Pipeline — SAP CPI Transmission
#
#  Reads staged records from INT.Transfer_Order_SAP_Header / Item / Note
#  and POSTs each Transfer Order to the SAP CPI endpoint.
#
#  Delivery_Date in the payload comes directly from
#  INT.Transfer_Order_SAP_Header.Delivery_Date which was written by
#  INT.usp_Stage_Transfer_Orders_For_SAP from
#  INT.TransferOrderHeader.RequestedReceiptDate.
#  That value was set in Step 2 (STO_Delivery_Scheduling.py) and must
#  flow through unchanged — it is the SAP CPI Requested Delivery Date.
#
#  Pipeline position:
#    Step 1  EPOS_TransferRequest_Ingest.py
#    Step 2  STO_Delivery_Scheduling.py
#    Step 3  EXC.usp_Load_Transfer_Order_Request  (via Run_Stage_Transfer_Orders.py)
#    Step 4  d365_create_transfer_orders.py
#    Step 5  INT.usp_Stage_Transfer_Orders_For_SAP (via Run_SAP_Staging.py)
#    Step 6  sap_cpi_transmit.py   ← THIS SCRIPT
#
#  Source tables (read):
#    INT.Transfer_Order_SAP_Header
#    INT.Transfer_Order_SAP_Item
#    INT.Transfer_Order_SAP_Note
#
#  Write tables:
#    INT.Transfer_Order_SAP_Control
#    INT.Transfer_Order_SAP_Request_Log
#    INT.Transfer_Order_SAP_Response_Log
#    EXC.TransferOrder_Control        (SapStatus, SapDocumentNo, ...)
#    EXC.TransferOrder_InterfaceLog   (REQUEST + RESPONSE rows)
#
#  INI section: [SAP_CPI_Production]
# =============================================================================

import json
import sys
import time
import logging
import traceback
import socket
import pyodbc
import requests
import configparser
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler

SCRIPT_NAME  = "sap_cpi_transmit.py"
MACHINE_NAME = socket.gethostname()

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
    """Read a configparser value and strip any surrounding quotes."""
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
db_cfg = cfg[CONFIG_SECTION_DB]

def get_conn():
    cs = (
        f"Driver={{{ini_val(db_cfg, 'driver')}}};"
        f"Server={ini_val(db_cfg, 'server')};"
        f"Database={ini_val(db_cfg, 'database')};"
        f"UID={ini_val(db_cfg, 'user')};"
        f"PWD={ini_val(db_cfg, 'password')};"
        f"Encrypt={ini_val(db_cfg, 'encrypt', 'yes')};"
        f"TrustServerCertificate={ini_val(db_cfg, 'trust_server_certificate', 'no')};"
    )
    conn = pyodbc.connect(cs)
    conn.autocommit = False
    return conn

# =========================================================
# CPI CONFIG
# =========================================================
cpi_cfg           = cfg[CONFIG_SECTION_CPI]
CPI_TOKEN_URL     = ini_val(cpi_cfg, "token_url")
CPI_CLIENT_ID     = ini_val(cpi_cfg, "client_id")
CPI_CLIENT_SECRET = ini_val(cpi_cfg, "client_secret")
CPI_ENDPOINT      = ini_val(cpi_cfg, "cpi_endpoint")
CPI_TIMEOUT       = int(ini_val(cpi_cfg, "timeout") or "60")

# =========================================================
# LOGGING
# =========================================================
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "sap_cpi_transmit.log"
logger   = logging.getLogger("sap_cpi_transmit")
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

_W = 76

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

def divider():
    print(f"  {C.DIM}{'─'*(_W-2)}{C.RST}")

def kv(label, val, vc=C.BWHT):
    print(f"  {C.DIM}{label:<32}{C.RST}{vc}{val}{C.RST}")

def con_info(m):  print(f"  {C.BCYN}[{_ts()}]{C.RST}  {C.BCYN}ℹ{C.RST}  {m}")
def con_ok(m):    print(f"  {C.BGRN}[{_ts()}]{C.RST}  {C.BGRN}✔{C.RST}  {m}")
def con_warn(m):  print(f"  {C.BYLW}[{_ts()}]{C.RST}  {C.BYLW}⚠{C.RST}  {m}")
def con_err(m):   print(f"  {C.RED}[{_ts()}]{C.RST}  {C.RED}✘{C.RST}  {m}")

def order_banner(idx, total, so_number, header_id, customer, n_lines,
                 delivery_date):
    pct  = int((idx - 1) / total * 30) if total else 0
    bar  = f"{C.BGRN}{'█' * pct}{C.DIM}{'░' * (30 - pct)}{C.RST}"
    done = int((idx - 1) / total * 100) if total else 0
    print(f"\n{C.BGBLU}{C.BWHT}{C.BOLD}"
          f"  [{idx}/{total}]  {so_number:<22}"
          f"  HeaderId={header_id}  "
          f"Cust={customer}  "
          f"Lines={n_lines}  {C.RST}")
    print(f"  {bar}  {C.DIM}{done}%{C.RST}")
    kv("SAP Delivery Date  ★",
       str(delivery_date)[:19] if delivery_date else "—",
       C.BGRN + C.BOLD)
    divider()

def api_req(method, url, payload):
    safe = {k: "***" if any(x in k.lower()
                             for x in ("secret", "password", "token"))
            else v
            for k, v in (payload.items()
                         if isinstance(payload, dict) else {}.items())}
    print(f"\n  {C.DIM}┌─ API REQUEST {'─'*(_W-18)}{C.RST}")
    print(f"  {C.DIM}│{C.RST}  {C.BOLD}{method}{C.RST}  {C.BBLU}{url}{C.RST}")
    for k, v in list(safe.items())[:10]:
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
        for k, v in list(body.items())[:10]:
            print(f"  {C.DIM}│  {k}: {v}{C.RST}")
        if len(body) > 10:
            print(f"  {C.DIM}│  ... (+{len(body)-10} more){C.RST}")
    else:
        print(f"  {C.DIM}│  {str(body)[:300]}{C.RST}")
    print(f"  {C.DIM}└{'─'*(_W-4)}{C.RST}\n")

def order_result(so_number, http_status, sap_doc, delivery_date,
                 success, duration_s, error=""):
    bg  = C.BGGRN + C.BOLD if success else C.BGRED + C.BWHT + C.BOLD
    ico = "✅" if success else "❌"
    st  = "SUCCESS" if success else "FAILED"
    print(f"\n  {bg}  {ico}  {so_number}  —  {st}  {C.RST}")
    kv("HTTP Status",
       str(http_status),
       C.BGRN if success else C.RED)
    kv("SAP Document No",   sap_doc or "(none)",
       C.BGRN + C.BOLD if sap_doc else C.DIM)
    kv("SAP Delivery Date ★",
       str(delivery_date)[:10] if delivery_date else "—",
       C.BGRN if success else C.BYLW)
    kv("Duration",          f"{duration_s:.1f}s")
    if error:
        kv("Error", error[:120], C.RED)

def progress_line(idx, total, success, failed):
    remaining = total - idx
    pct = int(idx / total * 100) if total else 0
    bar = f"{C.BGRN}{'█' * int(pct/4)}{C.DIM}{'░' * (25 - int(pct/4))}{C.RST}"
    print(f"\n  {bar}  "
          f"{C.BOLD}{idx}/{total}{C.RST} ({pct}%)  "
          f"{C.BGRN}✔ {success}{C.RST}  "
          f"{C.RED}✘ {failed}{C.RST}  "
          f"{C.DIM}Remaining: {remaining}{C.RST}")

def run_summary(total, success, failed, skipped, duration_s):
    rate = f"{success/total*100:.1f}%" if total else "n/a"
    avg  = f"{duration_s/total:.1f}s"  if total else "n/a"
    section("RUN COMPLETE")
    kv("Total Queued",   str(total))
    kv("Sent OK",        f"{success}  ({rate})",
       C.BGRN + C.BOLD if success else C.DIM)
    kv("Failed",         str(failed),
       C.RED + C.BOLD if failed else C.DIM)
    kv("Skipped",        str(skipped), C.DIM)
    kv("Total Elapsed",  f"{duration_s:.1f}s")
    kv("Avg per Order",  avg)
    kv("Log File",       str(LOG_FILE), C.DIM)
    kv("JSON Output",    str(OUTPUT_DIR), C.DIM)

# =========================================================
# DATE HELPERS
# =========================================================
def fmt_date(val) -> str:
    """
    Normalise any date/datetime value from pyodbc into the ISO-8601
    string that SAP CPI expects: YYYY-MM-DDTHH:MM:SS

    This is the critical function for the SAP Delivery Date.
    The value comes from INT.Transfer_Order_SAP_Header.Delivery_Date
    which was written from INT.TransferOrderHeader.RequestedReceiptDate
    (set by the scheduling script in Step 2). It must not be altered —
    only formatted correctly for the API call.
    """
    if val is None:
        return ""
    if isinstance(val, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d", "%b %d %Y %I:%M%p",
                    "%b %d %Y %I:%M %p"):
            try:
                return datetime.strptime(val.strip(), fmt).strftime(
                    "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        return val
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
# SQL — READ
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
    """Match back to EXC.TransferOrder_Control via DynamicsDocumentNo."""
    row = conn.execute("""
        SELECT TransferControlId
        FROM EXC.TransferOrder_Control
        WHERE DynamicsDocumentNo = ?
           OR SourceID           = ?
    """, so_number, so_number).fetchone()
    return row.TransferControlId if row else None

def sql_get_queue_summary(conn) -> dict:
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
# SQL — WRITE
# =========================================================
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
        parsed.get("Delivery_Date"),    # SAP Delivery Date — as sent
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
            MasterStatus       = CASE WHEN ? = 'SENT_SAP'
                                      THEN 'SENT_SAP'
                                      ELSE MasterStatus END,
            RetryCount         = RetryCount
                                 + CASE WHEN ? = 'ERROR' THEN 1 ELSE 0 END
        WHERE TransferControlId = ?
    """,
        sap_status, sap_doc_no, resp_code, resp_msg, resp_json,
        last_error, sap_status, sap_status, transfer_ctrl_id,
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
    """
    Build the SAP CPI JSON payload.

    Delivery_Date is sourced from header.Delivery_Date which flows from:
      STG.WASP_STO_Scheduled.ScheduledDeliveryDate (Step 2)
        → INT.TransferOrderHeader.RequestedReceiptDate (Step 3 SP)
          → INT.Transfer_Order_SAP_Header.Delivery_Date (Step 5 SP)
            → payload["Delivery_Date"] here  ← SAP CPI Requested Delivery Date

    It is formatted to ISO-8601 via fmt_date() but the date itself
    is never recalculated or overridden at this stage.
    """
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
                "Delivery_Date":      fmt_date(header.Delivery_Date),  # ★ SAP Delivery Date
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
        "Delivery_Date":         h["Delivery_Date"],    # ★ SAP Delivery Date as sent
        "Delivery_Note":         h["Delivery_Note"],
        "Reason_Code":           h["Reason_Code"],
        "Language":              h["PO_Note_Details"]["Language"],
        "Special_Delivery_Note": h["PO_Note_Details"]["Special_Delivery_Note"],
    }

def parse_response_errors(resp: dict) -> list:
    try:
        arr = resp["Response"]["SalesOrder_Response"]
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
    api_req("POST", CPI_TOKEN_URL, {**data, "client_secret": "***"})

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
            for k, v in (body.items()
                         if isinstance(body, dict) else {}.items())}
    api_resp(r.status_code, safe, label="CPI AUTH")

    if r.status_code != 200:
        raise RuntimeError(
            f"CPI token failed: HTTP {r.status_code} | {body}")

    _token_cache["token"]      = body["access_token"]
    _token_cache["expires_at"] = now + int(body.get("expires_in", 3600))
    con_ok(f"Token acquired  (expires in {body.get('expires_in','?')}s)")
    return _token_cache["token"]

# =========================================================
# PROCESS ONE ORDER
# =========================================================
def process_one(conn, header, items, note) -> bool:
    start     = time.time()
    header_id = header.TransferOrderSAPHeader_ID
    so_number = header.SO_Number
    cust_code = header.Customer_Code

    # Log the delivery date we will send — this is the critical value
    con_info(
        f"SAP Delivery Date ★ : "
        f"{C.BGRN}{C.BOLD}{fmt_date(header.Delivery_Date)}{C.RST}"
        f"  (from INT.Transfer_Order_SAP_Header.Delivery_Date)"
    )

    # ── Build payload ─────────────────────────────────────
    payload        = build_payload(header, items, note)
    parsed_payload = parse_request_fields(payload)
    payload_str    = to_json_str(payload)

    # ── Resolve EXC control link ──────────────────────────
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

    # ── Save request JSON to disk ─────────────────────────
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
            message=(f"Payload built  SO={so_number}  "
                     f"Lines={len(items)}  "
                     f"Delivery={fmt_date(header.Delivery_Date)}"),
        )
        conn.commit()

    # ── Get token ─────────────────────────────────────────
    token = get_cpi_token()
    http_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    # ── POST to SAP CPI ───────────────────────────────────
    con_info(f"POST  {CPI_ENDPOINT}")
    api_req("POST", CPI_ENDPOINT, payload)

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

    api_resp(http_status, resp_body, label="CPI RESPONSE")

    # ── Save response JSON to disk ────────────────────────
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
            message=(error_msg
                     or f"Accepted  SO={so_number}  DocNo={sap_doc_no}  "
                        f"Delivery={fmt_date(header.Delivery_Date)}"),
        )

    conn.commit()

    duration = time.time() - start
    order_result(so_number, http_status, sap_doc_no,
                 header.Delivery_Date,
                 success, duration, error=error_msg or "")

    if success:
        log(f"OK  SO={so_number}  SAPDoc={sap_doc_no}  "
            f"Delivery={fmt_date(header.Delivery_Date)}  "
            f"HTTP={http_status}")
    else:
        err(f"FAIL  SO={so_number}  HTTP={http_status}  "
            f"Delivery={fmt_date(header.Delivery_Date)}  "
            f"Err={error_msg}")

    return success

# =========================================================
# MAIN
# =========================================================
def main():
    run_start = time.time()

    banner("SAP CPI TRANSMISSION  —  STEP 6")
    section("STARTUP")
    kv("Script",       SCRIPT_NAME)
    kv("Machine",      MACHINE_NAME)
    kv("INI",          str(INI_PATH))
    kv("DB",           f"{ini_val(db_cfg,'server')} / {ini_val(db_cfg,'database')}")
    kv("Token URL",    CPI_TOKEN_URL)
    kv("CPI Endpoint", CPI_ENDPOINT)
    kv("Output Dir",   str(OUTPUT_DIR))
    kv("Log File",     str(LOG_FILE))
    log(f"SAP CPI Transmit started. INI={INI_PATH}")

    conn = get_conn()
    con_ok("DB connection established")

    counts = {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    try:
        # ── Queue summary ─────────────────────────────────
        section("QUEUE SUMMARY")
        qs = sql_get_queue_summary(conn)
        divider()
        kv("Total headers staged",    str(qs["total_headers"]))
        kv("Pending (to send now)",   str(qs["pending"]),
           C.BYLW + C.BOLD if qs["pending"] else C.DIM)
        kv("Already processed",       str(qs["already_processed"]), C.DIM)
        kv("Total line items staged", str(qs["total_items"]))
        kv("Previous run successes",  str(qs["prev_success"]),  C.BGRN)
        kv("Previous run errors",     str(qs["prev_errors"]),
           C.RED if qs["prev_errors"] else C.DIM)
        divider()

        headers         = sql_get_pending_headers(conn)
        counts["total"] = len(headers)

        if not headers:
            con_warn("No pending headers — nothing to transmit.")
            con_warn("Run Run_SAP_Staging.py first to stage orders.")
            log("No pending SAP headers.")
            run_summary(0, 0, 0, 0, time.time() - run_start)
            return

        con_ok(f"Queued for transmission: {C.BOLD}{counts['total']}{C.RST} order(s)")

        # Pre-flight — show what will be sent
        section("ORDERS QUEUED FOR SAP CPI")
        for h in headers:
            items = sql_get_items(conn, h.TransferOrderSAPHeader_ID)
            print(f"  {C.BGRN}▶{C.RST}  "
                  f"{C.BOLD}{h.SO_Number:<22}{C.RST}  "
                  f"Cust={C.DIM}{h.Customer_Code}{C.RST}  "
                  f"Lines={len(items)}  "
                  f"{C.DIM}SAP Delivery Date ★:{C.RST}  "
                  f"{C.BGRN}{C.BOLD}{fmt_date(h.Delivery_Date)[:10]}{C.RST}")

        # ── Transmit ──────────────────────────────────────
        section(f"TRANSMITTING  ({counts['total']} order(s))")

        for idx, h in enumerate(headers, start=1):
            items = sql_get_items(conn, h.TransferOrderSAPHeader_ID)
            note  = sql_get_note(conn,  h.TransferOrderSAPHeader_ID)

            order_banner(idx, counts["total"],
                         h.SO_Number,
                         h.TransferOrderSAPHeader_ID,
                         h.Customer_Code,
                         len(items),
                         h.Delivery_Date)

            try:
                if not items:
                    raise RuntimeError(
                        f"No items for HeaderID={h.TransferOrderSAPHeader_ID}")
                if not note:
                    raise RuntimeError(
                        f"No note for HeaderID={h.TransferOrderSAPHeader_ID}")

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
                    sql_update_sap_control(
                        conn, sid, "Error", str(ex)[:2000])
                    conn.commit()
                except Exception:
                    pass
                con_err(f"SO {h.SO_Number} failed — continuing to next")

            progress_line(idx, counts["total"],
                          counts["success"], counts["failed"])

    finally:
        conn.close()
        run_summary(
            counts["total"], counts["success"],
            counts["failed"], counts["skipped"],
            time.time() - run_start,
        )
        log(f"Complete. Total={counts['total']} "
            f"Success={counts['success']} "
            f"Failed={counts['failed']}")


if __name__ == "__main__":
    main()
