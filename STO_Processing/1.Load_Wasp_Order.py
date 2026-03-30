#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# EPOS_TransferRequest_Ingest.py

import os
import shutil
import socket
import pandas as pd
import pyodbc
import configparser
from datetime import datetime

PROCESS_CODE = "INGEST"
PROCESS_NAME = "CSV Ingestion"
SCRIPT_NAME  = "EPOS_TransferRequest_Ingest.py"
MACHINE_NAME = socket.gethostname()

INBOUND_DIR   = r"\\PL-AZ-INT-PRD\D_Drive\FusionHub\Fusion_EPOS\Transfer_Request_Inbound"
PROCESSED_DIR = os.path.join(INBOUND_DIR, "Processed")
LOG_DIR       = r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs"

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

CONFIG_SECTION = "Fusion_EPOS_Production"
CONFIG_PATH    = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"

def _ts(): return datetime.now().strftime("%H:%M:%S")
def con_info(msg):    print(f"[{_ts()}]  INFO  {msg}")
def con_ok(msg):      print(f"[{_ts()}]  OK    {msg}")
def con_warn(msg):    print(f"[{_ts()}]  WARN  {msg}")
def con_error(msg):   print(f"[{_ts()}]  ERROR {msg}")
def con_section(msg): print(f"\n{'='*60}\n  {msg}\n{'='*60}")
def con_summary(total, success, fail, status):
    bar = "=" * 60
    print(f"\n{bar}\n  SUMMARY\n  Total Rows  : {total}\n  Successful  : {success}\n  Failed      : {fail}\n  Status      : {status}\n{bar}\n")

def write_run_log(message: str):
    log_path = os.path.join(LOG_DIR, f"EPOS_TransferRequest_{datetime.now().strftime('%Y%m%d')}.log")
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{stamp} | {message}\n")

def resolve_ini_path(path: str) -> str:
    path = os.path.normpath(path)
    if os.path.isdir(path):
        candidates = [
            os.path.join(path, "Master_ini_config.ini"),
            os.path.join(path, "Master_ini_Config.ini"),
            os.path.join(path, "master_ini_config.ini"),
            os.path.join(path, "Master.ini"),
            os.path.join(path, "config.ini"),
        ]
        for c in candidates:
            if os.path.isfile(c):
                return c
        ini_files = sorted([os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(".ini")])
        if ini_files:
            return ini_files[0]
        raise RuntimeError(f"No .ini files found in: {path}")
    if not os.path.isfile(path):
        raise RuntimeError(f"Config path not found: {path}")
    return path

def load_config_section(config_path, section):
    ini_file = resolve_ini_path(config_path)
    cfg = configparser.ConfigParser()
    with open(ini_file, "r", encoding="utf-8-sig") as f:
        cfg.read_file(f)
    if section not in cfg:
        raise RuntimeError(f"Missing section [{section}] in {ini_file}")
    write_run_log(f"Config loaded OK: {ini_file} | section [{section}]")
    con_ok(f"Config loaded: {ini_file}  [{section}]")
    return cfg[section]

con_section("STARTUP")
con_info("Loading configuration...")
db_cfg = load_config_section(CONFIG_PATH, CONFIG_SECTION)

CONN_STR = (
    f"DRIVER={{{db_cfg.get('driver')}}};"
    f"SERVER={db_cfg.get('server')};"
    f"DATABASE={db_cfg.get('database')};"
    f"UID={db_cfg.get('user')};PWD={db_cfg.get('password')};"
    f"Encrypt={db_cfg.get('encrypt','yes')};"
    f"TrustServerCertificate={db_cfg.get('trust_server_certificate','no')};"
)

INSERT_RAW_SQL = """
INSERT INTO RAW.EPOS_TransferRequest
(
    LoadFileName, LoadTimestamp,
    OrderId, OrderDate, DateRequired,
    CustomerCode, CustomerName,
    Address1, Address2, Address3,
    UserCode, FirstName,
    CustomerRef, PONumber, DeliveryInstructions,
    OrderType, ProductCategory, ProductSubcategory,
    ProductCode, ProductDescription,
    Price, Quantity, FreeOfCharge, FOCType,
    DiscountPercentage, CompletionCode
)
VALUES (?, SYSDATETIME(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

INSERT_LOG_SQL = """
INSERT INTO LOG.EPOS_IngestionSummary
(LoadFileName, LoadTimestamp, TotalRows, SuccessfulRows, FailedRows, Status, ErrorMessage)
VALUES (?, SYSDATETIME(), ?, ?, ?, ?, ?);
"""

PROC_LOG_SQL = """
INSERT INTO EXC.STO_Processing_Log
(ProcessCode, ProcessName, StepCode, StepDescription, OrderId,
 TransferControlId, DynamicsDocumentNo, Status, Message, MachineName, ScriptName)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

REQUIRED_COLS = [
    "OrderId","OrderDate","DateRequired","CustomerCode","CustomerName",
    "Address1","Address2","Address3","UserCode","FirstName",
    "CustomerRef","PONumber","DeliveryInstructions","OrderType",
    "ProductCategory","ProductSubcategory","ProductCode","ProductDescription",
    "Price","Quantity","FreeOfCharge","FOCType","DiscountPercentage","CompletionCode"
]

def get_conn():
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = False
    return conn

def proc_log(conn, step_code, step_desc, status, message,
             order_id=None, control_id=None, dynamics_no=None):
    try:
        conn.execute(PROC_LOG_SQL,
            PROCESS_CODE, PROCESS_NAME, step_code, step_desc,
            order_id, control_id, dynamics_no,
            status, message, MACHINE_NAME, SCRIPT_NAME)
        conn.commit()
    except Exception as e:
        write_run_log(f"PROC_LOG FAILED: {e}")

def parse_date(value):
    if value is None: return None
    v = str(value).strip()
    if not v: return None
    for fmt in ("%d/%m/%Y %H:%M:%S","%d/%m/%Y %H:%M","%d/%m/%Y",
                "%d/%m//%Y %H:%M:%S","%d/%m//%Y %H:%M","%d/%m//%Y"):
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            pass
    return None

def float_or_none(val):
    try:
        v = str(val).strip()
        return None if v == "" else float(v)
    except Exception:
        return None

def log_ingestion(filename, total, success, fail, status, error_msg):
    try:
        conn = get_conn()
        conn.execute(INSERT_LOG_SQL, filename, int(total), int(success),
                     int(fail), status, (error_msg[:4000] if error_msg else None))
        conn.commit()
        con_info(f"Ingestion log written to DB  [{status}]")
    except Exception as e:
        con_warn(f"DB log write failed: {e}")
        write_run_log(f"DB LOGGING FAILED for {filename}: {e}")
    finally:
        try: conn.close()
        except Exception: pass

def ingest_file(csv_path):
    file_name = os.path.basename(csv_path)
    con_section(f"PROCESSING: {file_name}")
    write_run_log(f"START Processing file: {file_name}")

    log_conn = get_conn()

    # INGEST_001 — Scan
    proc_log(log_conn, "INGEST_001", "Scan inbound directory",
             "INFO", f"Found file: {file_name}")

    # ── Read CSV ──────────────────────────────────────────
    con_info("Reading CSV...")
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False).fillna("")
    except Exception as e:
        msg = f"Failed to read CSV: {e}"
        con_error(msg)
        write_run_log(f"ERROR {msg}")
        proc_log(log_conn, "INGEST_002", "Read and validate CSV", "ERROR", msg)
        log_ingestion(file_name, 0, 0, 0, "FAILED", msg)
        log_conn.close()
        return

    con_ok(f"CSV read OK  —  {len(df)} rows, {len(df.columns)} columns")

    # ── Validate columns ──────────────────────────────────
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        msg = f"Missing columns: {missing}"
        con_error(msg)
        write_run_log(f"ERROR {msg}")
        proc_log(log_conn, "INGEST_002", "Read and validate CSV", "ERROR", msg)
        log_ingestion(file_name, 0, 0, 0, "FAILED", msg)
        log_conn.close()
        return

    total = len(df)
    if total == 0:
        msg = "CSV contains 0 rows"
        con_warn(msg)
        proc_log(log_conn, "INGEST_002", "Read and validate CSV", "WARN", msg)
        log_ingestion(file_name, 0, 0, 0, "FAILED", msg)
        log_conn.close()
        return

    proc_log(log_conn, "INGEST_002", "Read and validate CSV",
             "OK", f"{total} rows validated OK — file: {file_name}")

    # ── Transform rows ────────────────────────────────────
    con_info(f"Transforming {total} rows...")
    params = []
    transform_fail = 0

    for idx, row in df.iterrows():
        try:
            params.append((
                file_name,
                row["OrderId"], parse_date(row["OrderDate"]), parse_date(row["DateRequired"]),
                row["CustomerCode"], row["CustomerName"],
                row["Address1"], row["Address2"], row["Address3"],
                row["UserCode"], row["FirstName"],
                row["CustomerRef"], row["PONumber"], row["DeliveryInstructions"],
                row["OrderType"], row["ProductCategory"], row["ProductSubcategory"],
                row["ProductCode"], row["ProductDescription"],
                float_or_none(row["Price"]), float_or_none(row["Quantity"]),
                row["FreeOfCharge"] or None, row["FOCType"],
                float_or_none(row["DiscountPercentage"]), row["CompletionCode"]
            ))
        except Exception as e:
            transform_fail += 1
            con_warn(f"Row {idx + 1} transform failed: {e}")
            write_run_log(f"WARN Row {idx + 1} transform failed: {e}")

    # ── DB Insert ─────────────────────────────────────────
    con_info(f"Inserting {len(params)} rows into RAW.EPOS_TransferRequest...")
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(INSERT_RAW_SQL, params)
        conn.commit()

        success = len(params)
        fail    = transform_fail
        status  = "SUCCESS" if fail == 0 else "PARTIAL"
        con_ok(f"DB insert committed  —  {success} rows inserted")

        proc_log(log_conn, "INGEST_003", "Insert rows to RAW.EPOS_TransferRequest",
                 "OK", f"{success} rows inserted, {fail} failed — file: {file_name}")

    except Exception as e:
        if conn: conn.rollback()
        msg = f"DB insert failed (rolled back): {e}"
        con_error(msg)
        write_run_log(f"ERROR {msg}")
        proc_log(log_conn, "INGEST_003", "Insert rows to RAW.EPOS_TransferRequest",
                 "ERROR", msg)
        log_ingestion(file_name, total, 0, total, "FAILED", msg)
        log_conn.close()
        return
    finally:
        try:
            if conn: conn.close()
        except Exception: pass

    log_ingestion(file_name, total, success, fail, status, "")

    # ── Move file to Processed ────────────────────────────
    dest = os.path.join(PROCESSED_DIR, file_name)
    con_info("Moving file to Processed subfolder...")
    try:
        if os.path.exists(dest):
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base, ext = os.path.splitext(file_name)
            dest = os.path.join(PROCESSED_DIR, f"{base}_{stamp}{ext}")
            con_warn(f"Destination exists — renaming to: {os.path.basename(dest)}")

        shutil.move(csv_path, dest)
        con_ok(f"Moved  ->  {dest}")
        write_run_log(f"MOVED {file_name} -> {dest}")
        proc_log(log_conn, "INGEST_004", "Move file to Processed",
                 "OK", f"Moved to: {dest}")
    except Exception as e:
        con_error(f"Could not move file to Processed: {e}")
        write_run_log(f"ERROR Could not move file to Processed: {e}")
        proc_log(log_conn, "INGEST_004", "Move file to Processed",
                 "ERROR", str(e))

    con_summary(total, success, fail, status)
    write_run_log(f"END {file_name} Total={total} Success={success} Fail={fail} Status={status}")
    log_conn.close()


if __name__ == "__main__":
    try:
        con_section("SCANNING INBOUND DIRECTORY")
        log_conn = get_conn()

        files = [f for f in os.listdir(INBOUND_DIR) if f.lower().endswith(".csv")]

        if not files:
            con_warn("No CSV files found in inbound directory. Nothing to do.")
            write_run_log("No files to ingest.")
            proc_log(log_conn, "INGEST_001", "Scan inbound directory",
                     "WARN", "No CSV files found in inbound directory")
            log_conn.close()
            raise SystemExit(0)

        con_ok(f"Found {len(files)} file(s) to process: {', '.join(files)}")
        proc_log(log_conn, "INGEST_001", "Scan inbound directory",
                 "OK", f"Found {len(files)} file(s): {', '.join(files)}")
        log_conn.close()

        for f in files:
            ingest_file(os.path.join(INBOUND_DIR, f))

        con_section("ALL FILES PROCESSED")
        write_run_log("All files processed.")

    except SystemExit:
        raise
    except Exception as e:
        con_error(f"FATAL: {e}")
        write_run_log(f"FATAL: {e}")
        raise