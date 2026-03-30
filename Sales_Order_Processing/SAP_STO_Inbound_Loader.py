#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
#  Synovia Fusion – SAP STO Confirmation Inbound Loader
# =============================================================================
#
#  Script Name:   SAP_STO_Inbound_Loader.py
#  Version:       1.3.0
#  Release Date:  2026-03-30
#
# -----------------------------------------------------------------------------
#  v1.3.0 changes:
#    FIX 1 — Startup pre-flight checks INT.SAP_STO_Staging exists before
#            processing any files. Exits cleanly with a clear message if the
#            DDL has not been run yet, rather than failing per-file mid-run.
#
#    FIX 2 — When GoodIssueRefNumber has no match in either control table
#            (e.g. pure SAP goods-issue numbers like 0000000004541828 that
#            pre-date the D365 pipeline), a placeholder row is created in
#            INT.Transfer_Order_SAP_Control so the confirmation is recorded
#            and not lost. Status = 'SAP_Confirmed_NoD365Match'.
#            INT.SAP_STO_Staging MatchStatus = 'NoControlFound' as before,
#            but the file now processes successfully rather than erroring.
#
#  v1.2.0 changes:
#    Both Transfer Order control tables now updated on SAP confirmation:
#      INT.Transfer_Order_SAP_Control  — later batches (Mar 2026+),
#                                        keyed on TransferOrderNumber
#      EXC.TransferOrder_Control       — earlier batches (Feb 2026),
#                                        keyed on DynamicsDocumentNo
#    GoodIssueRefNumber in SAP JSON = JBRO-xxxxxxx = D365 TO number
#    = join key for both tables.
#
# -----------------------------------------------------------------------------
#  What this does
#  --------------
#  Picks up SAP STO goods-issue confirmation JSON files dropped into the
#  inbound folder, loads them into RAW tables, enriches them into
#  INT.SAP_STO_Staging, and updates both TO control tables.
#
#  Flow per file:
#    1. Parse JSON
#    2. Duplicate check  (GoodIssueRefNumber already in RAW with SynProc=1)
#    3. Insert RAW.SAP_TO_Responses (header)
#    4. Insert RAW.SAP_TO_Responses_Lines (one row per SalesOrderLine)
#    5. Resolve TransferControlId from BOTH control tables via JBRO number
#       — if no match, create a placeholder in INT.Transfer_Order_SAP_Control
#    6. Insert INT.SAP_STO_Staging (enriched, with MatchStatus)
#    7a. Update INT.Transfer_Order_SAP_Control → Status = 'SAP_Confirmed'
#    7b. Update EXC.TransferOrder_Control → MasterStatus = 'SAP_CONFIRMED'
#    8. Mark RAW header SynProc = 1
#    9. Commit + move file to \processed
#
#  JSON structure (SAP STO goods-issue confirmation):
#    { "request": [{
#        "GooodIssueCode":           "...",   <- SAP typo, preserved
#        "SalesOrderRefNumber":      "...",
#        "GoodIssueRefNumber":       "...",   <- dedup key
#        "ConsolidationOrderNumber": "...",
#        "SalesOrderLine": [{
#          "LineNumber":             "1",
#          "ProductNumber":          "...",
#          "GoodIssuePostingDate":   "YYYY-MM-DD",
#          "Quantity":               "10",
#          "UOM":                    "EA",
#          "Site":                   "...",
#          "LocationNumber":         "...",
#          "WarehouseNumber":        "...",
#          "Batchnumber":            "..."    <- SAP lowercase n, preserved
#        }]
#    }]}
#
# -----------------------------------------------------------------------------
#  DB schema verified against live diagnostic (2026-03-30):
#
#  RAW.SAP_TO_Responses:
#    SAPResponseID (bigint IDENTITY), FileName, GoodIssueCode,
#    SalesOrderRefNumber, GoodIssueRefNumber, ConsolidationOrderNumber,
#    RawJSON, SynProc (bit default 0), SynProcDate (datetime2), CreatedAtUtc
#
#  RAW.SAP_TO_Responses_Lines:
#    SAPResponseLineID (bigint IDENTITY), SAPResponseID, LineNumber,
#    ProductNumber, GoodIssuePostingDate (nvarchar), Quantity, UOM,
#    Site, LocationNumber, WarehouseNumber, BatchNumber, CreatedAtUtc
#
#  INT.Transfer_Order_SAP_Control:
#    SAPControlID, TransferOrderNumber, SourceID, CustomerCode,
#    Status, ErrorMessage, SentOn, CreatedAtUtc
#
#  EXC.TransferOrder_Control:
#    TransferControlId, SourceID, MasterStatus, SapDocumentNo,
#    SapResponseCode, SapResponseMessage (NVARCHAR 400),
#    SapResponseJson, SapLastSentAt, SapLastResponseAt,
#    SapStatus, StatusUpdatedAt, LastError
#
#  INT.SAP_STO_Staging:
#    Created by SAP_STO_DDL_Targeted.sql — MUST be run before first execution.
#    Script exits with a clear error message if this table is missing.
#
# =============================================================================

import os
import sys
import json
import shutil
import logging
import traceback
import configparser
import uuid
import time
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import RotatingFileHandler

import pyodbc


# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG_PATH       = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"
CONFIG_SECTION_DB = "Fusion_EPOS_Production"

INBOUND_DIR   = r"\\PL-AZ-INT-PRD\D_Drive\FusionHub\Fusion_EPOS\SAP_STO_Confimrations"
PROCESSED_DIR = os.path.join(INBOUND_DIR, "processed")

LOG_DIR       = (
    r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub"
    r"\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs"
)
LOG_FILE_NAME = "SAP_STO_Inbound_Loader.log"

SYSTEM_NAME   = "Fusion_EPOS"
ENVIRONMENT   = "PRD"
ORCHESTRATOR  = "PYTHON"
AUDIT_PROCESS = "SAP_STO_Inbound_Loader"


# =============================================================================
# LOGGING
# =============================================================================

Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("SAP_STO_Inbound_Loader")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()

_fh = RotatingFileHandler(
    os.path.join(LOG_DIR, LOG_FILE_NAME),
    maxBytes=5_000_000, backupCount=10, encoding="utf-8"
)
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_fh)

_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_sh)

def log(msg):  logger.info(msg)
def dbg(msg):  logger.debug(msg)
def err(msg):  logger.error(msg)
def warn(msg): logger.warning(msg)


# =============================================================================
# INI / DB CONNECTION
# =============================================================================

def _resolve_ini() -> Path:
    p = Path(CONFIG_PATH)
    if p.is_dir():
        candidate = p / "Master_ini_config.ini"
        if candidate.exists():
            return candidate
    if p.is_file():
        return p
    raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")


def get_db_connection() -> pyodbc.Connection:
    ini_path = _resolve_ini()
    cfg = configparser.ConfigParser()
    cfg.read(ini_path)
    if CONFIG_SECTION_DB not in cfg:
        raise RuntimeError(f"Section [{CONFIG_SECTION_DB}] not found in {ini_path}")
    db = cfg[CONFIG_SECTION_DB]
    conn_str = (
        f"Driver={{{db.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
        f"Server={db.get('server')};"
        f"Database={db.get('database')};"
        f"UID={db.get('user', db.get('username', ''))};"
        f"PWD={db.get('password')};"
        f"Encrypt={db.get('encrypt', 'yes')};"
        f"TrustServerCertificate={db.get('trust_server_certificate', 'no')};"
        f"APP=SAP_STO_Inbound_Loader;"
    )
    log(f"Connecting via INI: {ini_path}")
    return pyodbc.connect(conn_str, autocommit=False)


# =============================================================================
# PRE-FLIGHT CHECK
# =============================================================================

def check_prerequisites(conn: pyodbc.Connection) -> bool:
    """
    Verify INT.SAP_STO_Staging exists before processing any files.
    Returns True if all required objects are present.
    Logs clearly what is missing so the fix is obvious.
    """
    required = [
        ("INT", "SAP_STO_Staging"),
        ("RAW", "SAP_TO_Responses"),
        ("RAW", "SAP_TO_Responses_Lines"),
        ("INT", "Transfer_Order_SAP_Control"),
        ("EXC", "TransferOrder_Control"),
    ]
    missing = []
    for schema, table in required:
        row = conn.execute("""
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?;
        """, schema, table).fetchone()
        if not row:
            missing.append(f"{schema}.{table}")

    if missing:
        err("=" * 65)
        err("PRE-FLIGHT FAILED — required DB objects not found:")
        for m in missing:
            err(f"  MISSING: {m}")
        if "INT.SAP_STO_Staging" in missing:
            err("")
            err("  ACTION: Run SAP_STO_DDL_Targeted.sql in SSMS first,")
            err("          then re-run this script.")
        err("=" * 65)
        return False

    log("Pre-flight OK — all required tables exist.")
    return True


# =============================================================================
# AUDIT  (non-blocking — writes to EXC.Process_Transaction_Log)
# =============================================================================

class AuditLogger:
    def __init__(self, conn: pyodbc.Connection, run_id: int, corr_id: str):
        self._conn   = conn
        self._run_id = run_id
        self._corr   = corr_id
        self._ok     = self._probe()

    def _probe(self) -> bool:
        try:
            self._conn.execute(
                "SELECT TOP 1 1 FROM sys.objects o "
                "JOIN sys.schemas s ON s.schema_id = o.schema_id "
                "WHERE s.name = 'EXC' AND o.name = 'Process_Transaction_Log';"
            )
            return True
        except Exception:
            return False

    def step(self, *, subprocess: str, step: str, step_code: str,
             status: str, severity: str = "INFO", message: str = "",
             error: str = None, rows_read: int = None,
             rows_affected: int = None,
             source_reference: str = None) -> None:
        if not self._ok:
            return
        try:
            self._conn.execute("""
                INSERT INTO EXC.Process_Transaction_Log
                (Run_ID, Correlation_ID,
                 System_Name, Environment, Orchestrator,
                 Process_Name, SubProcess_Name,
                 Step_Name, Step_Code, Step_Category,
                 Severity_Level, Execution_Status,
                 Rows_Read, Rows_Affected,
                 Source_Reference, Message, Error_Message,
                 Start_UTC, End_UTC)
                VALUES (?,?,?,?,?,?,?,?,?,'IO',?,?,?,?,?,?,?,
                        SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
            self._run_id, self._corr,
            SYSTEM_NAME, ENVIRONMENT, ORCHESTRATOR,
            AUDIT_PROCESS, subprocess,
            step, step_code,
            severity, status,
            rows_read, rows_affected,
            source_reference, message,
            error[:4000] if error else None)
        except Exception:
            self._ok = False


# =============================================================================
# TYPE HELPERS
# =============================================================================

def _safe_int(val):
    if val is None:
        return None
    try:
        return int(float(str(val).replace(",", "").strip()))
    except Exception:
        return None


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except Exception:
        return None


def _parse_date(val):
    """Parse GoodIssuePostingDate from multiple SAP date formats → Python date."""
    if not val:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except ValueError:
            pass
    return None


# =============================================================================
# SQL — all column names verified against live DB diagnostic 2026-03-30
# =============================================================================

def already_processed(cur: pyodbc.Cursor, good_issue_ref: str) -> bool:
    """Returns True if this GoodIssueRefNumber has already been successfully loaded."""
    row = cur.execute(
        "SELECT COUNT(*) FROM RAW.SAP_TO_Responses "
        "WHERE GoodIssueRefNumber = ? AND SynProc = 1;",
        good_issue_ref
    ).fetchone()
    return (row[0] if row else 0) > 0


def insert_raw_header(cur: pyodbc.Cursor,
                      filename: str, entry: dict, raw_json: dict) -> int:
    """Insert header into RAW.SAP_TO_Responses. Returns SAPResponseID (bigint)."""
    row = cur.execute("""
        INSERT INTO RAW.SAP_TO_Responses
        (FileName, GoodIssueCode, SalesOrderRefNumber,
         GoodIssueRefNumber, ConsolidationOrderNumber, RawJSON)
        OUTPUT INSERTED.SAPResponseID
        VALUES (?, ?, ?, ?, ?, ?)
    """,
    filename,
    entry.get("GooodIssueCode"),            # SAP typo — three o's — preserved
    entry.get("SalesOrderRefNumber"),
    entry.get("GoodIssueRefNumber"),
    entry.get("ConsolidationOrderNumber"),
    json.dumps(raw_json, ensure_ascii=False)
    ).fetchone()
    return int(row[0])


def insert_raw_line(cur: pyodbc.Cursor,
                    sap_response_id: int, line: dict) -> None:
    """Insert one line into RAW.SAP_TO_Responses_Lines."""
    cur.execute("""
        INSERT INTO RAW.SAP_TO_Responses_Lines
        (SAPResponseID, LineNumber, ProductNumber, GoodIssuePostingDate,
         Quantity, UOM, Site, LocationNumber, WarehouseNumber, BatchNumber)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    sap_response_id,
    _safe_int(line.get("LineNumber")),
    line.get("ProductNumber"),
    line.get("GoodIssuePostingDate"),       # NVARCHAR in RAW; cast to DATE in INT staging
    _safe_float(line.get("Quantity")),
    line.get("UOM"),
    line.get("Site"),
    line.get("LocationNumber"),
    line.get("WarehouseNumber"),
    line.get("Batchnumber"))                # SAP lowercase n


def mark_raw_synproc(cur: pyodbc.Cursor, sap_response_id: int) -> None:
    """Flag RAW header as processed: SynProc=1, SynProcDate=now."""
    cur.execute("""
        UPDATE RAW.SAP_TO_Responses
        SET SynProc     = 1,
            SynProcDate = SYSUTCDATETIME()
        WHERE SAPResponseID = ?;
    """, sap_response_id)


def get_raw_line_ids(cur: pyodbc.Cursor, sap_response_id: int) -> list:
    """Return SAPResponseLineIDs in LineNumber order."""
    rows = cur.execute(
        "SELECT SAPResponseLineID FROM RAW.SAP_TO_Responses_Lines "
        "WHERE SAPResponseID = ? ORDER BY LineNumber;",
        sap_response_id
    ).fetchall()
    return [r[0] for r in rows]


def resolve_control_ids(cur: pyodbc.Cursor, good_issue_ref: str) -> dict:
    """
    Search BOTH control tables for the GoodIssueRefNumber.

    For JBRO-xxxxxxx numbers (D365 pipeline):
      INT.Transfer_Order_SAP_Control  via TransferOrderNumber  (Mar 2026+)
      EXC.TransferOrder_Control       via DynamicsDocumentNo   (Feb 2026)

    For pure SAP numbers (e.g. 0000000004541828, pre-D365):
      Neither table will match — caller handles this via create_placeholder_control.

    Returns:
      { 'sap_control_id': int|None, 'exc_control_id': int|None, 'matched_either': bool }
    """
    result = {
        'sap_control_id': None,
        'exc_control_id': None,
        'matched_either': False,
    }

    row = cur.execute("""
        SELECT TOP 1 SAPControlID
        FROM INT.Transfer_Order_SAP_Control
        WHERE TransferOrderNumber = ?
        ORDER BY SAPControlID DESC;
    """, good_issue_ref).fetchone()
    if row:
        result['sap_control_id'] = int(row[0])

    row = cur.execute("""
        SELECT TOP 1 TransferControlId
        FROM EXC.TransferOrder_Control
        WHERE DynamicsDocumentNo = ?
        ORDER BY TransferControlId DESC;
    """, good_issue_ref).fetchone()
    if row:
        result['exc_control_id'] = int(row[0])

    result['matched_either'] = bool(
        result['sap_control_id'] or result['exc_control_id']
    )
    return result


def create_placeholder_control(cur: pyodbc.Cursor,
                                good_issue_ref: str,
                                entry: dict,
                                sap_response_id: int) -> int:
    """
    Create a placeholder row in INT.Transfer_Order_SAP_Control for SAP
    goods-issue confirmations that have no matching D365 Transfer Order.

    This covers earlier batches processed directly through SAP before the
    D365 pipeline existed, where GoodIssueRefNumber is a pure SAP document
    number (e.g. 0000000004541828) rather than a JBRO number.

    Status = 'SAP_Confirmed_NoD365Match' clearly distinguishes these from
    normal pipeline rows. Returns the new SAPControlID.
    """
    row = cur.execute("""
        INSERT INTO INT.Transfer_Order_SAP_Control
            (TransferOrderNumber, SourceID, CustomerCode, Status)
        OUTPUT INSERTED.SAPControlID
        VALUES (?, ?, ?, 'SAP_Confirmed_NoD365Match')
    """,
    good_issue_ref,
    f"SAP_{good_issue_ref}",                # SourceID prefixed to distinguish from JBRO rows
    entry.get("ConsolidationOrderNumber") or ""
    ).fetchone()
    return int(row[0])


def update_int_sap_control(cur: pyodbc.Cursor,
                            sap_control_id: int,
                            good_issue_ref: str) -> None:
    """Update INT.Transfer_Order_SAP_Control: Success → SAP_Confirmed."""
    cur.execute("""
        UPDATE INT.Transfer_Order_SAP_Control
        SET Status       = 'SAP_Confirmed',
            ErrorMessage = NULL
        WHERE SAPControlID = ?;
    """, sap_control_id)


def update_exc_transfer_control(cur: pyodbc.Cursor,
                                  exc_control_id: int,
                                  good_issue_ref: str,
                                  sap_response_id: int) -> None:
    """
    Update EXC.TransferOrder_Control.
    MasterStatus only advances from SENT_DYNAMICS or SENT_SAP → SAP_CONFIRMED.
    SapResponseMessage is NVARCHAR(400) — message is within that limit.
    """
    cur.execute("""
        UPDATE EXC.TransferOrder_Control
        SET MasterStatus         = CASE
                                       WHEN MasterStatus IN ('SENT_DYNAMICS', 'SENT_SAP')
                                       THEN 'SAP_CONFIRMED'
                                       ELSE MasterStatus
                                   END,
            SapDocumentNo        = ?,
            SapResponseCode      = 'Success',
            SapResponseMessage   = 'SAP goods-issue confirmation received',
            SapResponseJson      = ?,
            SapLastResponseAt    = SYSUTCDATETIME(),
            SapStatus            = 'Confirmed',
            StatusUpdatedAt      = SYSUTCDATETIME(),
            LastError            = NULL
        WHERE TransferControlId  = ?;
    """,
    good_issue_ref,
    json.dumps({
        "GoodIssueRefNumber": good_issue_ref,
        "SAPResponseID":      sap_response_id
    }),
    exc_control_id)


def insert_int_staging(cur: pyodbc.Cursor,
                        sap_response_id: int,
                        sap_response_line_id: int,
                        control_ids: dict,
                        entry: dict,
                        line: dict) -> None:
    """
    Insert one enriched row into INT.SAP_STO_Staging.
    MatchStatus values:
      Matched              — found in EXC.TransferOrder_Control
      MatchedINTOnly       — found in INT.Transfer_Order_SAP_Control only
      NoControlFound       — not in either table; placeholder was created
    """
    exc_id = control_ids.get('exc_control_id')
    sap_id = control_ids.get('sap_control_id')

    if exc_id:
        match_status = "Matched"
    elif sap_id:
        match_status = "MatchedINTOnly"
    else:
        match_status = "NoControlFound"

    cur.execute("""
        INSERT INTO INT.SAP_STO_Staging
        (SAPResponseID, SAPResponseLineID,
         TransferControlId,
         GoodIssueRefNumber, SalesOrderRefNumber,
         ConsolidationOrderNumber, GoodIssueCode,
         LineNumber, ProductNumber, GoodIssuePostingDate,
         Quantity, UOM, Site, LocationNumber, WarehouseNumber, BatchNumber,
         MatchStatus, IntegrationStatus)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'Staged')
    """,
    sap_response_id,
    sap_response_line_id,
    exc_id,                                         # FK — EXC preferred; NULL if no match
    entry.get("GoodIssueRefNumber"),
    entry.get("SalesOrderRefNumber"),
    entry.get("ConsolidationOrderNumber"),
    entry.get("GooodIssueCode"),                    # SAP typo preserved
    _safe_int(line.get("LineNumber")),
    line.get("ProductNumber"),
    _parse_date(line.get("GoodIssuePostingDate")),  # cast NVARCHAR → DATE
    _safe_float(line.get("Quantity")),
    line.get("UOM"),
    line.get("Site"),
    line.get("LocationNumber"),
    line.get("WarehouseNumber"),
    line.get("Batchnumber"),                        # SAP lowercase n
    match_status)


# =============================================================================
# FILE MOVE HELPER
# =============================================================================

def _move_file(src: str, dest_dir: str, dest_name: str) -> None:
    """Move src to dest_dir/dest_name. Timestamps the name if destination exists."""
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, dest_name)
    if os.path.exists(dest):
        stem, ext = os.path.splitext(dest_name)
        ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(dest_dir, f"{stem}_{ts}{ext}")
    shutil.move(src, dest)
    dbg(f"Moved → {os.path.basename(dest)}")


# =============================================================================
# CORE FILE PROCESSOR
# =============================================================================

def process_file(filepath: str,
                 conn: pyodbc.Connection,
                 audit: AuditLogger) -> str:
    """
    Process one SAP STO goods-issue confirmation JSON file.
    Returns: 'processed' | 'duplicate' | 'error'
    """
    filename        = os.path.basename(filepath)
    sap_response_id = None
    log(f"→ {filename}")

    try:
        # ------------------------------------------------------------------
        # 1. Parse JSON
        # ------------------------------------------------------------------
        with open(filepath, "r", encoding="utf-8") as f:
            raw_json = json.load(f)

        if not raw_json.get("request"):
            raise ValueError("JSON 'request' array is missing or empty")

        entry          = raw_json["request"][0]
        good_issue_ref = entry.get("GoodIssueRefNumber")
        lines          = entry.get("SalesOrderLine", [])

        if not good_issue_ref:
            raise ValueError("JSON missing GoodIssueRefNumber")

        audit.step(
            subprocess="Parse", step="Parse JSON", step_code="IO_PARSE_JSON",
            status="PASS",
            message=f"GoodIssueRefNumber={good_issue_ref}  lines={len(lines)}",
            rows_read=len(lines), source_reference=filename
        )

        cur = conn.cursor()

        # ------------------------------------------------------------------
        # 2. Duplicate check
        # ------------------------------------------------------------------
        if already_processed(cur, good_issue_ref):
            warn(f"  DUPLICATE: {good_issue_ref} — moving to processed, skipping.")
            audit.step(
                subprocess="Parse", step="Duplicate check",
                step_code="CHK_DUPLICATE", status="SKIPPED", severity="WARN",
                message=f"Already processed: {good_issue_ref}",
                source_reference=filename
            )
            _move_file(filepath, PROCESSED_DIR, filename)
            return "duplicate"

        # ------------------------------------------------------------------
        # 3. Insert RAW header
        # ------------------------------------------------------------------
        sap_response_id = insert_raw_header(cur, filename, entry, raw_json)
        dbg(f"  RAW header inserted: SAPResponseID={sap_response_id}")

        # ------------------------------------------------------------------
        # 4. Insert RAW lines
        # ------------------------------------------------------------------
        for line in lines:
            insert_raw_line(cur, sap_response_id, line)
        dbg(f"  RAW lines inserted: {len(lines)}")

        audit.step(
            subprocess="DB_Write", step="Insert RAW header + lines",
            step_code="DB_INS_RAW", status="PASS",
            rows_read=len(lines), rows_affected=(1 + len(lines)),
            message=f"SAPResponseID={sap_response_id}",
            source_reference=filename
        )

        # ------------------------------------------------------------------
        # 5. Resolve control IDs from BOTH tables
        #    If no match found, create a placeholder in INT control table
        #    so the confirmation is recorded and not silently lost.
        # ------------------------------------------------------------------
        control_ids = resolve_control_ids(cur, good_issue_ref)
        dbg(f"  Control match: "
            f"EXC={control_ids['exc_control_id']}  "
            f"INT={control_ids['sap_control_id']}")

        if not control_ids['matched_either']:
            warn(f"  No D365 control match for {good_issue_ref} — "
                 f"creating placeholder in INT.Transfer_Order_SAP_Control")
            placeholder_id = create_placeholder_control(
                cur, good_issue_ref, entry, sap_response_id
            )
            control_ids['sap_control_id'] = placeholder_id
            control_ids['matched_either'] = True
            dbg(f"  Placeholder created: SAPControlID={placeholder_id}")
            audit.step(
                subprocess="DB_Write",
                step="Create placeholder control record",
                step_code="DB_INS_PLACEHOLDER_CTRL",
                status="PASS", severity="WARN",
                rows_affected=1,
                message=(f"No D365 match for {good_issue_ref}. "
                         f"Placeholder SAPControlID={placeholder_id}. "
                         f"Status=SAP_Confirmed_NoD365Match"),
                source_reference=filename
            )

        # ------------------------------------------------------------------
        # 6. Insert INT.SAP_STO_Staging (one row per line)
        # ------------------------------------------------------------------
        line_ids = get_raw_line_ids(cur, sap_response_id)
        for line, line_id in zip(lines, line_ids):
            insert_int_staging(
                cur, sap_response_id, line_id, control_ids, entry, line
            )

        match_msg = (
            f"EXC_ControlId={control_ids['exc_control_id']}  "
            f"INT_ControlId={control_ids['sap_control_id']}"
        )
        audit.step(
            subprocess="DB_Write", step="Enrich into INT.SAP_STO_Staging",
            step_code="DB_INS_INT_STAGING", status="PASS",
            rows_affected=len(lines), message=match_msg,
            source_reference=filename
        )

        # ------------------------------------------------------------------
        # 7a. Update INT.Transfer_Order_SAP_Control  (Mar 2026+ / placeholders)
        # ------------------------------------------------------------------
        if control_ids['sap_control_id']:
            update_int_sap_control(
                cur, control_ids['sap_control_id'], good_issue_ref
            )
            log(f"  INT.Transfer_Order_SAP_Control "
                f"SAPControlID={control_ids['sap_control_id']} → SAP_Confirmed")
            audit.step(
                subprocess="DB_Write",
                step="Update INT.Transfer_Order_SAP_Control",
                step_code="DB_UPD_INT_SAP_CTRL",
                status="PASS", rows_affected=1,
                message=f"SAPControlID={control_ids['sap_control_id']}",
                source_reference=filename
            )

        # ------------------------------------------------------------------
        # 7b. Update EXC.TransferOrder_Control  (Feb 2026 batches)
        # ------------------------------------------------------------------
        if control_ids['exc_control_id']:
            update_exc_transfer_control(
                cur, control_ids['exc_control_id'],
                good_issue_ref, sap_response_id
            )
            log(f"  EXC.TransferOrder_Control "
                f"TransferControlId={control_ids['exc_control_id']} → SAP_CONFIRMED")
            audit.step(
                subprocess="DB_Write",
                step="Update EXC.TransferOrder_Control",
                step_code="DB_UPD_EXC_CTRL",
                status="PASS", rows_affected=1,
                message=f"TransferControlId={control_ids['exc_control_id']}",
                source_reference=filename
            )

        # ------------------------------------------------------------------
        # 8. Mark RAW header SynProc = 1  +  commit
        # ------------------------------------------------------------------
        mark_raw_synproc(cur, sap_response_id)
        conn.commit()

        # ------------------------------------------------------------------
        # 9. Move file to processed
        # ------------------------------------------------------------------
        _move_file(filepath, PROCESSED_DIR, filename)
        log(f"  ✔ {filename}  SAPResponseID={sap_response_id}")
        return "processed"

    except Exception as e:
        err(f"  ERROR processing {filename}: {e}")
        dbg(traceback.format_exc())
        try:
            conn.rollback()
        except Exception:
            pass
        audit.step(
            subprocess="Parse", step="Process file",
            step_code="IO_PROCESS_FILE",
            status="FAIL", severity="ERROR",
            message=f"Failed: {filename}",
            error=str(e), source_reference=filename
        )
        try:
            _move_file(filepath, PROCESSED_DIR, f"ERROR_{filename}")
        except Exception:
            pass
        return "error"


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    run_start      = time.time()
    run_id         = int(time.time() * 1000)
    correlation_id = str(uuid.uuid4())

    log("=" * 65)
    log(f"SAP_STO_Inbound_Loader  v1.3.0  starting")
    log(f"Run_ID={run_id}  Correlation_ID={correlation_id}")
    log(f"Inbound  : {INBOUND_DIR}")
    log(f"Processed: {PROCESSED_DIR}")
    log("=" * 65)

    try:
        conn = get_db_connection()
        log("DB connection OK")
    except Exception as e:
        err(f"FATAL: DB connection failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # Pre-flight: verify all required tables exist before touching files
    # ------------------------------------------------------------------
    if not check_prerequisites(conn):
        conn.close()
        return 1

    audit  = AuditLogger(conn, run_id, correlation_id)
    counts = {"total": 0, "processed": 0, "duplicate": 0, "error": 0}

    try:
        files = sorted(
            f for f in os.listdir(INBOUND_DIR)
            if f.lower().endswith(".json")
            and not f.lower().startswith("error_")
        )
        counts["total"] = len(files)
        log(f"Found {counts['total']} file(s) to process")

        if not files:
            log("Nothing to do.")
            return 0

        for fname in files:
            result = process_file(
                os.path.join(INBOUND_DIR, fname), conn, audit
            )
            counts[result] = counts.get(result, 0) + 1

    except Exception as e:
        err(f"FATAL: Unexpected error in main scan loop: {e}")
        dbg(traceback.format_exc())
        return 1

    finally:
        try:
            conn.close()
        except Exception:
            pass

        elapsed = time.time() - run_start
        log("=" * 65)
        log(
            f"COMPLETE  {elapsed:.1f}s  |  "
            f"Total={counts['total']}  "
            f"OK={counts.get('processed', 0)}  "
            f"Dup={counts.get('duplicate', 0)}  "
            f"Err={counts.get('error', 0)}"
        )
        log("=" * 65)

    return 0 if counts.get("error", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

# -*- coding: utf-8 -*-
# =============================================================================
#  Synovia Fusion – SAP STO Confirmation Inbound Loader
# =============================================================================
#
#  Script Name:   SAP_STO_Inbound_Loader.py
#  Version:       1.2.0  (dual-table control update)
#  Release Date:  2026-03-30
#
# -----------------------------------------------------------------------------
#  v1.2.0 changes:
#    Both Transfer Order control tables now updated on SAP confirmation:
#      INT.Transfer_Order_SAP_Control  — later batches (Mar 2026+),
#                                        keyed on TransferOrderNumber
#      EXC.TransferOrder_Control       — earlier batches (Feb 2026),
#                                        keyed on DynamicsDocumentNo
#    GoodIssueRefNumber in SAP JSON = JBRO-xxxxxxx = D365 TO number
#    = join key for both tables.
#    INT.vw_Transfer_Order_Unified view (TO_Reconciliation.sql) gives
#    unified lifecycle view across both tables.
#
# -----------------------------------------------------------------------------
#  What this does
#  --------------
#  Picks up SAP STO goods-issue confirmation JSON files dropped into the
#  inbound folder, loads them into RAW tables, enriches them into
#  INT.SAP_STO_Staging, and updates both TO control tables.
#
#  Flow per file:
#    1. Parse JSON
#    2. Duplicate check  (GoodIssueRefNumber already in RAW with SynProc=1)
#    3. Insert RAW.SAP_TO_Responses (header)
#    4. Insert RAW.SAP_TO_Responses_Lines (one row per SalesOrderLine)
#    5. Resolve TransferControlId from BOTH control tables via JBRO number
#    6. Insert INT.SAP_STO_Staging (enriched, with MatchStatus)
#    7a. Update INT.Transfer_Order_SAP_Control → Status = 'SAP_Confirmed'
#    7b. Update EXC.TransferOrder_Control → MasterStatus = 'SAP_CONFIRMED'
#    8. Mark RAW header SynProc = 1
#    9. Commit + move file to \processed
#
#  JSON structure (SAP STO goods-issue confirmation):
#    { "request": [{
#        "GooodIssueCode":           "...",   <- SAP typo, preserved
#        "SalesOrderRefNumber":      "...",
#        "GoodIssueRefNumber":       "...",   <- dedup key = JBRO-xxxxxxx
#        "ConsolidationOrderNumber": "...",
#        "SalesOrderLine": [{
#          "LineNumber":             "1",
#          "ProductNumber":          "...",
#          "GoodIssuePostingDate":   "YYYY-MM-DD",
#          "Quantity":               "10",
#          "UOM":                    "EA",
#          "Site":                   "...",
#          "LocationNumber":         "...",
#          "WarehouseNumber":        "...",
#          "Batchnumber":            "..."    <- SAP lowercase n, preserved
#        }]
#    }]}
#
# -----------------------------------------------------------------------------
#  DB schema verified against live diagnostic (2026-03-30):
#
#  RAW.SAP_TO_Responses:
#    SAPResponseID (bigint IDENTITY), FileName, GoodIssueCode,
#    SalesOrderRefNumber, GoodIssueRefNumber, ConsolidationOrderNumber,
#    RawJSON, SynProc (bit default 0), SynProcDate (datetime2), CreatedAtUtc
#
#  RAW.SAP_TO_Responses_Lines:
#    SAPResponseLineID (bigint IDENTITY), SAPResponseID, LineNumber,
#    ProductNumber, GoodIssuePostingDate (nvarchar), Quantity, UOM,
#    Site, LocationNumber, WarehouseNumber, BatchNumber, CreatedAtUtc
#
#  INT.Transfer_Order_SAP_Control:
#    SAPControlID, TransferOrderNumber, SourceID, CustomerCode,
#    Status, ErrorMessage, SentOn, CreatedAtUtc
#
#  EXC.TransferOrder_Control:
#    TransferControlId, SourceID, MasterStatus, SapDocumentNo,
#    SapResponseCode, SapResponseMessage (NVARCHAR 400),
#    SapResponseJson, SapLastSentAt, SapLastResponseAt,
#    SapStatus, StatusUpdatedAt, LastError
#
#  INT.SAP_STO_Staging:
#    Created by SAP_STO_DDL_Targeted.sql — run that first.
#
# -----------------------------------------------------------------------------
#  Prerequisite:
#    Run SAP_STO_DDL_Targeted.sql once in SSMS to create INT.SAP_STO_Staging
#    before executing this script for the first time.
#
# =============================================================================

import os
import sys
import json
import shutil
import logging
import traceback
import configparser
import uuid
import time
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import RotatingFileHandler

import pyodbc


# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG_PATH       = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"
CONFIG_SECTION_DB = "Fusion_EPOS_Production"

INBOUND_DIR   = r"\\PL-AZ-INT-PRD\D_Drive\FusionHub\Fusion_EPOS\SAP_STO_Confimrations"
PROCESSED_DIR = os.path.join(INBOUND_DIR, "processed")

LOG_DIR       = (
    r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub"
    r"\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs"
)
LOG_FILE_NAME = "SAP_STO_Inbound_Loader.log"

SYSTEM_NAME   = "Fusion_EPOS"
ENVIRONMENT   = "PRD"
ORCHESTRATOR  = "PYTHON"
AUDIT_PROCESS = "SAP_STO_Inbound_Loader"


# =============================================================================
# LOGGING
# =============================================================================

Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("SAP_STO_Inbound_Loader")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()

_fh = RotatingFileHandler(
    os.path.join(LOG_DIR, LOG_FILE_NAME),
    maxBytes=5_000_000, backupCount=10, encoding="utf-8"
)
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_fh)

_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_sh)

def log(msg):  logger.info(msg)
def dbg(msg):  logger.debug(msg)
def err(msg):  logger.error(msg)
def warn(msg): logger.warning(msg)


# =============================================================================
# INI / DB CONNECTION
# =============================================================================

def _resolve_ini() -> Path:
    p = Path(CONFIG_PATH)
    if p.is_dir():
        candidate = p / "Master_ini_config.ini"
        if candidate.exists():
            return candidate
    if p.is_file():
        return p
    raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")


def get_db_connection() -> pyodbc.Connection:
    ini_path = _resolve_ini()
    cfg = configparser.ConfigParser()
    cfg.read(ini_path)
    if CONFIG_SECTION_DB not in cfg:
        raise RuntimeError(f"Section [{CONFIG_SECTION_DB}] not found in {ini_path}")
    db = cfg[CONFIG_SECTION_DB]
    conn_str = (
        f"Driver={{{db.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
        f"Server={db.get('server')};"
        f"Database={db.get('database')};"
        f"UID={db.get('user', db.get('username', ''))};"
        f"PWD={db.get('password')};"
        f"Encrypt={db.get('encrypt', 'yes')};"
        f"TrustServerCertificate={db.get('trust_server_certificate', 'no')};"
        f"APP=SAP_STO_Inbound_Loader;"
    )
    log(f"Connecting via INI: {ini_path}")
    return pyodbc.connect(conn_str, autocommit=False)


# =============================================================================
# AUDIT  (non-blocking — writes to EXC.Process_Transaction_Log)
# =============================================================================

class AuditLogger:
    def __init__(self, conn: pyodbc.Connection, run_id: int, corr_id: str):
        self._conn   = conn
        self._run_id = run_id
        self._corr   = corr_id
        self._ok     = self._probe()

    def _probe(self) -> bool:
        try:
            self._conn.execute(
                "SELECT TOP 1 1 FROM sys.objects o "
                "JOIN sys.schemas s ON s.schema_id = o.schema_id "
                "WHERE s.name = 'EXC' AND o.name = 'Process_Transaction_Log';"
            )
            return True
        except Exception:
            return False

    def step(self, *, subprocess: str, step: str, step_code: str,
             status: str, severity: str = "INFO", message: str = "",
             error: str = None, rows_read: int = None,
             rows_affected: int = None,
             source_reference: str = None) -> None:
        if not self._ok:
            return
        try:
            self._conn.execute("""
                INSERT INTO EXC.Process_Transaction_Log
                (Run_ID, Correlation_ID,
                 System_Name, Environment, Orchestrator,
                 Process_Name, SubProcess_Name,
                 Step_Name, Step_Code, Step_Category,
                 Severity_Level, Execution_Status,
                 Rows_Read, Rows_Affected,
                 Source_Reference, Message, Error_Message,
                 Start_UTC, End_UTC)
                VALUES (?,?,?,?,?,?,?,?,?,'IO',?,?,?,?,?,?,?,
                        SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
            self._run_id, self._corr,
            SYSTEM_NAME, ENVIRONMENT, ORCHESTRATOR,
            AUDIT_PROCESS, subprocess,
            step, step_code,
            severity, status,
            rows_read, rows_affected,
            source_reference, message,
            error[:4000] if error else None)
        except Exception:
            self._ok = False  # disable on first write failure; never crash pipeline


# =============================================================================
# TYPE HELPERS
# =============================================================================

def _safe_int(val):
    if val is None:
        return None
    try:
        return int(float(str(val).replace(",", "").strip()))
    except Exception:
        return None


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except Exception:
        return None


def _parse_date(val):
    """Parse GoodIssuePostingDate from multiple SAP date formats → Python date."""
    if not val:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except ValueError:
            pass
    return None


# =============================================================================
# SQL — all column names verified against live DB diagnostic 2026-03-30
# =============================================================================

def already_processed(cur: pyodbc.Cursor, good_issue_ref: str) -> bool:
    """
    Duplicate check using SynProc flag on RAW.SAP_TO_Responses.
    Returns True if this GoodIssueRefNumber has already been successfully loaded.
    """
    row = cur.execute(
        "SELECT COUNT(*) FROM RAW.SAP_TO_Responses "
        "WHERE GoodIssueRefNumber = ? AND SynProc = 1;",
        good_issue_ref
    ).fetchone()
    return (row[0] if row else 0) > 0


def insert_raw_header(cur: pyodbc.Cursor,
                      filename: str, entry: dict, raw_json: dict) -> int:
    """
    Insert header into RAW.SAP_TO_Responses.
    SynProc defaults to 0 (DB default). Returns SAPResponseID (bigint).
    GooodIssueCode — SAP source field typo preserved intentionally.
    """
    row = cur.execute("""
        INSERT INTO RAW.SAP_TO_Responses
        (FileName, GoodIssueCode, SalesOrderRefNumber,
         GoodIssueRefNumber, ConsolidationOrderNumber, RawJSON)
        OUTPUT INSERTED.SAPResponseID
        VALUES (?, ?, ?, ?, ?, ?)
    """,
    filename,
    entry.get("GooodIssueCode"),           # SAP typo — three o's — preserved
    entry.get("SalesOrderRefNumber"),
    entry.get("GoodIssueRefNumber"),
    entry.get("ConsolidationOrderNumber"),
    json.dumps(raw_json, ensure_ascii=False)
    ).fetchone()
    return int(row[0])


def insert_raw_line(cur: pyodbc.Cursor,
                    sap_response_id: int, line: dict) -> None:
    """
    Insert one line into RAW.SAP_TO_Responses_Lines.
    GoodIssuePostingDate stored as NVARCHAR(30) in RAW — no casting here.
    Batchnumber — SAP uses lowercase n — preserved.
    """
    cur.execute("""
        INSERT INTO RAW.SAP_TO_Responses_Lines
        (SAPResponseID, LineNumber, ProductNumber, GoodIssuePostingDate,
         Quantity, UOM, Site, LocationNumber, WarehouseNumber, BatchNumber)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    sap_response_id,
    _safe_int(line.get("LineNumber")),
    line.get("ProductNumber"),
    line.get("GoodIssuePostingDate"),      # stored as-is; cast to DATE in INT staging
    _safe_float(line.get("Quantity")),
    line.get("UOM"),
    line.get("Site"),
    line.get("LocationNumber"),
    line.get("WarehouseNumber"),
    line.get("Batchnumber"))               # SAP lowercase n


def mark_raw_synproc(cur: pyodbc.Cursor, sap_response_id: int) -> None:
    """Flag RAW header as successfully processed (SynProc=1, SynProcDate=now)."""
    cur.execute("""
        UPDATE RAW.SAP_TO_Responses
        SET SynProc     = 1,
            SynProcDate = SYSUTCDATETIME()
        WHERE SAPResponseID = ?;
    """, sap_response_id)


def get_raw_line_ids(cur: pyodbc.Cursor, sap_response_id: int) -> list:
    """Return SAPResponseLineIDs in LineNumber order — used to FK into INT staging."""
    rows = cur.execute(
        "SELECT SAPResponseLineID FROM RAW.SAP_TO_Responses_Lines "
        "WHERE SAPResponseID = ? ORDER BY LineNumber;",
        sap_response_id
    ).fetchall()
    return [r[0] for r in rows]


def resolve_control_ids(cur: pyodbc.Cursor, good_issue_ref: str) -> dict:
    """
    GoodIssueRefNumber = JBRO-xxxxxxx = D365 Transfer Order number.
    Searches BOTH control tables:
      INT.Transfer_Order_SAP_Control  via TransferOrderNumber  (Mar 2026+ batches)
      EXC.TransferOrder_Control       via DynamicsDocumentNo   (Feb 2026 batches)
    Returns:
      { 'sap_control_id': int|None, 'exc_control_id': int|None, 'matched_either': bool }
    """
    result = {
        'sap_control_id': None,
        'exc_control_id': None,
        'matched_either': False,
    }

    row = cur.execute("""
        SELECT TOP 1 SAPControlID
        FROM INT.Transfer_Order_SAP_Control
        WHERE TransferOrderNumber = ?
        ORDER BY SAPControlID DESC;
    """, good_issue_ref).fetchone()
    if row:
        result['sap_control_id'] = int(row[0])

    row = cur.execute("""
        SELECT TOP 1 TransferControlId
        FROM EXC.TransferOrder_Control
        WHERE DynamicsDocumentNo = ?
        ORDER BY TransferControlId DESC;
    """, good_issue_ref).fetchone()
    if row:
        result['exc_control_id'] = int(row[0])

    result['matched_either'] = bool(
        result['sap_control_id'] or result['exc_control_id']
    )
    return result


def update_int_sap_control(cur: pyodbc.Cursor,
                            sap_control_id: int,
                            good_issue_ref: str) -> None:
    """
    Update INT.Transfer_Order_SAP_Control on goods-issue confirmation.
    Status: Success (D365 POST succeeded) → SAP_Confirmed (SAP has issued goods).
    """
    cur.execute("""
        UPDATE INT.Transfer_Order_SAP_Control
        SET Status       = 'SAP_Confirmed',
            ErrorMessage = NULL
        WHERE SAPControlID = ?;
    """, sap_control_id)


def update_exc_transfer_control(cur: pyodbc.Cursor,
                                  exc_control_id: int,
                                  good_issue_ref: str,
                                  sap_response_id: int) -> None:
    """
    Update EXC.TransferOrder_Control with goods-issue confirmation.
    MasterStatus only advances from SENT_DYNAMICS or SENT_SAP → SAP_CONFIRMED.
    SapResponseMessage is NVARCHAR(400) — message is within that limit.
    """
    cur.execute("""
        UPDATE EXC.TransferOrder_Control
        SET MasterStatus         = CASE
                                       WHEN MasterStatus IN ('SENT_DYNAMICS', 'SENT_SAP')
                                       THEN 'SAP_CONFIRMED'
                                       ELSE MasterStatus
                                   END,
            SapDocumentNo        = ?,
            SapResponseCode      = 'Success',
            SapResponseMessage   = 'SAP goods-issue confirmation received',
            SapResponseJson      = ?,
            SapLastResponseAt    = SYSUTCDATETIME(),
            SapStatus            = 'Confirmed',
            StatusUpdatedAt      = SYSUTCDATETIME(),
            LastError            = NULL
        WHERE TransferControlId  = ?;
    """,
    good_issue_ref,
    json.dumps({
        "GoodIssueRefNumber": good_issue_ref,
        "SAPResponseID":      sap_response_id
    }),
    exc_control_id)


def insert_int_staging(cur: pyodbc.Cursor,
                        sap_response_id: int,
                        sap_response_line_id: int,
                        control_ids: dict,
                        entry: dict,
                        line: dict) -> None:
    """
    Insert one enriched row into INT.SAP_STO_Staging.
    TransferControlId FK uses the EXC table ID (richer table, preferred).
    MatchStatus records which control table was found:
      Matched        — found in EXC.TransferOrder_Control
      MatchedINTOnly — found in INT.Transfer_Order_SAP_Control only
      NoControlFound — not found in either table
    GoodIssuePostingDate cast to DATE here (stored as NVARCHAR in RAW).
    """
    exc_id = control_ids.get('exc_control_id')
    sap_id = control_ids.get('sap_control_id')

    if exc_id:
        match_status = "Matched"
    elif sap_id:
        match_status = "MatchedINTOnly"
    else:
        match_status = "NoControlFound"

    cur.execute("""
        INSERT INTO INT.SAP_STO_Staging
        (SAPResponseID, SAPResponseLineID,
         TransferControlId,
         GoodIssueRefNumber, SalesOrderRefNumber,
         ConsolidationOrderNumber, GoodIssueCode,
         LineNumber, ProductNumber, GoodIssuePostingDate,
         Quantity, UOM, Site, LocationNumber, WarehouseNumber, BatchNumber,
         MatchStatus, IntegrationStatus)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'Staged')
    """,
    sap_response_id,
    sap_response_line_id,
    exc_id,                                         # FK — EXC preferred
    entry.get("GoodIssueRefNumber"),
    entry.get("SalesOrderRefNumber"),
    entry.get("ConsolidationOrderNumber"),
    entry.get("GooodIssueCode"),                    # SAP typo preserved
    _safe_int(line.get("LineNumber")),
    line.get("ProductNumber"),
    _parse_date(line.get("GoodIssuePostingDate")),  # cast NVARCHAR → DATE
    _safe_float(line.get("Quantity")),
    line.get("UOM"),
    line.get("Site"),
    line.get("LocationNumber"),
    line.get("WarehouseNumber"),
    line.get("Batchnumber"),                        # SAP lowercase n
    match_status)


# =============================================================================
# FILE MOVE HELPER
# =============================================================================

def _move_file(src: str, dest_dir: str, dest_name: str) -> None:
    """Move src to dest_dir/dest_name. Timestamps the name if destination exists."""
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, dest_name)
    if os.path.exists(dest):
        stem, ext = os.path.splitext(dest_name)
        ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(dest_dir, f"{stem}_{ts}{ext}")
    shutil.move(src, dest)
    dbg(f"Moved → {os.path.basename(dest)}")


# =============================================================================
# CORE FILE PROCESSOR
# =============================================================================

def process_file(filepath: str,
                 conn: pyodbc.Connection,
                 audit: AuditLogger) -> str:
    """
    Process one SAP STO goods-issue confirmation JSON file.
    Returns: 'processed' | 'duplicate' | 'error'
    All DB work is in a single transaction — rolled back on any exception.
    File is moved to \processed on success, ERROR_<name> on failure.
    """
    filename        = os.path.basename(filepath)
    sap_response_id = None
    log(f"→ {filename}")

    try:
        # ------------------------------------------------------------------
        # 1. Parse JSON
        # ------------------------------------------------------------------
        with open(filepath, "r", encoding="utf-8") as f:
            raw_json = json.load(f)

        if not raw_json.get("request"):
            raise ValueError("JSON 'request' array is missing or empty")

        entry          = raw_json["request"][0]
        good_issue_ref = entry.get("GoodIssueRefNumber")
        lines          = entry.get("SalesOrderLine", [])

        if not good_issue_ref:
            raise ValueError("JSON missing GoodIssueRefNumber")

        audit.step(
            subprocess="Parse", step="Parse JSON", step_code="IO_PARSE_JSON",
            status="PASS",
            message=f"GoodIssueRefNumber={good_issue_ref}  lines={len(lines)}",
            rows_read=len(lines), source_reference=filename
        )

        cur = conn.cursor()

        # ------------------------------------------------------------------
        # 2. Duplicate check
        # ------------------------------------------------------------------
        if already_processed(cur, good_issue_ref):
            warn(f"  DUPLICATE: {good_issue_ref} — moving to processed, skipping.")
            audit.step(
                subprocess="Parse", step="Duplicate check",
                step_code="CHK_DUPLICATE", status="SKIPPED", severity="WARN",
                message=f"Already processed: {good_issue_ref}",
                source_reference=filename
            )
            _move_file(filepath, PROCESSED_DIR, filename)
            return "duplicate"

        # ------------------------------------------------------------------
        # 3. Insert RAW header
        # ------------------------------------------------------------------
        sap_response_id = insert_raw_header(cur, filename, entry, raw_json)
        dbg(f"  RAW header inserted: SAPResponseID={sap_response_id}")

        # ------------------------------------------------------------------
        # 4. Insert RAW lines
        # ------------------------------------------------------------------
        for line in lines:
            insert_raw_line(cur, sap_response_id, line)
        dbg(f"  RAW lines inserted: {len(lines)}")

        audit.step(
            subprocess="DB_Write", step="Insert RAW header + lines",
            step_code="DB_INS_RAW", status="PASS",
            rows_read=len(lines), rows_affected=(1 + len(lines)),
            message=f"SAPResponseID={sap_response_id}",
            source_reference=filename
        )

        # ------------------------------------------------------------------
        # 5. Resolve control IDs from BOTH tables
        # ------------------------------------------------------------------
        control_ids = resolve_control_ids(cur, good_issue_ref)
        dbg(f"  Control match: "
            f"EXC={control_ids['exc_control_id']}  "
            f"INT={control_ids['sap_control_id']}")

        if not control_ids['matched_either']:
            warn(f"  No control record found for {good_issue_ref} in either table")

        # ------------------------------------------------------------------
        # 6. Insert INT.SAP_STO_Staging (one row per line)
        # ------------------------------------------------------------------
        line_ids = get_raw_line_ids(cur, sap_response_id)
        for line, line_id in zip(lines, line_ids):
            insert_int_staging(
                cur, sap_response_id, line_id, control_ids, entry, line
            )

        match_msg = (
            f"EXC_ControlId={control_ids['exc_control_id']}  "
            f"INT_ControlId={control_ids['sap_control_id']}"
        )
        audit.step(
            subprocess="DB_Write", step="Enrich into INT.SAP_STO_Staging",
            step_code="DB_INS_INT_STAGING", status="PASS",
            rows_affected=len(lines), message=match_msg,
            source_reference=filename
        )

        # ------------------------------------------------------------------
        # 7a. Update INT.Transfer_Order_SAP_Control  (Mar 2026+ batches)
        # ------------------------------------------------------------------
        if control_ids['sap_control_id']:
            update_int_sap_control(
                cur, control_ids['sap_control_id'], good_issue_ref
            )
            log(f"  INT.Transfer_Order_SAP_Control "
                f"SAPControlID={control_ids['sap_control_id']} → SAP_Confirmed")
            audit.step(
                subprocess="DB_Write",
                step="Update INT.Transfer_Order_SAP_Control",
                step_code="DB_UPD_INT_SAP_CTRL",
                status="PASS", rows_affected=1,
                message=f"SAPControlID={control_ids['sap_control_id']}",
                source_reference=filename
            )

        # ------------------------------------------------------------------
        # 7b. Update EXC.TransferOrder_Control  (Feb 2026 batches)
        # ------------------------------------------------------------------
        if control_ids['exc_control_id']:
            update_exc_transfer_control(
                cur, control_ids['exc_control_id'],
                good_issue_ref, sap_response_id
            )
            log(f"  EXC.TransferOrder_Control "
                f"TransferControlId={control_ids['exc_control_id']} → SAP_CONFIRMED")
            audit.step(
                subprocess="DB_Write",
                step="Update EXC.TransferOrder_Control",
                step_code="DB_UPD_EXC_CTRL",
                status="PASS", rows_affected=1,
                message=f"TransferControlId={control_ids['exc_control_id']}",
                source_reference=filename
            )

        if not control_ids['matched_either']:
            audit.step(
                subprocess="DB_Write", step="Update control tables",
                step_code="DB_UPD_CTRL_NOMATCH",
                status="WARN", severity="WARN",
                message=f"No control record matched {good_issue_ref}",
                source_reference=filename
            )

        # ------------------------------------------------------------------
        # 8. Mark RAW header SynProc = 1  +  commit
        # ------------------------------------------------------------------
        mark_raw_synproc(cur, sap_response_id)
        conn.commit()

        # ------------------------------------------------------------------
        # 9. Move file to processed
        # ------------------------------------------------------------------
        _move_file(filepath, PROCESSED_DIR, filename)
        log(f"  ✔ {filename}  SAPResponseID={sap_response_id}")
        return "processed"

    except Exception as e:
        err(f"  ERROR processing {filename}: {e}")
        dbg(traceback.format_exc())

        # Best-effort rollback
        try:
            conn.rollback()
        except Exception:
            pass

        audit.step(
            subprocess="Parse", step="Process file",
            step_code="IO_PROCESS_FILE",
            status="FAIL", severity="ERROR",
            message=f"Failed: {filename}",
            error=str(e), source_reference=filename
        )

        # Move to processed with ERROR_ prefix so it isn't re-picked next cycle
        try:
            _move_file(filepath, PROCESSED_DIR, f"ERROR_{filename}")
        except Exception:
            pass

        return "error"


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    run_start      = time.time()
    run_id         = int(time.time() * 1000)
    correlation_id = str(uuid.uuid4())

    log("=" * 65)
    log(f"SAP_STO_Inbound_Loader  v1.2.0  starting")
    log(f"Run_ID={run_id}  Correlation_ID={correlation_id}")
    log(f"Inbound  : {INBOUND_DIR}")
    log(f"Processed: {PROCESSED_DIR}")
    log("=" * 65)

    try:
        conn = get_db_connection()
        log("DB connection OK")
    except Exception as e:
        err(f"FATAL: DB connection failed: {e}")
        return 1

    audit  = AuditLogger(conn, run_id, correlation_id)
    counts = {"total": 0, "processed": 0, "duplicate": 0, "error": 0}

    try:
        files = sorted(
            f for f in os.listdir(INBOUND_DIR)
            if f.lower().endswith(".json")
            and not f.lower().startswith("error_")   # skip previous error files
        )
        counts["total"] = len(files)
        log(f"Found {counts['total']} file(s) to process")

        if not files:
            log("Nothing to do.")
            return 0

        for fname in files:
            result = process_file(
                os.path.join(INBOUND_DIR, fname), conn, audit
            )
            counts[result] = counts.get(result, 0) + 1

    except Exception as e:
        err(f"FATAL: Unexpected error in main scan loop: {e}")
        dbg(traceback.format_exc())
        return 1

    finally:
        try:
            conn.close()
        except Exception:
            pass

        elapsed = time.time() - run_start
        log("=" * 65)
        log(
            f"COMPLETE  {elapsed:.1f}s  |  "
            f"Total={counts['total']}  "
            f"OK={counts.get('processed', 0)}  "
            f"Dup={counts.get('duplicate', 0)}  "
            f"Err={counts.get('error', 0)}"
        )
        log("=" * 65)

    return 0 if counts.get("error", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
