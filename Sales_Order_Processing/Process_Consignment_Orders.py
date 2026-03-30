# =============================================================================
#  Synovia Fusion – Process Consignment Orders (Enterprise Standard, Dual Audit)
# =============================================================================
#
#  Product:        Synovia Fusion Platform
#  Module:         EPOS Control + D365 Transmission
#  Script Name:    Process_Consignment_Orders.py
#
#  Version:        3.0.0
#  Release Date:   2026-02-12
#
# -----------------------------------------------------------------------------
#  What this does (Step 3: Process_Consignment_Orders)
#  ---------------------------------------------------
#  Python controls the process end-to-end (no SQL orchestration dependency):
#    A) Stage sales orders into INT.SalesOrderStaging from CTL (business rules)
#    B) Transmit staged orders to D365 (headers then lines), idempotent + retriable
#    C) Update staging statuses consistently
#
#  Enterprise standards baked in:
#   - Direct bootstrap DB connection (no dependency on CFG.v_Profile_Database)
#   - Dual audit logging (best-effort, non-blocking):
#       * EXC.Process_Transaction_Log  (canonical)
#       * EXC.Process_Steps            (compatibility)
#   - Full Process/SubProcess/Step classification with category + severity inference
#   - Every check and action logged (PASS/WARN/FAIL/SKIPPED)
#   - Atomic claim on HeaderGroupId to prevent double-send
#   - Durable error capture (HTTP status + response payload in Details_JSON)
#   - Safe transaction handling (commits at sensible boundaries)
#
#  Configuration:
#   - DB password from ENV: FUSION_EPOS_BOOTSTRAP_PASSWORD  (preferred)
#     or INI: D:\Configuration\Fusion_EPOS_Production.ini [Fusion_EPOS_Production] password
#   - D365 config INI: D:\Configuration\Production_365.ini  (AUTH + API sections)
#
#  Notes:
#   - This script introspects INT.SalesOrderStaging columns at runtime and
#     adapts inserts/selects to the actual schema (reduces brittleness).
# =============================================================================

import os
import sys
import uuid
import json
import time
import logging
import configparser
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List, Iterable

import pyodbc
import requests

try:
    from exc_logger import ExcLogger  # optional — provides Run_ID lineage
except ImportError:
    ExcLogger = None

# -----------------------------------------------------------------------------
# Logging (console capture by SQL Agent / service)
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =============================================================================
# RUNTIME IDENTIFIERS
# =============================================================================

SYSTEM_NAME     = "Fusion_EPOS"
ENVIRONMENT     = "PROD"

PROCESS_KEY     = "FUSION_EPOS_PROCESS_CONSIGNMENT_ORDERS"
PROCESS_VERSION = "3.0.0"

ORCHESTRATOR    = "PYTHON"
TRIGGER_TYPE    = "SCHEDULE"
TRIGGERED_BY    = "SQLAGENT"

AUDIT_PROCESS   = "Process_Consignment_Orders"


# =============================================================================
# CONFIG PATHS
# =============================================================================

CONFIG_PATH     = r"D:\Configuration"
INI_DB_PATH     = os.path.join(CONFIG_PATH, "Fusion_EPOS_Production.ini")
INI_DB_SECTION  = "Fusion_EPOS_Production"

INI_D365_PATH   = os.path.join(CONFIG_PATH, "Production_365.ini")

BOOTSTRAP = {
    "driver":   "ODBC Driver 17 for SQL Server",
    "server":   "futureworks-sdi-db.database.windows.net",
    "database": "Fusion_EPOS_Production",
    "username": "SynFW_DB",
    "timeout":  60,
}
BOOTSTRAP_PASSWORD_ENV = "FUSION_EPOS_BOOTSTRAP_PASSWORD"

# Staging/Source tables
STAGING_TABLE = "INT.SalesOrderStaging"   # outbound staging table
CTL_HEADER    = "CTL.EPOS_Header"
CTL_LINES     = "CTL.EPOS_Line_Item"

# Optional operational log table used by previous SP (kept if present)
EXC_PROCESSED_LINES = "EXC.Processed_Lines"

# D365 API entities
D365_HEADERS_ENTITY = "SalesOrderHeadersV2"
D365_LINES_ENTITY   = "SalesOrderLinesV3"

# D365 retry policy
API_ATTEMPTS = 3
API_BASE_DELAY_SEC = 1.0
API_TIMEOUT_SEC = 60


# =============================================================================
# CONFIG HELPERS
# =============================================================================

def _read_ini(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if os.path.exists(path):
        cfg.read(path)
    return cfg

def _get_secret_from_env_or_ini(env_key: str, ini_path: str, ini_section: str, ini_key: str) -> str:
    v = os.environ.get(env_key)
    if v:
        return v
    cfg = _read_ini(ini_path)
    if ini_section in cfg and ini_key in cfg[ini_section]:
        return cfg[ini_section][ini_key]
    raise RuntimeError(f"Missing secret. Set env var '{env_key}' or set '{ini_key}' in {ini_path} [{ini_section}].")

def _build_conn_str(password: str) -> str:
    return (
        f"DRIVER={{{BOOTSTRAP['driver']}}};"
        f"SERVER={BOOTSTRAP['server']};"
        f"DATABASE={BOOTSTRAP['database']};"
        f"UID={BOOTSTRAP['username']};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout={int(BOOTSTRAP['timeout'])};"
        f"APP=Fusion_EPOS_Process_Consignment_Orders;"
    )

def connect_meta_data(conn_str: str) -> Tuple[pyodbc.Connection, pyodbc.Connection]:
    conn_meta = pyodbc.connect(conn_str, autocommit=True)
    conn_data = pyodbc.connect(conn_str, autocommit=False)
    return conn_meta, conn_data

def _load_d365_config() -> Dict[str, str]:
    cfg = _read_ini(INI_D365_PATH)
    if "AUTH" not in cfg or "API" not in cfg:
        raise RuntimeError(f"D365 config must contain [AUTH] and [API] sections: {INI_D365_PATH}")
    return {
        "tenant_id": cfg["AUTH"]["tenant_id"],
        "client_id": cfg["AUTH"]["client_id"],
        "client_secret": cfg["AUTH"]["client_secret"],
        "resource": cfg["AUTH"]["resource"],
        "login_url": cfg["API"]["login_url"],
        "odata_root": cfg["API"]["odata_service_root"].rstrip("/"),
    }


# =============================================================================
# DB INTROSPECTION HELPERS
# =============================================================================

def _object_exists(conn: pyodbc.Connection, schema: str, name: str) -> bool:
    cur = conn.cursor()
    row = cur.execute("""
        SELECT TOP (1) 1
        FROM (
            SELECT 1 AS x
            FROM sys.objects o
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ? AND o.type IN ('U','V')
            UNION ALL
            SELECT 1
            FROM sys.synonyms sn
            JOIN sys.schemas s ON s.schema_id = sn.schema_id
            WHERE s.name = ? AND sn.name = ?
        ) q;
    """, schema, name, schema, name).fetchone()
    return row is not None

def _get_table_columns(conn: pyodbc.Connection, schema: str, table: str) -> List[str]:
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT c.name
        FROM sys.columns c
        JOIN sys.objects o ON o.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = ? AND o.name = ? AND o.type = 'U'
        ORDER BY c.column_id;
    """, schema, table).fetchall()
    return [r[0] for r in rows]


# =============================================================================
# DUAL AUDIT LOGGER
# =============================================================================

def _infer_category(step_code: str) -> str:
    sc = (step_code or "").upper()
    if sc.startswith(("CHK_", "VAL_")): return "VALIDATION"
    if sc.startswith(("DB_", "INS_", "UPD_", "DEL_", "SP_")): return "DB"
    if sc.startswith(("API_", "HTTP_", "AUTH_")): return "IO"
    if sc.startswith(("TRN_", "XFM_", "TRANSFORM_")): return "TRANSFORM"
    if sc.startswith(("FS_", "MOVE_", "STAGE_")): return "FILESYSTEM"
    if sc.startswith(("CFG_", "CONF_")): return "CONFIG"
    if sc.startswith(("SEC_",)): return "SECURITY"
    if sc.startswith(("SUM_", "SUMMARY_")): return "SUMMARY"
    return "ACTION"

def _infer_severity(status: str, category: str) -> str:
    st = (status or "").upper()
    cat = (category or "").upper()
    if st == "FAIL" and cat in ("DB", "CONFIG", "SECURITY"):
        return "CRITICAL"
    if st == "FAIL":
        return "ERROR"
    if st == "WARN":
        return "WARN"
    return "INFO"

@dataclass
class _StepHandles:
    ptl_log_id: Optional[int] = None   # EXC.Process_Transaction_Log
    ps_step_id: Optional[int] = None   # EXC.Process_Steps

class DualAuditLogger:
    """
    Best-effort dual logger:
      - EXC.Process_Transaction_Log (canonical)
      - EXC.Process_Steps (compat)
    If either table is missing or errors, it continues without breaking the pipeline.
    """
    def __init__(self, conn_meta: pyodbc.Connection, *,
                 system_name: str, environment: str, orchestrator: str, default_process: str):
        self.conn = conn_meta
        self.system_name = system_name
        self.environment = environment
        self.orchestrator = orchestrator
        self.default_process = default_process

        self.has_ptl = _object_exists(conn_meta, "EXC", "Process_Transaction_Log")
        self.has_ps  = _object_exists(conn_meta, "EXC", "Process_Steps")

    def step(self, run_id: Optional[int], txn_id: Optional[int], *,
             process: Optional[str],
             subprocess: str,
             step: str,
             step_code: str,
             source_reference: Optional[str] = None,
             reference_number: Optional[str] = None,
             idempotency_key: Optional[str] = None,
             details: Optional[Dict[str, Any]] = None):
        return _StepContext(
            audit=self,
            run_id=run_id,
            txn_id=txn_id,
            process=process or self.default_process,
            subprocess=subprocess,
            step=step,
            step_code=step_code,
            source_reference=source_reference,
            reference_number=reference_number,
            idempotency_key=idempotency_key,
            details=details or {}
        )

    def _start(self, run_id: Optional[int], txn_id: Optional[int], *,
               process: str, subprocess: str, step: str, step_code: str,
               source_reference: Optional[str], reference_number: Optional[str],
               idempotency_key: Optional[str], details: Dict[str, Any]) -> _StepHandles:

        cat = _infer_category(step_code)
        h = _StepHandles()

        # 1) Process_Transaction_Log
        if self.has_ptl:
            try:
                cur = self.conn.cursor()
                cur.execute("""
                    INSERT INTO EXC.Process_Transaction_Log
                    (Run_ID, Txn_ID, Correlation_ID, System_Name, Environment,
                     Process_Name, SubProcess_Name, Step_Name, Step_Code,
                     Step_Category, Severity_Level, Execution_Status,
                     Source_Reference, Reference_Number, Idempotency_Key,
                     Message, Details_JSON, Start_UTC)
                    OUTPUT INSERTED.Log_ID
                    VALUES (?,?,?,?,?,
                            ?,?,?,?,
                            ?,?, 'STARTED',
                            ?,?,?,?,
                            ?, ?, SYSUTCDATETIME());
                """,
                run_id, txn_id, None, self.system_name, self.environment,
                process, subprocess, step, step_code,
                cat, "INFO",
                source_reference, reference_number, idempotency_key,
                "STARTED", json.dumps(details, default=str) if details else None
                )
                h.ptl_log_id = int(cur.fetchone()[0])
            except Exception:
                self.has_ptl = False

        # 2) Process_Steps
        if self.has_ps:
            try:
                cur = self.conn.cursor()
                cur.execute("""
                    INSERT INTO EXC.Process_Steps
                    (Run_ID, Txn_ID, Parent_Step_ID, System_Name, Environment, Orchestrator,
                     Process_Key, Subprocess_Key, Step_Group, Step_Code, Step_Name, Step_Type,
                     Status, Severity, Source_Reference, Reference_Number, Transaction_Code,
                     Idempotency_Key, Details_JSON, Start_UTC)
                    OUTPUT INSERTED.Step_ID
                    VALUES (?,?,?,?,?,?,
                            ?,?,?,?, ?,?,
                            'STARTED',0, ?,?,?,?,
                            ?,?, SYSUTCDATETIME());
                """,
                run_id, txn_id, None, self.system_name, self.environment, self.orchestrator,
                process, subprocess, cat, step_code, step, "STEP",
                source_reference, reference_number, None,
                idempotency_key, json.dumps(details, default=str) if details else None
                )
                h.ps_step_id = int(cur.fetchone()[0])
            except Exception:
                self.has_ps = False

        return h

    def _end(self, handles: _StepHandles, *, status: str, message: Optional[str],
             error: Optional[str], rows_read: Optional[int], rows_affected: Optional[int],
             bytes_processed: Optional[int], details: Optional[Dict[str, Any]]):
        # Process_Transaction_Log update
        if self.has_ptl and handles.ptl_log_id:
            try:
                cur = self.conn.cursor()
                # determine category from stored row (or re-infer if needed)
                cat = _infer_category("X")  # default
                row = cur.execute("SELECT Step_Category FROM EXC.Process_Transaction_Log WHERE Log_ID=?;", handles.ptl_log_id).fetchone()
                if row and row[0]:
                    cat = str(row[0])
                sev = _infer_severity(status, cat)
                cur.execute("""
                    UPDATE EXC.Process_Transaction_Log
                    SET Execution_Status=?,
                        Severity_Level=?,
                        Rows_Read=COALESCE(?, Rows_Read),
                        Rows_Affected=COALESCE(?, Rows_Affected),
                        Bytes_Processed=COALESCE(?, Bytes_Processed),
                        Message=COALESCE(?, Message),
                        Error_Message=COALESCE(?, Error_Message),
                        Details_JSON=COALESCE(?, Details_JSON),
                        End_UTC=SYSUTCDATETIME()
                    WHERE Log_ID=?;
                """,
                status, sev,
                rows_read, rows_affected, bytes_processed,
                message, error,
                json.dumps(details, default=str) if details else None,
                handles.ptl_log_id
                )
            except Exception:
                self.has_ptl = False

        # Process_Steps update
        if self.has_ps and handles.ps_step_id:
            try:
                cur = self.conn.cursor()
                # get Step_Group for severity inference
                row = cur.execute("SELECT Step_Group FROM EXC.Process_Steps WHERE Step_ID=?;", handles.ps_step_id).fetchone()
                cat = str(row[0]) if row and row[0] else "ACTION"
                sev = _infer_severity(status, cat)
                sev_num = {"INFO": 0, "WARN": 1, "ERROR": 2, "CRITICAL": 3}.get(sev, 0)
                cur.execute("""
                    UPDATE EXC.Process_Steps
                    SET Status=?,
                        Severity=?,
                        Rows_Read=COALESCE(?, Rows_Read),
                        Rows_Affected=COALESCE(?, Rows_Affected),
                        Bytes_Read=COALESCE(?, Bytes_Read),
                        Outcome_Message=COALESCE(?, Outcome_Message),
                        Error_Message=COALESCE(?, Error_Message),
                        Details_JSON=COALESCE(?, Details_JSON),
                        End_UTC=SYSUTCDATETIME()
                    WHERE Step_ID=?;
                """,
                status, sev_num,
                rows_read, rows_affected, bytes_processed,
                message, error,
                json.dumps(details, default=str) if details else None,
                handles.ps_step_id
                )
            except Exception:
                self.has_ps = False


class _StepContext:
    def __init__(self, *, audit: DualAuditLogger, run_id: Optional[int], txn_id: Optional[int],
                 process: str, subprocess: str, step: str, step_code: str,
                 source_reference: Optional[str], reference_number: Optional[str],
                 idempotency_key: Optional[str], details: Dict[str, Any]):
        self.audit = audit
        self.run_id = run_id
        self.txn_id = txn_id
        self.process = process
        self.subprocess = subprocess
        self.step = step
        self.step_code = step_code
        self.source_reference = source_reference
        self.reference_number = reference_number
        self.idempotency_key = idempotency_key
        self.details = details
        self.handles = _StepHandles()
        self._ended = False

    def __enter__(self):
        self.handles = self.audit._start(
            self.run_id, self.txn_id,
            process=self.process, subprocess=self.subprocess, step=self.step, step_code=self.step_code,
            source_reference=self.source_reference, reference_number=self.reference_number,
            idempotency_key=self.idempotency_key, details=self.details
        )
        return self

    def pass_(self, message: str = "PASS", *, rows_read: Optional[int] = None,
              rows_affected: Optional[int] = None, bytes_processed: Optional[int] = None,
              details: Optional[Dict[str, Any]] = None, reference_number: Optional[str] = None):
        if reference_number:
            self.reference_number = reference_number
        self._end("PASS", message, None, rows_read, rows_affected, bytes_processed, details)

    def warn_(self, message: str = "WARN", *, rows_read: Optional[int] = None,
              rows_affected: Optional[int] = None, bytes_processed: Optional[int] = None,
              details: Optional[Dict[str, Any]] = None):
        self._end("WARN", message, None, rows_read, rows_affected, bytes_processed, details)

    def fail_(self, message: str = "FAIL", *, error: Optional[Exception] = None,
              rows_read: Optional[int] = None, rows_affected: Optional[int] = None,
              bytes_processed: Optional[int] = None, details: Optional[Dict[str, Any]] = None):
        err = str(error)[:4000] if error else message
        self._end("FAIL", message, err, rows_read, rows_affected, bytes_processed, details)

    def skip_(self, message: str = "SKIPPED", *, details: Optional[Dict[str, Any]] = None):
        self._end("SKIPPED", message, None, None, None, None, details)

    def _end(self, status: str, message: Optional[str], error: Optional[str],
             rows_read: Optional[int], rows_affected: Optional[int], bytes_processed: Optional[int],
             details: Optional[Dict[str, Any]]):
        if self._ended:
            return
        self.audit._end(
            self.handles,
            status=status,
            message=message,
            error=error,
            rows_read=rows_read,
            rows_affected=rows_affected,
            bytes_processed=bytes_processed,
            details=details
        )
        self._ended = True

    def __exit__(self, exc_type, exc, tb):
        if exc and not self._ended:
            self.fail_(message="Exception", error=exc)
        elif not self._ended:
            self.pass_(message="PASS")
        return False


# =============================================================================
# D365 HELPERS
# =============================================================================

def _get_access_token(d365: Dict[str, str], timeout_sec: int = 30) -> str:
    resp = requests.post(
        d365["login_url"].format(tenant_id=d365["tenant_id"]),
        data={
            "grant_type": "client_credentials",
            "client_id": d365["client_id"],
            "client_secret": d365["client_secret"],
            "scope": f"{d365['resource']}/.default",
        },
        timeout=timeout_sec,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def _d365_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _post_with_retry(url: str, headers: Dict[str, str], payload: Dict[str, Any],
                     *, attempts: int = API_ATTEMPTS, base_delay: float = API_BASE_DELAY_SEC,
                     timeout: int = API_TIMEOUT_SEC) -> Tuple[int, str]:
    last_status = 0
    last_text = ""
    for i in range(attempts):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            last_status = resp.status_code
            last_text = resp.text
            if resp.status_code in (200, 201):
                return resp.status_code, resp.text
            if resp.status_code in (408, 429, 500, 502, 503, 504):
                time.sleep(base_delay * (2 ** i))
                continue
            return resp.status_code, resp.text
        except requests.RequestException as e:
            last_status = 0
            last_text = str(e)
            time.sleep(base_delay * (2 ** i))
    return last_status, last_text


# =============================================================================
# STAGING (PYTHON-CONTROLLED, NO SP DEPENDENCY)
# =============================================================================

def _qname(name: str) -> Tuple[str, str]:
    sch, obj = name.split(".", 1)
    return sch, obj

def _scalar(cur: pyodbc.Cursor, sql: str, params: Tuple = ()) -> Any:
    row = cur.execute(sql, params).fetchone()
    return row[0] if row else None

def _table_required(conn_meta: pyodbc.Connection, qn: str) -> None:
    sch, tbl = _qname(qn)
    if not _object_exists(conn_meta, sch, tbl):
        raise RuntimeError(f"Missing required table: {qn}")

def _insert_staging_adaptive(cur: pyodbc.Cursor, rows: List[Dict[str, Any]], schema: str, table: str) -> int:
    if not rows:
        return 0
    cols_existing = set(_get_table_columns(cur.connection, schema, table))
    # Keep only columns that exist
    cols = [c for c in rows[0].keys() if c in cols_existing]
    if not cols:
        raise RuntimeError(f"No compatible columns found to insert into {schema}.{table}.")
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join([f"[{c}]" for c in cols])
    sql = f"INSERT INTO {schema}.{table} ({col_list}) VALUES ({placeholders});"
    params = [tuple(r.get(c) for c in cols) for r in rows]
    cur.fast_executemany = True
    cur.executemany(sql, params)
    return len(params)

def stage_sales_orders(conn_data: pyodbc.Connection, audit: DualAuditLogger, run_id: int) -> Dict[str, Any]:
    """
    Recreates the essence of the old INT.usp_Stage_SalesOrders:
      - Default Integration_Status to Pending
      - Remove invalid qty (<=0) and optionally log to EXC.Processed_Lines if exists
      - Stage order lines into INT.SalesOrderStaging (grouped by docket+item; idempotency guard by Status/RunId can be applied)
    """
    cur = conn_data.cursor()
    sch_stage, tbl_stage = _qname(STAGING_TABLE)
    sch_h, tbl_h = _qname(CTL_HEADER)
    sch_l, tbl_l = _qname(CTL_LINES)

    # 1) Default pending
    with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="Stage", step="Default Integration_Status NULL -> Pending", step_code="UPD_DEFAULT_PENDING") as s:
        cur.execute(f"UPDATE {CTL_LINES} SET Integration_Status='Pending' WHERE Integration_Status IS NULL;")
        r1 = cur.rowcount
        cur.execute(f"UPDATE {CTL_HEADER} SET Integration_Status='Pending' WHERE Integration_Status IS NULL;")
        r2 = cur.rowcount
        conn_data.commit()
        s.pass_(message="Defaulted statuses", rows_affected=(r1 + r2), details={"lines_defaulted": r1, "headers_defaulted": r2})

    # 2) Invalid quantities
    with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="Stage", step="Mark invalid quantities as Removed", step_code="UPD_REMOVE_INVALID_QTY") as s:
        # Count first (audit intelligence)
        invalid_cnt = _scalar(cur, f"SELECT COUNT(*) FROM {CTL_LINES} WHERE Units_Sold <= 0 AND ISNULL(Integration_Status,'') <> 'Removed';") or 0
        # Optional: log to EXC.Processed_Lines if it exists
        try:
            sch_pl, tbl_pl = _qname(EXC_PROCESSED_LINES)
            if _object_exists(conn_data, sch_pl, tbl_pl):
                cur.execute(f"""
                    INSERT INTO {EXC_PROCESSED_LINES}
                    (EPOS_Fusion_Key, EPOS_Line_ID, CustomersOrderReference, ItemNumber, Original_Quantity, Processed_Action, Reason)
                    SELECT
                        l.EPOS_Fusion_Key,
                        l.EPOS_Line_ID,
                        h.Docket_Reference,
                        l.Dynamics_Code,
                        l.Units_Sold,
                        'Removed',
                        CASE WHEN l.Units_Sold = 0 THEN 'Quantity = 0' ELSE 'Negative quantity' END
                    FROM {CTL_LINES} l
                    JOIN {CTL_HEADER} h ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
                    WHERE l.Units_Sold <= 0
                      AND ISNULL(l.Integration_Status,'') <> 'Removed';
                """)
        except Exception:
            # Do not fail staging due to optional log insert
            pass

        cur.execute(f"UPDATE {CTL_LINES} SET Integration_Status='Removed' WHERE Units_Sold <= 0;")
        upd = cur.rowcount
        conn_data.commit()
        s.pass_(message="Invalid qty handled", rows_affected=upd, details={"invalid_count": int(invalid_cnt), "updated_removed": int(upd)})

    # 3) Build stage rows (aggregate duplicates per order+item)
    #    We keep it simple and robust: group by Docket_Reference + Dynamics_Code, sum qty and amount.
    with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="Stage", step="Insert staged rows (aggregated by order+item)", step_code="INS_STAGE_AGGREGATED", source_reference=STAGING_TABLE) as s:
        # Pull aggregated source
        cur.execute(f"""
            SELECT
                h.Docket_Reference       AS HeaderGroupId,
                h.Docket_Reference       AS CustomersOrderReference,
                h.Fusion_Code            AS CustomerRequisitionNumber,
                h.Account                AS OrderingCustomerAccountNumber,
                l.Dynamics_Code          AS ItemNumber,
                SUM(CAST(l.Units_Sold AS FLOAT)) AS OrderedSalesQuantity,
                SUM(CAST(l.Sales_Value_Inc_VAT AS FLOAT)) AS LineAmount
            FROM {CTL_LINES} l
            JOIN {CTL_HEADER} h
              ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
            WHERE l.Units_Sold > 0
              AND l.Integration_Status = 'Pending'
              AND ISNULL(l.Dynamics_Code,'') <> ''
              AND ISNULL(h.Docket_Reference,'') <> ''
            GROUP BY
                h.Docket_Reference, h.Fusion_Code, h.Account, l.Dynamics_Code;
        """)
        rows = cur.fetchall()

        # Build adaptive insert rows based on existing staging columns
        staging_rows: List[Dict[str, Any]] = []
        today = datetime.date.today().isoformat()

        for i, (hg, cust_ref, cust_req, acct, item, qty, amount) in enumerate(rows, start=1):
            staging_rows.append({
                "HeaderGroupId": hg,
                "Status": "Pending",
                "RunId": None,
                "dataAreaId": "jbro",
                "CustomersOrderReference": cust_ref,
                "CustomerRequisitionNumber": cust_req,
                "OrderingCustomerAccountNumber": acct,
                "RequestedShippingDate": today,
                "ItemNumber": item,
                "OrderedSalesQuantity": float(qty) if qty is not None else 0.0,
                "SalesUnitSymbol": "EA",
                "LineNumber": i,  # may be overridden by LineNumber per order if needed
                "LineAmount": float(amount) if amount is not None else 0.0,
                "LineRequestedShippingDate": today,
                "SalesOrderNumber": None,
                "LineCreated": 0,
            })

        inserted = _insert_staging_adaptive(cur, staging_rows, sch_stage, tbl_stage)
        conn_data.commit()
        s.pass_(message="Staging inserted", rows_read=len(rows), rows_affected=inserted, details={"aggregated_rows": len(rows), "inserted": inserted})

    # 4) Mark CTL statuses staged
    with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="Stage", step="Mark CTL lines/headers as Staged", step_code="UPD_MARK_STAGED") as s:
        cur.execute(f"UPDATE {CTL_LINES} SET Integration_Status='Staged' WHERE Integration_Status='Pending';")
        l_upd = cur.rowcount
        cur.execute(f"""
            UPDATE h
            SET Integration_Status='Staged'
            FROM {CTL_HEADER} h
            WHERE EXISTS (
                SELECT 1 FROM {CTL_LINES} l
                WHERE l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
                  AND l.Integration_Status='Staged'
            );
        """)
        h_upd = cur.rowcount
        conn_data.commit()
        s.pass_(message="CTL statuses updated", rows_affected=(l_upd + h_upd), details={"lines_staged": l_upd, "headers_staged": h_upd})

    return {"staged_groups_estimate": None}


# =============================================================================
# D365 TRANSMISSION
# =============================================================================

def transmit_to_d365(conn_data: pyodbc.Connection, audit: DualAuditLogger, exc: Optional[Any],
                     run_id: int, correlation_id: uuid.UUID) -> None:
    cur = conn_data.cursor()
    sch_stage, tbl_stage = _qname(STAGING_TABLE)

    # Token
    with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="D365", step="Get D365 access token", step_code="AUTH_GET_TOKEN") as s:
        d365 = _load_d365_config()
        token = _get_access_token(d365)
        headers = _d365_headers(token)
        s.pass_(message="Token acquired", details={"odata_root": d365["odata_root"]})

    # Determine which columns exist in staging table
    cols = set(_get_table_columns(conn_data, sch_stage, tbl_stage))
    required_cols = {"HeaderGroupId", "Status"}
    missing = [c for c in required_cols if c not in cols]
    if missing:
        raise RuntimeError(f"Staging table {STAGING_TABLE} missing required columns: {missing}")

    # Fetch distinct pending groups
    with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="D365", step="Fetch pending HeaderGroupIds", step_code="DB_SEL_PENDING_GROUPS", source_reference=STAGING_TABLE) as s:
        cur.execute(f"SELECT DISTINCT HeaderGroupId FROM {STAGING_TABLE} WHERE Status='Pending';")
        groups = [r[0] for r in cur.fetchall()]
        s.pass_(message="Fetched pending groups", rows_read=len(groups), details={"pending_groups": len(groups)})

    # Per group
    for header_group_id in groups:
        txn_id = None
        if exc:
            txn_id = exc.txn_start(
                run_id,
                txn_type="ORDER",
                txn_level="GROUP",
                txn_name="Consignment order lifecycle",
                source_reference=str(header_group_id),
                idempotency_key=str(header_group_id),
                attributes={"module": "Process_Consignment_Orders", "phase": "D365"}
            )

        try:
            # Atomic claim
            with audit.step(run_id, txn_id, process=AUDIT_PROCESS, subprocess="D365", step="Atomic claim HeaderGroupId", step_code="DB_UPD_CLAIM_GROUP",
                            source_reference=str(header_group_id), details={"HeaderGroupId": header_group_id}) as s:
                # RunId column may exist; set if present.
                if "RunId" in cols:
                    cur.execute(f"""
                        UPDATE {STAGING_TABLE}
                        SET Status='InProgress', RunId=?
                        WHERE HeaderGroupId=? AND Status='Pending';
                    """, (str(correlation_id), header_group_id))
                else:
                    cur.execute(f"""
                        UPDATE {STAGING_TABLE}
                        SET Status='InProgress'
                        WHERE HeaderGroupId=? AND Status='Pending';
                    """, header_group_id)
                claimed = cur.rowcount
                if claimed == 0:
                    conn_data.rollback()
                    s.skip_(message="Already claimed/processed")
                    if exc and txn_id:
                        exc.txn_end(txn_id, "SKIPPED", error_message="Already claimed/processed")
                    continue
                conn_data.commit()
                s.pass_(message="Claimed", rows_affected=claimed)

            # Load header row (top 1)
            header_select_cols = []
            for c in ["dataAreaId", "CustomersOrderReference", "CustomerRequisitionNumber",
                      "OrderingCustomerAccountNumber", "SalesOrderNumber"]:
                if c in cols:
                    header_select_cols.append(c)
            # minimal must-haves
            if "CustomersOrderReference" not in cols or "OrderingCustomerAccountNumber" not in cols:
                raise RuntimeError(f"Staging table missing header fields needed for D365 header creation.")

            sel = ", ".join(header_select_cols) if header_select_cols else "CustomersOrderReference, OrderingCustomerAccountNumber"
            with audit.step(run_id, txn_id, process=AUDIT_PROCESS, subprocess="D365", step="Load header fields from staging", step_code="DB_SEL_HEADER",
                            source_reference=str(header_group_id)) as s:
                cur.execute(f"SELECT TOP 1 {sel} FROM {STAGING_TABLE} WHERE HeaderGroupId=?;", header_group_id)
                row = cur.fetchone()
                if not row:
                    s.fail_(message="No staging rows for group")
                    raise RuntimeError(f"No staging rows for HeaderGroupId={header_group_id}")

                # Map row to dict by selected columns
                hdr = dict(zip(header_select_cols, row))
                data_area = hdr.get("dataAreaId") or "jbro"
                cust_ref = hdr.get("CustomersOrderReference")
                cust_req = hdr.get("CustomerRequisitionNumber")
                cust_account = hdr.get("OrderingCustomerAccountNumber")
                existing_so = hdr.get("SalesOrderNumber")
                s.pass_(message="Header loaded", details={"dataAreaId": data_area, "cust_ref": cust_ref, "existing_so": existing_so})

            # Create header if missing
            so_number = existing_so
            if not so_number:
                with audit.step(run_id, txn_id, process=AUDIT_PROCESS, subprocess="D365", step="Create SalesOrderHeader in D365", step_code="API_POST_HEADER",
                                source_reference=f"{d365['odata_root']}/{D365_HEADERS_ENTITY}", reference_number=str(header_group_id)) as s:
                    payload = {
                        "dataAreaId": data_area,
                        "CustomersOrderReference": cust_ref,
                        "CustomerRequisitionNumber": cust_req,
                        "OrderingCustomerAccountNumber": cust_account,
                    }
                    status, text = _post_with_retry(f"{d365['odata_root']}/{D365_HEADERS_ENTITY}", headers, payload)
                    if status not in (200, 201):
                        cur.execute(f"UPDATE {STAGING_TABLE} SET Status='HeaderFailed' WHERE HeaderGroupId=?;", header_group_id)
                        conn_data.commit()
                        s.fail_(message="Header failed", details={"http_status": status, "response": text, "payload": payload})
                        if exc and txn_id:
                            exc.txn_end(txn_id, "FAILED", error_message="HeaderFailed")
                        continue

                    try:
                        so_number = json.loads(text).get("SalesOrderNumber")
                    except Exception:
                        so_number = None

                    if not so_number:
                        cur.execute(f"UPDATE {STAGING_TABLE} SET Status='HeaderFailed' WHERE HeaderGroupId=?;", header_group_id)
                        conn_data.commit()
                        s.fail_(message="SalesOrderNumber missing", details={"http_status": status, "response": text})
                        if exc and txn_id:
                            exc.txn_end(txn_id, "FAILED", error_message="HeaderFailed")
                        continue

                    if "SalesOrderNumber" in cols:
                        cur.execute(f"UPDATE {STAGING_TABLE} SET SalesOrderNumber=? WHERE HeaderGroupId=?;", so_number, header_group_id)
                        conn_data.commit()

                    s.pass_(message="Header created", reference_number=str(so_number), details={"SalesOrderNumber": so_number})
            else:
                with audit.step(run_id, txn_id, process=AUDIT_PROCESS, subprocess="D365", step="Reuse existing SalesOrderNumber", step_code="CHK_REUSE_SO",
                                source_reference=str(header_group_id), reference_number=str(so_number)) as s:
                    s.pass_(message="Reusing existing SO")

            # Fetch lines not created
            line_cols = ["SalesOrderStaging_ID", "ItemNumber", "OrderedSalesQuantity", "SalesUnitSymbol", "LineNumber", "LineCreated"]
            # tolerate missing SalesOrderStaging_ID by using LineNumber+ItemNumber as identifier
            available_line_cols = [c for c in line_cols if c in cols]
            if "ItemNumber" not in cols or "OrderedSalesQuantity" not in cols or "LineNumber" not in cols:
                raise RuntimeError("Staging table missing line fields needed for D365 line creation (ItemNumber/OrderedSalesQuantity/LineNumber).")

            with audit.step(run_id, txn_id, process=AUDIT_PROCESS, subprocess="D365", step="Fetch lines to create", step_code="DB_SEL_LINES_TODO",
                            source_reference=str(header_group_id), reference_number=str(so_number)) as s:
                if "LineCreated" in cols:
                    cur.execute(f"""
                        SELECT {", ".join(available_line_cols)}
                        FROM {STAGING_TABLE}
                        WHERE HeaderGroupId=? AND ISNULL(LineCreated,0)=0
                        ORDER BY LineNumber;
                    """, header_group_id)
                else:
                    cur.execute(f"""
                        SELECT {", ".join([c for c in available_line_cols if c != "LineCreated"])}
                        FROM {STAGING_TABLE}
                        WHERE HeaderGroupId=?
                        ORDER BY LineNumber;
                    """, header_group_id)
                lines = cur.fetchall()
                s.pass_(message="Fetched lines", rows_read=len(lines), details={"lines_to_create": len(lines)})

            line_failed = False
            for row in lines:
                # Build dict for convenience
                d = dict(zip(available_line_cols, row))
                staging_id = d.get("SalesOrderStaging_ID")
                item = d.get("ItemNumber")
                qty = d.get("OrderedSalesQuantity")
                uom = d.get("SalesUnitSymbol") or "EA"
                line_no = d.get("LineNumber")

                with audit.step(run_id, txn_id, process=AUDIT_PROCESS, subprocess="D365", step=f"Create D365 line {line_no}", step_code="API_POST_LINE",
                                source_reference=f"{d365['odata_root']}/{D365_LINES_ENTITY}",
                                reference_number=str(so_number),
                                details={"item": item, "qty": float(qty), "uom": uom, "line_no": int(line_no), "staging_id": staging_id}) as s:
                    payload = {
                        "dataAreaId": data_area,
                        "SalesOrderNumber": so_number,
                        "ItemNumber": item,
                        "OrderedSalesQuantity": float(qty),
                        "SalesUnitSymbol": uom,
                        "LineNumber": int(line_no),
                    }
                    status, text = _post_with_retry(f"{d365['odata_root']}/{D365_LINES_ENTITY}", headers, payload)
                    if status not in (200, 201):
                        line_failed = True
                        s.fail_(message="Line failed", details={"http_status": status, "response": text, "payload": payload})
                        break

                    # mark LineCreated if column exists
                    if "LineCreated" in cols:
                        if staging_id is not None and "SalesOrderStaging_ID" in cols:
                            cur.execute(f"UPDATE {STAGING_TABLE} SET LineCreated=1 WHERE SalesOrderStaging_ID=?;", staging_id)
                        else:
                            cur.execute(f"""
                                UPDATE {STAGING_TABLE}
                                SET LineCreated=1
                                WHERE HeaderGroupId=? AND LineNumber=? AND ItemNumber=?;
                            """, header_group_id, line_no, item)
                        conn_data.commit()
                    s.pass_(message="Line created", rows_affected=1)

            # Finalise
            with audit.step(run_id, txn_id, process=AUDIT_PROCESS, subprocess="D365", step="Finalise staging status", step_code="DB_UPD_FINAL_STATUS",
                            source_reference=str(header_group_id), reference_number=str(so_number)) as s:
                final_status = "Complete" if not line_failed else "LineFailed"
                cur.execute(f"UPDATE {STAGING_TABLE} SET Status=? WHERE HeaderGroupId=?;", final_status, header_group_id)
                conn_data.commit()
                s.pass_(message="Finalised", rows_affected=cur.rowcount, details={"final_status": final_status})

            if exc and txn_id:
                exc.txn_end(txn_id, "SUCCESS")

        except Exception as e:
            # Best effort mark group failed
            try:
                cur.execute(f"UPDATE {STAGING_TABLE} SET Status='Failed' WHERE HeaderGroupId=?;", header_group_id)
                conn_data.commit()
            except Exception:
                pass
            if exc and txn_id:
                exc.txn_end(txn_id, "FAILED", error_message=str(e)[:4000])
            continue


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    correlation_id = uuid.uuid4()

    # ---- Secrets / DB connection ----
    pw = _get_secret_from_env_or_ini(
        env_key=BOOTSTRAP_PASSWORD_ENV,
        ini_path=INI_DB_PATH,
        ini_section=INI_DB_SECTION,
        ini_key="password"
    )
    conn_str = _build_conn_str(pw)
    conn_meta, conn_data = connect_meta_data(conn_str)

    # ---- ExcLogger (existing platform lineage) ----
    exc = None
    if ExcLogger is not None:
        exc = ExcLogger(conn_meta)

    # ---- Dual audit logger (Process_Steps + Process_Transaction_Log) ----
    audit = DualAuditLogger(
        conn_meta,
        system_name=SYSTEM_NAME,
        environment=ENVIRONMENT,
        orchestrator=ORCHESTRATOR,
        default_process=AUDIT_PROCESS
    )

    # ---- Preflight ----
    with audit.step(None, None, process=AUDIT_PROCESS, subprocess="Init", step="Preflight: required tables exist", step_code="CHK_PREFLIGHT_TABLES") as s:
        _table_required(conn_meta, STAGING_TABLE)
        _table_required(conn_meta, CTL_HEADER)
        _table_required(conn_meta, CTL_LINES)
        s.pass_(message="Required tables OK", details={"staging": STAGING_TABLE, "ctl_header": CTL_HEADER, "ctl_lines": CTL_LINES})

    # ---- Start run ----
    run_id = None
    if exc:
        run_id = exc.run_start(
            PROCESS_KEY,
            PROCESS_VERSION,
            ORCHESTRATOR,
            ENVIRONMENT,
            TRIGGER_TYPE,
            TRIGGERED_BY,
            correlation_id,
            attributes={"bootstrap_direct": True, "dual_audit": True}
        )
    else:
        # If ExcLogger isn't available, still log in audit tables with correlation-only context (Run_ID NULL is tolerated by our updates)
        run_id = None

    try:
        # Stage
        with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="Stage", step="Stage sales orders into INT.SalesOrderStaging", step_code="PROC_STAGE_ORDERS") as s:
            res = stage_sales_orders(conn_data, audit, run_id or 0)
            s.pass_(message="Staging complete", details=res)

        # Transmit
        with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="D365", step="Transmit staged orders to D365", step_code="API_TRANSMIT_ORDERS") as s:
            transmit_to_d365(conn_data, audit, exc, run_id or 0, correlation_id)
            s.pass_(message="Transmission cycle complete")

        # Summary
        sch_stage, tbl_stage = _qname(STAGING_TABLE)
        cols = set(_get_table_columns(conn_meta, sch_stage, tbl_stage))
        with audit.step(run_id, None, process=AUDIT_PROCESS, subprocess="Summary", step="Status counts", step_code="SUM_STATUS_COUNTS", source_reference=STAGING_TABLE) as s:
            cur = conn_meta.cursor()
            cur.execute(f"SELECT Status, COUNT(*) FROM {STAGING_TABLE} GROUP BY Status;")
            counts = {str(st): int(cnt) for st, cnt in cur.fetchall()}
            s.pass_(message="Summary captured", rows_read=len(counts), details={"status_counts": counts})

        if exc and run_id is not None:
            exc.run_end(run_id, "SUCCESS")

    except Exception as e:
        if exc and run_id is not None:
            exc.run_end(run_id, "FAILED", str(e)[:4000])
        raise

    finally:
        try:
            conn_data.close()
        except Exception:
            pass
        try:
            conn_meta.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
