"""
Synovia Fusion - Enterprise Audit / Transaction Logging

This is the reusable "Process / SubProcess / Step" audit layer.

It is intentionally generic:
- Any pipeline script can use it
- It links to Execution Framework lineage via Run_ID and Txn_ID
- It logs every check/action with intelligent classification
- It is designed to be non-blocking: if logging fails, pipeline should still run

Table target (recommended):
  EXC.Process_Transaction_Log

Compatibility aliases supported:
  EXC.Process_Steps
  EXC.EPOS_Process_Steps
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, Iterable, Tuple
import pyodbc


# --- Enumerations (keep values stable for dashboards/alerts) ------------------

STATUS_STARTED = "STARTED"
STATUS_PASS    = "PASS"
STATUS_FAIL    = "FAIL"
STATUS_SKIPPED = "SKIPPED"
STATUS_WARN    = "WARN"

SEV_INFO     = "INFO"
SEV_WARN     = "WARN"
SEV_ERROR    = "ERROR"
SEV_CRITICAL = "CRITICAL"

# Categories are "what kind of thing was this step?"
CAT_VALIDATION = "VALIDATION"
CAT_CHECK      = "CHECK"
CAT_ACTION     = "ACTION"
CAT_DB         = "DB"
CAT_FILESYSTEM = "FILESYSTEM"
CAT_CONFIG     = "CONFIG"
CAT_SECURITY   = "SECURITY"
CAT_TRANSFORM  = "TRANSFORM"
CAT_IO         = "IO"


def _safe_json(details: Optional[Dict[str, Any]]) -> Optional[str]:
    if not details:
        return None
    try:
        return json.dumps(details, default=str, ensure_ascii=False)
    except Exception:
        return json.dumps({"detail": str(details)})


def _object_exists(conn: pyodbc.Connection, schema: str, name: str) -> bool:
    cur = conn.cursor()
    row = cur.execute("""
        SELECT TOP (1) 1
        FROM (
            SELECT 1 AS x
            FROM sys.objects o
            INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ? AND o.type IN ('U','V')
            UNION ALL
            SELECT 1
            FROM sys.synonyms sn
            INNER JOIN sys.schemas s ON s.schema_id = sn.schema_id
            WHERE s.name = ? AND sn.name = ?
        ) q;
    """, schema, name, schema, name).fetchone()
    return row is not None


def infer_category(step_code: str) -> str:
    """
    Infer category from stable step_code prefixes.

    This prevents every developer inventing a new category taxonomy.
    """
    c = (step_code or "").upper().strip()

    if c.startswith(("CHK_", "VAL_")):
        return CAT_VALIDATION
    if c.startswith(("DB_", "INS_", "UPD_", "DEL_")):
        return CAT_DB
    if c.startswith(("FS_", "FILE_", "MOVE_", "STAGE_")):
        return CAT_FILESYSTEM
    if c.startswith(("CFG_", "CONF_")):
        return CAT_CONFIG
    if c.startswith(("SEC_", "AUTH_")):
        return CAT_SECURITY
    if c.startswith(("TRN_", "XFM_", "TRANSFORM_")):
        return CAT_TRANSFORM
    if c.startswith(("IO_", "READ_", "WRITE_")):
        return CAT_IO

    # default
    return CAT_ACTION


def infer_severity(status: str, category: str, *, error: Optional[BaseException] = None) -> str:
    """
    Intelligent severity mapping:
    - FAIL in DB/CONFIG/SECURITY = CRITICAL
    - FAIL elsewhere = ERROR
    - WARN = WARN
    - SKIPPED/PASS/STARTED = INFO
    """
    st = (status or "").upper().strip()
    cat = (category or "").upper().strip()

    if st == STATUS_FAIL:
        if cat in (CAT_DB, CAT_CONFIG, CAT_SECURITY):
            return SEV_CRITICAL
        return SEV_ERROR
    if st == STATUS_WARN:
        return SEV_WARN
    return SEV_INFO


@dataclass
class AuditTarget:
    table_qualified: str


class FusionAuditLogger:
    """
    Enterprise step logger.

    Usage pattern:
      audit = FusionAuditLogger(conn_meta, system_name, environment, orchestrator)
      with audit.step(run_id, txn_id, process=..., subprocess=..., step=..., step_code=...):
          ... do work ...

    Logging is best-effort:
      - If the table is missing or writes fail, audit disables itself.
    """

    def __init__(
        self,
        conn_meta: pyodbc.Connection,
        *,
        system_name: str,
        environment: str,
        orchestrator: str,
        default_process: Optional[str] = None,
    ):
        self.conn_meta = conn_meta
        self.system_name = system_name
        self.environment = environment
        self.orchestrator = orchestrator
        self.default_process = default_process

        self.enabled = False
        self.target: Optional[AuditTarget] = None

        self._detect_target()

    def _detect_target(self) -> None:
        """
        Prefer synonyms if present; otherwise fall back to EXC.Process_Transaction_Log.
        """
        try:
            if _object_exists(self.conn_meta, "EXC", "EPOS_Process_Steps"):
                self.target = AuditTarget("EXC.EPOS_Process_Steps")
                self.enabled = True
                return
            if _object_exists(self.conn_meta, "EXC", "Process_Steps"):
                self.target = AuditTarget("EXC.Process_Steps")
                self.enabled = True
                return
            if _object_exists(self.conn_meta, "EXC", "Process_Transaction_Log"):
                self.target = AuditTarget("EXC.Process_Transaction_Log")
                self.enabled = True
                return
        except Exception:
            pass

        self.enabled = False
        self.target = None

    def disable(self) -> None:
        self.enabled = False
        self.target = None

    def _insert_start(self, *,
                      run_id: int,
                      txn_id: Optional[int],
                      correlation_id: Optional[str],
                      process: str,
                      subprocess: Optional[str],
                      step: str,
                      step_code: str,
                      category: str,
                      source_reference: Optional[str],
                      reference_number: Optional[str],
                      idempotency_key: Optional[str],
                      message: Optional[str],
                      details: Optional[Dict[str, Any]]) -> Optional[int]:
        if not self.enabled or not self.target:
            return None

        try:
            cur = self.conn_meta.cursor()
            cur.execute(f"""
                INSERT INTO {self.target.table_qualified}
                (Run_ID, Txn_ID, Correlation_ID,
                 System_Name, Environment, Orchestrator,
                 Process_Name, SubProcess_Name, Step_Name,
                 Step_Code, Step_Category,
                 Severity_Level, Execution_Status,
                 Source_Reference, Reference_Number, Idempotency_Key,
                 Message, Details_JSON)
                OUTPUT INSERTED.Log_ID
                VALUES (?,?,?,?,?,?,
                        ?,?,?, ?,?,
                        ?,?,
                        ?,?,?,
                        ?,?);
            """,
            run_id, txn_id, correlation_id,
            self.system_name, self.environment, self.orchestrator,
            process, subprocess, step,
            step_code, category,
            SEV_INFO, STATUS_STARTED,
            source_reference, reference_number, idempotency_key,
            message, _safe_json(details))

            return int(cur.fetchone()[0])
        except Exception:
            self.disable()
            return None

    def _update_end(self, log_id: int, *,
                    status: str,
                    category: str,
                    severity: str,
                    rows_read: Optional[int],
                    rows_affected: Optional[int],
                    bytes_processed: Optional[int],
                    message: Optional[str],
                    error_message: Optional[str],
                    details: Optional[Dict[str, Any]],
                    reference_number: Optional[str]) -> None:
        if not self.enabled or not self.target:
            return

        try:
            cur = self.conn_meta.cursor()
            cur.execute(f"""
                UPDATE {self.target.table_qualified}
                SET Execution_Status=?,
                    Severity_Level=?,
                    Rows_Read=COALESCE(?, Rows_Read),
                    Rows_Affected=COALESCE(?, Rows_Affected),
                    Bytes_Processed=COALESCE(?, Bytes_Processed),
                    Message=COALESCE(?, Message),
                    Error_Message=COALESCE(?, Error_Message),
                    Details_JSON=COALESCE(?, Details_JSON),
                    Reference_Number=COALESCE(?, Reference_Number),
                    End_UTC=SYSUTCDATETIME()
                WHERE Log_ID=?;
            """,
            status, severity,
            rows_read, rows_affected, bytes_processed,
            message,
            error_message[:4000] if error_message else None,
            _safe_json(details),
            reference_number,
            log_id)
        except Exception:
            self.disable()

    # ---------------------------- Public API ---------------------------------

    def step(self,
             run_id: int,
             txn_id: Optional[int],
             *,
             process: Optional[str] = None,
             subprocess: Optional[str] = None,
             step: str,
             step_code: str,
             category: Optional[str] = None,
             correlation_id: Optional[str] = None,
             source_reference: Optional[str] = None,
             reference_number: Optional[str] = None,
             idempotency_key: Optional[str] = None,
             message: Optional[str] = None,
             details: Optional[Dict[str, Any]] = None) -> "StepContext":
        p = process or self.default_process or "UNSPECIFIED_PROCESS"
        cat = category or infer_category(step_code)
        return StepContext(
            audit=self,
            run_id=run_id,
            txn_id=txn_id,
            correlation_id=correlation_id,
            process=p,
            subprocess=subprocess,
            step=step,
            step_code=step_code,
            category=cat,
            source_reference=source_reference,
            reference_number=reference_number,
            idempotency_key=idempotency_key,
            start_message=message,
            start_details=details
        )

    def check(self,
              run_id: int,
              txn_id: Optional[int],
              *,
              process: Optional[str],
              subprocess: Optional[str],
              step: str,
              step_code: str,
              condition: bool,
              source_reference: Optional[str] = None,
              reference_number: Optional[str] = None,
              idempotency_key: Optional[str] = None,
              pass_message: Optional[str] = None,
              fail_message: Optional[str] = None,
              details: Optional[Dict[str, Any]] = None,
              warn_only: bool = False) -> bool:
        """
        One-liner check logger. Does not raise.
        """
        with self.step(
            run_id, txn_id,
            process=process,
            subprocess=subprocess,
            step=step,
            step_code=step_code,
            category=CAT_VALIDATION,
            source_reference=source_reference,
            reference_number=reference_number,
            idempotency_key=idempotency_key,
            details=details
        ) as s:
            if condition:
                s.pass_(message=pass_message or "PASS")
                return True
            if warn_only:
                s.warn_(message=fail_message or "WARN")
                return False
            s.fail_(message=fail_message or "FAIL")
            return False


class StepContext:
    """
    Context manager for a single audit step.

    Behavior:
    - INSERT STARTED at __enter__
    - If exception bubbles out, mark FAIL and re-raise
    - Caller can explicitly pass_/warn_/skip_/fail_ for deterministic status
    """

    def __init__(self,
                 *,
                 audit: FusionAuditLogger,
                 run_id: int,
                 txn_id: Optional[int],
                 correlation_id: Optional[str],
                 process: str,
                 subprocess: Optional[str],
                 step: str,
                 step_code: str,
                 category: str,
                 source_reference: Optional[str],
                 reference_number: Optional[str],
                 idempotency_key: Optional[str],
                 start_message: Optional[str],
                 start_details: Optional[Dict[str, Any]]):

        self.audit = audit
        self.run_id = run_id
        self.txn_id = txn_id
        self.correlation_id = correlation_id
        self.process = process
        self.subprocess = subprocess
        self.step = step
        self.step_code = step_code
        self.category = category
        self.source_reference = source_reference
        self.reference_number = reference_number
        self.idempotency_key = idempotency_key

        self.log_id: Optional[int] = None
        self._closed = False

        self._start_message = start_message
        self._start_details = start_details

    def __enter__(self) -> "StepContext":
        self.log_id = self.audit._insert_start(
            run_id=self.run_id,
            txn_id=self.txn_id,
            correlation_id=self.correlation_id,
            process=self.process,
            subprocess=self.subprocess,
            step=self.step,
            step_code=self.step_code,
            category=self.category,
            source_reference=self.source_reference,
            reference_number=self.reference_number,
            idempotency_key=self.idempotency_key,
            message=self._start_message,
            details=self._start_details
        )
        return self

    def _end(self,
             *,
             status: str,
             message: Optional[str] = None,
             error: Optional[BaseException] = None,
             rows_read: Optional[int] = None,
             rows_affected: Optional[int] = None,
             bytes_processed: Optional[int] = None,
             details: Optional[Dict[str, Any]] = None,
             reference_number: Optional[str] = None) -> None:
        if self._closed:
            return
        self._closed = True

        if self.log_id is None:
            return

        sev = infer_severity(status, self.category, error=error)
        self.audit._update_end(
            self.log_id,
            status=status,
            category=self.category,
            severity=sev,
            rows_read=rows_read,
            rows_affected=rows_affected,
            bytes_processed=bytes_processed,
            message=message,
            error_message=str(error) if error else None,
            details=details,
            reference_number=reference_number or self.reference_number
        )

    def pass_(self, *, message: Optional[str] = None,
              rows_read: Optional[int] = None,
              rows_affected: Optional[int] = None,
              bytes_processed: Optional[int] = None,
              details: Optional[Dict[str, Any]] = None,
              reference_number: Optional[str] = None) -> None:
        self._end(status=STATUS_PASS, message=message, rows_read=rows_read,
                  rows_affected=rows_affected, bytes_processed=bytes_processed,
                  details=details, reference_number=reference_number)

    def warn_(self, *, message: Optional[str] = None,
              details: Optional[Dict[str, Any]] = None) -> None:
        self._end(status=STATUS_WARN, message=message, details=details)

    def skip_(self, *, message: Optional[str] = None,
              details: Optional[Dict[str, Any]] = None) -> None:
        self._end(status=STATUS_SKIPPED, message=message, details=details)

    def fail_(self, *, message: Optional[str] = None,
              error: Optional[BaseException] = None,
              details: Optional[Dict[str, Any]] = None) -> None:
        self._end(status=STATUS_FAIL, message=message, error=error, details=details)

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None:
            self._end(status=STATUS_FAIL, message="Exception", error=exc)
            return False  # re-raise
        if not self._closed:
            # If caller forgot to close, treat as PASS (but don't hide that)
            self._end(status=STATUS_PASS, message="PASS (implicit)")
        return False
