# =============================================================================
#  Synovia Fusion – EPOS PreFlight & PostLoad Validation Engine (Enterprise)
# =============================================================================
#
#  Version:        2.1.0
#  Release Date:   2026-02-12
#
#  Purpose:
#    Formal validation gate BEFORE further processing (CTL/INT/etc).
#    - Technical validations (tables, columns, prerequisites)
#    - Business validations (store/product mapping & uniqueness)
#    - PostLoad validations (file/footer/line reconciliation)
#
#  Logging (enterprise):
#    1) EXC.Process_Transaction_Log  (step-by-step audit telemetry)
#       - Process/SubProcess/Step classification
#       - Stable Step_Code taxonomy
#       - Severity inference (INFO/WARN/ERROR/CRITICAL)
#       - PASS/WARN/FAIL
#    2) EXC.PreFlight_PostLoad       (formal validation report rows)
#
#  Stop Rule:
#    - Any validation with severity in {ERROR, CRITICAL} and status FAIL
#      halts the pipeline (controlled exit + final OVERALL_STATUS row).
#
#  Notes:
#    - Uses direct bootstrap DB connection (no CFG.v_Profile_Database dependency).
#    - If ExcLogger is available, it will create a Run_ID and use it in logs.
#      Otherwise, Run_ID is NULL and Correlation_ID is used as the trace key.
# =============================================================================

from __future__ import annotations

import os
import sys
import uuid
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pyodbc


try:
    from exc_logger import ExcLogger  # optional — provides Run_ID lineage
except ImportError:
    ExcLogger = None

# -----------------------------------------------------------------------------
# Shared enterprise classes (if deployed)
# -----------------------------------------------------------------------------

from synovia_fusion_config import IniEnvContext
from synovia_fusion_db import build_sqlserver_conn_str, connect_meta_data, object_exists
from synovia_fusion_audit import FusionAuditLogger

# --- Console helpers ---------------------------------------------------------
# Console mode controls verbosity:
#   FULL      -> print PASS/WARN/FAIL for each step
#   FAIL_ONLY -> print only WARN/FAIL (default in schedulers)
CONSOLE_MODE = os.environ.get("FUSION_EPOS_PREFLIGHT_CONSOLE_MODE", "").strip().upper()
if not CONSOLE_MODE:
    try:
        CONSOLE_MODE = "FULL" if sys.stdout.isatty() else "FAIL_ONLY"
    except Exception:
        CONSOLE_MODE = "FAIL_ONLY"

def _p(msg: str) -> None:
    print(msg, flush=True)

def _section(title: str) -> None:
    _p("\n" + "=" * 78)
    _p(title)
    _p("=" * 78)

def _console_event(status: str, step_code: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
    st = (status or "").upper().strip()
    if CONSOLE_MODE == "FULL" or st in ("FAIL", "WARN"):
        line = f"[{st:<4}] {step_code} - {message}"
        _p(line)
        if details and (CONSOLE_MODE == "FULL" or st == "FAIL"):
            # Print compact JSON for failures (or full mode)
            try:
                _p("       " + json.dumps(details, default=str, ensure_ascii=False))
            except Exception:
                _p("       " + str(details))


# =============================================================================
# RUNTIME IDENTIFIERS
# =============================================================================

SYSTEM_NAME   = "Fusion_EPOS"
ENVIRONMENT   = "PROD"
ORCHESTRATOR  = "PYTHON"

# Telemetry classification
AUDIT_PROCESS = "PreProcessing_Validation"

# Execution Framework identifiers (optional)
PROCESS_KEY     = "FUSION_EPOS_PREFLIGHT_POSTLOAD"
PROCESS_VERSION = "2.1.0"
TRIGGER_TYPE    = "SCHEDULE"
TRIGGERED_BY    = "SQLAGENT"

# =============================================================================
# CONFIG
# =============================================================================

BOOTSTRAP = {
    "driver":   "ODBC Driver 17 for SQL Server",
    "server":   "futureworks-sdi-db.database.windows.net",
    "database": "Fusion_EPOS_Production",
    "username": "SynFW_DB",
    "timeout":  60,
}

BOOTSTRAP_PASSWORD_ENV = "FUSION_EPOS_BOOTSTRAP_PASSWORD"
INI_PATH    = r"D:\Configuration\Fusion_EPOS_Production.ini"
INI_SECTION = "Fusion_EPOS_Production"

# PostLoad reconciliation tolerance (e.g., rounding)
TOLERANCE_CURRENCY = float(os.environ.get("FUSION_EPOS_FOOTER_TOLERANCE", "0.01"))

# Whether to validate only latest completed files, or all completed
# Options: ALL | LATEST
FILE_SCOPE = os.environ.get("FUSION_EPOS_PREFLIGHT_FILE_SCOPE", "ALL").strip().upper() or "ALL"

# =============================================================================
# EXC.PreFlight_PostLoad target
# =============================================================================

PREFLIGHT_TABLE = ("EXC", "PreFlight_PostLoad")


# =============================================================================
# Models
# =============================================================================

@dataclass
class ValidationResult:
    phase: str                # TECHNICAL | BUSINESS | POSTLOAD
    code: str                 # stable code
    name: str
    severity: str             # INFO | WARN | ERROR | CRITICAL
    status: str               # PASS | FAIL
    file_id: Optional[int] = None
    rows_impacted: Optional[int] = None
    expected_value: Optional[float] = None
    actual_value: Optional[float] = None
    details: Optional[Dict[str, Any]] = None

    def is_blocking_fail(self) -> bool:
        return self.status == "FAIL" and self.severity in ("ERROR", "CRITICAL")


# =============================================================================
# DB helpers
# =============================================================================

def _scalar(cur: pyodbc.Cursor, sql: str, params: Tuple = ()) -> Any:
    row = cur.execute(sql, params).fetchone()
    return row[0] if row else None


def _table_columns(cur: pyodbc.Cursor, schema: str, table: str) -> List[str]:
    rows = cur.execute("""
        SELECT c.name
        FROM sys.columns c
        JOIN sys.objects o ON o.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = ? AND o.name = ? AND o.type = 'U'
        ORDER BY c.column_id;
    """, schema, table).fetchall()
    return [r[0] for r in rows]


def _round2(v: Optional[float]) -> float:
    return round(float(v or 0.0), 2)


# =============================================================================
# Logging helpers
# =============================================================================

def _log_formal(cur_meta: pyodbc.Cursor, run_id: Optional[int], correlation_id: str, vr: ValidationResult) -> None:
    """
    Insert into EXC.PreFlight_PostLoad (formal report).

    Best-effort:
      - If the table is missing or the insert fails, continue without raising.
      - Tries inserting Run_ID as the numeric run id (if >0) first, then falls back to correlation_id
        (useful if Run_ID column is UNIQUEIDENTIFIER).
    """
    def _try(run_key: str) -> None:
        cur_meta.execute(f"""
            INSERT INTO {PREFLIGHT_TABLE[0]}.{PREFLIGHT_TABLE[1]}
            (
                Run_ID, File_ID, Phase,
                Validation_Code, Validation_Name,
                Severity, Status,
                Rows_Impacted, Expected_Value, Actual_Value,
                Details_JSON,
                Overall_Run_Status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        run_key,
        vr.file_id,
        vr.phase,
        vr.code,
        vr.name,
        vr.severity,
        vr.status,
        vr.rows_impacted,
        vr.expected_value,
        vr.actual_value,
        json.dumps(vr.details, default=str) if vr.details else None,
        None
        )

    # Prefer numeric run id when available and > 0
    run_key_primary = correlation_id
    try:
        if run_id is not None and int(run_id) > 0:
            run_key_primary = str(int(run_id))
    except Exception:
        run_key_primary = correlation_id

    try:
        _try(run_key_primary)
    except Exception:
        if run_key_primary != correlation_id:
            try:
                _try(correlation_id)
            except Exception:
                pass



def _audit_step(audit: FusionAuditLogger,
                run_id: int,
                txn_id: Optional[int],
                *,
                subprocess: str,
                step: str,
                step_code: str,
                source_reference: Optional[str] = None,
                reference_number: Optional[str] = None,
                details: Optional[Dict[str, Any]] = None):
    return audit.step(
        run_id, txn_id,
        process=AUDIT_PROCESS,
        subprocess=subprocess,
        step=step,
        step_code=step_code,
        source_reference=source_reference,
        reference_number=reference_number,
        details=details
    )


# =============================================================================
# Validations
# =============================================================================

def validate_required_objects(conn_meta: pyodbc.Connection, audit: FusionAuditLogger, run_id: int, corr: str) -> List[ValidationResult]:
    cur = conn_meta.cursor()
    out: List[ValidationResult] = []

    required_tables = [
        ("RAW", "EPOS_File"),
        ("RAW", "EPOS_LineItems"),
        ("RAW", "EPOS_FileFooter"),
        ("CFG", "Dynamics_Stores"),
        ("CFG", "Products"),
        PREFLIGHT_TABLE,
        ("EXC", "Process_Transaction_Log"),
    ]

    with _audit_step(audit, run_id, None,
                     subprocess="Technical",
                     step="Validate required tables exist",
                     step_code="CHK_REQUIRED_TABLES",
                     details={"required": [f"{s}.{t}" for s, t in required_tables]}) as s:
        missing = [f"{sch}.{tbl}" for sch, tbl in required_tables if not object_exists(conn_meta, sch, tbl)]
        if missing:
            vr = ValidationResult(
                phase="TECHNICAL",
                code="REQ_TABLES",
                name="Required tables exist",
                severity="CRITICAL",
                status="FAIL",
                details={"missing": missing}
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.fail_(message="Missing required tables", details={"missing": missing})
            return out

        vr = ValidationResult(
            phase="TECHNICAL",
            code="REQ_TABLES",
            name="Required tables exist",
            severity="INFO",
            status="PASS"
        )
        _log_formal(cur, run_id, corr, vr)
        out.append(vr)
        s.pass_(message="All required tables present")
        return out


def validate_required_columns(conn_meta: pyodbc.Connection, audit: FusionAuditLogger, run_id: int, corr: str) -> List[ValidationResult]:
    cur = conn_meta.cursor()
    out: List[ValidationResult] = []

    requirements: Dict[Tuple[str, str], List[str]] = {
        ("RAW", "EPOS_File"): ["File_ID", "FileName", "Supplier_Number", "Year", "Week", "Record_Count", "Status"],
        ("RAW", "EPOS_FileFooter"): ["Footer_ID", "File_ID", "NumberOfLines", "Qty_Total", "Retail_Total"],
        ("RAW", "EPOS_LineItems"): ["LineItem_ID", "File_ID", "Year", "Week", "Store", "Docket_Reference",
                                   "Dunnes_Prod_Code", "Units_Sold", "Sales_Value_Inc_VAT", "SynProcessed"],
        ("CFG", "Dynamics_Stores"): ["StoreCode", "Account_status"],
        ("CFG", "Products"): ["Dunnes_Prod_Code", "Dynamics_Code"],
    }

    with _audit_step(audit, run_id, None,
                     subprocess="Technical",
                     step="Validate required columns exist",
                     step_code="CHK_REQUIRED_COLUMNS") as s:
        missing: Dict[str, List[str]] = {}
        for (sch, tbl), cols in requirements.items():
            try:
                existing = set(_table_columns(cur, sch, tbl))
                miss = [c for c in cols if c not in existing]
                if miss:
                    missing[f"{sch}.{tbl}"] = miss
            except Exception:
                missing[f"{sch}.{tbl}"] = cols

        if missing:
            vr = ValidationResult(
                phase="TECHNICAL",
                code="REQ_COLUMNS",
                name="Required columns exist",
                severity="CRITICAL",
                status="FAIL",
                details={"missing": missing}
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.fail_(message="Missing required columns", details={"missing": missing})
            return out

        vr = ValidationResult(
            phase="TECHNICAL",
            code="REQ_COLUMNS",
            name="Required columns exist",
            severity="INFO",
            status="PASS"
        )
        _log_formal(cur, run_id, corr, vr)
        out.append(vr)
        s.pass_(message="All required columns present")
        return out


def validate_mapping_uniqueness(conn_meta: pyodbc.Connection, audit: FusionAuditLogger, run_id: int, corr: str) -> List[ValidationResult]:
    """
    Enforces:
      - 1 active store per StoreCode
      - 1 active product per Dunnes_Prod_Code
    Since you didn't provide explicit active flags, we infer "active" as Account_status='Active'
    and Products where Dynamics_Code is not null (or you can adjust).
    """
    cur = conn_meta.cursor()
    out: List[ValidationResult] = []

    # Store uniqueness: StoreCode should not duplicate for active stores
    with _audit_step(audit, run_id, None,
                     subprocess="Business",
                     step="Validate one active store per StoreCode",
                     step_code="CHK_STORE_UNIQUE_ACTIVE",
                     source_reference="CFG.Dynamics_Stores") as s:
        dup = _scalar(cur, """
            SELECT COUNT(*)
            FROM (
                SELECT StoreCode
                FROM CFG.Dynamics_Stores
                WHERE ISNULL(Account_status,'') = 'Active'
                GROUP BY StoreCode
                HAVING COUNT(*) > 1
            ) d;
        """) or 0

        if dup > 0:
            vr = ValidationResult(
                phase="BUSINESS",
                code="STORE_UNIQUE_ACTIVE",
                name="One active store per StoreCode",
                severity="ERROR",
                status="FAIL",
                rows_impacted=int(dup),
                details={"duplicate_active_storecodes": int(dup)}
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.fail_(message="Duplicate active stores found", details={"duplicate_storecode_groups": int(dup)})
        else:
            vr = ValidationResult(
                phase="BUSINESS",
                code="STORE_UNIQUE_ACTIVE",
                name="One active store per StoreCode",
                severity="INFO",
                status="PASS",
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.pass_(message="Store mapping uniqueness OK")

    # Product uniqueness: One row per Dunnes_Prod_Code with a Dynamics_Code
    with _audit_step(audit, run_id, None,
                     subprocess="Business",
                     step="Validate one active product per Dunnes_Prod_Code",
                     step_code="CHK_PRODUCT_UNIQUE_ACTIVE",
                     source_reference="CFG.Products") as s:
        dup = _scalar(cur, """
            SELECT COUNT(*)
            FROM (
                SELECT Dunnes_Prod_Code
                FROM CFG.Products
                WHERE Dynamics_Code IS NOT NULL AND LTRIM(RTRIM(Dynamics_Code)) <> ''
                GROUP BY Dunnes_Prod_Code
                HAVING COUNT(*) > 1
            ) d;
        """) or 0

        if dup > 0:
            vr = ValidationResult(
                phase="BUSINESS",
                code="PRODUCT_UNIQUE_ACTIVE",
                name="One active product per Dunnes_Prod_Code",
                severity="ERROR",
                status="FAIL",
                rows_impacted=int(dup),
                details={"duplicate_active_product_groups": int(dup)}
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.fail_(message="Duplicate active products found", details={"duplicate_product_groups": int(dup)})
        else:
            vr = ValidationResult(
                phase="BUSINESS",
                code="PRODUCT_UNIQUE_ACTIVE",
                name="One active product per Dunnes_Prod_Code",
                severity="INFO",
                status="PASS",
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.pass_(message="Product mapping uniqueness OK")

    return out


def validate_store_product_coverage(conn_meta: pyodbc.Connection, audit: FusionAuditLogger, run_id: int, corr: str) -> List[ValidationResult]:
    """
    Ensures every RAW line can join to Store and Product mappings.
    """
    cur = conn_meta.cursor()
    out: List[ValidationResult] = []

    # Store coverage
    with _audit_step(audit, run_id, None,
                     subprocess="Business",
                     step="Validate all RAW stores are mapped",
                     step_code="CHK_STORE_MAPPING",
                     source_reference="RAW.EPOS_LineItems") as s:
        missing = _scalar(cur, """
            SELECT COUNT(*)
            FROM RAW.EPOS_LineItems l
            LEFT JOIN CFG.Dynamics_Stores s
              ON LTRIM(RTRIM(s.StoreCode)) = LTRIM(RTRIM(l.Store))
             AND ISNULL(s.Account_status,'') = 'Active'
            WHERE s.StoreCode IS NULL;
        """) or 0

        if missing > 0:
            vr = ValidationResult(
                phase="BUSINESS",
                code="STORE_MAPPING",
                name="All RAW stores mapped to active Dynamics store",
                severity="ERROR",
                status="FAIL",
                rows_impacted=int(missing),
                details={"unmapped_store_line_count": int(missing)}
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.fail_(message="Unmapped stores detected", details={"unmapped_store_line_count": int(missing)})
        else:
            vr = ValidationResult(
                phase="BUSINESS",
                code="STORE_MAPPING",
                name="All RAW stores mapped to active Dynamics store",
                severity="INFO",
                status="PASS",
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.pass_(message="Store mapping coverage OK")

    # Product coverage
    with _audit_step(audit, run_id, None,
                     subprocess="Business",
                     step="Validate all RAW products are mapped",
                     step_code="CHK_PRODUCT_MAPPING",
                     source_reference="RAW.EPOS_LineItems") as s:
        missing = _scalar(cur, """
            SELECT COUNT(*)
            FROM RAW.EPOS_LineItems l
            LEFT JOIN CFG.Products p
              ON p.Dunnes_Prod_Code = l.Dunnes_Prod_Code
             AND p.Dynamics_Code IS NOT NULL AND LTRIM(RTRIM(p.Dynamics_Code)) <> ''
            WHERE p.Dunnes_Prod_Code IS NULL;
        """) or 0

        if missing > 0:
            vr = ValidationResult(
                phase="BUSINESS",
                code="PRODUCT_MAPPING",
                name="All RAW products mapped to Dynamics_Code",
                severity="ERROR",
                status="FAIL",
                rows_impacted=int(missing),
                details={"unmapped_product_line_count": int(missing)}
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.fail_(message="Unmapped products detected", details={"unmapped_product_line_count": int(missing)})
        else:
            vr = ValidationResult(
                phase="BUSINESS",
                code="PRODUCT_MAPPING",
                name="All RAW products mapped to Dynamics_Code",
                severity="INFO",
                status="PASS",
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.pass_(message="Product mapping coverage OK")

    # Join resolution (both)
    with _audit_step(audit, run_id, None,
                     subprocess="Business",
                     step="Validate all RAW lines can resolve store + product mappings",
                     step_code="CHK_JOIN_RESOLUTION",
                     source_reference="RAW.EPOS_LineItems") as s:
        unresolved = _scalar(cur, """
            SELECT COUNT(*)
            FROM RAW.EPOS_LineItems l
            LEFT JOIN CFG.Dynamics_Stores s
              ON LTRIM(RTRIM(s.StoreCode)) = LTRIM(RTRIM(l.Store))
             AND ISNULL(s.Account_status,'') = 'Active'
            LEFT JOIN CFG.Products p
              ON p.Dunnes_Prod_Code = l.Dunnes_Prod_Code
             AND p.Dynamics_Code IS NOT NULL AND LTRIM(RTRIM(p.Dynamics_Code)) <> ''
            WHERE s.StoreCode IS NULL OR p.Dunnes_Prod_Code IS NULL;
        """) or 0

        if unresolved > 0:
            vr = ValidationResult(
                phase="BUSINESS",
                code="JOIN_RESOLUTION",
                name="All RAW lines resolve store + product",
                severity="ERROR",
                status="FAIL",
                rows_impacted=int(unresolved),
                details={"unresolved_line_count": int(unresolved)}
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.fail_(message="Unresolved RAW lines detected", details={"unresolved_line_count": int(unresolved)})
        else:
            vr = ValidationResult(
                phase="BUSINESS",
                code="JOIN_RESOLUTION",
                name="All RAW lines resolve store + product",
                severity="INFO",
                status="PASS",
            )
            _log_formal(cur, run_id, corr, vr)
            out.append(vr)
            s.pass_(message="Join resolution OK")

    return out


def _get_completed_files(cur: pyodbc.Cursor) -> List[int]:
    if FILE_SCOPE == "LATEST":
        rows = cur.execute("""
            SELECT TOP (1) File_ID
            FROM RAW.EPOS_File
            WHERE Status = 'Completed'
            ORDER BY Completed_Timestamp DESC, File_ID DESC;
        """).fetchall()
        return [int(r[0]) for r in rows]
    rows = cur.execute("""
        SELECT File_ID
        FROM RAW.EPOS_File
        WHERE Status = 'Completed';
    """).fetchall()
    return [int(r[0]) for r in rows]


def validate_postload_file_integrity(conn_meta: pyodbc.Connection, audit: FusionAuditLogger, run_id: int, corr: str) -> List[ValidationResult]:
    cur = conn_meta.cursor()
    out: List[ValidationResult] = []

    file_ids = _get_completed_files(cur)

    with _audit_step(audit, run_id, None,
                     subprocess="PostLoad",
                     step="Enumerate completed files for PostLoad validations",
                     step_code="CHK_COMPLETED_FILES",
                     source_reference="RAW.EPOS_File",
                     details={"file_scope": FILE_SCOPE}) as s:
        s.pass_(message="Completed files enumerated", rows_read=len(file_ids), details={"completed_files": len(file_ids)})

    for file_id in file_ids:
        ref = str(file_id)

        # A) Single Year/Week per file
        with _audit_step(audit, run_id, None,
                         subprocess="PostLoad",
                         step=f"Validate single Year/Week in file {file_id}",
                         step_code="CHK_FILE_YEAR_WEEK_SINGLE",
                         source_reference="RAW.EPOS_LineItems",
                         reference_number=ref) as s:
            ycnt = _scalar(cur, "SELECT COUNT(DISTINCT [Year]) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)) or 0
            wcnt = _scalar(cur, "SELECT COUNT(DISTINCT [Week]) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)) or 0
            ok = (int(ycnt) == 1 and int(wcnt) == 1)
            if not ok:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FILE_YEAR_WEEK_SINGLE",
                    name="Each file contains exactly one Year and one Week",
                    severity="CRITICAL",
                    status="FAIL",
                    file_id=file_id,
                    details={"distinct_years": int(ycnt), "distinct_weeks": int(wcnt)}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.fail_(message="Multiple years/weeks detected", details=vr.details)
            else:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FILE_YEAR_WEEK_SINGLE",
                    name="Each file contains exactly one Year and one Week",
                    severity="INFO",
                    status="PASS",
                    file_id=file_id,
                    details={"distinct_years": int(ycnt), "distinct_weeks": int(wcnt)}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.pass_(message="Single Year/Week confirmed", details=vr.details)

        # B) Record_Count = COUNT(lines)
        with _audit_step(audit, run_id, None,
                         subprocess="PostLoad",
                         step=f"Validate RAW.EPOS_File.Record_Count matches lines for file {file_id}",
                         step_code="CHK_FILE_RECORDCOUNT",
                         source_reference="RAW.EPOS_File",
                         reference_number=ref) as s:
            rc = _scalar(cur, "SELECT Record_Count FROM RAW.EPOS_File WHERE File_ID=?;", (file_id,))
            cnt = _scalar(cur, "SELECT COUNT(*) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)) or 0
            ok = (int(rc or 0) == int(cnt))
            if not ok:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FILE_RECORDCOUNT",
                    name="Header Record_Count matches line item count",
                    severity="CRITICAL",
                    status="FAIL",
                    file_id=file_id,
                    rows_impacted=int(cnt),
                    expected_value=float(rc or 0),
                    actual_value=float(cnt),
                    details={"record_count": int(rc or 0), "line_count": int(cnt)}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.fail_(message="Record_Count mismatch", details=vr.details)
            else:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FILE_RECORDCOUNT",
                    name="Header Record_Count matches line item count",
                    severity="INFO",
                    status="PASS",
                    file_id=file_id,
                    rows_impacted=int(cnt),
                    details={"record_count": int(rc or 0), "line_count": int(cnt)}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.pass_(message="Record_Count OK", rows_read=int(cnt), details=vr.details)

        # C) Footer NumberOfLines = line count
        with _audit_step(audit, run_id, None,
                         subprocess="PostLoad",
                         step=f"Validate footer NumberOfLines matches lines for file {file_id}",
                         step_code="CHK_FOOTER_LINECOUNT",
                         source_reference="RAW.EPOS_FileFooter",
                         reference_number=ref) as s:
            f_lines = _scalar(cur, "SELECT NumberOfLines FROM RAW.EPOS_FileFooter WHERE File_ID=?;", (file_id,))
            cnt = _scalar(cur, "SELECT COUNT(*) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)) or 0
            ok = (f_lines is not None and int(f_lines) == int(cnt))
            if not ok:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FOOTER_LINECOUNT",
                    name="Footer NumberOfLines matches line item count",
                    severity="CRITICAL",
                    status="FAIL",
                    file_id=file_id,
                    expected_value=float(f_lines or 0),
                    actual_value=float(cnt),
                    details={"footer_lines": int(f_lines or 0), "line_count": int(cnt)}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.fail_(message="Footer line count mismatch", details=vr.details)
            else:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FOOTER_LINECOUNT",
                    name="Footer NumberOfLines matches line item count",
                    severity="INFO",
                    status="PASS",
                    file_id=file_id,
                    details={"footer_lines": int(f_lines or 0), "line_count": int(cnt)}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.pass_(message="Footer line count OK", details=vr.details)

        # D) Footer Qty_Total = SUM Units_Sold
        with _audit_step(audit, run_id, None,
                         subprocess="PostLoad",
                         step=f"Validate footer Qty_Total matches SUM(Units_Sold) for file {file_id}",
                         step_code="CHK_FOOTER_QTYTOTAL",
                         source_reference="RAW.EPOS_FileFooter",
                         reference_number=ref) as s:
            f_qty = _scalar(cur, "SELECT Qty_Total FROM RAW.EPOS_FileFooter WHERE File_ID=?;", (file_id,))
            sum_qty = _scalar(cur, "SELECT SUM(CAST(Units_Sold AS BIGINT)) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)) or 0
            ok = (f_qty is not None and int(f_qty) == int(sum_qty))
            if not ok:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FOOTER_QTYTOTAL",
                    name="Footer Qty_Total matches SUM(Units_Sold)",
                    severity="CRITICAL",
                    status="FAIL",
                    file_id=file_id,
                    expected_value=float(f_qty or 0),
                    actual_value=float(sum_qty),
                    details={"footer_qty": int(f_qty or 0), "sum_units_sold": int(sum_qty)}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.fail_(message="Footer Qty_Total mismatch", details=vr.details)
            else:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FOOTER_QTYTOTAL",
                    name="Footer Qty_Total matches SUM(Units_Sold)",
                    severity="INFO",
                    status="PASS",
                    file_id=file_id,
                    details={"footer_qty": int(f_qty or 0), "sum_units_sold": int(sum_qty)}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.pass_(message="Footer Qty_Total OK", details=vr.details)

        # E) Footer Retail_Total = SUM Sales_Value_Inc_VAT (± tolerance)
        with _audit_step(audit, run_id, None,
                         subprocess="PostLoad",
                         step=f"Validate footer Retail_Total matches SUM(Sales_Value_Inc_VAT) for file {file_id}",
                         step_code="CHK_FOOTER_RETAILTOTAL",
                         source_reference="RAW.EPOS_FileFooter",
                         reference_number=ref,
                         details={"tolerance": TOLERANCE_CURRENCY}) as s:
            f_sales = _scalar(cur, "SELECT Retail_Total FROM RAW.EPOS_FileFooter WHERE File_ID=?;", (file_id,))
            sum_sales = _scalar(cur, "SELECT SUM(CAST(Sales_Value_Inc_VAT AS FLOAT)) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)) or 0.0
            exp = _round2(float(f_sales or 0.0))
            act = _round2(float(sum_sales or 0.0))
            ok = abs(act - exp) <= TOLERANCE_CURRENCY
            if not ok:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FOOTER_RETAILTOTAL",
                    name="Footer Retail_Total matches SUM(Sales_Value_Inc_VAT)",
                    severity="CRITICAL",
                    status="FAIL",
                    file_id=file_id,
                    expected_value=float(exp),
                    actual_value=float(act),
                    details={"footer_retail_total": exp, "sum_sales_value": act, "tolerance": TOLERANCE_CURRENCY}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.fail_(message="Footer Retail_Total mismatch", details=vr.details)
            else:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="FOOTER_RETAILTOTAL",
                    name="Footer Retail_Total matches SUM(Sales_Value_Inc_VAT)",
                    severity="INFO",
                    status="PASS",
                    file_id=file_id,
                    expected_value=float(exp),
                    actual_value=float(act),
                    details={"footer_retail_total": exp, "sum_sales_value": act, "tolerance": TOLERANCE_CURRENCY}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.pass_(message="Footer Retail_Total OK", details=vr.details)

        # F) SynProcessed consistency: Completed file should have SynProcessed=1 for all lines
        with _audit_step(audit, run_id, None,
                         subprocess="PostLoad",
                         step=f"Validate SynProcessed consistency for completed file {file_id}",
                         step_code="CHK_SYNPROCESSED_COMPLETED",
                         source_reference="RAW.EPOS_LineItems",
                         reference_number=ref) as s:
            # SynProcessed meaning:
            #   0 = not yet processed by downstream stages (expected BEFORE CTL/INT)
            #   1 = processed by downstream stages
            # We enforce: no NULLs, only 0/1, and not a mixture within the same file (partial processing).
            row = cur.execute("""
                SELECT
                    COUNT(*) AS total_lines,
                    SUM(CASE WHEN SynProcessed = 1 THEN 1 ELSE 0 END) AS cnt_1,
                    SUM(CASE WHEN SynProcessed = 0 THEN 1 ELSE 0 END) AS cnt_0,
                    SUM(CASE WHEN SynProcessed IS NULL THEN 1 ELSE 0 END) AS cnt_null,
                    SUM(CASE WHEN SynProcessed NOT IN (0,1) AND SynProcessed IS NOT NULL THEN 1 ELSE 0 END) AS cnt_bad
                FROM RAW.EPOS_LineItems
                WHERE File_ID = ?;
            """, (file_id,)).fetchone()

            total = int(row[0] or 0)
            cnt1 = int(row[1] or 0)
            cnt0 = int(row[2] or 0)
            cntnull = int(row[3] or 0)
            cntbad = int(row[4] or 0)

            details = {
                "total_lines": total,
                "synprocessed_0": cnt0,
                "synprocessed_1": cnt1,
                "synprocessed_null": cntnull,
                "synprocessed_bad": cntbad,
            }

            # Blocking conditions
            if cntnull > 0 or cntbad > 0:
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="SYNPROCESSED_COMPLETED",
                    name="Completed file has valid SynProcessed flags (0/1) and no NULLs",
                    severity="ERROR",
                    status="FAIL",
                    file_id=file_id,
                    rows_impacted=int(cntnull + cntbad),
                    details=details
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.fail_(message="Invalid SynProcessed values detected", details=details)

            elif cnt0 > 0 and cnt1 > 0:
                # Partial processing within a file is an operational red flag.
                impacted = min(cnt0, cnt1)
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="SYNPROCESSED_COMPLETED",
                    name="Completed file is not partially processed (SynProcessed not mixed)",
                    severity="ERROR",
                    status="FAIL",
                    file_id=file_id,
                    rows_impacted=int(impacted),
                    details=details
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.fail_(message="SynProcessed mixture detected (partial processing)", details=details)

            else:
                # All 0 OR all 1 are both acceptable.
                state = "UNPROCESSED" if cnt0 == total else "PROCESSED"
                vr = ValidationResult(
                    phase="POSTLOAD",
                    code="SYNPROCESSED_COMPLETED",
                    name="Completed file has consistent SynProcessed state (all 0 or all 1)",
                    severity="INFO",
                    status="PASS",
                    file_id=file_id,
                    details={**details, "file_state": state}
                )
                _log_formal(cur, run_id, corr, vr)
                out.append(vr)
                s.pass_(message=f"SynProcessed consistency OK ({state})", details=vr.details)


    return out



def log_overall_status(cur_meta: pyodbc.Cursor,
                       run_id: Optional[int],
                       correlation_id: str,
                       *,
                       status: str,
                       severity: str,
                       blocking: List[ValidationResult]) -> None:
    """
    Writes a final formal row into EXC.PreFlight_PostLoad for reporting/email.
    """
    vr = ValidationResult(
        phase="SUMMARY",
        code="OVERALL_STATUS",
        name="Overall PreFlight/PostLoad Result",
        severity=severity,
        status=status,
        details={
            "blocking_failures": len(blocking),
            "blocking_codes": [b.code for b in blocking],
        }
    )
    _log_formal(cur_meta, run_id, correlation_id, vr)




def _set_overall_run_status(cur_meta: pyodbc.Cursor, correlation_id: str, status: str) -> None:
    """
    Sets Overall_Run_Status on all EXC.PreFlight_PostLoad rows for this Run_ID.
    This supports operational reporting without having to parse OVERALL_STATUS rows.
    Best-effort: will not raise if column/table is missing.
    """
    try:
        cur_meta.execute("""
            UPDATE EXC.PreFlight_PostLoad
            SET Overall_Run_Status = ?
            WHERE Run_ID = ?;
        """, (status, correlation_id))
    except Exception:
        pass

# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    """
    Exit codes:
      0 = PASS (processing may continue)
      1 = FAIL (blocking validation failures; processing paused)
      2 = ERROR (technical/runtime error executing validations)
    """
    correlation_id = str(uuid.uuid4())

    _section(f"Synovia Fusion – EPOS PreFlight & PostLoad Validation (v{PROCESS_VERSION})")
    _p(f"System: {SYSTEM_NAME} | Environment: {ENVIRONMENT} | File Scope: {FILE_SCOPE} | Tolerance: ±{TOLERANCE_CURRENCY}")
    _p(f"Console mode: {CONSOLE_MODE}")
    _p(f"Correlation_ID: {correlation_id}")

    # ---- Resolve secrets/config (ENV → INI → FAIL) ----
    _p("Resolving bootstrap password (ENV → INI → FAIL)...")
    ctx = IniEnvContext(ini_path=INI_PATH, ini_section=INI_SECTION)
    try:
        pw = ctx.get_secret(env_key=BOOTSTRAP_PASSWORD_ENV, ini_key="password", required=True)
        _console_event("PASS", "CFG_BOOTSTRAP_PASSWORD", "Bootstrap password resolved")
    except Exception as e:
        _console_event(
            "FAIL",
            "CFG_BOOTSTRAP_PASSWORD",
            "Bootstrap password resolution failed",
            {"env_key": BOOTSTRAP_PASSWORD_ENV, "ini_path": INI_PATH, "ini_section": INI_SECTION, "error": str(e)}
        )
        return 2

    # ---- Connect to DB ----
    conn_str = build_sqlserver_conn_str(
        driver=BOOTSTRAP["driver"],
        server=BOOTSTRAP["server"],
        database=BOOTSTRAP["database"],
        username=BOOTSTRAP["username"],
        password=pw,
        timeout_sec=int(BOOTSTRAP["timeout"]),
        encrypt=True,
        trust_server_certificate=False,
        app_name="Fusion_EPOS_PreFlight_PostLoad_Validation"
    )

    conn_meta = None
    conn_data = None
    exc = None
    run_id: Optional[int] = None

    try:
        _p("Connecting to SQL Server...")
        conn_meta, conn_data = connect_meta_data(conn_str)
        _console_event("PASS", "CFG_DB_CONNECT", "Database connection established",
                       {"server": BOOTSTRAP["server"], "database": BOOTSTRAP["database"]})
    except Exception as e:
        _console_event("FAIL", "CFG_DB_CONNECT", "Database connection failed",
                       {"server": BOOTSTRAP["server"], "database": BOOTSTRAP["database"], "error": str(e)})
        return 2

    # ---- Create audit logger ----
    audit = FusionAuditLogger(
        conn_meta,
        system_name=SYSTEM_NAME,
        environment=ENVIRONMENT,
        orchestrator=ORCHESTRATOR,
        default_process=AUDIT_PROCESS
    )

    # ---- Execution Framework (optional) ----
    if ExcLogger is not None:
        try:
            exc = ExcLogger(conn_meta)
            run_id = exc.run_start(
                PROCESS_KEY,
                PROCESS_VERSION,
                ORCHESTRATOR,
                ENVIRONMENT,
                TRIGGER_TYPE,
                TRIGGERED_BY,
                uuid.UUID(correlation_id),
                attributes={"file_scope": FILE_SCOPE, "tolerance_currency": TOLERANCE_CURRENCY, "console_mode": CONSOLE_MODE}
            )
            _console_event("PASS", "EXC_RUN_START", f"Execution run started (Run_ID={run_id})")
        except Exception as e:
            # Continue without EXC run lineage
            _console_event("WARN", "EXC_RUN_START", "ExcLogger run_start failed; continuing without Run_ID",
                           {"error": str(e)})
            run_id = None

    run_id_for_audit = run_id if run_id is not None else 0
    cur_meta = conn_meta.cursor()

    results: List[ValidationResult] = []

    try:
        # ------------------------------------------------------------
        # PHASE 1: TECHNICAL
        # ------------------------------------------------------------
        _section("Phase 1 – Technical Validations")
        results += validate_required_objects(conn_meta, audit, run_id_for_audit, correlation_id)

        technical_blocking = [r for r in results if r.is_blocking_fail()]
        if not technical_blocking:
            results += validate_required_columns(conn_meta, audit, run_id_for_audit, correlation_id)
            technical_blocking = [r for r in results if r.is_blocking_fail()]

        if technical_blocking:
            _console_event("FAIL", "SUM_TECHNICAL_BLOCK", "Blocking technical failures; skipping Business/PostLoad",
                           {"blocking_codes": [r.code for r in technical_blocking]})
        else:
            # ------------------------------------------------------------
            # PHASE 2: BUSINESS
            # ------------------------------------------------------------
            _section("Phase 2 – Business Validations")
            results += validate_mapping_uniqueness(conn_meta, audit, run_id_for_audit, correlation_id)
            results += validate_store_product_coverage(conn_meta, audit, run_id_for_audit, correlation_id)

            # ------------------------------------------------------------
            # PHASE 3: POSTLOAD
            # ------------------------------------------------------------
            _section("Phase 3 – PostLoad Validations")
            results += validate_postload_file_integrity(conn_meta, audit, run_id_for_audit, correlation_id)

        # ------------------------------------------------------------
        # FINAL DECISION (GATE)
        # ------------------------------------------------------------
        blocking = [r for r in results if r.is_blocking_fail()]

        with _audit_step(
            audit, run_id_for_audit, None,
            subprocess="Summary",
            step="PreFlight/PostLoad gate decision",
            step_code="SUM_GATE_DECISION",
            details={"blocking_failures": len(blocking)}
        ) as st:

            if blocking:
                st.fail_(message="Validation gate FAILED", details={"blocking_codes": [r.code for r in blocking]})

                # Formal final report row
                log_overall_status(cur_meta, run_id, correlation_id, status="FAIL", severity="CRITICAL", blocking=blocking)
                _set_overall_run_status(cur_meta, correlation_id, "FAIL")

                # End execution framework run cleanly
                if exc and run_id is not None:
                    exc.run_end(run_id, "FAILED", f"PreFlight/PostLoad failed: {len(blocking)} blocking failures")

                _section("RESULT: FAIL")
                _p(f"Blocking failures: {len(blocking)}")
                for r in blocking[:50]:
                    _p(f" - {r.phase}:{r.code} [{r.severity}] {r.name}")
                if len(blocking) > 50:
                    _p(f" ... {len(blocking) - 50} more")
                _p("Processing is paused. Fix the issues and rerun.")
                return 1

            else:
                st.pass_(message="Validation gate PASSED")

                # Formal final report row
                log_overall_status(cur_meta, run_id, correlation_id, status="PASS", severity="INFO", blocking=[])
                _set_overall_run_status(cur_meta, correlation_id, "PASS")

                if exc and run_id is not None:
                    exc.run_end(run_id, "SUCCESS")

                _section("RESULT: PASS")
                _p("No blocking failures. Processing may continue.")
                return 0

    except Exception as e:
        # Unexpected runtime error while executing validations
        try:
            with _audit_step(
                audit, run_id_for_audit, None,
                subprocess="Summary",
                step="Unhandled runtime exception",
                step_code="SUM_RUNTIME_ERROR",
                details={"error": str(e)}
            ) as st:
                st.fail_(message="Unhandled exception", error=e, details={"error": str(e)})
        except Exception:
            pass

        try:
            vr = ValidationResult(
                phase="SUMMARY",
                code="RUNTIME_ERROR",
                name="Unhandled runtime exception",
                severity="CRITICAL",
                status="FAIL",
                details={"error": str(e)}
            )
            _log_formal(cur_meta, run_id, correlation_id, vr)
            log_overall_status(cur_meta, run_id, correlation_id, status="FAIL", severity="CRITICAL", blocking=[vr])
        except Exception:
            pass

        if exc and run_id is not None:
            try:
                exc.run_end(run_id, "FAILED", str(e)[:4000])
            except Exception:
                pass

        _section("RESULT: ERROR")
        _p(f"Unhandled error: {e}")
        _p("See EXC.Process_Transaction_Log and EXC.PreFlight_PostLoad for details (if available).")
        return 2

    finally:
        try:
            if conn_data is not None:
                conn_data.close()
        except Exception:
            pass
        try:
            if conn_meta is not None:
                conn_meta.close()
        except Exception:
            pass


if __name__ == "__main__":
    import os

    exit_code = main()

    # Avoid noisy "SystemExit" tracebacks in debuggers / wrappers by using os._exit
    # only when not running under an active debugger.
    if sys.gettrace() is not None:
        _p(f"(Debugger detected) Exit code would be: {exit_code}")
    else:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
        os._exit(int(exit_code))
