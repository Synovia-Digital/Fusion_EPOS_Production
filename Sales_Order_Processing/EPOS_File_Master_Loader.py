# =============================================================================
#  Synovia Fusion – EPOS Master Loader (Enterprise Standard)
# =============================================================================
#
#  Product:        Synovia Fusion Platform
#  Module:         EPOS Ingestion
#  Script Name:    EPOS_File_Master_Loader_Enterprise.py
#
#  Version:        3.3.0
#  Execution FW:   Execution Framework SDK v1.2.0
#  Release Date:  2026-02-11
#
# -----------------------------------------------------------------------------
#  Enterprise Standards baked in:
#   - Direct bootstrap connection (no dependency on CFG.v_Profile_Database)
#   - Optional CFG folder locations, with ENV/INI fallback
#   - Bullet-proof file staging (_processing), readiness checks, safe moves
#   - Durable control-row lifecycle (META autocommit vs DATA transactional)
#   - Reusable Process/SubProcess/Step audit logging (EXC.Process_Transaction_Log)
#   - Every check and action logged for audit and operational intelligence
# =============================================================================

import os
import sys
import time
import csv
import uuid
from typing import Any, Dict, Optional, Tuple, Iterable, List

import pandas as pd
import pyodbc

try:
    from exc_logger import ExcLogger  # optional — provides Run_ID lineage
except ImportError:
    ExcLogger = None


from synovia_fusion_config import IniEnvContext
from synovia_fusion_db import build_sqlserver_conn_str, connect_meta_data, resolve_file_locations, object_exists
from synovia_fusion_fileops import ensure_dir, stage_inbound_file, list_orphan_staged, original_name_from_staged, safe_move, sha256_file, file_size
from synovia_fusion_epos import parse_filename, parse_footer, normalise_headers, filter_footer_rows, coerce_numeric_fields
from synovia_fusion_audit import FusionAuditLogger


# =============================================================================
# RUNTIME IDENTIFIERS
# =============================================================================

SYSTEM_NAME     = "Fusion_EPOS"
ENVIRONMENT     = "PROD"

PROCESS_KEY     = "FUSION_EPOS_MASTERLOADER"
PROCESS_VERSION = "3.3.0"

ORCHESTRATOR    = "PYTHON"
TRIGGER_TYPE    = "SCHEDULE"
TRIGGERED_BY    = "SQLAGENT"

# Audit taxonomy (requested standard)
AUDIT_PROCESS   = "Load_EPOS_Data"
AUDIT_SUBPROC   = "File_Ingestion"

# Validation strictness (SOFT/HARD)
VALIDATION_MODE = os.environ.get("FUSION_EPOS_VALIDATION_MODE", "SOFT").strip().upper() or "SOFT"

# =============================================================================
# BOOTSTRAP DATABASE (MINIMAL, EXPLICIT)
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

FILE_LOCATIONS_ENV = {
    "Inbound_Folder":   "FUSION_EPOS_INBOUND_DIR",
    "Processed_Folder": "FUSION_EPOS_PROCESSED_DIR",
    "Duplicate_Folder": "FUSION_EPOS_DUPLICATE_DIR",
    "Error_Folder":     "FUSION_EPOS_ERROR_DIR",
}


# =============================================================================
# DB Helpers (RAW tables)
# =============================================================================

def get_existing_file(cur: pyodbc.Cursor, filename: str, supplier: int, year: int, week: int):
    return cur.execute("""
        SELECT TOP (1) File_ID, Status
        FROM RAW.EPOS_File
        WHERE FileName=? AND Supplier_Number=? AND [Year]=? AND [Week]=?
        ORDER BY File_ID DESC;
    """, filename, supplier, year, week).fetchone()


def raw_insert_file_row(conn_meta: pyodbc.Connection,
                        filename: str, region: str, supplier: int, year: int, week: int,
                        record_count: int) -> int:
    cur = conn_meta.cursor()
    cur.execute("""
        INSERT INTO RAW.EPOS_File
        (FileName, Region, Supplier_Number, [Year], [Week], Record_Count, Status)
        OUTPUT INSERTED.File_ID
        VALUES (?,?,?,?,?,?,'InProgress');
    """, filename, region, supplier, year, week, record_count)
    return int(cur.fetchone()[0])


def raw_insert_footer_row(conn_meta: pyodbc.Connection,
                          file_id: int,
                          footer_text: Optional[str],
                          footer_lines: Optional[int],
                          footer_qty: Optional[int],
                          footer_sales: Optional[float]) -> None:
    cur = conn_meta.cursor()
    cur.execute("""
        INSERT INTO RAW.EPOS_FileFooter
        (File_ID, Footer_Text, NumberOfLines, Qty_Total, Retail_Total)
        VALUES (?,?,?,?,?);
    """, file_id, footer_text, footer_lines, footer_qty, footer_sales)


def raw_mark_file_completed(conn_meta: pyodbc.Connection, file_id: int) -> None:
    cur = conn_meta.cursor()
    cur.execute("""
        UPDATE RAW.EPOS_File
        SET Status='Completed',
            Completed_Timestamp=SYSUTCDATETIME(),
            Error_Message=NULL
        WHERE File_ID=?;
    """, file_id)


def raw_mark_file_error(conn_meta: pyodbc.Connection, file_id: int, error_message: str) -> None:
    cur = conn_meta.cursor()
    cur.execute("""
        UPDATE RAW.EPOS_File
        SET Status='Error',
            Error_Message=?
        WHERE File_ID=?;
    """, str(error_message)[:4000], file_id)


LINEITEM_INSERT_SQL = """
    INSERT INTO RAW.EPOS_LineItems
    (File_ID,[Year],[Week],[Day],[Region],[Store],[Store_Name],
     [Docket_Reference],[Retail_Barcode],[Dunnes_Prod_Code],
     [Product_Description],[Supplier],[Suppl_Name],
     [Units_Sold],[Sales_Value_Inc_VAT],[Supplier_Product_Code])
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
"""


def _as_int(val: Any) -> Optional[int]:
    try:
        if val is None:
            return None
        v = str(val).strip()
        if v == "" or v.lower() == "nan":
            return None
        return int(float(v))
    except Exception:
        return None


def _as_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        v = str(val).strip()
        if v == "" or v.lower() == "nan":
            return None
        return float(v)
    except Exception:
        return None


def iter_lineitem_params(file_id: int, df: pd.DataFrame) -> Iterable[Tuple[Any, ...]]:
    for r in df.itertuples(index=False, name=None):
        # Column order matches EXPECTED_HEADERS from synovia_fusion_epos
        yield (
            file_id,
            _as_int(r[0]), _as_int(r[1]), _as_int(r[2]),
            r[3], r[4], r[5], r[6], r[7], r[8],
            r[9], r[10], r[11],
            _as_int(r[12]), _as_float(r[13]), r[14]
        )


def chunked(iterable: Iterable[Tuple[Any, ...]], size: int) -> Iterable[List[Tuple[Any, ...]]]:
    batch: List[Tuple[Any, ...]] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_lineitems(conn_data: pyodbc.Connection, file_id: int, df: pd.DataFrame, batch_size: int = 10_000) -> int:
    cur = conn_data.cursor()
    cur.fast_executemany = True

    total = 0
    for batch in chunked(iter_lineitem_params(file_id, df), batch_size):
        cur.executemany(LINEITEM_INSERT_SQL, batch)
        total += len(batch)
    return total


def hard_fail() -> bool:
    return VALIDATION_MODE == "HARD"


# =============================================================================
# File processing (per-file isolation)
# =============================================================================

def process_one_file(exc: ExcLogger,
                     audit: FusionAuditLogger,
                     conn_meta: pyodbc.Connection,
                     conn_data: pyodbc.Connection,
                     run_id: int,
                     staged_path: str,
                     original_filename: str,
                     processed_dir: str,
                     duplicate_dir: str,
                     error_dir: str) -> str:

    file_hash = sha256_file(staged_path)
    bytes_read = file_size(staged_path)

    file_txn = exc.txn_start(
        run_id,
        txn_type="FILE",
        txn_level="FILE",
        txn_name="EPOS file lifecycle",
        source_reference=original_filename,
        idempotency_key=file_hash,
        attributes={"script_version": PROCESS_VERSION}
    )

    file_id: Optional[int] = None

    try:
        # ---- Check: filename parsing ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="File_Validation",
            step="Validate filename format + extract metadata",
            step_code="CHK_FILENAME_PARSE",
            source_reference=original_filename,
            idempotency_key=file_hash,
            details={"validation_mode": VALIDATION_MODE}
        ) as s:
            region, supplier, year, week = parse_filename(original_filename)
            s.pass_(message="Filename OK", details={"region": region, "supplier": supplier, "year": year, "week": week},
                    bytes_processed=bytes_read)

        # ---- Check: duplicate ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="Duplicate_Check",
            step="Check RAW.EPOS_File for prior completed load",
            step_code="CHK_DUPLICATE_FILE",
            source_reference=original_filename,
            idempotency_key=file_hash
        ) as s:
            cur_meta = conn_meta.cursor()
            existing = get_existing_file(cur_meta, original_filename, supplier, year, week)
            if existing and str(existing[1]).upper() == "COMPLETED":
                s.skip_(message="Duplicate - already completed", details={"existing_file_id": int(existing[0])})
                # move file to duplicate folder
                with audit.step(
                    run_id, file_txn,
                    process=AUDIT_PROCESS, subprocess="File_Routing",
                    step="Move file to Duplicate folder",
                    step_code="FS_MOVE_DUPLICATE",
                    source_reference=original_filename
                ) as mv:
                    safe_move(staged_path, duplicate_dir, original_filename)
                    mv.pass_(message="Moved to Duplicate", bytes_processed=bytes_read)

                exc.txn_end(file_txn, "SKIPPED", error_message="Duplicate file")
                return "DUPLICATE"
            s.pass_(message="Not a duplicate")

        # ---- Check: footer parse ----
        footer_lines = footer_qty = footer_sales = footer_text = None
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="File_Validation",
            step="Parse footer totals",
            step_code="CHK_FOOTER_PARSE",
            source_reference=original_filename
        ) as s:
            footer_lines, footer_qty, footer_sales, footer_text, footer_ok = parse_footer(staged_path)
            if footer_ok:
                s.pass_(message="Footer recognised",
                        details={"footer_lines": footer_lines, "footer_qty": footer_qty, "footer_sales": footer_sales})
            else:
                if hard_fail():
                    s.fail_(message="Footer not recognised (HARD)", details={"footer_text": footer_text})
                    raise ValueError("Footer not recognised / invalid format")
                s.warn_(message="Footer not recognised (SOFT)", details={"footer_text": footer_text})

        # ---- Action: read CSV ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="Read_Parse",
            step="Read CSV",
            step_code="IO_READ_CSV",
            source_reference=original_filename
        ) as s:
            df_raw = pd.read_csv(staged_path, dtype=str, engine="python", quotechar='"',
                                 on_bad_lines="skip", keep_default_na=False)
            s.pass_(message="CSV read OK", rows_read=len(df_raw), bytes_processed=bytes_read,
                    details={"columns": list(df_raw.columns)})

        # ---- Check: headers ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="File_Validation",
            step="Validate and normalise headers",
            step_code="CHK_HEADERS",
            source_reference=original_filename
        ) as s:
            df, missing = normalise_headers(df_raw)
            if missing:
                s.fail_(message="Missing expected headers", details={"missing": missing, "found": list(df_raw.columns)})
                raise ValueError(f"Missing headers: {missing}")
            s.pass_(message="Headers OK")

        # ---- Action: filter footer rows embedded in data ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="Read_Parse",
            step="Filter footer rows embedded in data",
            step_code="TRN_FILTER_FOOTER_ROWS",
            source_reference=original_filename
        ) as s:
            df, removed = filter_footer_rows(df)
            s.pass_(message="Filtered footer rows", details={"removed_rows": removed, "rows_remaining": len(df)})

        # ---- Action: numeric coercion ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="Transform",
            step="Coerce numeric fields (Units_Sold, Sales_Value_Inc_VAT)",
            step_code="TRN_COERCE_NUMERIC",
            source_reference=original_filename
        ) as s:
            df, bad_counts = coerce_numeric_fields(df)
            s.pass_(message="Numeric coercion complete", details=bad_counts)

        # ---- Check: rowcount vs footer ----
        if footer_lines is not None:
            with audit.step(
                run_id, file_txn,
                process=AUDIT_PROCESS, subprocess="File_Validation",
                step="Validate footer rowcount matches data rowcount",
                step_code="CHK_FOOTER_ROWCOUNT",
                source_reference=original_filename,
                details={"footer_lines": footer_lines, "data_rows": len(df)}
            ) as s:
                if int(footer_lines) != len(df):
                    if hard_fail():
                        s.fail_(message="Rowcount mismatch (HARD)")
                        raise ValueError(f"Rowcount mismatch: footer={footer_lines} data={len(df)}")
                    s.warn_(message="Rowcount mismatch (SOFT)")
                else:
                    s.pass_(message="Rowcount OK")

        # ---- Check: mandatory fields presence (product / docket / codes) ----
        # This is an audit intelligence checkpoint, not a hard stop by default.
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="Data_Quality",
            step="Check key fields completeness (Products/Dockets/Codes)",
            step_code="CHK_KEY_FIELDS_COMPLETENESS",
            source_reference=original_filename
        ) as s:
            checks = {
                "Docket_Reference_blank": int((df["Docket_Reference"].astype(str).str.strip() == "").sum()),
                "Retail_Barcode_blank": int((df["Retail_Barcode"].astype(str).str.strip() == "").sum()),
                "Dunnes_Prod_Code_blank": int((df["Dunnes_Prod_Code"].astype(str).str.strip() == "").sum()),
                "Product_Description_blank": int((df["Product_Description"].astype(str).str.strip() == "").sum()),
            }
            # Heuristic: if >10% blanks in any key field, warn (SOFT) / fail (HARD)
            threshold = max(1, int(0.10 * len(df)))
            offenders = {k: v for k, v in checks.items() if v > threshold}
            if offenders:
                if hard_fail():
                    s.fail_(message="Key fields completeness failed (HARD)", details={"offenders": offenders, "total_rows": len(df)})
                    raise ValueError(f"Key fields completeness failed: {offenders}")
                s.warn_(message="Key fields completeness warning (SOFT)", details={"offenders": offenders, "total_rows": len(df)})
            else:
                s.pass_(message="Key fields completeness OK", details={"counts": checks})

        # ---- DB Write: insert RAW.EPOS_File (durable - conn_meta autocommit) ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="DB_Write",
            step="Insert RAW.EPOS_File (InProgress)",
            step_code="DB_INS_RAW_FILE",
            source_reference=original_filename,
            idempotency_key=file_hash,
            details={"record_count": len(df)}
        ) as s:
            file_id = raw_insert_file_row(conn_meta, original_filename, region, supplier, year, week, len(df))
            s.pass_(message="RAW.EPOS_File inserted", rows_affected=1, reference_number=str(file_id))

        # ---- DB Write: insert footer (durable) ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="DB_Write",
            step="Insert RAW.EPOS_FileFooter",
            step_code="DB_INS_FOOTER",
            source_reference=original_filename,
            reference_number=str(file_id)
        ) as s:
            raw_insert_footer_row(conn_meta, file_id, footer_text, footer_lines, footer_qty, footer_sales)
            s.pass_(message="Footer inserted", rows_affected=1)

        # ---- DB Write: insert line items (transactional) ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="DB_Write",
            step="Insert RAW.EPOS_LineItems (bulk, chunked)",
            step_code="DB_INS_LINEITEMS",
            source_reference=original_filename,
            reference_number=str(file_id),
            details={"batch_size": 10000}
        ) as s:
            try:
                inserted = insert_lineitems(conn_data, file_id, df, batch_size=10_000)
                conn_data.commit()
                s.pass_(message="Line items inserted + committed", rows_affected=inserted)
            except Exception as e:
                conn_data.rollback()
                s.fail_(message="Line items insert failed", error=e)
                raw_mark_file_error(conn_meta, file_id, str(e))
                raise

        # ---- DB Write: mark completed (durable) ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="DB_Write",
            step="Mark RAW.EPOS_File Completed",
            step_code="DB_UPD_MARK_COMPLETED",
            source_reference=original_filename,
            reference_number=str(file_id)
        ) as s:
            raw_mark_file_completed(conn_meta, file_id)
            s.pass_(message="Marked completed", rows_affected=1)

        # ---- Filesystem: move processed ----
        with audit.step(
            run_id, file_txn,
            process=AUDIT_PROCESS, subprocess="File_Routing",
            step="Move file to Processed folder",
            step_code="FS_MOVE_PROCESSED",
            source_reference=original_filename,
            reference_number=str(file_id)
        ) as s:
            safe_move(staged_path, processed_dir, original_filename)
            s.pass_(message="Moved to Processed", bytes_processed=bytes_read)

        exc.txn_end(file_txn, "SUCCESS", rows_read=len(df), rows_affected=len(df))
        return "SUCCESS"

    except Exception as e:
        # Best-effort move to error folder
        try:
            if os.path.exists(staged_path):
                with audit.step(
                    run_id, file_txn,
                    process=AUDIT_PROCESS, subprocess="File_Routing",
                    step="Move file to Error folder",
                    step_code="FS_MOVE_ERROR",
                    source_reference=original_filename,
                    reference_number=str(file_id) if file_id else None
                ) as s:
                    safe_move(staged_path, error_dir, original_filename)
                    s.pass_(message="Moved to Error", bytes_processed=bytes_read)
        except Exception:
            pass

        # Mark RAW file error if we have a file_id
        if file_id is not None:
            try:
                raw_mark_file_error(conn_meta, file_id, str(e))
            except Exception:
                pass

        exc.txn_end(file_txn, "FAILED", error_message=str(e)[:4000])
        return "FAILED"


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    correlation_id = uuid.uuid4()

    # ---- Resolve secrets/config ----
    ctx = IniEnvContext(ini_path=INI_PATH, ini_section=INI_SECTION)
    pw = ctx.get_secret(env_key=BOOTSTRAP_PASSWORD_ENV, ini_key="password", required=True)

    conn_str = build_sqlserver_conn_str(
        driver=BOOTSTRAP["driver"],
        server=BOOTSTRAP["server"],
        database=BOOTSTRAP["database"],
        username=BOOTSTRAP["username"],
        password=pw,
        timeout_sec=int(BOOTSTRAP["timeout"]),
        encrypt=True,
        trust_server_certificate=False,
        app_name="Fusion_EPOS_MasterLoader_Enterprise"
    )

    conn_meta, conn_data = connect_meta_data(conn_str)
    exc = ExcLogger(conn_meta)

    # Audit logger (non-blocking). Requires table EXC.Process_Transaction_Log (or synonyms).
    audit = FusionAuditLogger(
        conn_meta,
        system_name=SYSTEM_NAME,
        environment=ENVIRONMENT,
        orchestrator=ORCHESTRATOR,
        default_process=AUDIT_PROCESS
    )

    run_id = exc.run_start(
        PROCESS_KEY,
        PROCESS_VERSION,
        ORCHESTRATOR,
        ENVIRONMENT,
        TRIGGER_TYPE,
        TRIGGERED_BY,
        correlation_id,
        attributes={"validation_mode": VALIDATION_MODE, "bootstrap_direct": True}
    )

    # ---- Pre-flight: ensure RAW tables exist (fail fast; audit it) ----
    required_raw = [("RAW", "EPOS_File"), ("RAW", "EPOS_FileFooter"), ("RAW", "EPOS_LineItems")]
    with audit.step(
        run_id, None,
        process=AUDIT_PROCESS, subprocess="Run_Initialisation",
        step="Preflight: verify RAW tables exist",
        step_code="CHK_PREFLIGHT_RAW_TABLES",
        details={"required": [f"{s}.{t}" for s, t in required_raw]}
    ) as s:
        missing = [f"{sch}.{name}" for sch, name in required_raw if not object_exists(conn_meta, sch, name)]
        if missing:
            s.fail_(message="Missing RAW tables", details={"missing": missing})
            raise RuntimeError(f"Missing RAW tables: {missing}")
        s.pass_(message="RAW tables OK")

    # ---- Resolve file locations (CFG -> ENV -> INI) ----
    # INI supports either exact keys or common snake_case keys.
    ini_vals = {}
    # exact keys
    ini_vals.update(ctx.get_many(["Inbound_Folder", "Processed_Folder", "Duplicate_Folder", "Error_Folder"]))
    # common alternates
    alt = {
        "Inbound_Folder":   ctx.get_ini("inbound_folder") or ctx.get_ini("inbound_dir"),
        "Processed_Folder": ctx.get_ini("processed_folder") or ctx.get_ini("processed_dir"),
        "Duplicate_Folder": ctx.get_ini("duplicate_folder") or ctx.get_ini("duplicate_dir"),
        "Error_Folder":     ctx.get_ini("error_folder") or ctx.get_ini("error_dir"),
    }
    for k, v in alt.items():
        if v and k not in ini_vals:
            ini_vals[k] = v

    with audit.step(
        run_id, None,
        process=AUDIT_PROCESS, subprocess="Run_Initialisation",
        step="Resolve file locations (CFG/ENV/INI)",
        step_code="CFG_RESOLVE_FILE_LOCATIONS",
        details={"env_map": FILE_LOCATIONS_ENV, "ini_path": INI_PATH, "ini_section": INI_SECTION}
    ) as s:
        loc = resolve_file_locations(conn_meta=conn_meta, env_map=FILE_LOCATIONS_ENV, ini_values=ini_vals)
        inbound_dir = loc["Inbound_Folder"]
        processed_dir = loc["Processed_Folder"]
        duplicate_dir = loc["Duplicate_Folder"]
        error_dir = loc["Error_Folder"]

        if not os.path.isdir(inbound_dir):
            s.fail_(message="Inbound folder not accessible", details={"inbound_dir": inbound_dir})
            raise RuntimeError(f"Inbound folder not accessible: {inbound_dir}")

        ensure_dir(processed_dir)
        ensure_dir(duplicate_dir)
        ensure_dir(error_dir)

        processing_dir = os.path.join(inbound_dir, "_processing")
        ensure_dir(processing_dir)

        s.pass_(message="Locations resolved",
                details={
                    "inbound": inbound_dir,
                    "processing": processing_dir,
                    "processed": processed_dir,
                    "duplicate": duplicate_dir,
                    "error": error_dir
                })

    # ---- Crash recovery: orphan staged files ----
    with audit.step(
        run_id, None,
        process=AUDIT_PROCESS, subprocess="Inbound_Pickup",
        step="Recover orphan staged files from previous runs",
        step_code="FS_RECOVER_ORPHAN_STAGED"
    ) as s:
        orphans = list_orphan_staged(processing_dir)
        s.pass_(message="Recovery scan complete", details={"orphan_count": len(orphans)})

    for staged_path in list_orphan_staged(processing_dir):
        original_filename = original_name_from_staged(staged_path)
        process_one_file(exc, audit, conn_meta, conn_data, run_id,
                         staged_path, original_filename,
                         processed_dir, duplicate_dir, error_dir)

    # ---- Normal inbound processing ----
    with audit.step(
        run_id, None,
        process=AUDIT_PROCESS, subprocess="Inbound_Pickup",
        step="Scan inbound folder and process CSV files",
        step_code="FS_SCAN_INBOUND",
        details={"inbound_dir": inbound_dir}
    ) as s:
        s.pass_(message="Starting scan")

    try:
        for fname in sorted(os.listdir(inbound_dir)):
            if not fname.lower().endswith(".csv"):
                continue
            if fname.lower().startswith("_processing"):
                continue

            # Stage file for safe processing
            with audit.step(
                run_id, None,
                process=AUDIT_PROCESS, subprocess="Inbound_Pickup",
                step="Stage inbound file to _processing",
                step_code="FS_STAGE_FILE",
                source_reference=fname
            ) as st:
                staged = stage_inbound_file(inbound_dir, processing_dir, fname)
                if not staged:
                    st.warn_(message="File not ready or could not be staged (skipped this cycle)")
                    continue
                staged_path, original_filename = staged
                st.pass_(message="Staged", details={"staged_path": staged_path})

            process_one_file(exc, audit, conn_meta, conn_data, run_id,
                             staged_path, original_filename,
                             processed_dir, duplicate_dir, error_dir)

        exc.run_end(run_id, "SUCCESS")

    except Exception as e:
        exc.run_end(run_id, "FAILED", str(e))
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
