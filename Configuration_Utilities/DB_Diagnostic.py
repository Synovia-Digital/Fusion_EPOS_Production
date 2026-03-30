
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
#  Synovia Fusion – Database Diagnostic Script
# =============================================================================
#  Connects to Fusion_EPOS_Production and reports exactly what exists:
#    - Schemas
#    - Tables (with row counts and column lists)
#    - Stored Procedures
#    - Synonyms
#    - Views
#    - Indexes on key tables
#    - Sample data from control/config tables
#
#  Output: console + writes a timestamped report to the same folder as this script.
#
#  Usage:
#    python DB_Diagnostic.py
#
#  Credentials: reads from the same INI as the rest of the pipeline.
#  Edit INI_PATH or CONFIG_PATH below if needed.
# =============================================================================

import os
import sys
import configparser
import pyodbc
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIG  –  update ONE of these to match your environment
# =============================================================================

# Option A: UNC share (production server)
CONFIG_PATH = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"
CONFIG_SECTION = "Fusion_EPOS_Production"

# Option B: local INI (dev / fallback)
LOCAL_INI_PATH = r"D:\Configuration\Fusion_EPOS_Production.ini"
LOCAL_INI_SECTION = "Fusion_EPOS_Production"

# Output folder for the report file (defaults to same folder as this script)
REPORT_DIR = os.path.dirname(os.path.abspath(__file__))

# =============================================================================
# SCHEMAS / OBJECTS TO INSPECT
# =============================================================================

TARGET_SCHEMAS = ["RAW", "CTL", "INT", "EXC", "CFG", "LOG"]

# Tables we want column details + row counts for
TABLES_OF_INTEREST = [
    # SAP STO (new)
    ("RAW", "SAP_TO_Responses"),
    ("RAW", "SAP_TO_Responses_Lines"),
    ("INT", "SAP_STO_Staging"),
    ("EXC", "TransferOrder_Control"),
    ("INT", "TransferOrderHeader"),
    ("INT", "TransferOrderLine"),
    # EPOS pipeline (existing)
    ("RAW", "EPOS_File"),
    ("RAW", "EPOS_FileFooter"),
    ("RAW", "EPOS_LineItems"),
    ("CTL", "EPOS_Header"),
    ("CTL", "EPOS_Line_Item"),
    ("INT", "SalesOrderStage"),
    ("INT", "SalesOrderStaging"),
    ("EXC", "Processed_Lines"),
    ("EXC", "Process_Transaction_Log"),
    ("CFG", "Dynamics_Stores"),
    ("CFG", "Products"),
    ("CFG", "Email_Distribution"),
    ("CFG", "Fusion_EPOS_File_Locations"),
    ("LOG", "EPOS_Load_Run"),
]

# Stored procedures to look for
PROCS_OF_INTEREST = [
    ("EXC", "usp_EPOS_Generate_Weekly_Orders_Step4"),
    ("EXC", "usp_EPOS_Generate_Weekly_Orders_Step6"),
    ("INT", "usp_Stage_SalesOrders"),
]

# =============================================================================
# CONNECTION
# =============================================================================

def resolve_ini() -> tuple[str, configparser.ConfigParser]:
    """Try UNC config path first, fall back to local INI."""
    # Try UNC
    try:
        p = Path(CONFIG_PATH)
        if p.is_dir():
            candidate = p / "Master_ini_config.ini"
            if candidate.exists():
                cfg = configparser.ConfigParser()
                cfg.read(candidate)
                if CONFIG_SECTION in cfg:
                    return str(candidate), cfg
        elif p.is_file():
            cfg = configparser.ConfigParser()
            cfg.read(p)
            if CONFIG_SECTION in cfg:
                return str(p), cfg
    except Exception:
        pass

    # Try local INI
    if os.path.exists(LOCAL_INI_PATH):
        cfg = configparser.ConfigParser()
        cfg.read(LOCAL_INI_PATH)
        section = LOCAL_INI_SECTION
        if section in cfg:
            return LOCAL_INI_PATH, cfg

    raise RuntimeError(
        f"Could not find a usable INI file.\n"
        f"  Tried UNC : {CONFIG_PATH}\n"
        f"  Tried local: {LOCAL_INI_PATH}\n"
        f"Edit INI_PATH at the top of this script."
    )


def get_connection() -> pyodbc.Connection:
    ini_path, cfg = resolve_ini()
    section = CONFIG_SECTION if CONFIG_SECTION in cfg else LOCAL_INI_SECTION
    db = cfg[section]
    conn_str = (
        f"Driver={{{db.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
        f"Server={db.get('server')};"
        f"Database={db.get('database')};"
        f"UID={db.get('user', db.get('username', ''))};"
        f"PWD={db.get('password')};"
        f"Encrypt={db.get('encrypt', 'yes')};"
        f"TrustServerCertificate={db.get('trust_server_certificate', 'no')};"
        f"Connection Timeout=30;"
        f"APP=DB_Diagnostic;"
    )
    conn = pyodbc.connect(conn_str, autocommit=True)
    return conn, ini_path, db.get('server'), db.get('database')


# =============================================================================
# REPORT HELPERS
# =============================================================================

class Report:
    def __init__(self):
        self.lines = []

    def h1(self, title):
        sep = "=" * 80
        self._add(f"\n{sep}")
        self._add(f"  {title}")
        self._add(sep)

    def h2(self, title):
        self._add(f"\n  {'─' * 76}")
        self._add(f"  {title}")
        self._add(f"  {'─' * 76}")

    def h3(self, title):
        self._add(f"\n    ── {title}")

    def ok(self, msg):    self._add(f"    ✔  {msg}")
    def miss(self, msg):  self._add(f"    ✘  MISSING : {msg}")
    def warn(self, msg):  self._add(f"    ⚠  WARN    : {msg}")
    def info(self, msg):  self._add(f"    ·  {msg}")
    def col(self, msg):   self._add(f"       {msg}")
    def blank(self):      self._add("")

    def _add(self, line):
        self.lines.append(line)
        print(line)

    def save(self, path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines))
        print(f"\n  Report saved to: {path}")


# =============================================================================
# QUERY HELPERS
# =============================================================================

def object_exists(cur, schema, name, obj_types=("U", "V", "P", "SN")):
    types_str = ",".join(f"'{t}'" for t in obj_types)
    row = cur.execute(f"""
        SELECT TOP 1 o.type_desc
        FROM sys.objects o
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = ? AND o.name = ? AND o.type IN ({types_str});
    """, schema, name).fetchone()
    return row[0] if row else None


def get_columns(cur, schema, table):
    rows = cur.execute("""
        SELECT c.COLUMN_NAME, c.DATA_TYPE,
               c.CHARACTER_MAXIMUM_LENGTH,
               c.IS_NULLABLE,
               c.COLUMN_DEFAULT,
               COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IsIdentity
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION;
    """, schema, table).fetchall()
    return rows


def get_row_count(cur, schema, table):
    try:
        row = cur.execute(
            f"SELECT COUNT(*) FROM [{schema}].[{table}];"
        ).fetchone()
        return row[0] if row else "?"
    except Exception as e:
        return f"ERROR: {e}"


def get_indexes(cur, schema, table):
    rows = cur.execute("""
        SELECT i.name, i.type_desc, i.is_unique, i.is_primary_key,
               STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_cols
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
        JOIN sys.tables t ON t.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ?
          AND ic.is_included_column = 0
        GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
        ORDER BY i.index_id;
    """, schema, table).fetchall()
    return rows


def get_synonyms(cur):
    rows = cur.execute("""
        SELECT s.name AS schema_name, sn.name AS synonym_name,
               sn.base_object_name
        FROM sys.synonyms sn
        JOIN sys.schemas s ON s.schema_id = sn.schema_id
        ORDER BY s.name, sn.name;
    """).fetchall()
    return rows


def get_all_tables(cur):
    rows = cur.execute("""
        SELECT s.name AS schema_name, t.name AS table_name,
               t.create_date, t.modify_date
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        ORDER BY s.name, t.name;
    """).fetchall()
    return rows


def get_all_procs(cur):
    rows = cur.execute("""
        SELECT s.name AS schema_name, p.name AS proc_name,
               p.create_date, p.modify_date
        FROM sys.procedures p
        JOIN sys.schemas s ON s.schema_id = p.schema_id
        ORDER BY s.name, p.name;
    """).fetchall()
    return rows


def get_all_views(cur):
    rows = cur.execute("""
        SELECT s.name AS schema_name, v.name AS view_name
        FROM sys.views v
        JOIN sys.schemas s ON s.schema_id = v.schema_id
        ORDER BY s.name, v.name;
    """).fetchall()
    return rows


def get_cfg_sample(cur, schema, table, limit=20):
    try:
        rows = cur.execute(
            f"SELECT TOP ({limit}) * FROM [{schema}].[{table}];"
        ).fetchall()
        cols = [d[0] for d in cur.description]
        return cols, rows
    except Exception as e:
        return [], []


# =============================================================================
# MAIN DIAGNOSTIC
# =============================================================================

def main():
    r = Report()
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    r.h1(f"FUSION EPOS – DATABASE DIAGNOSTIC REPORT")
    r.info(f"Generated : {ts}")

    # --- Connect ---
    try:
        conn, ini_path, server, database = get_connection()
        cur = conn.cursor()
        r.info(f"INI File  : {ini_path}")
        r.info(f"Server    : {server}")
        r.info(f"Database  : {database}")
        r.ok("Connected successfully")
    except Exception as e:
        r.miss(f"CONNECTION FAILED: {e}")
        r.save(os.path.join(REPORT_DIR, f"DB_Diagnostic_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"))
        sys.exit(1)

    # =========================================================================
    # 1. SCHEMAS
    # =========================================================================
    r.h1("1. SCHEMAS")
    schema_rows = cur.execute(
        "SELECT name FROM sys.schemas WHERE name NOT IN "
        "('sys','INFORMATION_SCHEMA','guest','db_owner','db_accessadmin',"
        "'db_securityadmin','db_ddladmin','db_backupoperator','db_datareader',"
        "'db_datawriter','db_denydatareader','db_denydatawriter') "
        "ORDER BY name;"
    ).fetchall()
    existing_schemas = [r_[0] for r_ in schema_rows]

    for s in TARGET_SCHEMAS:
        if s in existing_schemas:
            r.ok(f"Schema exists: [{s}]")
        else:
            r.miss(f"Schema NOT FOUND: [{s}]")

    r.blank()
    r.info(f"All schemas in DB: {', '.join(existing_schemas)}")

    # =========================================================================
    # 2. ALL TABLES (full inventory)
    # =========================================================================
    r.h1("2. ALL TABLES IN DATABASE")
    all_tables = get_all_tables(cur)
    current_schema = None
    for row in all_tables:
        if row.schema_name != current_schema:
            current_schema = row.schema_name
            r.h3(f"Schema: [{current_schema}]")
        r.info(f"{row.schema_name}.{row.table_name}   "
               f"(created: {str(row.create_date)[:10]}  modified: {str(row.modify_date)[:10]})")

    if not all_tables:
        r.warn("No tables found in database!")

    # =========================================================================
    # 3. TABLES OF INTEREST – detailed inspection
    # =========================================================================
    r.h1("3. TABLES OF INTEREST – DETAIL")

    for schema, table in TABLES_OF_INTEREST:
        r.h2(f"{schema}.{table}")
        type_desc = object_exists(cur, schema, table, obj_types=("U",))

        if not type_desc:
            r.miss(f"Table does not exist: {schema}.{table}")
            continue

        # Row count
        rc = get_row_count(cur, schema, table)
        r.ok(f"EXISTS  |  Row count: {rc}")

        # Columns
        cols = get_columns(cur, schema, table)
        if cols:
            r.h3("Columns")
            for c in cols:
                identity_flag = " [IDENTITY]" if c.IsIdentity else ""
                nullable_flag = " NULL" if c.IS_NULLABLE == "YES" else " NOT NULL"
                length_info   = f"({c.CHARACTER_MAXIMUM_LENGTH})" if c.CHARACTER_MAXIMUM_LENGTH else ""
                default_info  = f"  DEFAULT={c.COLUMN_DEFAULT}" if c.COLUMN_DEFAULT else ""
                r.col(f"{c.COLUMN_NAME:<40} {c.DATA_TYPE}{length_info}{nullable_flag}{identity_flag}{default_info}")
        else:
            r.warn("Could not retrieve columns")

        # Indexes
        try:
            idxs = get_indexes(cur, schema, table)
            if idxs:
                r.h3("Indexes")
                for ix in idxs:
                    pk_flag   = " [PK]" if ix.is_primary_key else ""
                    uniq_flag = " [UNIQUE]" if ix.is_unique and not ix.is_primary_key else ""
                    r.col(f"{ix.name}  ({ix.type_desc}{pk_flag}{uniq_flag})  keys: {ix.key_cols}")
        except Exception as e:
            r.warn(f"Could not retrieve indexes: {e}")

    # =========================================================================
    # 4. STORED PROCEDURES
    # =========================================================================
    r.h1("4. STORED PROCEDURES")
    all_procs = get_all_procs(cur)
    r.info(f"Total procedures in DB: {len(all_procs)}")
    r.blank()

    for row in all_procs:
        r.info(f"{row.schema_name}.{row.proc_name}   "
               f"(created: {str(row.create_date)[:10]}  modified: {str(row.modify_date)[:10]})")

    r.h2("Procedures of interest")
    for schema, proc in PROCS_OF_INTEREST:
        type_desc = object_exists(cur, schema, proc, obj_types=("P",))
        if type_desc:
            r.ok(f"EXISTS: [{schema}].[{proc}]")
        else:
            r.miss(f"NOT FOUND: [{schema}].[{proc}]")

    # =========================================================================
    # 5. SYNONYMS
    # =========================================================================
    r.h1("5. SYNONYMS")
    synonyms = get_synonyms(cur)
    if synonyms:
        for s in synonyms:
            r.ok(f"[{s.schema_name}].[{s.synonym_name}]  →  {s.base_object_name}")
    else:
        r.info("No synonyms found")

    # =========================================================================
    # 6. VIEWS
    # =========================================================================
    r.h1("6. VIEWS")
    views = get_all_views(cur)
    if views:
        for v in views:
            r.info(f"{v.schema_name}.{v.view_name}")
    else:
        r.info("No views found")

    # =========================================================================
    # 7. KEY CONFIG / REFERENCE DATA SAMPLES
    # =========================================================================
    r.h1("7. CONFIG + REFERENCE DATA SAMPLES")

    sample_tables = [
        ("CFG", "Fusion_EPOS_File_Locations", 50),
        ("CFG", "Email_Distribution", 20),
        ("CFG", "Dynamics_Stores", 10),
        ("CFG", "Products", 5),
        ("EXC", "TransferOrder_Control", 10),
    ]

    for schema, table, limit in sample_tables:
        r.h2(f"{schema}.{table}  (up to {limit} rows)")
        type_desc = object_exists(cur, schema, table, obj_types=("U",))
        if not type_desc:
            r.miss(f"Table does not exist")
            continue

        cols, rows = get_cfg_sample(cur, schema, table, limit)
        if not rows:
            r.info("(empty table)")
            continue

        # Header row
        col_widths = [min(len(c), 30) for c in cols]
        header = "  ".join(str(c).ljust(w) for c, w in zip(cols, col_widths))
        r.col(header)
        r.col("-" * min(len(header), 120))
        for row_ in rows:
            values = "  ".join(str(v if v is not None else "NULL").ljust(w)[:w]
                                for v, w in zip(row_, col_widths))
            r.col(values)

    # =========================================================================
    # 8. INT STAGING TABLE NAME RESOLUTION (SalesOrderStage vs SalesOrderStaging)
    # =========================================================================
    r.h1("8. STAGING TABLE NAME RESOLUTION")
    r.info("Checking: INT.SalesOrderStage vs INT.SalesOrderStaging")
    r.blank()

    for variant in ("SalesOrderStage", "SalesOrderStaging"):
        type_desc = object_exists(cur, "INT", variant, obj_types=("U", "SN", "V"))
        if type_desc:
            rc = get_row_count(cur, "INT", variant)
            r.ok(f"INT.{variant}  EXISTS  ({type_desc})  rows={rc}")
        else:
            r.miss(f"INT.{variant}  does not exist")

    # =========================================================================
    # 9. RECENT EXC.Process_Transaction_Log ENTRIES
    # =========================================================================
    r.h1("9. RECENT AUDIT LOG ENTRIES (last 20)")
    type_desc = object_exists(cur, "EXC", "Process_Transaction_Log", obj_types=("U",))
    if not type_desc:
        r.miss("EXC.Process_Transaction_Log does not exist")
    else:
        try:
            rows = cur.execute("""
                SELECT TOP 20
                    Log_ID, Run_ID,
                    Process_Name, SubProcess_Name, Step_Code,
                    Execution_Status, Severity_Level,
                    Rows_Affected, Message,
                    CONVERT(varchar(19), Start_UTC, 120) AS Start_UTC
                FROM EXC.Process_Transaction_Log
                ORDER BY Log_ID DESC;
            """).fetchall()
            if not rows:
                r.info("(no rows yet)")
            for row_ in rows:
                r.info(f"[{row_[9]}] {row_[2]}.{row_[3]} | {row_[4]} | {row_[5]} | {row_[7]} rows | {row_[8]}")
        except Exception as e:
            r.warn(f"Could not query audit log: {e}")

    # =========================================================================
    # 10. SUMMARY
    # =========================================================================
    r.h1("10. SUMMARY – OBJECTS REQUIRED FOR SAP STO INBOUND")

    required = [
        ("TABLE",  "RAW",  "SAP_TO_Responses"),
        ("TABLE",  "RAW",  "SAP_TO_Responses_Lines"),
        ("TABLE",  "INT",  "SAP_STO_Staging"),
        ("TABLE",  "EXC",  "TransferOrder_Control"),
        ("TABLE",  "INT",  "TransferOrderHeader"),
        ("TABLE",  "INT",  "TransferOrderLine"),
        ("TABLE",  "EXC",  "Process_Transaction_Log"),
        ("PROC",   "EXC",  "usp_EPOS_Generate_Weekly_Orders_Step4"),
        ("PROC",   "EXC",  "usp_EPOS_Generate_Weekly_Orders_Step6"),
    ]

    all_present = True
    for obj_type, schema, name in required:
        type_filter = ("U",) if obj_type == "TABLE" else ("P",)
        found = object_exists(cur, schema, name, obj_types=type_filter)
        if found:
            r.ok(f"[{obj_type}] {schema}.{name}")
        else:
            r.miss(f"[{obj_type}] {schema}.{name}  ← NEEDS CREATING")
            all_present = False

    r.blank()
    if all_present:
        r.ok("ALL required objects present – DDL script does not need to run.")
    else:
        r.warn("One or more objects are missing – run SAP_STO_DDL.sql to create them.")

    # Save report
    stamp       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(REPORT_DIR, f"DB_Diagnostic_{stamp}.txt")
    r.save(report_path)

    conn.close()
    return 0 if all_present else 1


if __name__ == "__main__":
    raise SystemExit(main())
