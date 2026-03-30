"""
export_stored_procedures.py
===========================
Connects to Fusion_EPOS_Production, downloads every stored procedure
and writes one .sql file per procedure to the target UNC path.

Usage:
    python export_stored_procedures.py

Dependencies:
    pip install pyodbc
"""

import configparser
import os
import re
import sys
import pyodbc
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
INI_FILE   = r"D:\Configuration\Fusion_EPOS_Production.ini"
OUTPUT_DIR = r"\\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4\Fusion_Duracell_EPOS\Fusion_EPOS_Production\SQL"

# ── Helpers ───────────────────────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}]  {msg}")


def read_ini(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"INI file not found: {path}")
    cfg = configparser.ConfigParser()
    cfg.read(path, encoding="utf-8")
    section = "Fusion_EPOS_Production"
    if section not in cfg:
        raise KeyError(f"Section [{section}] not found in {path}")
    return dict(cfg[section])


def build_connection_string(c: dict) -> str:
    return (
        f"DRIVER={{{c['driver']}}};"
        f"SERVER={c['server']};"
        f"DATABASE={c['database']};"
        f"UID={c['user']};"
        f"PWD={c['password']};"
        f"Encrypt={c.get('encrypt', 'yes')};"
        f"TrustServerCertificate={c.get('trust_server_certificate', 'no')};"
    )


def safe_filename(schema: str, name: str) -> str:
    """Return  SCHEMA.ProcName.sql  with any unsafe chars stripped."""
    raw = f"{schema}.{name}"
    return re.sub(r'[\\/:*?"<>|]', "_", raw) + ".sql"


def get_procedures(cursor) -> list[dict]:
    """
    Return every user-defined stored procedure with schema, name and
    the full definition text (handles procedures split across multiple
    syscomments rows).
    """
    cursor.execute("""
        SELECT
            s.name          AS [schema],
            p.name          AS [procedure],
            p.object_id     AS [object_id],
            p.create_date   AS [created],
            p.modify_date   AS [modified]
        FROM sys.procedures  p
        JOIN sys.schemas     s ON s.schema_id = p.schema_id
        WHERE p.is_ms_shipped = 0
        ORDER BY s.name, p.name
    """)
    return [
        {
            "schema":    row[0],
            "name":      row[1],
            "object_id": row[2],
            "created":   row[3],
            "modified":  row[4],
        }
        for row in cursor.fetchall()
    ]


def get_definition(cursor, object_id: int) -> str:
    """
    Use OBJECT_DEFINITION() — returns the full text in one call,
    handles procedures > 4000 chars without the syscomments
    splitting problem.
    """
    cursor.execute("SELECT OBJECT_DEFINITION(?)", (object_id,))
    row = cursor.fetchone()
    return (row[0] or "").strip() if row else ""


def build_sql_file(proc: dict, definition: str, database: str) -> str:
    """Wrap the definition in a standard header and USE statement."""
    header = f"""\
-- =============================================================
--  Procedure : [{proc['schema']}].[{proc['name']}]
--  Database  : {database}
--  Exported  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
--  Created   : {proc['created']}
--  Modified  : {proc['modified']}
-- =============================================================
USE [{database}]
GO

"""
    # Ensure the definition starts with CREATE OR ALTER / CREATE
    body = definition

    # If definition uses CREATE PROCEDURE, emit a DROP-safe version
    # by replacing CREATE with CREATE OR ALTER (SQL Server 2016+)
    # Falls back to plain definition if already uses OR ALTER.
    if re.match(r'(?i)\s*CREATE\s+(PROCEDURE|PROC)\b', body) and \
       not re.match(r'(?i)\s*CREATE\s+OR\s+ALTER\s+(PROCEDURE|PROC)\b', body):
        body = re.sub(
            r'(?i)(CREATE)\s+(PROCEDURE|PROC)\b',
            r'CREATE OR ALTER \2',
            body,
            count=1
        )

    return header + body + "\nGO\n"


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log("=" * 65)
    log("Fusion_EPOS_Production — Stored Procedure Export")
    log("=" * 65)
    log(f"INI file   : {INI_FILE}")
    log(f"Output dir : {OUTPUT_DIR}")
    log("=" * 65)

    # 1. Read config & connect
    log("Reading connection config …")
    cfg     = read_ini(INI_FILE)
    db_name = cfg["database"]
    log(f"  Server   : {cfg['server']}")
    log(f"  Database : {db_name}")
    log(f"  User     : {cfg['user']}")

    log("Connecting …")
    try:
        conn   = pyodbc.connect(build_connection_string(cfg), timeout=30)
        conn.autocommit = True
        cursor = conn.cursor()
        log("  Connected OK")
    except pyodbc.Error as e:
        log(f"  ERROR – connection failed: {e}")
        sys.exit(1)

    # 2. Ensure output directory exists
    log(f"Checking output directory …")
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        log(f"  Directory OK")
    except OSError as e:
        log(f"  ERROR – cannot access/create output directory: {e}")
        cursor.close()
        conn.close()
        sys.exit(1)

    # 3. Fetch procedure list
    log("Fetching procedure list …")
    procs = get_procedures(cursor)
    log(f"  Found {len(procs)} stored procedure(s)")

    if not procs:
        log("  Nothing to export. Exiting.")
        cursor.close()
        conn.close()
        sys.exit(0)

    # 4. Export each procedure
    log("-" * 65)
    log(f"  {'#':<6}  {'Schema':<20}  {'Procedure'}")
    log("-" * 65)

    exported  = 0
    skipped   = 0
    errors    = []

    for i, proc in enumerate(procs, start=1):
        label = f"[{proc['schema']}].[{proc['name']}]"
        try:
            definition = get_definition(cursor, proc["object_id"])

            if not definition:
                log(f"  {i:<6}  SKIP (encrypted / no definition)  {label}")
                skipped += 1
                continue

            sql_content = build_sql_file(proc, definition, db_name)
            filename    = safe_filename(proc["schema"], proc["name"])
            filepath    = os.path.join(OUTPUT_DIR, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(sql_content)

            log(f"  {i:<6}  OK   {label}  →  {filename}")
            exported += 1

        except Exception as e:
            log(f"  {i:<6}  ERROR  {label}  :  {e}")
            errors.append((label, str(e)))

    # 5. Summary
    log("=" * 65)
    log(f"  Exported : {exported}")
    log(f"  Skipped  : {skipped}  (encrypted or no definition)")
    log(f"  Errors   : {len(errors)}")
    if errors:
        log("  Failed procedures:")
        for name, err in errors:
            log(f"    {name}  —  {err}")
    log(f"  Output   : {OUTPUT_DIR}")
    log("=" * 65)
    log("EXPORT COMPLETE")
    log("=" * 65)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
