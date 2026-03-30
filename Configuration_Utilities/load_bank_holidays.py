"""
load_bank_holidays.py
=====================
Full-refresh load of Irish Bank Holidays from CSV into
CFG.Bank_Holiday_Calendar on Fusion_EPOS_Production.

Usage:
    python load_bank_holidays.py

Dependencies:
    pip install pyodbc pandas
"""

import configparser
import csv
import os
import sys
import pyodbc
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
INI_FILE = r"D:\Configuration\Fusion_EPOS_Production.ini"
CSV_FILE = r"D:\Configuration\irish_bank_holidays_2026_2035.csv"
TARGET_TABLE = "CFG.Bank_Holiday_Calendar"

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


def read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        # Normalise header names to lowercase for case-insensitive lookup
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]
        headers_lower = {h.lower(): h for h in (reader.fieldnames or [])}

        def col(row: dict, name: str) -> str:
            """Case-insensitive column lookup — raises clear error if missing."""
            key = headers_lower.get(name.lower())
            if key is None:
                available = list(row.keys())
                raise KeyError(
                    f"Column '{name}' not found in CSV. "
                    f"Available columns: {available}"
                )
            return row[key].strip()

        for row in reader:
            rows.append({
                "Holiday_Date":    col(row, "Date"),
                "Year":            int(col(row, "Year")),
                "Day":             col(row, "Day"),
                "Holiday_Name":    col(row, "Holiday_Name"),
                "Irish_Name":      col(row, "Irish_Name"),
                "Is_Delivery_Day": int(col(row, "Is_Delivery_Day")),
            })
    return rows


def parse_date(date_str: str):
    """Accept formats:  01 Jan 2026  |  1-Jan-26  |  2026-01-01"""
    for fmt in ("%d %b %Y", "%d-%b-%y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unrecognised date format: '{date_str}'")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log("=" * 60)
    log(f"Target table : {TARGET_TABLE}")
    log(f"INI file     : {INI_FILE}")
    log(f"CSV file     : {CSV_FILE}")
    log("=" * 60)

    # 1. Read config
    log("Reading connection config …")
    cfg = read_ini(INI_FILE)
    conn_str = build_connection_string(cfg)
    log(f"  Server   : {cfg['server']}")
    log(f"  Database : {cfg['database']}")
    log(f"  User     : {cfg['user']}")

    # 2. Read CSV
    log("Reading CSV …")
    rows = read_csv(CSV_FILE)
    log(f"  Rows read: {len(rows)}")

    # Parse and validate dates
    for r in rows:
        r["Holiday_Date"] = parse_date(r["Holiday_Date"])

    # 3. Connect
    log("Connecting to SQL Server …")
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        conn.autocommit = False
        cursor = conn.cursor()
        log("  Connected OK")
    except pyodbc.Error as e:
        log(f"  ERROR – could not connect: {e}")
        sys.exit(1)

    # 4. Full refresh inside a transaction
    log("Starting full-refresh transaction …")
    try:
        # 4a. Truncate
        log(f"  Truncating {TARGET_TABLE} …")
        cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
        log("  Truncate complete")

        # 4b. Insert
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE}
                (Holiday_Date, Year, Day, Holiday_Name, Irish_Name, Is_Delivery_Day)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        params = [
            (
                r["Holiday_Date"],
                r["Year"],
                r["Day"],
                r["Holiday_Name"],
                r["Irish_Name"],
                r["Is_Delivery_Day"],
            )
            for r in rows
        ]

        log(f"  Inserting {len(params)} rows …")
        cursor.executemany(insert_sql, params)
        log(f"  Insert complete  ({cursor.rowcount} rows affected)")

        # 4c. Verify row count
        cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
        count = cursor.fetchone()[0]
        log(f"  Post-insert row count: {count}")

        if count != len(rows):
            raise RuntimeError(
                f"Row count mismatch — expected {len(rows)}, found {count}"
            )

        # 4d. Commit
        conn.commit()
        log("  Transaction committed")

    except Exception as e:
        conn.rollback()
        log(f"  ERROR – transaction rolled back: {e}")
        cursor.close()
        conn.close()
        sys.exit(1)

    # 5. Summary
    cursor.execute(
        f"SELECT Year, COUNT(*) AS Holidays FROM {TARGET_TABLE} GROUP BY Year ORDER BY Year"
    )
    log("-" * 40)
    log("  Year  |  Holidays loaded")
    log("-" * 40)
    for yr, cnt in cursor.fetchall():
        log(f"  {yr}  |  {cnt}")
    log("-" * 40)

    cursor.close()
    conn.close()

    log("=" * 60)
    log(f"LOAD COMPLETE — {len(rows)} rows loaded into {TARGET_TABLE}")
    log("=" * 60)


if __name__ == "__main__":
    main()
