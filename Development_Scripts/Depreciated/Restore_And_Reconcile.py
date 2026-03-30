#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Restore_And_Reconcile.py
# Checks DB for each CSV in Processed folder
# Restores to Inbound if OrderId not in RAW.EPOS_TransferRequest
# Outputs reconciliation Excel to D:\April2026\DUR

import os
import shutil
import socket
import pyodbc
import configparser
import pandas as pd
from pathlib import Path
from datetime import datetime

SCRIPT_NAME  = "Restore_And_Reconcile.py"
MACHINE_NAME = socket.gethostname()

PROCESSED_DIR = r"\\PL-AZ-INT-PRD\D_Drive\FusionHub\Fusion_EPOS\Transfer_Request_Inbound\Processed"
INBOUND_DIR   = r"\\PL-AZ-INT-PRD\D_Drive\FusionHub\Fusion_EPOS\Transfer_Request_Inbound"
OUTPUT_DIR    = Path(r"D:\April2026\DUR")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TIMESTAMP     = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE   = OUTPUT_DIR / f"Order_Reconciliation_{TIMESTAMP}.xlsx"

CONFIG_SECTION = "Fusion_EPOS_Production"
CONFIG_PATH    = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"

# =========================================================
# INI / DB
# =========================================================
def resolve_ini(path_str):
    p = Path(path_str)
    if p.is_dir():
        c = p / "Master_ini_config.ini"
        if not c.exists():
            raise FileNotFoundError(f"Master_ini_config.ini not found in {p}")
        return c
    if p.is_file():
        return p
    raise FileNotFoundError(f"Config path not found: {p}")

INI_PATH = resolve_ini(CONFIG_PATH)
cfg = configparser.ConfigParser()
cfg.read(INI_PATH)
db = cfg[CONFIG_SECTION]

CONN_STR = (
    f"DRIVER={{{db.get('driver')}}};"
    f"SERVER={db.get('server')};"
    f"DATABASE={db.get('database')};"
    f"UID={db.get('user')};PWD={db.get('password')};"
    f"Encrypt={db.get('encrypt','yes')};"
    f"TrustServerCertificate={db.get('trust_server_certificate','no')};"
)

def get_conn():
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = True
    return conn

# =========================================================
# CONSOLE HELPERS
# =========================================================
_W = 65
def _ts(): return datetime.now().strftime("%H:%M:%S")
def con_section(t): print(f"\n{'='*_W}\n  {t}\n{'='*_W}")
def con_info(m):    print(f"[{_ts()}]  INFO   {m}")
def con_ok(m):      print(f"[{_ts()}]  OK     {m}")
def con_warn(m):    print(f"[{_ts()}]  WARN   {m}")
def con_error(m):   print(f"[{_ts()}]  ERROR  {m}")
def con_skip(m):    print(f"[{_ts()}]  SKIP   {m}")

# =========================================================
# DB CHECKS
# =========================================================
CHECK_RAW_SQL = """
SELECT
    OrderId,
    COUNT(*)            AS [RecordCount],
    MIN(LoadTimestamp)  AS FirstLoaded,
    MAX(LoadTimestamp)  AS LastLoaded,
    MAX(SynProcessed)   AS SynProcessed,
    MAX(SynStaged)      AS SynStaged
FROM RAW.EPOS_TransferRequest
WHERE OrderId = ?
GROUP BY OrderId;
"""

CHECK_STG_SQL = """
SELECT
    OrderId,
    ScheduledDeliveryDate,
    ScheduledShippingDate,
    DateChanged,
    ChangeReason,
    SynProcessed
FROM STG.WASP_STO_Scheduled
WHERE OrderId = ?;
"""

CHECK_CONTROL_SQL = """
SELECT
    c.TransferControlId,
    c.OrderId,
    c.MasterStatus,
    c.DynamicsDocumentNo,
    c.DynamicsResponseCode,
    c.SapStatus,
    c.SapResponseCode,
    c.CreatedAt
FROM EXC.TransferOrder_Control c
WHERE c.OrderId = ?;
"""

def check_order_in_db(conn, order_id):
    """Returns dict with status across all pipeline tables."""
    result = {
        "OrderId":              order_id,
        "InRAW":                False,
        "RAW_RecordCount":      0,
        "RAW_FirstLoaded":      None,
        "RAW_SynProcessed":     None,
        "RAW_SynStaged":        None,
        "InSTG":                False,
        "STG_DeliveryDate":     None,
        "STG_ShippingDate":     None,
        "STG_ChangeReason":     None,
        "STG_SynProcessed":     None,
        "InControl":            False,
        "ControlId":            None,
        "MasterStatus":         None,
        "DynamicsDocumentNo":   None,
        "DynamicsResponseCode": None,
        "SapStatus":            None,
        "SapResponseCode":      None,
    }

    # RAW check
    row = conn.execute(CHECK_RAW_SQL, order_id).fetchone()
    if row:
        result["InRAW"]            = True
        result["RAW_RecordCount"]  = row[1]
        result["RAW_FirstLoaded"]  = row[2]
        result["RAW_SynProcessed"] = row[4]
        result["RAW_SynStaged"]    = row[5]

    # STG check
    row = conn.execute(CHECK_STG_SQL, order_id).fetchone()
    if row:
        result["InSTG"]            = True
        result["STG_DeliveryDate"] = row[1]
        result["STG_ShippingDate"] = row[2]
        result["STG_ChangeReason"] = row[4]
        result["STG_SynProcessed"] = row[5]

    # Control check
    row = conn.execute(CHECK_CONTROL_SQL, order_id).fetchone()
    if row:
        result["InControl"]            = True
        result["ControlId"]            = row[0]
        result["MasterStatus"]         = row[2]
        result["DynamicsDocumentNo"]   = row[3]
        result["DynamicsResponseCode"] = row[4]
        result["SapStatus"]            = row[5]
        result["SapResponseCode"]      = row[6]

    return result

# =========================================================
# MAIN
# =========================================================
def main():
    con_section("RESTORE & RECONCILE — STARTUP")
    con_info(f"Processed Dir : {PROCESSED_DIR}")
    con_info(f"Inbound Dir   : {INBOUND_DIR}")
    con_info(f"Output File   : {OUTPUT_FILE}")
    con_info(f"DB            : {db.get('server')} / {db.get('database')}")

    conn = get_conn()
    con_ok("DB connection established")

    # ── Scan Processed folder ─────────────────────────────
    con_section("SCANNING PROCESSED FOLDER")
    try:
        all_files = [
            f for f in os.listdir(PROCESSED_DIR)
            if f.lower().endswith(".csv")
        ]
    except Exception as e:
        con_error(f"Cannot read Processed folder: {e}")
        return

    if not all_files:
        con_warn("No CSV files found in Processed folder.")
        return

    con_ok(f"Found {len(all_files)} CSV file(s) in Processed folder")

    # ── Process each file ─────────────────────────────────
    con_section("CHECKING EACH FILE AGAINST DATABASE")

    recon_rows  = []
    restored    = []
    skipped     = []
    errors      = []

    for file_name in sorted(all_files):
        # Extract OrderId from filename (strip timestamp suffix if present)
        base       = os.path.splitext(file_name)[0]
        order_id   = base.split("_")[0]   # handles 594689_20260330_153131 → 594689
        source_path = os.path.join(PROCESSED_DIR, file_name)

        con_info(f"  File: {file_name}  |  OrderId: {order_id}")

        # Check DB
        try:
            db_status = check_order_in_db(conn, order_id)
        except Exception as e:
            con_error(f"  DB check failed for {order_id}: {e}")
            errors.append(file_name)
            db_status = {"OrderId": order_id}

        in_raw     = db_status.get("InRAW", False)
        in_control = db_status.get("InControl", False)
        master_status = db_status.get("MasterStatus")

        # Determine pipeline stage
        if in_control and master_status in ("SENT_DYNAMICS", "SENT_SAP", "VALIDATED"):
            pipeline_stage = f"PROCESSED — {master_status}"
        elif in_raw:
            pipeline_stage = "IN RAW — Awaiting scheduling/staging"
        elif db_status.get("InSTG"):
            pipeline_stage = "IN STG — Awaiting SP"
        else:
            pipeline_stage = "NOT IN DB — Needs restore"

        # Restore decision
        if not in_raw and not in_control:
            dest = os.path.join(INBOUND_DIR, os.path.basename(file_name))
            # Avoid overwriting if already exists in inbound
            if os.path.exists(dest):
                con_skip(f"  Already exists in Inbound — skipping restore: {file_name}")
                action = "SKIPPED — Already in Inbound"
                skipped.append(file_name)
            else:
                try:
                    shutil.copy2(source_path, dest)
                    con_ok(f"  RESTORED → {dest}")
                    action = "RESTORED to Inbound"
                    restored.append(file_name)
                except Exception as e:
                    con_error(f"  RESTORE FAILED: {e}")
                    action = f"RESTORE FAILED: {e}"
                    errors.append(file_name)
        else:
            con_skip(f"  Already in DB ({pipeline_stage}) — no restore needed")
            action = "LEFT IN PROCESSED — Already in DB"
            skipped.append(file_name)

        recon_rows.append({
            "FileName":             file_name,
            "OrderId":              order_id,
            "Action":               action,
            "PipelineStage":        pipeline_stage,
            "InRAW":                db_status.get("InRAW"),
            "RAW_RecordCount":      db_status.get("RAW_RecordCount"),
            "RAW_FirstLoaded":      db_status.get("RAW_FirstLoaded"),
            "RAW_SynProcessed":     db_status.get("RAW_SynProcessed"),
            "RAW_SynStaged":        db_status.get("RAW_SynStaged"),
            "InSTG":                db_status.get("InSTG"),
            "STG_DeliveryDate":     db_status.get("STG_DeliveryDate"),
            "STG_ShippingDate":     db_status.get("STG_ShippingDate"),
            "STG_ChangeReason":     db_status.get("STG_ChangeReason"),
            "InControl":            db_status.get("InControl"),
            "ControlId":            db_status.get("ControlId"),
            "MasterStatus":         db_status.get("MasterStatus"),
            "DynamicsDocumentNo":   db_status.get("DynamicsDocumentNo"),
            "DynamicsResponseCode": db_status.get("DynamicsResponseCode"),
            "SapStatus":            db_status.get("SapStatus"),
            "SapResponseCode":      db_status.get("SapResponseCode"),
        })

    conn.close()

    # ── Write reconciliation Excel ────────────────────────
    con_section("WRITING RECONCILIATION OUTPUT")

    summary_rows = [
        {"Metric": "Total Files Found",    "Count": len(all_files)},
        {"Metric": "Restored to Inbound",  "Count": len(restored)},
        {"Metric": "Left in Processed",    "Count": len(skipped)},
        {"Metric": "Errors",               "Count": len(errors)},
        {"Metric": "Run Timestamp",        "Count": TIMESTAMP},
        {"Metric": "Machine",              "Count": MACHINE_NAME},
    ]

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:

        # Sheet 1 — Summary
        pd.DataFrame(summary_rows).to_excel(
            writer, sheet_name="00_Summary", index=False
        )

        # Sheet 2 — Full Reconciliation
        df_recon = pd.DataFrame(recon_rows)
        df_recon.to_excel(writer, sheet_name="01_Reconciliation", index=False)

        # Sheet 3 — Restored files only
        df_restored = df_recon[df_recon["Action"] == "RESTORED to Inbound"]
        df_restored.to_excel(writer, sheet_name="02_Restored", index=False)

        # Sheet 4 — Already in DB (left in Processed)
        df_skipped = df_recon[df_recon["Action"].str.startswith("LEFT")]
        df_skipped.to_excel(writer, sheet_name="03_Already_In_DB", index=False)

        # Sheet 5 — Errors
        df_errors = df_recon[df_recon["Action"].str.startswith("RESTORE FAILED")]
        df_errors.to_excel(writer, sheet_name="04_Errors", index=False)

        # Auto-fit all sheets
        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = max(
                    (len(str(cell.value)) for cell in col if cell.value),
                    default=10
                )
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    con_ok(f"Reconciliation written to: {OUTPUT_FILE}")

    # ── Final Summary ─────────────────────────────────────
    con_section("RUN COMPLETE")
    print(f"  Total files found  : {len(all_files)}")
    print(f"  Restored to Inbound: {len(restored)}")
    print(f"  Left in Processed  : {len(skipped)}")
    print(f"  Errors             : {len(errors)}")
    print(f"  Output file        : {OUTPUT_FILE}")

    if restored:
        print(f"\n  Files restored:")
        for f in restored:
            print(f"    → {f}")
    if errors:
        print(f"\n  Errors:")
        for f in errors:
            print(f"    ✘ {f}")


if __name__ == "__main__":
    main()