"""
Synovia Fusion – Step 6 Runner: Stage Weekly Orders into INT.SalesOrderStage
----------------------------------------------------------------------------
- Calls: EXC.usp_EPOS_Generate_Weekly_Orders_Step6
- Stages CTL READY proposal into INT.SalesOrderStage as STAGED
- Updates CTL statuses to SO_STAGED
- Logs steps into EXC.Process_Transaction_Log

Hard rules (enforced):
- dataAreaId = 'jbro'
- SalesUnitSymbol = 'EA'

Secrets:
- DB password from ENV FUSION_EPOS_BOOTSTRAP_PASSWORD, else INI password=
- Do not print passwords/conn strings
"""

from __future__ import annotations

import os
import time
import uuid
import configparser
from datetime import date
from typing import Any, Optional, Tuple

import pyodbc

# =============================================================================
# CONFIG
# =============================================================================
ENVIRONMENT = "PROD"
SYSTEM_NAME = "Fusion_EPOS"

# Scope (None means "auto-pick latest READY Year/Week")
YEAR: Optional[int] = None
WEEK: Optional[int] = None
FILE_ID: Optional[int] = None

# D365 staging fields (HARD RULES)
DATA_AREA_ID = "jbro"
SALES_UNIT_SYMBOL = "EA"
REQUESTED_SHIP_DATE: Optional[date] = None  # None -> SP defaults to today (UTC)

# =============================================================================
# DB BOOTSTRAP
# =============================================================================
BOOTSTRAP = {
    "driver": "ODBC Driver 17 for SQL Server",
    "server": "futureworks-sdi-db.database.windows.net",
    "database": "Fusion_EPOS_Production",
    "username": "SynFW_DB",
    "timeout": 60,
}

DB_PASSWORD_ENV = "FUSION_EPOS_BOOTSTRAP_PASSWORD"
INI_PATH = r"D:\Configuration\Fusion_EPOS_Production.ini"
INI_SECTION = "Fusion_EPOS_Production"


def resolve_ini(section: str, key: str) -> Optional[str]:
    if os.path.exists(INI_PATH):
        cfg = configparser.ConfigParser()
        cfg.read(INI_PATH)
        if section in cfg and key in cfg[section] and cfg[section][key].strip():
            return cfg[section][key].strip()
    return None


def resolve_db_password() -> str:
    pw = os.environ.get(DB_PASSWORD_ENV)
    if pw and pw.strip():
        return pw.strip()
    ini_pw = resolve_ini(INI_SECTION, "password")
    if ini_pw:
        return ini_pw
    raise RuntimeError(
        f"DB password not found. Set ENV '{DB_PASSWORD_ENV}' or INI '{INI_PATH}' [{INI_SECTION}] password=..."
    )


def connect() -> pyodbc.Connection:
    pw = resolve_db_password()
    conn_str = (
        f"DRIVER={{{BOOTSTRAP['driver']}}};"
        f"SERVER={BOOTSTRAP['server']};"
        f"DATABASE={BOOTSTRAP['database']};"
        f"UID={BOOTSTRAP['username']};"
        f"PWD={pw};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout={int(BOOTSTRAP['timeout'])};"
        f"APP=Fusion_EPOS_Step6_StageOrders;"
    )
    # autocommit True: SP controls its own transaction; logs insert cleanly
    return pyodbc.connect(conn_str, autocommit=True)


def main() -> int:
    run_id = int(time.time() * 1000)
    txn_id = run_id + 10
    correlation_id = uuid.uuid4()

    # Don’t print secrets, but do print the enforced constants.
    print(f"Step6 starting: Run_ID={run_id}, Txn_ID={txn_id}, Correlation_ID={correlation_id}")
    print(f"Scope: YEAR={YEAR}, WEEK={WEEK}, FILE_ID={FILE_ID}")
    print(f"Staging (ENFORCED): dataAreaId={DATA_AREA_ID}, unit={SALES_UNIT_SYMBOL}, requestedShipDate={REQUESTED_SHIP_DATE}")

    with connect() as cn:
        cur = cn.cursor()

        sql = """
        EXEC EXC.usp_EPOS_Generate_Weekly_Orders_Step6
              @Environment           = ?
            , @System_Name           = ?
            , @Run_ID                = ?
            , @Txn_ID                = ?
            , @Correlation_ID        = ?
            , @Year                  = ?
            , @Week                  = ?
            , @File_ID               = ?
            , @DataAreaId            = ?
            , @SalesUnitSymbol       = ?
            , @RequestedShippingDate = ?;
        """

        params: Tuple[Any, ...] = (
            ENVIRONMENT,
            SYSTEM_NAME,
            run_id,
            txn_id,
            str(correlation_id),  # pyodbc handles GUID as string
            YEAR,
            WEEK,
            FILE_ID,
            DATA_AREA_ID,
            SALES_UNIT_SYMBOL,
            REQUESTED_SHIP_DATE,
        )

        print("Calling EXC.usp_EPOS_Generate_Weekly_Orders_Step6 ...")
        cur.execute(sql, params)

        # SP returns a single-row summary resultset
        row = cur.fetchone()
        if row:
            print("\n=== Step 6 Summary ===")
            cols = [d[0] for d in cur.description]
            for c, v in zip(cols, row):
                print(f"{c}: {v}")
        else:
            print("No summary row returned (unexpected).")

        # Dump the latest logs for this run
        print("\n=== Latest EXC.Process_Transaction_Log rows for this Run_ID ===")
        cur.execute(
            """
            SELECT TOP (50)
                  Log_ID
                , Run_ID
                , Txn_ID
                , Correlation_ID
                , Step_Code
                , Step_Name
                , Step_Category
                , Severity_Level
                , Execution_Status
                , Rows_Read
                , Rows_Affected
                , Message
                , Error_Message
                , Start_UTC
                , End_UTC
            FROM EXC.Process_Transaction_Log
            WHERE Run_ID = ?
            ORDER BY Log_ID DESC;
            """,
            (run_id,),
        )
        for r in cur.fetchall():
            print(r)

    print("\nStep 6 complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
