from _fusion_shared import connect_autocommit
import time, uuid
from typing import Any, Optional, Tuple

ENVIRONMENT = "PROD"
SYSTEM_NAME = "Fusion_EPOS"
FILE_ID = None
YEAR = None
WEEK = None
MARK_RAW_PROCESSED = True

def main() -> None:

    # Run_ID + Txn_ID are BIGINT; Correlation_ID is GUID
    run_id = int(time.time() * 1000)
    txn_id = run_id + 10
    correlation_id = uuid.uuid4()


    conn_str = (
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )

    cn = pyodbc.connect(conn_str)
    cn.autocommit = True

    try:
        cur = cn.cursor()

        sql = """
        EXEC EXC.usp_EPOS_Generate_Weekly_Orders_Step4
              @Environment = ?
            , @System_Name = ?
            , @Run_ID = ?
            , @Txn_ID = ?
            , @Correlation_ID = ?
            , @File_ID = ?
            , @Year = ?
            , @Week = ?
            , @MarkRawProcessed = ?;
        """

        params = (
            ENVIRONMENT,
            SYSTEM_NAME,
            run_id,
            txn_id,
            str(correlation_id),  # pyodbc accepts GUID as string
            FILE_ID,
            YEAR,
            WEEK,
            1 if MARK_RAW_PROCESSED else 0,
        )
        cur.execute(sql, params)
        row = cur.fetchone()

        verify_sql = """
        SELECT TOP (50)
              Log_ID, Run_ID, Txn_ID, Correlation_ID,
              Step_Code, Step_Name, Step_Category,
              Severity_Level, Execution_Status,
              Rows_Read, Rows_Affected,
              Message, Error_Message,
              Start_UTC, End_UTC
        FROM EXC.Process_Transaction_Log
        WHERE Run_ID = ?
        ORDER BY Log_ID DESC;
        """
        cur.execute(verify_sql, run_id)
        logs = cur.fetchall()
        for r in logs:
                "Log_ID": r[0],
                "Run_ID": r[1],
                "Txn_ID": r[2],
                "Correlation_ID": str(r[3]),
                "Step_Code": r[4],
                "Step_Name": r[5],
                "Step_Category": r[6],
                "Severity_Level": r[7],
                "Execution_Status": r[8],
                "Rows_Read": r[9],
                "Rows_Affected": r[10],
                "Message": r[11],
                "Error_Message": r[12],
                "Start_UTC": str(r[13]),
                "End_UTC": str(r[14]),
            }, default=str))

    except Exception as e:
        raise
    finally:
        cn.close()

if __name__ == "__main__":
    main()
