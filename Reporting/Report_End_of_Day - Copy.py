"""
Synovia Fusion – Step 6 Backlog Runner
=======================================
Stages multiple READY Year/Week combinations into INT.SalesOrderStage
in sequence, oldest-first.

Use this when weeks have accumulated in CTL as READY without being staged —
e.g. Weeks 3, 4, 5, 6 all READY after Week 7 was last processed.

Edit BACKLOG_WEEKS below, then run once. Each week is a separate SP call
with its own Run_ID/Txn_ID. The script stops on any failure so later
weeks are not staged on top of a broken earlier one.

Secrets:
  ENV FUSION_EPOS_BOOTSTRAP_PASSWORD → INI password= → RuntimeError
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from typing import List, Tuple, Optional

from _fusion_shared import connect_autocommit

# =============================================================================
# !! EDIT THIS BEFORE RUNNING !!
# List weeks to stage in order: oldest first.
# Format: (year_2digit, week)
# =============================================================================
BACKLOG_WEEKS: List[Tuple[int, int]] = [
    (26, 3),
    (26, 4),
    (26, 5),
    (26, 6),
]

# =============================================================================
# RUNTIME CONFIG
# =============================================================================
ENVIRONMENT       = "PROD"
SYSTEM_NAME       = "Fusion_EPOS"
DATA_AREA_ID      = "jbro"    # hard-enforced by SP anyway
SALES_UNIT_SYMBOL = "EA"      # hard-enforced by SP anyway
FILE_ID: Optional[int] = None # None = auto-resolve per week
REQUESTED_SHIP_DATE = None    # None = SP defaults to today UTC


# =============================================================================
# HELPERS
# =============================================================================

def run_step6_for_week(cn, year: int, week: int) -> dict:
    """Call Step6 SP for a single Year/Week. Returns the summary row as dict."""
    run_id         = int(time.time() * 1000)
    txn_id         = run_id + 10
    correlation_id = uuid.uuid4()

    print(f"\n  Run_ID={run_id}  Txn_ID={txn_id}  Correlation_ID={correlation_id}")

    cur = cn.cursor()
    cur.execute("""
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
    """, (
        ENVIRONMENT,
        SYSTEM_NAME,
        run_id,
        txn_id,
        str(correlation_id),
        year,
        week,
        FILE_ID,
        DATA_AREA_ID,
        SALES_UNIT_SYMBOL,
        REQUESTED_SHIP_DATE,
    ))

    row = cur.fetchone()
    summary = {}
    if row:
        summary = {d[0]: v for d, v in zip(cur.description, row)}

    # Pull telemetry for this run
    cur.execute("""
        SELECT Step_Code, Severity_Level, Execution_Status,
               Rows_Read, Rows_Affected, Message, Error_Message
        FROM EXC.Process_Transaction_Log
        WHERE Run_ID = ?
        ORDER BY Log_ID ASC;
    """, (run_id,))

    failed = []
    for r in cur.fetchall():
        status = r[2]
        flag = "  ✓" if status in ("SUCCESS", "INFO") else "  ✗"
        print(f"    {flag} [{r[1]:<8}] {r[0]:<20} {status:<10}  rows={r[4]}  {r[5] or ''}")
        if r[6]:
            print(f"         ERROR: {r[6]}")
        if status in ("FAIL", "FAILED", "ERROR"):
            failed.append(r[0])

    summary["_failed_steps"] = failed
    summary["_run_id"]       = run_id
    return summary


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    if not BACKLOG_WEEKS:
        print("BACKLOG_WEEKS is empty — nothing to do.")
        return 0

    print("=" * 70)
    print("Synovia Fusion – Step 6 Backlog Runner")
    print("=" * 70)
    print(f"Weeks to stage (oldest first): {BACKLOG_WEEKS}")
    print()

    results = []

    with connect_autocommit("Fusion_EPOS_Step6_Backlog") as cn:
        for year, week in BACKLOG_WEEKS:
            print(f"─── Staging Year={year} Week={week:02d} " + "─" * 40)
            try:
                summary = run_step6_for_week(cn, year, week)
                results.append((year, week, summary))

                inserted = summary.get("INT_RowsInserted", 0)
                healed   = summary.get("INT_RowsHealed",   0)
                lines    = summary.get("CTL_LinesUpdated", 0)
                headers  = summary.get("CTL_HeadersUpdated", 0)

                print(f"\n  Summary → INT inserted={inserted}  healed={healed}"
                      f"  CTL lines={lines}  CTL headers={headers}")

                if summary["_failed_steps"]:
                    print(f"\n  ✗ FAILED steps: {summary['_failed_steps']}")
                    print(f"  Stopping backlog — fix Week {week} before continuing.")
                    break

                print(f"  ✓ Week {week} staged successfully.")

                # Brief pause between weeks to avoid run_id collision (ms precision)
                time.sleep(0.1)

            except Exception as e:
                print(f"\n  ✗ EXCEPTION staging Year={year} Week={week}: {e}")
                print(f"  Stopping backlog run.")
                results.append((year, week, {"_failed_steps": [str(e)], "_run_id": None}))
                break

    # ── Final report ─────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("Backlog Run Complete")
    print("=" * 70)

    all_ok = True
    for year, week, s in results:
        ok = not s.get("_failed_steps")
        mark = "✓ PASS" if ok else "✗ FAIL"
        print(f"  {mark}  Year={year} Week={week:02d}"
              f"  INT_Inserted={s.get('INT_RowsInserted','?')}"
              f"  CTL_Lines={s.get('CTL_LinesUpdated','?')}"
              f"  CTL_Headers={s.get('CTL_HeadersUpdated','?')}")
        if not ok:
            all_ok = False

    weeks_done   = sum(1 for _, _, s in results if not s.get("_failed_steps"))
    weeks_remain = len(BACKLOG_WEEKS) - weeks_done

    print(f"\n  Staged: {weeks_done}/{len(BACKLOG_WEEKS)} weeks")
    if weeks_remain:
        print(f"  Remaining: {weeks_remain} week(s) NOT staged — review errors above.")
        return 1

    print("\n  All backlog weeks staged. Proceed to Step7_PreOrdersCreationChecks.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())