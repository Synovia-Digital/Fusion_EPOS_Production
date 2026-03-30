"""
Synovia Fusion – EPOS Consignment Console Processor
====================================================
Interactive console tool that:

  1. Connects and shows the full STAGED queue (grouped by file/week)
  2. Asks how many orders to transmit
  3. Processes each order in real-time:
       - Claims (STAGED → InProgress)
       - Creates D365 Sales Order header  (SalesOrderHeadersV2)
       - Creates D365 Sales Order lines   (SalesOrderLinesV3)
       - Updates INT.SalesOrderStage      → SO_Confirmed  + D365_SONumber
       - Updates CTL.EPOS_Header          → SO_Confirmed  + D365_SO_Number
       - Updates CTL.EPOS_Line_Item       → SO_Confirmed
  4. Prints a final pass/fail summary

Secrets:
  ENV FUSION_EPOS_BOOTSTRAP_PASSWORD  → INI password=         → RuntimeError
  ENV FUSION_D365_CLIENT_SECRET       → Production_365.ini    → RuntimeError

Test mode (TEST_MODE = True):
  Builds all payloads and writes JSON to disk.
  No D365 API calls. No DB status changes.
"""

from __future__ import annotations

import configparser
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import pyodbc
import requests

from _fusion_shared import (
    connect_autocommit,
    resolve_d365_secret,
    ensure_dir,
    fmt_int,
    fmt_money,
    LOG_DIR,
    OUTPUT_ROOT,
)

# =============================================================================
# SWITCHES
# =============================================================================
TEST_MODE        = False   # True = JSON only, no D365 calls, no DB writes
WRITE_JSON       = True    # Write request/response bundles to disk
SHOW_PAYLOADS    = False   # Print full JSON payloads to console

# =============================================================================
# D365 CONFIG
# =============================================================================
D365_INI_PATH    = r"D:\Configuration\Production_365.ini"

SOURCE_UOM_ALLOWED = {"EA"}
UOM_MAP            = {"EA": "UN"}   # INT.SalesOrderStage → D365

# =============================================================================
# STATUS CONSTANTS
# =============================================================================
STAGE_READY       = "STAGED"
STAGE_INPROGRESS  = "InProgress"
STAGE_CONFIRMED   = "SO_Confirmed"
STAGE_FAILED      = "Failed"

CTL_HDR_CONFIRMED = "SO_Confirmed"
CTL_HDR_FAILED    = "SO_Failed"
CTL_LN_CONFIRMED  = "SO_Confirmed"

# =============================================================================
# RETRY
# =============================================================================
RETRY_STATUSES   = {429, 500, 502, 503, 504}
MAX_RETRIES      = 6
BACKOFF_BASE     = 2

# =============================================================================
# PATHS
# =============================================================================
JSON_ROOT = Path(OUTPUT_ROOT) / "D365_SalesOrders"

# =============================================================================
# LOGGING
# =============================================================================
ensure_dir(LOG_DIR)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(LOG_DIR) / "Process_Consignment_Console.log", encoding="utf-8"
        ),
    ],
)
log = logging.getLogger("Fusion_Consignment_Console")


# =============================================================================
# CONSOLE HELPERS
# =============================================================================

def _hr(char: str = "─", width: int = 70) -> str:
    return char * width

def _bar(done: int, total: int, width: int = 30) -> str:
    pct  = min(done / max(total, 1), 1.0)
    fill = int(pct * width)
    return f"[{'█' * fill}{'░' * (width - fill)}] {done}/{total}"

def _ask(prompt: str) -> str:
    try:
        return input(prompt).strip()
    except (KeyboardInterrupt, EOFError):
        print("\nAborted.")
        raise SystemExit(1)

def _confirm(prompt: str) -> bool:
    return _ask(f"{prompt} (Y/N): ").upper() == "Y"


# =============================================================================
# D365 AUTH
# =============================================================================

def _load_d365_config() -> dict:
    cfg = configparser.ConfigParser()
    cfg.read(D365_INI_PATH, encoding="utf-8")
    return {
        "tenant_id":    cfg["AUTH"]["tenant_id"],
        "client_id":    cfg["AUTH"]["client_id"],
        "client_secret": resolve_d365_secret(D365_INI_PATH),
        "resource":     cfg["AUTH"]["resource"],
        "login_url":    cfg["API"]["V1_login_url"],
        "odata_root":   cfg["API"]["odata_service_root"],
    }


def _get_token(d365: dict) -> str:
    resp = requests.post(
        d365["login_url"].format(tenant_id=d365["tenant_id"]),
        data={
            "grant_type":    "client_credentials",
            "client_id":     d365["client_id"],
            "client_secret": d365["client_secret"],
            "resource":      d365["resource"],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _post_with_retry(
    session: requests.Session,
    url: str,
    token: str,
    payload: dict,
    d365: dict,
    timeout: int = 60,
) -> tuple[requests.Response, str]:
    tok, last_exc = token, None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(url, headers=_auth_headers(tok), json=payload, timeout=timeout)
            if r.status_code == 401 and attempt == 1:
                tok = _get_token(d365)
                continue
            if r.status_code in RETRY_STATUSES:
                wait = BACKOFF_BASE ** min(attempt, 5)
                log.warning("HTTP %s — retry %d/%d in %ds", r.status_code, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            return r, tok
        except Exception as ex:
            last_exc = ex
            wait = BACKOFF_BASE ** min(attempt, 5)
            log.warning("POST exception attempt %d/%d: %s — retry in %ds", attempt, MAX_RETRIES, ex, wait)
            time.sleep(wait)
    raise RuntimeError(f"POST failed after {MAX_RETRIES} retries. Last: {last_exc}")


# =============================================================================
# QUEUE QUERY
# =============================================================================

def _get_queue(conn: pyodbc.Connection) -> list[dict]:
    """One row per file/week with STAGED orders."""
    rows = conn.cursor().execute("""
        SELECT
              f.File_ID
            , f.FileName
            , f.[Year]
            , f.[Week]
            , COUNT(DISTINCT i.CustomersOrderReference)  AS Store_Orders
            , COUNT(i.ItemNumber)                        AS Total_Lines
            , CAST(SUM(i.OrderedSalesQuantity) AS bigint) AS Total_Units
            , SUM(i.LineAmount)                          AS Total_Sales
        FROM RAW.EPOS_File f
        JOIN CTL.EPOS_Header h  ON h.Source_File_ID = f.File_ID
        JOIN INT.SalesOrderStage i
          ON  i.CustomersOrderReference = CAST(h.Docket_Reference AS varchar(50))
         AND  i.Integration_Status      = ?
        GROUP BY f.File_ID, f.FileName, f.[Year], f.[Week]
        ORDER BY f.[Year] DESC, f.[Week] DESC, f.File_ID DESC;
    """, (STAGE_READY,)).fetchall()
    return [
        {
            "File_ID":      r[0],
            "FileName":     r[1] or "",
            "Year":         int(r[2]),
            "Week":         int(r[3]),
            "Store_Orders": int(r[4]),
            "Total_Lines":  int(r[5]),
            "Total_Units":  int(r[6] or 0),
            "Total_Sales":  float(r[7] or 0),
        }
        for r in rows
    ]


def _get_staged_orders(conn: pyodbc.Connection, limit: int) -> list[str]:
    """Returns list of CustomersOrderReference to process, oldest-file-first."""
    rows = conn.cursor().execute("""
        SELECT DISTINCT TOP (?)
            i.CustomersOrderReference
        FROM INT.SalesOrderStage i
        JOIN CTL.EPOS_Header h
          ON  CAST(h.Docket_Reference AS varchar(50)) = i.CustomersOrderReference
        JOIN RAW.EPOS_File f ON f.File_ID = h.Source_File_ID
        WHERE i.Integration_Status = ?
        ORDER BY i.CustomersOrderReference;
    """, (limit, STAGE_READY)).fetchall()
    return [r[0] for r in rows]


# =============================================================================
# DB CAPABILITY CHECKS
# =============================================================================

def _stage_cols(conn: pyodbc.Connection) -> dict:
    rows = conn.cursor().execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'INT' AND TABLE_NAME = 'SalesOrderStage'
    """).fetchall()
    cols = {r[0].lower() for r in rows}
    return {
        "has_so_number":   "d365_sonumber" in cols,
        "has_error_msg":   "errormessage"  in cols or "error_message" in cols,
        "err_col":         "ErrorMessage"  if "errormessage" in cols
                           else ("Error_Message" if "error_message" in cols else None),
    }


# =============================================================================
# ORDER PROCESSOR
# =============================================================================

def _process_order(
    conn: pyodbc.Connection,
    cust_ref: str,
    run_dir: Path | None,
    session: requests.Session | None,
    token: str | None,
    d365: dict | None,
    stage_cap: dict,
    order_num: int,
    total: int,
) -> tuple[bool, str | None, str]:
    """
    Process a single order end-to-end.
    Returns (success, so_number, token_refreshed).
    """
    so_number = None
    tok = token

    try:
        print(f"\n  {_bar(order_num, total)}  {cust_ref}")

        # ── CLAIM ────────────────────────────────────────────────────────
        if not TEST_MODE:
            rows_claimed = conn.cursor().execute("""
                UPDATE INT.SalesOrderStage
                SET Integration_Status = ?
                WHERE CustomersOrderReference = ?
                  AND Integration_Status = ?
            """, (STAGE_INPROGRESS, cust_ref, STAGE_READY)).rowcount
            conn.commit()
            if rows_claimed == 0:
                log.warning("  %s — not claimable (status already changed)", cust_ref)
                return False, None, tok

        # ── HEADER DATA ──────────────────────────────────────────────────
        status_to_read = STAGE_INPROGRESS if not TEST_MODE else STAGE_READY
        row = conn.cursor().execute("""
            SELECT TOP 1
                dataAreaId,
                CustomerRequisitionNumber,
                OrderingCustomerAccountNumber,
                RequestedShippingDate
            FROM INT.SalesOrderStage
            WHERE CustomersOrderReference = ?
              AND Integration_Status      = ?
        """, (cust_ref, status_to_read)).fetchone()

        if not row:
            raise RuntimeError("No header row found in INT.SalesOrderStage after claim.")

        data_area, cust_req, cust_account, ship_date = row

        header_payload = {
            "dataAreaId":                    data_area,
            "CustomersOrderReference":       cust_ref,
            "CustomerRequisitionNumber":     cust_req,
            "OrderingCustomerAccountNumber": cust_account,
        }
        if ship_date:
            header_payload["RequestedShippingDate"] = str(ship_date)

        if SHOW_PAYLOADS:
            print("  HDR →", json.dumps(header_payload, indent=2))

        # ── D365 HEADER ──────────────────────────────────────────────────
        if TEST_MODE:
            so_number = f"TEST-{cust_ref}"
            hdr_response_text = json.dumps({"TEST_MODE": True, "SalesOrderNumber": so_number})
            log.info("  [TEST] Header payload built: %s", cust_ref)
        else:
            hdr_url      = f"{d365['odata_root']}/SalesOrderHeadersV2"
            hdr_resp, tok = _post_with_retry(session, hdr_url, tok, header_payload, d365)
            hdr_response_text = hdr_resp.text
            if hdr_resp.status_code not in (200, 201):
                raise RuntimeError(f"Header HTTP {hdr_resp.status_code}: {hdr_resp.text[:500]}")
            so_number = hdr_resp.json().get("SalesOrderNumber")
            if not so_number:
                raise RuntimeError(f"No SalesOrderNumber in D365 response: {hdr_resp.text[:300]}")
            log.info("  Header created: %s → D365 SO %s", cust_ref, so_number)

        # ── LINES ────────────────────────────────────────────────────────
        lines = conn.cursor().execute("""
            SELECT ItemNumber, OrderedSalesQuantity, SalesUnitSymbol, LineNumber
            FROM INT.SalesOrderStage
            WHERE CustomersOrderReference = ?
              AND Integration_Status      = ?
            ORDER BY LineNumber
        """, (cust_ref, status_to_read)).fetchall()

        if not lines:
            raise RuntimeError("No lines found in INT.SalesOrderStage.")

        line_url      = f"{d365['odata_root']}/SalesOrderLinesV3" if not TEST_MODE else ""
        line_payloads: list[tuple[int, dict]] = []
        line_responses: list[tuple[int, str]] = []

        for item, qty, src_uom, ln in lines:
            qty_f = float(qty or 0)
            if qty_f <= 0:
                raise RuntimeError(f"Invalid qty {qty_f} for item {item} line {ln}")
            if src_uom not in SOURCE_UOM_ALLOWED:
                raise RuntimeError(f"Unsupported UOM '{src_uom}' for item {item}")

            lp = {
                "dataAreaId":           data_area,
                "SalesOrderNumber":     so_number,
                "ItemNumber":           item,
                "OrderedSalesQuantity": qty_f,
                "SalesUnitSymbol":      UOM_MAP[src_uom],
                "LineNumber":           int(ln),
            }
            if SHOW_PAYLOADS:
                print(f"  LN {ln} →", json.dumps(lp, indent=2))

            if TEST_MODE:
                lr_text = json.dumps({"TEST_MODE": True, "Result": "NO API CALL"})
            else:
                lr, tok = _post_with_retry(session, line_url, tok, lp, d365)
                lr_text = lr.text
                if lr.status_code not in (200, 201):
                    raise RuntimeError(f"Line {ln} HTTP {lr.status_code}: {lr.text[:500]}")

            line_payloads.append((int(ln), lp))
            line_responses.append((int(ln), lr_text))

        log.info("  Lines sent: %d  (%s)", len(lines), cust_ref)

        # ── JSON BUNDLE ──────────────────────────────────────────────────
        if (WRITE_JSON or TEST_MODE) and run_dir:
            bundle = {
                "CustomersOrderReference": cust_ref,
                "D365_SONumber":           so_number,
                "HeaderRequest":           header_payload,
                "HeaderResponse":          hdr_response_text,
                "LineRequests":            [lp for _, lp in line_payloads],
                "LineResponses":           [lr for _, lr in line_responses],
            }
            (run_dir / f"{cust_ref}_bundle.json").write_text(
                json.dumps(bundle, indent=2), encoding="utf-8"
            )

        # ── FINALISE DB ──────────────────────────────────────────────────
        if not TEST_MODE:
            cur = conn.cursor()

            # INT.SalesOrderStage
            if stage_cap["has_so_number"]:
                cur.execute("""
                    UPDATE INT.SalesOrderStage
                    SET Integration_Status = ?, D365_SONumber = ?
                    WHERE CustomersOrderReference = ?
                """, (STAGE_CONFIRMED, so_number, cust_ref))
            else:
                cur.execute("""
                    UPDATE INT.SalesOrderStage
                    SET Integration_Status = ?
                    WHERE CustomersOrderReference = ?
                """, (STAGE_CONFIRMED, cust_ref))

            # CTL.EPOS_Header
            cur.execute("""
                UPDATE CTL.EPOS_Header
                SET Integration_Status = ?, D365_SO_Number = ?
                WHERE CAST(Docket_Reference AS varchar(50)) = ?
            """, (CTL_HDR_CONFIRMED, so_number, cust_ref))

            # CTL.EPOS_Line_Item
            cur.execute("""
                UPDATE l
                SET l.Integration_Status = ?
                FROM CTL.EPOS_Line_Item l
                JOIN CTL.EPOS_Header h ON h.EPOS_Fusion_Key = l.EPOS_Fusion_Key
                WHERE CAST(h.Docket_Reference AS varchar(50)) = ?
            """, (CTL_LN_CONFIRMED, cust_ref))

            conn.commit()

        print(f"  ✓  {cust_ref}  →  D365 SO: {so_number}")
        return True, so_number, tok

    except Exception as ex:
        err = str(ex)[:4000]
        log.error("  ✗  %s  FAILED: %s", cust_ref, err)
        print(f"  ✗  {cust_ref}  FAILED — {err[:120]}")

        # Unwind — mark failed, never leave InProgress
        if not TEST_MODE:
            try:
                cur = conn.cursor()
                if stage_cap["has_so_number"] and stage_cap["has_error_msg"] and stage_cap["err_col"]:
                    cur.execute(f"""
                        UPDATE INT.SalesOrderStage
                        SET Integration_Status = ?, D365_SONumber = ?, {stage_cap['err_col']} = ?
                        WHERE CustomersOrderReference = ?
                    """, (STAGE_FAILED, so_number, err, cust_ref))
                elif stage_cap["has_error_msg"] and stage_cap["err_col"]:
                    cur.execute(f"""
                        UPDATE INT.SalesOrderStage
                        SET Integration_Status = ?, {stage_cap['err_col']} = ?
                        WHERE CustomersOrderReference = ?
                    """, (STAGE_FAILED, err, cust_ref))
                else:
                    cur.execute("""
                        UPDATE INT.SalesOrderStage
                        SET Integration_Status = ?
                        WHERE CustomersOrderReference = ?
                    """, (STAGE_FAILED, cust_ref))

                cur.execute("""
                    UPDATE CTL.EPOS_Header
                    SET Integration_Status = ?
                    WHERE CAST(Docket_Reference AS varchar(50)) = ?
                """, (CTL_HDR_FAILED, cust_ref))
                conn.commit()
            except Exception as ex2:
                log.error("  Failed to mark %s as FAILED in DB: %s", cust_ref, ex2)
                conn.rollback()

        return False, None, tok


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    run_id  = str(uuid.uuid4())
    run_dir = None

    print(_hr("═"))
    print("  Synovia Fusion – EPOS Consignment Console Processor")
    if TEST_MODE:
        print("  ⚠  TEST MODE — No D365 calls. No DB status changes.")
    print(_hr("═"))
    print()

    with connect_autocommit("Fusion_EPOS_ConsignmentConsole") as conn:

        # ── Stage capability check ────────────────────────────────────────
        stage_cap = _stage_cols(conn)

        # ── Queue summary ─────────────────────────────────────────────────
        queue = _get_queue(conn)
        total_staged = sum(q["Store_Orders"] for q in queue)

        if not queue:
            print("  No STAGED orders found. Queue is empty.\n")
            return 0

        print(f"  STAGED Queue ({len(queue)} file(s) | {fmt_int(total_staged)} total store orders)\n")
        print(f"  {'File ID':<8} {'Year':<6} {'Wk':<4} {'Orders':>7} {'Lines':>7} {'Units':>8} {'Sales':>12}  File")
        print("  " + _hr("─", 68))
        for q in queue:
            print(
                f"  {q['File_ID']:<8} {q['Year']:<6} {q['Week']:0>2}    "
                f"{q['Store_Orders']:>7} {q['Total_Lines']:>7} "
                f"{q['Total_Units']:>8}  €{q['Total_Sales']:>10,.0f}  "
                f"{q['FileName'][:35]}"
            )
        print("  " + _hr("─", 68))
        print(
            f"  {'TOTAL':<8} {'':6} {'':4} "
            f"{sum(q['Store_Orders'] for q in queue):>7} "
            f"{sum(q['Total_Lines'] for q in queue):>7} "
            f"{sum(q['Total_Units'] for q in queue):>8}  "
            f"€{sum(q['Total_Sales'] for q in queue):>10,.0f}"
        )
        print()

        # ── How many? ─────────────────────────────────────────────────────
        raw = _ask(
            f"  How many orders to transmit? "
            f"[Enter = ALL {total_staged}]: "
        )
        to_send = total_staged if raw == "" else int(raw)
        if to_send <= 0 or to_send > total_staged:
            print(f"  Invalid. Must be 1–{total_staged}.")
            return 1

        print()
        if not _confirm(
            f"  Confirm: transmit {to_send} order(s) to D365"
            + (" [TEST MODE — no real calls]" if TEST_MODE else " [LIVE]")
        ):
            print("  Cancelled.")
            return 0

        # ── D365 setup ────────────────────────────────────────────────────
        d365    = None
        session = None
        token   = None

        if not TEST_MODE:
            print("\n  Authenticating with D365...")
            d365    = _load_d365_config()
            session = requests.Session()
            token   = _get_token(d365)
            print("  Token acquired ✓")

        # ── JSON output folder ────────────────────────────────────────────
        if WRITE_JSON or TEST_MODE:
            run_dir = JSON_ROOT / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            log.info("JSON output: %s", run_dir)

        # ── Load order refs ───────────────────────────────────────────────
        orders = _get_staged_orders(conn, to_send)
        total  = len(orders)

        print(f"\n  Processing {total} order(s)...\n")
        print("  " + _hr("─", 68))

        ok_count   = 0
        fail_count = 0
        failed_refs: list[str] = []
        start_ts   = time.time()

        for i, cust_ref in enumerate(orders, 1):
            success, so_number, token = _process_order(
                conn, cust_ref, run_dir,
                session, token, d365,
                stage_cap, i, total,
            )
            if success:
                ok_count += 1
            else:
                fail_count += 1
                failed_refs.append(cust_ref)

    # ── Summary ───────────────────────────────────────────────────────────
    elapsed = time.time() - start_ts
    print()
    print(_hr("═"))
    print(f"  Run ID  : {run_id}")
    print(f"  Elapsed : {elapsed:.1f}s")
    print(f"  ✓ Sent  : {ok_count}")
    print(f"  ✗ Failed: {fail_count}")
    if failed_refs:
        print(f"\n  Failed order refs:")
        for ref in failed_refs:
            print(f"    • {ref}")
    if run_dir:
        print(f"\n  JSON bundles: {run_dir}")
    print(_hr("═"))

    if fail_count:
        log.error("Run complete with %d failure(s). Review log.", fail_count)
        return 1

    log.info("Run complete. All %d order(s) transmitted successfully.", ok_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())