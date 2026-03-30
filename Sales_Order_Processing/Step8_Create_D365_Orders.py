# -*- coding: utf-8 -*-
"""
Synovia Fusion – Step 8: Create Dynamics 365 Sales Orders (Consignment)
========================================================================
Runs AFTER Step 6 has staged CTL READY rows into INT.SalesOrderStage.

Core process
------------
1) Show STAGED queue grouped by file/week
2) Decide how many orders to transmit (interactive or scheduler)
3) Claim each order (STAGED → InProgress)
4) POST D365 Sales Order header  (SalesOrderHeadersV2)
5) POST D365 Sales Order lines   (SalesOrderLinesV3)
6) Finalise:
     INT.SalesOrderStage   → SO_Confirmed  + D365_SONumber
     CTL.EPOS_Header       → SO_Confirmed  + D365_SO_Number
     CTL.EPOS_Line_Item    → SO_Confirmed
7) On failure: unwind to Failed / SO_Failed, never leave InProgress

Secrets (no hardcoded values):
  DB:   ENV FUSION_EPOS_BOOTSTRAP_PASSWORD → INI password=    → RuntimeError
  D365: ENV FUSION_D365_CLIENT_SECRET      → Production_365.ini → RuntimeError
"""

from __future__ import annotations

import json
import logging
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
    LOG_DIR,
    OUTPUT_ROOT,
)
import configparser

# =============================================================================
# SWITCHES
# =============================================================================
MANUAL_RUN                   = False   # True → prompt for how many + optional per-order confirm
TEST_MODE                    = False   # True → JSON only, no D365 calls, no DB updates
WRITE_JSON_TO_DISK           = True
SHOW_API_ON_SCREEN           = True
CONFIRM_EACH_ORDER_IN_MANUAL = False   # only applies when MANUAL_RUN = True

# =============================================================================
# SCHEDULER MODE (ignored when MANUAL_RUN = True)
# =============================================================================
SEND_ALL_AVAILABLE = True
ORDERS_TO_SEND     = 250

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
# UOM
# =============================================================================
SOURCE_UOM_ALLOWED = {"EA"}
UOM_MAP            = {"EA": "UN"}

# =============================================================================
# RETRY
# =============================================================================
RETRY_HTTP       = {429, 500, 502, 503, 504}
MAX_RETRIES      = 6
BACKOFF_BASE_SEC = 2

# =============================================================================
# PATHS
# =============================================================================
D365_INI_PATH = r"D:\Configuration\Production_365.ini"
JSON_ROOT     = Path(OUTPUT_ROOT) / "D365_SalesOrders"

# =============================================================================
# LOGGING
# =============================================================================
ensure_dir(LOG_DIR)
_LOG_FILE = Path(LOG_DIR) / "Step8_Create_D365_Orders.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("Fusion_Step8_D365_SO")


# =============================================================================
# D365 AUTH
# =============================================================================

def _load_d365(ini_path: str) -> dict:
    cfg = configparser.ConfigParser()
    cfg.read(ini_path, encoding="utf-8")
    return {
        "tenant_id":    cfg["AUTH"]["tenant_id"],
        "client_id":    cfg["AUTH"]["client_id"],
        "client_secret": resolve_d365_secret(ini_path),
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
            if r.status_code in RETRY_HTTP:
                wait = BACKOFF_BASE_SEC ** min(attempt, 5)
                log.warning("HTTP %s — retry %d/%d in %ds", r.status_code, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            return r, tok
        except Exception as ex:
            last_exc = ex
            wait = BACKOFF_BASE_SEC ** min(attempt, 5)
            log.warning("POST exception attempt %d/%d: %s — sleep %ds", attempt, MAX_RETRIES, ex, wait)
            time.sleep(wait)
    raise RuntimeError(f"POST failed after {MAX_RETRIES} retries. Last: {last_exc}")


# =============================================================================
# DB HELPERS
# =============================================================================

def _scalar(cur: pyodbc.Cursor, sql: str, params=()):
    row = cur.execute(sql, params).fetchone()
    return row[0] if row else None


def _obj_exists(cur: pyodbc.Cursor, name: str) -> bool:
    return _scalar(cur, "SELECT OBJECT_ID(?)", (name,)) is not None


def _has_col(cur: pyodbc.Cursor, schema: str, table: str, col: str) -> bool:
    return bool(cur.execute("""
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?
    """, (schema, table, col)).fetchone())


def _safe_json(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"


def _write_bundle(
    run_dir: Path,
    cust_ref: str,
    hdr_payload: dict,
    hdr_response: str,
    line_payloads: list[tuple[int, dict]],
    line_responses: list[tuple[int, str]],
) -> None:
    bundle = {
        "CustomersOrderReference": cust_ref,
        "HeaderRequest":  hdr_payload,
        "HeaderResponse": hdr_response,
        "LineRequests":   [lp for _, lp in line_payloads],
        "LineResponses":  [lr for _, lr in line_responses],
    }
    (run_dir / f"{cust_ref}_bundle.json").write_text(
        json.dumps(bundle, indent=2), encoding="utf-8"
    )


# =============================================================================
# QUEUE DISPLAY
# =============================================================================

def _show_queue(cur: pyodbc.Cursor) -> int:
    """Print grouped queue summary. Returns total available order count."""
    rows = cur.execute("""
        SELECT
              f.File_ID
            , f.FileName
            , f.[Year]
            , f.[Week]
            , COUNT(DISTINCT i.CustomersOrderReference) AS Store_Orders
            , COUNT(i.ItemNumber)                       AS Lines
            , CAST(SUM(i.OrderedSalesQuantity) AS bigint) AS Units
            , SUM(i.LineAmount)                         AS Sales
        FROM RAW.EPOS_File f
        JOIN CTL.EPOS_Header h ON h.Source_File_ID = f.File_ID
        JOIN INT.SalesOrderStage i
          ON  i.CustomersOrderReference = CAST(h.Docket_Reference AS varchar(50))
         AND  i.Integration_Status = ?
        GROUP BY f.File_ID, f.FileName, f.[Year], f.[Week]
        ORDER BY f.[Year] DESC, f.[Week] DESC, f.File_ID DESC;
    """, (STAGE_READY,)).fetchall()

    if not rows:
        return 0

    total_orders = sum(int(r[4]) for r in rows)

    print()
    print(f"  {'File ID':<8} {'Yr':<5} {'Wk':<4} {'Orders':>7} {'Lines':>7} "
          f"{'Units':>8} {'Sales':>12}  File")
    print("  " + "─" * 70)
    for r in rows:
        print(
            f"  {r[0]:<8} {r[2]:<5} {r[3]:0>2}    "
            f"{r[4]:>7} {r[5]:>7} {r[6]:>8}  "
            f"€{float(r[7] or 0):>10,.0f}  {str(r[1] or '')[:35]}"
        )
    print("  " + "─" * 70)
    print(
        f"  {'TOTAL':<8} {'':5} {'':4} "
        f"{sum(r[4] for r in rows):>7} "
        f"{sum(r[5] for r in rows):>7} "
        f"{sum(r[6] or 0 for r in rows):>8}  "
        f"€{sum(float(r[7] or 0) for r in rows):>10,.0f}"
    )
    print()
    return total_orders


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    log.info("=== STEP 8: CREATE D365 SALES ORDERS STARTED ===")
    if TEST_MODE:
        log.warning("TEST MODE — JSON generation only. No D365 calls. No DB updates.")

    run_id  = str(uuid.uuid4())
    run_dir = None

    with connect_autocommit("Fusion_EPOS_Step8_D365_SO") as conn:
        cur = conn.cursor()

        # ── Capability detection ──────────────────────────────────────────
        has_run_log  = _obj_exists(cur, "LOG.Consignment_Sales_Order_Creation_Run")
        has_tx_log   = _obj_exists(cur, "LOG.Consignment_Sales_Order_Creation_Transaction")
        has_api_hdr  = _obj_exists(cur, "API.RawCall_SalesOrderHeader")
        has_api_line = _obj_exists(cur, "API.RawCall_SalesOrderLine")

        stage_has_so  = _has_col(cur, "INT", "SalesOrderStage", "D365_SONumber")
        err_col = (
            "ErrorMessage"  if _has_col(cur, "INT", "SalesOrderStage", "ErrorMessage")
            else "Error_Message" if _has_col(cur, "INT", "SalesOrderStage", "Error_Message")
            else None
        )

        # ── Queue ─────────────────────────────────────────────────────────
        print("═" * 72)
        print("  Synovia Fusion – Step 8: Create D365 Sales Orders")
        print("═" * 72)

        available = _show_queue(cur)

        if available == 0:
            log.info("No orders with status '%s'. Nothing to do.", STAGE_READY)
            return 0

        # ── How many? ─────────────────────────────────────────────────────
        if MANUAL_RUN:
            raw = input(
                f"  {available} order(s) available. "
                f"How many to transmit? [Enter = ALL]: "
            ).strip()
            to_send = available if raw == "" else int(raw)
            if to_send <= 0 or to_send > available:
                print(f"  Invalid. Must be 1–{available}.")
                return 1
            confirm = input(
                f"  Confirm: transmit {to_send} order(s)"
                f"{' [TEST MODE]' if TEST_MODE else ' [LIVE — D365 LIVE]'} (Y/N): "
            ).strip().upper()
            if confirm != "Y":
                print("  Cancelled.")
                return 0
        else:
            to_send = available if SEND_ALL_AVAILABLE else min(max(ORDERS_TO_SEND, 0), available)
            if to_send <= 0:
                log.info("Nothing to do (ORDERS_TO_SEND=0 and SEND_ALL_AVAILABLE=False).")
                return 0

        # ── JSON output folder ────────────────────────────────────────────
        if WRITE_JSON_TO_DISK or TEST_MODE:
            run_dir = JSON_ROOT / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            log.info("JSON output: %s", run_dir)

        # ── Run log ───────────────────────────────────────────────────────
        if has_run_log:
            cur.execute("""
                INSERT INTO LOG.Consignment_Sales_Order_Creation_Run
                (RunId, RecordsAvailable, RecordsRequested, OutputJsonEnabled, OutputJsonPath)
                VALUES (?, ?, ?, ?, ?)
            """, (run_id, int(available), int(to_send),
                  1 if (WRITE_JSON_TO_DISK or TEST_MODE) else 0,
                  str(run_dir) if run_dir else None))
            conn.commit()

        # ── Load order refs ───────────────────────────────────────────────
        orders = [
            r[0] for r in cur.execute("""
                SELECT DISTINCT TOP (?)
                    CustomersOrderReference
                FROM INT.SalesOrderStage
                WHERE Integration_Status = ?
                ORDER BY CustomersOrderReference
            """, (int(to_send), STAGE_READY)).fetchall()
        ]

        # ── D365 session ──────────────────────────────────────────────────
        session = None
        token   = None
        d365    = None
        if not TEST_MODE:
            log.info("Authenticating with D365...")
            d365    = _load_d365(D365_INI_PATH)
            session = requests.Session()
            token   = _get_token(d365)
            log.info("Token acquired ✓")

        # ── Process orders ────────────────────────────────────────────────
        ok_count   = 0
        fail_count = 0
        total      = len(orders)

        print(f"\n  Processing {total} order(s)...\n")

        for idx, cust_ref in enumerate(orders, 1):
            api_call_id = str(uuid.uuid4())
            so_number   = None
            pct         = int((idx / total) * 30)
            bar         = f"[{'█'*pct}{'░'*(30-pct)}] {idx}/{total}"

            try:
                if MANUAL_RUN and CONFIRM_EACH_ORDER_IN_MANUAL:
                    if input(f"  Proceed with {cust_ref}? (Y/N): ").strip().upper() != "Y":
                        log.info("Skipped: %s", cust_ref)
                        continue

                print(f"\n  {bar}  {cust_ref}")
                log.info("Processing %s", cust_ref)

                # ── CLAIM ─────────────────────────────────────────────────
                if not TEST_MODE:
                    claimed = cur.execute("""
                        UPDATE INT.SalesOrderStage
                        SET Integration_Status = ?
                        WHERE CustomersOrderReference = ?
                          AND Integration_Status = ?
                    """, (STAGE_INPROGRESS, cust_ref, STAGE_READY)).rowcount
                    conn.commit()
                    if claimed == 0:
                        log.warning("%s not claimable — status changed, skipping.", cust_ref)
                        continue

                # ── HEADER DATA ───────────────────────────────────────────
                read_status = STAGE_INPROGRESS if not TEST_MODE else STAGE_READY
                row = cur.execute("""
                    SELECT TOP 1
                        dataAreaId,
                        CustomerRequisitionNumber,
                        OrderingCustomerAccountNumber,
                        RequestedShippingDate
                    FROM INT.SalesOrderStage
                    WHERE CustomersOrderReference = ?
                      AND Integration_Status = ?
                """, (cust_ref, read_status)).fetchone()

                if not row:
                    raise RuntimeError("No header row in INT.SalesOrderStage after claim.")
                data_area, cust_req, cust_account, ship_date = row

                header_payload = {
                    "dataAreaId":                    data_area,
                    "CustomersOrderReference":       cust_ref,
                    "CustomerRequisitionNumber":     cust_req,
                    "OrderingCustomerAccountNumber": cust_account,
                }
                if ship_date:
                    header_payload["RequestedShippingDate"] = str(ship_date)

                if SHOW_API_ON_SCREEN:
                    print("  HDR →", json.dumps(header_payload))

                # ── D365 HEADER ───────────────────────────────────────────
                call_ts = datetime.utcnow()

                if TEST_MODE:
                    so_number     = f"TEST-{cust_ref}"
                    hdr_resp_text = json.dumps({"TEST_MODE": True, "SalesOrderNumber": so_number})
                else:
                    hdr_url       = f"{d365['odata_root']}/SalesOrderHeadersV2"
                    hdr_resp, token = _post_with_retry(session, hdr_url, token, header_payload, d365)
                    hdr_resp_text = hdr_resp.text

                    if has_api_hdr:
                        cur.execute("""
                            INSERT INTO API.RawCall_SalesOrderHeader
                            (ApiCallId, ApiName, HttpMethod, ApiUrl, CallTimestampUtc,
                             HttpStatus, IsSuccess, ErrorMessage, DataAreaId, SalesOrderNumber,
                             OrderingCustomerAccount, CustomersOrderReference,
                             CustomerRequisitionNumber, RequestJson, ResponseJson)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (api_call_id, "SalesOrderHeadersV2", "POST", hdr_url, call_ts,
                              hdr_resp.status_code,
                              1 if hdr_resp.status_code in (200, 201) else 0,
                              None if hdr_resp.status_code in (200, 201) else hdr_resp.text[:4000],
                              data_area, None, cust_account, cust_ref, cust_req,
                              _safe_json(header_payload), hdr_resp.text[:4000]))
                        conn.commit()

                    if hdr_resp.status_code not in (200, 201):
                        raise RuntimeError(
                            f"Header HTTP {hdr_resp.status_code}: {hdr_resp.text[:500]}"
                        )
                    so_number = hdr_resp.json().get("SalesOrderNumber")
                    if not so_number:
                        raise RuntimeError(f"No SalesOrderNumber in response: {hdr_resp.text[:300]}")

                log.info("  Header → D365 SO: %s", so_number)

                if WRITE_JSON_TO_DISK and run_dir:
                    (run_dir / f"{cust_ref}_header_request.json").write_text(
                        json.dumps(header_payload, indent=2), encoding="utf-8"
                    )
                    (run_dir / f"{cust_ref}_header_response.json").write_text(
                        hdr_resp_text, encoding="utf-8"
                    )

                # ── LINES ─────────────────────────────────────────────────
                lines = cur.execute("""
                    SELECT ItemNumber, OrderedSalesQuantity, SalesUnitSymbol, LineNumber
                    FROM INT.SalesOrderStage
                    WHERE CustomersOrderReference = ?
                      AND Integration_Status = ?
                    ORDER BY LineNumber
                """, (cust_ref, read_status)).fetchall()

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
                        raise RuntimeError(
                            f"Unsupported UOM '{src_uom}' for item {item} "
                            f"(allowed: {sorted(SOURCE_UOM_ALLOWED)})"
                        )

                    lp = {
                        "dataAreaId":           data_area,
                        "SalesOrderNumber":     so_number,
                        "ItemNumber":           item,
                        "OrderedSalesQuantity": qty_f,
                        "SalesUnitSymbol":      UOM_MAP[src_uom],
                        "LineNumber":           int(ln),
                    }

                    if SHOW_API_ON_SCREEN:
                        print(f"  LN {ln} →", json.dumps(lp))

                    if TEST_MODE:
                        lr_text = json.dumps({"TEST_MODE": True, "Result": "NO API CALL"})
                    else:
                        lr, token = _post_with_retry(session, line_url, token, lp, d365)
                        lr_text = lr.text

                        if has_api_line:
                            cur.execute("""
                                INSERT INTO API.RawCall_SalesOrderLine
                                (ApiCallId, SalesOrderNumber, LineNumber, ItemNumber,
                                 OrderedQuantity, HttpStatus, IsSuccess, RequestJson, ResponseJson)
                                VALUES (?,?,?,?,?,?,?,?,?)
                            """, (api_call_id, so_number, int(ln), item, qty_f,
                                  lr.status_code,
                                  1 if lr.status_code in (200, 201) else 0,
                                  _safe_json(lp), lr.text[:4000]))
                            conn.commit()

                        if lr.status_code not in (200, 201):
                            raise RuntimeError(
                                f"Line {ln} HTTP {lr.status_code}: {lr.text[:500]}"
                            )

                    line_payloads.append((int(ln), lp))
                    line_responses.append((int(ln), lr_text))

                    if WRITE_JSON_TO_DISK and run_dir:
                        (run_dir / f"{cust_ref}_line_{int(ln)}_request.json").write_text(
                            json.dumps(lp, indent=2), encoding="utf-8"
                        )
                        (run_dir / f"{cust_ref}_line_{int(ln)}_response.json").write_text(
                            lr_text, encoding="utf-8"
                        )

                if WRITE_JSON_TO_DISK and run_dir:
                    _write_bundle(run_dir, cust_ref, header_payload,
                                  hdr_resp_text, line_payloads, line_responses)

                # ── FINALISE DB ───────────────────────────────────────────
                if not TEST_MODE:
                    # INT.SalesOrderStage
                    if stage_has_so:
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

                    if has_tx_log:
                        cur.execute("""
                            INSERT INTO LOG.Consignment_Sales_Order_Creation_Transaction
                            (RunId, CustomersOrderReference, D365_SONumber, Status)
                            VALUES (?, ?, ?, ?)
                        """, (run_id, cust_ref, so_number, STAGE_CONFIRMED))

                    conn.commit()

                ok_count += 1
                print(f"  ✓  {cust_ref}  →  {so_number}")
                log.info("  ✓ %s → %s", cust_ref, so_number)

            except Exception as ex:
                conn.rollback()
                fail_count += 1
                err = str(ex)[:4000]
                log.error("  ✗ %s FAILED: %s", cust_ref, err)
                print(f"  ✗  {cust_ref}  FAILED — {err[:120]}")

                # Zombie protection — never leave InProgress
                if not TEST_MODE:
                    try:
                        if err_col:
                            cur.execute(f"""
                                UPDATE INT.SalesOrderStage
                                SET Integration_Status = ?
                                  {', D365_SONumber = ?' if stage_has_so and so_number else ''}
                                  , {err_col} = ?
                                WHERE CustomersOrderReference = ?
                            """, *(
                                (STAGE_FAILED, so_number, err, cust_ref)
                                if stage_has_so and so_number
                                else (STAGE_FAILED, err, cust_ref)
                            ))
                        elif stage_has_so and so_number:
                            cur.execute("""
                                UPDATE INT.SalesOrderStage
                                SET Integration_Status = ?, D365_SONumber = ?
                                WHERE CustomersOrderReference = ?
                            """, (STAGE_FAILED, so_number, cust_ref))
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

                        if has_tx_log:
                            cur.execute("""
                                INSERT INTO LOG.Consignment_Sales_Order_Creation_Transaction
                                (RunId, CustomersOrderReference, D365_SONumber, Status, ErrorMessage)
                                VALUES (?, ?, ?, ?, ?)
                            """, (run_id, cust_ref, so_number, STAGE_FAILED, err))

                        conn.commit()
                    except Exception as ex2:
                        conn.rollback()
                        log.error("Failed to mark %s as FAILED: %s", cust_ref, ex2)

    # ── Summary ───────────────────────────────────────────────────────────
    print()
    print("═" * 72)
    print(f"  RunId  : {run_id}")
    print(f"  ✓ Sent : {ok_count}")
    print(f"  ✗ Fail : {fail_count}")
    if run_dir:
        print(f"  JSON   : {run_dir}")
    print("═" * 72)

    log.info("=== STEP 8 COMPLETE  RunId=%s  Success=%d  Failed=%d ===",
             run_id, ok_count, fail_count)
    return 1 if fail_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
