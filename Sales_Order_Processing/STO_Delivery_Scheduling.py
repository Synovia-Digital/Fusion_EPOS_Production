#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# STO_Delivery_Scheduling.py
# Validates customers, products and schedules delivery dates.
# Writes to STG.WASP_STO_Scheduled and EXC.STO_Order_Audit_Log.

import pyodbc
import configparser
import logging
import sys
import socket
import traceback
from pathlib import Path
from datetime import datetime, date, timedelta
from logging.handlers import RotatingFileHandler

# =========================================================
# CONSTANTS
# =========================================================
PROCESS_CODE  = "SCHEDULE"
PROCESS_NAME  = "Delivery Date Scheduling"
SCRIPT_NAME   = "STO_Delivery_Scheduling.py"
MACHINE_NAME  = socket.gethostname()

CONFIG_SECTION_DB = "Fusion_EPOS_Production"
CONFIG_PATH       = r"\\PL-AZ-FUSION-CO\FusionProduction\Configuration"

BASE_DIR = Path(r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs")
BASE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = BASE_DIR / "STO_Delivery_Scheduling.log"

CUTOFF_HOUR   = 14    # Orders received before 14:00 get same-day base date
MIN_LEAD_DAYS = 2     # Minimum business days from order date to delivery

# Products not in CFG.Products where category/subcategory
# contains any of these strings are FSDU/co-packed displays —
# they pass through without blocking the order.
PASSTHROUGH_PATTERNS = ["duracell"]

# =========================================================
# LOGGING
# =========================================================
logger = logging.getLogger("STO_Delivery_Scheduling")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()
fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=10, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(fh)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(sh)

def log(msg): logger.info(msg)
def dbg(msg): logger.debug(msg)
def err(msg): logger.error(msg)

# =========================================================
# CONSOLE HELPERS
# =========================================================
_W = 68
def _ts(): return datetime.now().strftime("%H:%M:%S")

def con_section(t):
    print(f"\n{'═'*_W}\n  ◆  {t}\n{'═'*_W}")

def con_order_header(order_id, customer_name, customer_code, idx, total):
    print(f"\n  {'─'*(_W-2)}")
    print(f"  📋  [{idx}/{total}]  Order {order_id}  |  {customer_name}  ({customer_code})")
    print(f"  {'─'*(_W-2)}")

def con_info(m):        print(f"  [{_ts()}]  ℹ️   {m}")
def con_ok(m):          print(f"  [{_ts()}]  ✅  {m}")
def con_warn(m):        print(f"  [{_ts()}]  ⚠️   {m}")
def con_error(m):       print(f"  [{_ts()}]  ❌  {m}")
def con_skip(m):        print(f"  [{_ts()}]  ⏭️   {m}")
def con_hold(m):        print(f"  [{_ts()}]  🛑  {m}")
def con_reschedule(m):  print(f"  [{_ts()}]  📅  {m}")
def con_confirmed(m):   print(f"  [{_ts()}]  📦  {m}")
def con_removed(m):     print(f"  [{_ts()}]  🗑️   {m}")
def con_passthrough(m): print(f"  [{_ts()}]  🔄  {m}")

def con_summary(counts):
    print(f"\n{'═'*_W}")
    print(f"  ◆  RUN COMPLETE")
    print(f"  {'─'*(_W-4)}")
    print(f"  📊  Total      : {counts['total']}")
    print(f"  ✅  Confirmed  : {counts['confirmed']}")
    print(f"  📅  Rescheduled: {counts['rescheduled']}")
    print(f"  🛑  Held       : {counts['held']}")
    print(f"  ❌  Failed     : {counts['failed']}")
    print(f"{'═'*_W}\n")

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
db = cfg[CONFIG_SECTION_DB]

CONN_STR = (
    f"Driver={{{db.get('driver')}}};"
    f"Server={db.get('server')};"
    f"Database={db.get('database')};"
    f"UID={db.get('user')};PWD={db.get('password')};"
    f"Encrypt={db.get('encrypt','yes')};"
    f"TrustServerCertificate={db.get('trust_server_certificate','no')};"
)

def get_conn():
    """Transactional — autocommit OFF. Use for all data writes."""
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = False
    return conn

def get_conn_ac():
    """Autocommit ON. Use for reads, logging, audit inserts."""
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = True
    return conn

# =========================================================
# SQL
# =========================================================
PROC_LOG_SQL = """
INSERT INTO EXC.STO_Processing_Log
(ProcessCode, ProcessName, StepCode, StepDescription, OrderId,
 TransferControlId, DynamicsDocumentNo, Status, Message, MachineName, ScriptName)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

AUDIT_SQL = """
INSERT INTO EXC.STO_Order_Audit_Log
(
    OrderId, CustomerCode, CustomerName, TransferControlId,
    EventType, StepCode, StepDescription, ValidationStatus,
    ProductCode, ProductCategory, ProductSubcategory, ProductDescription,
    OrderReceivedAt, OriginalDeliveryDate, NewDeliveryDate, ScheduledShipDate,
    CutoffApplied, RuleApplied, ChangeReason,
    LoggedAt, LoggedBy, MachineName
)
VALUES (
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, ?,
    SYSUTCDATETIME(), ?, ?
);
"""

GET_PENDING_SQL = """
SELECT
    r.OrderId,
    MAX(r.CustomerCode)  AS CustomerCode,
    MAX(r.CustomerName)  AS CustomerName,
    MAX(r.OrderDate)     AS OrderDate,
    MAX(r.DateRequired)  AS DateRequired
FROM RAW.EPOS_TransferRequest r
WHERE r.SynProcessed = 0
  AND r.SynStaged    = 0
  AND NULLIF(LTRIM(RTRIM(r.OrderId)), '') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM STG.WASP_STO_Scheduled s
      WHERE s.OrderId = r.OrderId
        AND s.SynProcessed = 0
  )
GROUP BY r.OrderId
ORDER BY r.OrderId;
"""

GET_ORDER_LINES_SQL = """
SELECT DISTINCT
    r.ProductCode,
    MAX(r.ProductCategory)    AS ProductCategory,
    MAX(r.ProductSubcategory) AS ProductSubcategory,
    MAX(r.ProductDescription) AS ProductDescription
FROM RAW.EPOS_TransferRequest r
WHERE r.OrderId     = ?
  AND r.SynProcessed = 0
  AND r.SynStaged    = 0
  AND NULLIF(LTRIM(RTRIM(r.ProductCode)), '') IS NOT NULL
GROUP BY r.ProductCode;
"""

INSERT_STG_SQL = """
INSERT INTO STG.WASP_STO_Scheduled
(
    OrderId, CustomerCode, CustomerName, OrderDate,
    OriginalDateRequired, ScheduledDeliveryDate, ScheduledShippingDate,
    DateChanged, ChangeReason, RuleApplied, ProcessCode,
    SynProcessed, SynProcessedDate
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL);
"""

INSERT_STG_HELD_SQL = """
INSERT INTO STG.WASP_STO_Scheduled
(
    OrderId, CustomerCode, CustomerName, OrderDate,
    OriginalDateRequired, ScheduledDeliveryDate, ScheduledShippingDate,
    DateChanged, ChangeReason, RuleApplied, ProcessCode,
    SynProcessed, SynProcessedDate
)
VALUES (?, ?, ?, ?, ?, NULL, NULL, 0, ?, 'HELD', ?, 0, NULL);
"""

REMOVE_PRODUCT_SQL = """
UPDATE RAW.EPOS_TransferRequest
SET SynProcessed     = 1,
    SynProcessedDate = SYSUTCDATETIME()
WHERE OrderId      = ?
  AND ProductCode  = ?
  AND SynProcessed = 0
  AND SynStaged    = 0;
"""

# =========================================================
# REFERENCE DATA
# =========================================================
def load_bank_holidays(conn) -> set:
    rows = conn.execute(
        "SELECT Holiday_Date FROM CFG.Bank_Holiday_Calendar "
        "WHERE Is_Delivery_Day = 0"
    ).fetchall()
    holidays = {r[0].date() if hasattr(r[0], 'date') else r[0] for r in rows}
    log(f"Loaded {len(holidays)} bank holidays")
    return holidays

def load_approved_customers(conn) -> set:
    rows = conn.execute(
        "SELECT Account FROM CFG.Dynamics_Stores "
        "WHERE NULLIF(LTRIM(RTRIM(Account)),'') IS NOT NULL"
    ).fetchall()
    approved = {r[0].strip() for r in rows}
    log(f"Loaded {len(approved)} approved customers")
    return approved

def load_approved_products(conn) -> set:
    rows = conn.execute(
        "SELECT Dynamics_Code FROM CFG.Products "
        "WHERE NULLIF(LTRIM(RTRIM(Dynamics_Code)),'') IS NOT NULL"
    ).fetchall()
    approved = {r[0].strip() for r in rows}
    log(f"Loaded {len(approved)} approved products")
    return approved

# =========================================================
# LOGGING HELPERS
# =========================================================
def proc_log(lc, step_code, step_desc, status, message,
             order_id=None, control_id=None, dynamics_no=None):
    """Write to EXC.STO_Processing_Log — non-blocking."""
    try:
        lc.execute(PROC_LOG_SQL,
            PROCESS_CODE, PROCESS_NAME, step_code, step_desc,
            order_id, control_id, dynamics_no,
            status, message, MACHINE_NAME, SCRIPT_NAME)
    except Exception as e:
        err(f"PROC_LOG FAILED: {e}")

def audit(lc, order_id, customer_code, customer_name,
          event_type, step_code, step_desc, validation_status,
          # Product context — all optional
          product_code=None, product_category=None,
          product_subcategory=None, product_description=None,
          # Schedule context — all optional
          received_at=None, original_date=None, new_date=None,
          ship_date=None, cutoff=None, rule=None,
          # Reason
          reason=None,
          # FK
          control_id=None):
    """
    Write to EXC.STO_Order_Audit_Log.
    All contextual fields are optional — only pass what's relevant
    for the event type. No NOT NULL surprises.
    """
    try:
        lc.execute(AUDIT_SQL,
            order_id, customer_code, customer_name, control_id,
            event_type, step_code, step_desc, validation_status,
            product_code, product_category, product_subcategory, product_description,
            received_at, original_date, new_date, ship_date,
            cutoff, rule, reason,
            SCRIPT_NAME, MACHINE_NAME
        )
    except Exception as e:
        err(f"AUDIT FAILED [{event_type} / {order_id}]: {e}")

# =========================================================
# BUSINESS DAY HELPERS
# =========================================================
def is_business_day(d: date, holidays: set) -> bool:
    return d.weekday() < 5 and d not in holidays

def add_business_days(start: date, n: int, holidays: set) -> date:
    cur, added = start, 0
    while added < n:
        cur += timedelta(days=1)
        if is_business_day(cur, holidays):
            added += 1
    return cur

def prev_business_day(d: date, holidays: set) -> date:
    d -= timedelta(days=1)
    while not is_business_day(d, holidays):
        d -= timedelta(days=1)
    return d

def calculate_earliest_delivery(received_at: datetime, holidays: set):
    hour       = received_at.hour
    order_date = received_at.date()
    if hour < CUTOFF_HOUR:
        earliest = add_business_days(order_date, MIN_LEAD_DAYS, holidays)
        rule     = (f"Received before {CUTOFF_HOUR}:00 — "
                    f"order date + {MIN_LEAD_DAYS} business days")
    else:
        next_bd  = add_business_days(order_date, 1, holidays)
        earliest = add_business_days(next_bd, MIN_LEAD_DAYS, holidays)
        rule     = (f"Received at/after {CUTOFF_HOUR}:00 — "
                    f"next business day + {MIN_LEAD_DAYS} business days")
    return earliest, rule

def resolve_delivery_date(requested: date, earliest: date, holidays: set):
    effective = requested
    if not is_business_day(requested, holidays):
        while not is_business_day(effective, holidays):
            effective += timedelta(days=1)
    if effective >= earliest:
        if effective != requested:
            return (effective,
                    f"Requested {requested} falls on non-business day "
                    f"— moved to {effective}",
                    True)
        return requested, "Requested date meets minimum lead time — no change required", False
    return (earliest,
            f"Requested {requested} is within minimum lead time "
            f"(earliest={earliest}) — rescheduled",
            True)

# =========================================================
# FSDU / CO-PACK PASSTHROUGH
# =========================================================
def is_passthrough(product_code: str, category: str, subcategory: str) -> bool:
    """
    True if a product absent from CFG.Products should be allowed through
    because it is a Duracell FSDU or co-packed display unit.
    Matches on ProductCategory OR ProductSubcategory containing
    any of the PASSTHROUGH_PATTERNS (case-insensitive).
    """
    cat = (category or "").lower()
    sub = (subcategory or "").lower()
    return any(p in cat or p in sub for p in PASSTHROUGH_PATTERNS)

# =========================================================
# VALIDATION — CUSTOMER
# =========================================================
def validate_customer(lc, order_id, customer_code, customer_name,
                      received_at, requested_date,
                      approved_customers) -> bool:
    step      = "SCHEDULE_VAL_001"
    step_desc = "Customer validation against CFG.Dynamics_Stores"

    if customer_code.strip() in approved_customers:
        con_ok(f"  Customer {customer_code} ({customer_name}) — APPROVED")
        proc_log(lc, step, step_desc, "OK",
                 f"Customer {customer_code} ({customer_name}) approved",
                 order_id=order_id)
        audit(lc, order_id, customer_code, customer_name,
              event_type="CUSTOMER_APPROVED",
              step_code=step, step_desc=step_desc,
              validation_status="APPROVED",
              received_at=received_at, original_date=requested_date,
              reason=f"Customer {customer_code} ({customer_name}) found in CFG.Dynamics_Stores")
        return True

    msg = (f"CustomerCode {customer_code} ({customer_name}) "
           f"not found in CFG.Dynamics_Stores — ORDER HELD")
    con_hold(f"  {msg}")
    err(msg)
    proc_log(lc, step, step_desc, "WARN", msg, order_id=order_id)
    audit(lc, order_id, customer_code, customer_name,
          event_type="CUSTOMER_HELD",
          step_code=step, step_desc=step_desc,
          validation_status="HELD",
          received_at=received_at, original_date=requested_date,
          reason=msg)
    return False

# =========================================================
# VALIDATION — PRODUCTS
# =========================================================
def validate_products(conn, lc, order_id, customer_code, customer_name,
                      received_at, requested_date,
                      approved_products) -> tuple[list, list, list]:
    """
    Returns (approved, removed, passthrough).
    approved    — in CFG.Products.Dynamics_Code
    passthrough — not in CFG.Products but Duracell category (FSDU/co-pack)
    removed     — not in CFG.Products and not Duracell — stripped from order
    """
    step      = "SCHEDULE_VAL_002"
    step_desc = "Product validation against CFG.Products.Dynamics_Code"

    lines       = conn.execute(GET_ORDER_LINES_SQL, order_id).fetchall()
    approved    = []
    removed     = []
    passthrough = []

    for ln in lines:
        pc   = ln[0].strip()
        cat  = (ln[1] or "").strip()
        sub  = (ln[2] or "").strip()
        desc = (ln[3] or "").strip()

        if pc in approved_products:
            approved.append(pc)
            proc_log(lc, step, step_desc, "OK",
                     f"Product {pc} approved",
                     order_id=order_id)
            audit(lc, order_id, customer_code, customer_name,
                  event_type="PRODUCT_APPROVED",
                  step_code=step, step_desc=step_desc,
                  validation_status="APPROVED",
                  product_code=pc, product_category=cat,
                  product_subcategory=sub, product_description=desc,
                  received_at=received_at, original_date=requested_date,
                  reason=f"Product {pc} found in CFG.Products.Dynamics_Code")

        elif is_passthrough(pc, cat, sub):
            passthrough.append(pc)
            msg = (f"Product {pc} ({desc}) not in CFG.Products — "
                   f"PASSTHROUGH (Duracell FSDU/co-pack) "
                   f"[{cat} / {sub}]")
            con_passthrough(f"  {pc} — {desc} [{cat} / {sub}]")
            log(msg)
            proc_log(lc, step, step_desc, "INFO", msg, order_id=order_id)
            audit(lc, order_id, customer_code, customer_name,
                  event_type="PRODUCT_PASSTHROUGH",
                  step_code=step, step_desc=step_desc,
                  validation_status="PASSTHROUGH",
                  product_code=pc, product_category=cat,
                  product_subcategory=sub, product_description=desc,
                  received_at=received_at, original_date=requested_date,
                  reason=msg)

        else:
            removed.append(pc)
            msg = (f"Product {pc} not in CFG.Products.Dynamics_Code "
                   f"and not Duracell category — removed "
                   f"[Cat={cat} / Sub={sub}]")
            con_removed(f"  {pc} | {cat} / {sub}")
            err(msg)
            conn.execute(REMOVE_PRODUCT_SQL, order_id, pc)
            proc_log(lc, step, step_desc, "WARN", msg, order_id=order_id)
            audit(lc, order_id, customer_code, customer_name,
                  event_type="PRODUCT_REMOVED",
                  step_code=step, step_desc=step_desc,
                  validation_status="REMOVED",
                  product_code=pc, product_category=cat,
                  product_subcategory=sub, product_description=desc,
                  received_at=received_at, original_date=requested_date,
                  reason=msg)

    conn.commit()

    summary = (f"Order {order_id} — "
               f"{len(approved)} approved | "
               f"{len(passthrough)} passthrough | "
               f"{len(removed)} removed"
               + (f" ({', '.join(removed)})" if removed else ""))
    proc_log(lc, step, step_desc,
             "WARN" if removed else "OK",
             summary, order_id=order_id)

    return approved, removed, passthrough

# =========================================================
# PROCESS ONE ORDER
# =========================================================
def process_one(conn, lc, row, holidays,
                approved_customers, approved_products,
                idx, total) -> str:

    order_id      = row.OrderId
    customer_code = row.CustomerCode
    customer_name = row.CustomerName
    received_raw  = row.OrderDate
    requested_raw = row.DateRequired

    requested_date = requested_raw.date() if hasattr(requested_raw, 'date') else requested_raw
    received_at    = received_raw if isinstance(received_raw, datetime) else \
                     datetime.combine(received_raw, datetime.min.time())

    con_order_header(order_id, customer_name, customer_code, idx, total)
    con_info(f"Received : {received_at.strftime('%Y-%m-%d %H:%M')}  "
             f"|  Requested: {requested_date}")

    # ── VAL_001: Customer ─────────────────────────────────
    con_info("[VAL_001] Validating customer...")
    if not validate_customer(lc, order_id, customer_code, customer_name,
                             received_at, requested_date, approved_customers):
        conn.execute(INSERT_STG_HELD_SQL,
            order_id, customer_code, customer_name, received_at,
            requested_date,
            f"Customer {customer_code} not in CFG.Dynamics_Stores",
            PROCESS_CODE)
        conn.commit()
        log(f"HELD | Order={order_id} | Customer not approved")
        return 'held'

    # ── VAL_002: Products ─────────────────────────────────
    con_info("[VAL_002] Validating products...")
    approved_lines, removed_lines, passthrough_lines = validate_products(
        conn, lc, order_id, customer_code, customer_name,
        received_at, requested_date, approved_products
    )

    active = len(approved_lines) + len(passthrough_lines)

    if active == 0:
        msg = f"No active products remain for Order {order_id} — ORDER HELD"
        con_hold(f"  {msg}")
        conn.execute(INSERT_STG_HELD_SQL,
            order_id, customer_code, customer_name, received_at,
            requested_date, msg, PROCESS_CODE)
        conn.commit()
        proc_log(lc, "SCHEDULE_VAL_002",
                 "Product validation against CFG.Products.Dynamics_Code",
                 "WARN", msg, order_id=order_id)
        audit(lc, order_id, customer_code, customer_name,
              event_type="ORDER_HELD",
              step_code="SCHEDULE_VAL_002",
              step_desc="All products removed — order held",
              validation_status="HELD",
              received_at=received_at, original_date=requested_date,
              reason=msg)
        log(f"HELD | Order={order_id} | No active products")
        return 'held'

    if removed_lines:
        con_warn(f"  {len(removed_lines)} removed | "
                 f"{len(approved_lines)} approved | "
                 f"{len(passthrough_lines)} passthrough — order continues")

    # ── SCHEDULE_002: Delivery Date ───────────────────────
    con_info("[SCHEDULE_002] Evaluating delivery date...")
    earliest, rule = calculate_earliest_delivery(received_at, holidays)
    final_date, reason, was_changed = resolve_delivery_date(
        requested_date, earliest, holidays
    )
    ship_date = prev_business_day(final_date, holidays)
    cutoff    = f"{CUTOFF_HOUR}:00"

    if was_changed:
        con_reschedule(f"  {requested_date} → {final_date} | {reason}")
    else:
        con_confirmed(f"  {final_date} | {reason}")

    proc_log(lc, "SCHEDULE_002", "Evaluate delivery date per order",
             "INFO",
             f"Order {order_id} | Requested: {requested_date} | "
             f"Final: {final_date} | Ship: {ship_date} | Changed: {was_changed}",
             order_id=order_id)

    # ── SCHEDULE_003: Write STG ───────────────────────────
    con_info("[SCHEDULE_003] Writing to STG.WASP_STO_Scheduled...")
    conn.execute(INSERT_STG_SQL,
        order_id, customer_code, customer_name, received_at,
        requested_date, final_date, ship_date,
        1 if was_changed else 0,
        reason, rule, PROCESS_CODE)

    proc_log(lc, "SCHEDULE_003", "Write to STG.WASP_STO_Scheduled",
             "OK",
             f"Order {order_id} | Delivery={final_date} | Ship={ship_date} | "
             f"Approved={len(approved_lines)} | "
             f"Passthrough={len(passthrough_lines)} | "
             f"Removed={len(removed_lines)}",
             order_id=order_id)

    # ── SCHEDULE_004: Audit ───────────────────────────────
    con_info("[SCHEDULE_004] Writing audit log...")
    audit(lc, order_id, customer_code, customer_name,
          event_type="SCHEDULE",
          step_code="SCHEDULE_002",
          step_desc="Delivery date evaluation",
          validation_status="RESCHEDULED" if was_changed else "CONFIRMED",
          received_at=received_at,
          original_date=requested_date,
          new_date=final_date,
          ship_date=ship_date,
          cutoff=cutoff,
          rule=rule,
          reason=reason)

    conn.commit()

    log(f"{'RESCHEDULED' if was_changed else 'CONFIRMED'} | "
        f"Order={order_id} | "
        f"{requested_date} → {final_date} | Ship={ship_date} | "
        f"Approved={len(approved_lines)} | "
        f"Passthrough={len(passthrough_lines)} | "
        f"Removed={len(removed_lines)} | {reason}")

    return 'rescheduled' if was_changed else 'confirmed'

# =========================================================
# MAIN
# =========================================================
def main():
    con_section("STO DELIVERY SCHEDULING — STARTUP")
    con_info(f"INI         : {INI_PATH}")
    con_info(f"DB          : {db.get('server')} / {db.get('database')}")
    con_info(f"Cutoff      : {CUTOFF_HOUR}:00  |  Min lead: {MIN_LEAD_DAYS} business days")
    con_info(f"Passthrough : {PASSTHROUGH_PATTERNS}")

    conn = get_conn()
    lc   = get_conn_ac()
    con_ok("DB connections established")

    # ── Reference data ────────────────────────────────────
    con_section("LOADING REFERENCE DATA")

    proc_log(lc, "SCHEDULE_001", "Load reference data",
             "INFO", "Loading bank holidays, customers and products")

    holidays           = load_bank_holidays(lc)
    approved_customers = load_approved_customers(lc)
    approved_products  = load_approved_products(lc)

    con_ok(f"Bank holidays    : {len(holidays)}")
    con_ok(f"Approved customers: {len(approved_customers)}")
    con_ok(f"Approved products : {len(approved_products)}")

    proc_log(lc, "SCHEDULE_001", "Load reference data",
             "OK",
             f"Holidays={len(holidays)} | "
             f"Customers={len(approved_customers)} | "
             f"Products={len(approved_products)}")

    # ── Pending orders ────────────────────────────────────
    con_section("SCANNING FOR PENDING ORDERS")
    read_conn = get_conn_ac()
    rows      = read_conn.execute(GET_PENDING_SQL).fetchall()
    read_conn.close()

    if not rows:
        con_warn("No orders pending scheduling — nothing to do.")
        proc_log(lc, "SCHEDULE_002", "Scan pending orders",
                 "WARN", "No pending orders found")
        log("No pending orders.")
        conn.close()
        lc.close()
        return

    con_ok(f"Found {len(rows)} order(s) to evaluate")
    proc_log(lc, "SCHEDULE_002", "Scan pending orders",
             "INFO", f"Found {len(rows)} order(s)")

    # ── Process ───────────────────────────────────────────
    con_section("PROCESSING ORDERS")
    counts = {"total": len(rows), "confirmed": 0,
              "rescheduled": 0, "held": 0, "failed": 0}

    for idx, row in enumerate(rows, start=1):
        try:
            result = process_one(conn, lc, row, holidays,
                                 approved_customers, approved_products,
                                 idx, len(rows))
            counts[result] += 1
        except Exception as ex:
            counts["failed"] += 1
            err(f"FAILED OrderId={row.OrderId}: {ex}")
            dbg(traceback.format_exc())
            proc_log(lc, "SCHEDULE_002", "Process order",
                     "ERROR", f"Failed OrderId={row.OrderId}: {ex}",
                     order_id=row.OrderId)
            try:
                conn.rollback()
            except Exception:
                pass

    # ── Summary ───────────────────────────────────────────
    summary = (f"Total={counts['total']} "
               f"Confirmed={counts['confirmed']} "
               f"Rescheduled={counts['rescheduled']} "
               f"Held={counts['held']} "
               f"Failed={counts['failed']}")
    log(f"Run complete — {summary}")
    proc_log(lc, "SCHEDULE_002", "Run complete", "OK", summary)

    con_summary(counts)
    conn.close()
    lc.close()


if __name__ == "__main__":
    main()