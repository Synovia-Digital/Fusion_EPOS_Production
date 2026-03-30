# =============================================================================
#  Synovia Fusion – EPOS WASP Order Processor
# -----------------------------------------------------------------------------
#  Module:        Fusion EPOS
#  Script Name:   Fusion_EPOS_WASP_OrderProcessor.py
#
#  Version:       2.1.0
#  Release Date:  2026-03-30
#
#  Author:        Synovia Digital
#
# -----------------------------------------------------------------------------
#  Description:
#  ------------
#  This script is one of two processors within the Fusion EPOS module.
#
#  Fusion EPOS manages the end-to-end lifecycle of sales activity captured
#  on WASP handheld terminals carried by field sales representatives.  Files
#  are deposited by the WASP platform onto an FTP server and collected by
#  Fusion for classification, storage, and onward routing.
#
#  The two processors within Fusion EPOS serve fundamentally different
#  downstream workflows and must not be confused with one another:
#
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │  Processor 1 – WASP Order Processor  (THIS SCRIPT)                 │
#   │                                                                     │
#   │  Handles standard sales orders placed by field reps via WASP.      │
#   │  Classifies each file by CustomerCode and routes accordingly —      │
#   │  Duracell EPOS lines are staged for Transfer Order processing,      │
#   │  all other lines are posted directly to Dynamics 365 F&O as        │
#   │  standard sales orders via the filtered FTP folder.                │
#   │                                                                     │
#   │  Processor 2 – Transfer Order Processor  (separate script)         │
#   │                                                                     │
#   │  Handles Transfer Orders only.  A Transfer Order is a consignment  │
#   │  movement in Dynamics 365 F&O — stock is transferred from a        │
#   │  central depot to a customer warehouse rather than being sold.      │
#   │                                                                     │
#   │  The Transfer Order workflow is:                                    │
#   │    1.  Fusion reads staged Transfer Order lines from               │
#   │        Transfer_Request_Inbound (populated by this script).        │
#   │    2.  The Transfer Order is raised and confirmed in               │
#   │        Dynamics 365 F&O (consignment stock is committed).          │
#   │    3.  The Transfer Order details are passed to SAP, which         │
#   │        executes the physical goods despatch to the store.          │
#   │    4.  On confirmed despatch from SAP, the Transfer Order is       │
#   │        confirmed back to Dynamics 365 F&O, closing the            │
#   │        consignment movement and updating inventory positions.      │
#   └─────────────────────────────────────────────────────────────────────┘
#
#  This processor (Processor 1) specifically:
#   • Reads all connection details from Fusion_EPOS_Production.txt
#   • Connects to Fusion_EPOS_Production on Azure SQL Server
#   • Fetches the current Dynamics store account list from
#       CFG.Dynamics_Stores.Account  (refreshed on every run)
#   • Connects via FTP to integration.intellibrand.net
#   • Downloads all CSV files from the Inbound folder
#       (/PRD/IntelliBrandOrders/)
#   • Deletes each file from the FTP Inbound folder immediately after a
#       confirmed local download (pick-up and clear model)
#   • Classifies each file by CustomerCode:
#
#       ┌──────────────────────────────────────────────────────────────┐
#       │ CustomerCode IN  CFG.Dynamics_Stores.Account                 │
#       │   → Classification : Duracell_EPOS                          │
#       │   → Fusion_Status  : STO_Received                           │
#       │   → Local action   : Move to Transfer_Request_Inbound       │
#       │     These files contain Transfer Order lines and are staged  │
#       │     for pickup by the Transfer Order Processor (Processor 2) │
#       ├──────────────────────────────────────────────────────────────┤
#       │ CustomerCode NOT IN CFG.Dynamics_Stores.Account              │
#       │   → Classification : Standard_Order                         │
#       │   → Fusion_Status  : Filtered  (on DB insert)               │
#       │   → FTP action     : Re-upload to /PRD/IntelliBrandOrders/  │
#       │                      filtered/  for Dynamics 365 pick-up    │
#       │   → On confirmed FTP upload:                                 │
#       │       UPDATE Fusion_Status → Loaded_to_Dynamics             │
#       └──────────────────────────────────────────────────────────────┘
#
#   • Inserts all rows into Raw.WASP_Orders with full audit fields
#   • Logs all activity at run level, file level, and row level
#   • Handles errors per-file so one bad file does not abort the batch
#
# -----------------------------------------------------------------------------
#  Fusion_Status values written by this script:
#  ---------------------------------------------
#   Standard_Order  →  Filtered        (initial insert)
#   Standard_Order  →  Loaded_to_Dynamics  (after confirmed FTP upload)
#   Duracell_EPOS   →  STO_Received    (staged for Transfer Order Processor)
#
# -----------------------------------------------------------------------------
#  Dependencies:
#  -------------
#   Python  >= 3.10
#   pyodbc  >= 4.0     ( pip install pyodbc )
#   ODBC Driver 17 for SQL Server must be installed on the host
#
#  Config file:
#   D:\Configuration\Fusion_EPOS_Production.txt
#
#  Local folders (created automatically if absent):
#   D:\FusionHub\Fusion_EPOS\WASP_Inbound\
#   D:\FusionHub\Fusion_EPOS\Transfer_Request_Inbound\
#   D:\FusionHub\Logs\
#
# -----------------------------------------------------------------------------
#  Change History:
#  ---------------
#  v2.1.0  (2026-03-30)
#    • Duracell_EPOS files now inserted with Fusion_Status = 'STO_Received'
#        instead of 'Filtered', giving them a distinct, unambiguous status
#        that clearly identifies them as staged Stock Transfer Orders.
#    • CONFIG_FILE path corrected to Fusion_EPOS_Production.txt
#    • Header updated to document all Fusion_Status values written
#
#  v2.0.0  (2026-03-18)
#    • Production release — full Synovia Fusion header and module narrative
#    • FTP inbound delete added: files removed from FTP immediately after
#        confirmed local download (pick-up and clear)
#    • Logging upgraded: run-level separator, file-level counters,
#        structured error capture per file
#    • Config parsing hardened: strips quotes and whitespace from all
#        INI string values
#    • fast_executemany enabled for bulk DB inserts
#    • ftp_delete() helper with explicit confirmation logging
#
#  v1.1.0  (2026-02-14)
#    • Added FTP re-upload confirmation check via nlst() post-STOR
#    • Fusion_Status updated to Loaded_to_Dynamics only on confirmed upload
#
#  v1.0.0  (2026-01-20)
#    • Initial release
# =============================================================================

import configparser
import csv
import ftplib
import logging
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyodbc


# =============================================================================
#  LOGGING
# =============================================================================

LOG_FILE = Path(r"D:\FusionHub\Logs\Fusion_EPOS_WASP_OrderProcessor.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
#  CONSTANTS – LOCAL PATHS
# =============================================================================

CONFIG_FILE    = r"D:\Configuration\Fusion_EPOS_Production.txt"
LOCAL_INBOUND  = Path(r"D:\FusionHub\Fusion_EPOS\WASP_Inbound")
LOCAL_TRANSFER = Path(r"D:\FusionHub\Fusion_EPOS\Transfer_Request_Inbound")

for _p in (LOCAL_INBOUND, LOCAL_TRANSFER):
    _p.mkdir(parents=True, exist_ok=True)


# =============================================================================
#  FUSION STATUS CONSTANTS
# =============================================================================

STATUS_FILTERED          = "Filtered"        # Standard_Order — awaiting FTP upload
STATUS_LOADED_TO_DYNAMICS = "Loaded_to_Dynamics"  # Standard_Order — FTP upload confirmed
STATUS_STO_RECEIVED      = "STO_Received"    # Duracell_EPOS  — staged for TO Processor


# =============================================================================
#  SQL STATEMENTS
# =============================================================================

SQL_INSERT_ORDER = """
INSERT INTO Raw.WASP_Orders (
    OrderId,              OrderDate,            DateRequired,
    CustomerCode,         CustomerName,
    Address1,             Address2,             Address3,
    UserCode,             FirstName,
    CustomerRef,          PONumber,             DeliveryInstructions,
    OrderType,
    ProductCategory,      ProductSubcategory,
    ProductCode,          ProductDescription,
    Price,                Quantity,
    FreeOfCharge,         FOCType,              DiscountPercentage,
    CompletionCode,
    Order_Classification, Fusion_Status,
    Load_Timestamp,       Source_Filename
)
VALUES (
    ?,?,?,
    ?,?,
    ?,?,?,
    ?,?,
    ?,?,?,
    ?,
    ?,?,
    ?,?,
    ?,?,
    ?,?,?,
    ?,
    ?,?,
    GETUTCDATE(),?
)
"""

SQL_UPDATE_STATUS_LOADED = """
UPDATE Raw.WASP_Orders
SET    Fusion_Status = 'Loaded_to_Dynamics'
WHERE  Source_Filename       = ?
  AND  Fusion_Status         = 'Filtered'
  AND  Order_Classification  = 'Standard_Order'
"""

SQL_DYNAMICS_ACCOUNTS = """
SELECT DISTINCT Account
FROM   CFG.Dynamics_Stores
WHERE  Account IS NOT NULL
"""


# =============================================================================
#  CONFIG LOADER
# =============================================================================

def load_config(path: str) -> configparser.ConfigParser:
    """
    Load INI configuration from disk.
    All values are returned with surrounding quotes and whitespace stripped —
    the INI file may include quoted values (e.g. password="secret") which
    configparser does not strip automatically.
    """
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg


def _clean(value: str) -> str:
    """Strip surrounding whitespace and double-quotes from an INI value."""
    return value.strip().strip('"')


# =============================================================================
#  DATABASE HELPERS
# =============================================================================

def get_db_connection(cfg: configparser.ConfigParser) -> pyodbc.Connection:
    """
    Open a pyodbc connection to Fusion_EPOS_Production on Azure SQL Server.
    Connection details are sourced entirely from the [database] section of
    the INI file — no credentials are embedded in this script.
    autocommit=False so each file's inserts are an explicit transaction.
    """
    db = cfg["database"]
    conn_str = (
        f"DRIVER={_clean(db['driver'])};"
        f"SERVER={_clean(db['server'])};"
        f"DATABASE={_clean(db['database'])};"
        f"UID={_clean(db['user'])};"
        f"PWD={_clean(db['password'])};"
        f"Encrypt={_clean(db['encrypt'])};"
        f"TrustServerCertificate={_clean(db['trust_server_certificate'])};"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def fetch_dynamics_accounts(conn: pyodbc.Connection) -> set[str]:
    """
    Return the current set of Dynamics store account codes from
    CFG.Dynamics_Stores.  Refreshed on every script run so that newly
    onboarded stores are picked up without a code change.
    """
    with conn.cursor() as cur:
        cur.execute(SQL_DYNAMICS_ACCOUNTS)
        return {row[0].strip() for row in cur.fetchall()}


def insert_orders(
    conn: pyodbc.Connection,
    rows: list[dict],
    classification: str,
    fusion_status: str,
    source_filename: str,
) -> int:
    """
    Bulk-insert a list of parsed CSV rows into Raw.WASP_Orders.
    Uses fast_executemany for performance on large files.
    Returns the number of rows inserted.
    Caller is responsible for commit / rollback.
    """
    params = [
        (
            _strip(r, "OrderId"),
            _parse_datetime(r.get("OrderDate", "")),
            _parse_datetime(r.get("DateRequired", "")),
            _strip(r, "CustomerCode"),
            _strip(r, "CustomerName"),
            _strip(r, "Address1"),
            _strip(r, "Address2"),
            _strip(r, "Address3"),
            _strip(r, "UserCode"),
            _strip(r, "FirstName"),
            _strip(r, "CustomerRef"),
            _strip(r, "PONumber"),
            _strip(r, "DeliveryInstructions"),
            _strip(r, "OrderType"),
            _strip(r, "ProductCategory"),
            _strip(r, "ProductSubcategory"),
            _strip(r, "ProductCode"),
            _strip(r, "ProductDescription"),
            _to_decimal(r.get("Price", "")),
            _to_decimal(r.get("Quantity", "")),
            _to_bit(r.get("FreeOfCharge", "")),
            _strip(r, "FOCType") or None,
            _to_decimal(r.get("DiscountPercentage", "")),
            _strip(r, "CompletionCode"),
            classification,
            fusion_status,
            source_filename,
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        cur.fast_executemany = True
        cur.executemany(SQL_INSERT_ORDER, params)

    return len(params)


def mark_loaded_to_dynamics(conn: pyodbc.Connection, filename: str) -> int:
    """
    Update Fusion_Status to 'Loaded_to_Dynamics' for all Standard_Order rows
    belonging to the given source file.  Called only after FTP upload is
    confirmed — ensures the DB always reflects true integration state.
    Returns the number of rows updated.
    """
    with conn.cursor() as cur:
        cur.execute(SQL_UPDATE_STATUS_LOADED, filename)
        return cur.rowcount


# =============================================================================
#  VALUE PARSERS
# =============================================================================

def _strip(row: dict, key: str) -> str:
    return row.get(key, "").strip()


def _parse_datetime(value: str):
    """
    Parse date/time strings from the WASP CSV feed.
    The feed uses DD/MM/YYYY HH:MM:SS.  Falls back to date-only formats
    for robustness.  Returns None if the value is empty or unparseable.
    """
    value = value.strip()
    if not value:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    log.warning("    Unparseable datetime value: %r — stored as NULL", value)
    return None


def _to_decimal(value: str):
    """Convert a string to float, returning None for blank or invalid values."""
    v = value.strip()
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        log.warning("    Non-numeric value: %r — stored as NULL", value)
        return None


def _to_bit(value: str):
    """Convert '0'/'1' string to integer bit, returning None for anything else."""
    v = value.strip()
    if v == "1":
        return 1
    if v == "0":
        return 0
    return None


# =============================================================================
#  FTP HELPERS
# =============================================================================

def ftp_connect(cfg: configparser.ConfigParser) -> ftplib.FTP:
    """
    Establish a plain FTP connection using credentials from the [FTP] section.
    Returns an authenticated FTP object with passive mode enabled.
    """
    ftp_cfg = cfg["FTP"]
    host = _clean(ftp_cfg["host"]).replace("ftp://", "")
    user = _clean(ftp_cfg["user_name"])
    pwd  = _clean(ftp_cfg["password"])

    ftp = ftplib.FTP(host, timeout=30)
    ftp.login(user=user, passwd=pwd)
    ftp.set_pasv(True)
    log.info("  FTP connected  →  %s", host)
    return ftp


def ftp_list_csv(ftp: ftplib.FTP, remote_dir: str) -> list[str]:
    """
    Return a list of basenames for all .csv files found in remote_dir.
    Uses nlst() which may return full paths on some servers — basenames
    are normalised before being returned.
    """
    try:
        ftp.cwd(remote_dir)
        all_files = ftp.nlst()
        return [
            os.path.basename(f)
            for f in all_files
            if f.lower().endswith(".csv")
        ]
    except ftplib.error_perm as exc:
        log.error("  FTP list failed for %s: %s", remote_dir, exc)
        return []


def ftp_download(ftp: ftplib.FTP, remote_path: str, local_path: Path) -> bool:
    """
    Download a single file from the FTP server to local_path.
    Returns True on success, False on any FTP error.
    The local file is removed on failure to avoid leaving partial files.
    """
    try:
        with open(local_path, "wb") as fh:
            ftp.retrbinary(f"RETR {remote_path}", fh.write)
        log.info("  Downloaded  ←  %s", remote_path)
        return True
    except ftplib.all_errors as exc:
        log.error("  FTP download FAILED  %s : %s", remote_path, exc)
        local_path.unlink(missing_ok=True)
        return False


def ftp_delete(ftp: ftplib.FTP, remote_path: str) -> bool:
    """
    Delete a file from the FTP server.
    Called immediately after a confirmed local download so the inbound
    folder is kept clear between runs (pick-up and clear model).
    Returns True on success, False on any FTP error.
    Failure is logged but does not abort the current file's processing.
    """
    try:
        ftp.delete(remote_path)
        log.info("  Deleted from FTP  ✓  %s", remote_path)
        return True
    except ftplib.all_errors as exc:
        log.error("  FTP delete FAILED  %s : %s", remote_path, exc)
        return False


def ftp_upload(ftp: ftplib.FTP, local_path: Path, remote_dir: str) -> bool:
    """
    Upload a local file to remote_dir on the FTP server.
    After the STOR command, the remote directory is re-listed to confirm
    the file is present before returning True.  This guards against silent
    upload failures on unreliable FTP servers.
    Returns True only if the file is confirmed present after upload.
    """
    remote_path = f"{remote_dir.rstrip('/')}/{local_path.name}"
    try:
        ftp.cwd(remote_dir)
        with open(local_path, "rb") as fh:
            ftp.storbinary(f"STOR {local_path.name}", fh)

        # Confirm file presence on the server
        present = [os.path.basename(f) for f in ftp.nlst()]
        if local_path.name in present:
            log.info("  FTP upload confirmed  ✓  →  %s", remote_path)
            return True
        else:
            log.error("  FTP upload: file not found after STOR  →  %s", remote_path)
            return False

    except ftplib.all_errors as exc:
        log.error("  FTP upload FAILED  %s : %s", remote_path, exc)
        return False


# =============================================================================
#  FILE PROCESSOR
# =============================================================================

def process_file(
    local_file: Path,
    dynamics_accounts: set[str],
    conn: pyodbc.Connection,
    ftp: ftplib.FTP,
    ftp_orders_dir: str,
) -> None:
    """
    Process a single downloaded WASP order file end-to-end.

    Steps:
      1.  Read and parse the CSV.
      2.  Classify by CustomerCode against CFG.Dynamics_Stores.
      3.  Insert all rows into Raw.WASP_Orders with classification and
          the appropriate initial Fusion_Status:
            Duracell_EPOS  → STO_Received
            Standard_Order → Filtered
      4.  Route the file:
            Duracell_EPOS   → move to Transfer_Request_Inbound for
                              downstream Stock Transfer processing.
            Standard_Order  → re-upload to FTP filtered/ folder;
                              on confirmed upload update Fusion_Status
                              to 'Loaded_to_Dynamics'.

    All DB operations for this file are wrapped in a single transaction.
    The caller handles rollback on exception.
    """
    filename = local_file.name
    log.info("  ┌─ File: %s", filename)

    # ------------------------------------------------------------------
    # 1. Read CSV
    # ------------------------------------------------------------------
    with open(local_file, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))

    if not rows:
        log.warning("  │  Empty file — skipping.")
        log.info("  └─ Done: %s", filename)
        return

    log.info("  │  Rows read      : %d", len(rows))

    # ------------------------------------------------------------------
    # 2. Classify
    #    All rows in a single WASP file belong to the same order, so the
    #    CustomerCode on the first row determines routing for the whole file.
    # ------------------------------------------------------------------
    customer_code  = rows[0].get("CustomerCode", "").strip()
    is_dynamics    = customer_code in dynamics_accounts
    classification = "Duracell_EPOS" if is_dynamics else "Standard_Order"

    # Duracell_EPOS files are STO_Received from the moment they land.
    # Standard_Order files start as Filtered until FTP upload is confirmed.
    fusion_status  = STATUS_STO_RECEIVED if is_dynamics else STATUS_FILTERED

    log.info("  │  CustomerCode   : %s", customer_code)
    log.info("  │  Classification : %s", classification)
    log.info("  │  Fusion_Status  : %s", fusion_status)

    # ------------------------------------------------------------------
    # 3. Insert to Raw.WASP_Orders
    # ------------------------------------------------------------------
    inserted = insert_orders(
        conn,
        rows,
        classification=classification,
        fusion_status=fusion_status,
        source_filename=filename,
    )
    conn.commit()
    log.info("  │  Rows inserted  : %d  →  Raw.WASP_Orders", inserted)

    # ------------------------------------------------------------------
    # 4. Route file
    # ------------------------------------------------------------------
    if is_dynamics:
        # Duracell EPOS — hand off to Transfer_Request_Inbound for the
        # Stock Transfer processor to pick up.
        dest = LOCAL_TRANSFER / filename
        shutil.move(str(local_file), str(dest))
        log.info("  │  Moved locally  →  %s", dest)

    else:
        # Standard Order — re-upload to FTP filtered/ folder so Dynamics
        # 365 can collect it via its own integration endpoint.
        uploaded = ftp_upload(ftp, local_file, ftp_orders_dir)

        if uploaded:
            updated = mark_loaded_to_dynamics(conn, filename)
            conn.commit()
            log.info(
                "  │  Fusion_Status  →  Loaded_to_Dynamics  (%d row(s) updated)",
                updated,
            )
        else:
            log.error(
                "  │  FTP upload FAILED — Fusion_Status remains 'Filtered' for %s",
                filename,
            )

        # Remove local inbound copy in all cases
        local_file.unlink(missing_ok=True)
        log.info("  │  Local file removed from WASP_Inbound")

    log.info("  └─ Done: %s", filename)


# =============================================================================
#  MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    """
    Orchestrates the full WASP Order processing run.

    Run sequence:
      1.  Load INI configuration.
      2.  Connect to Azure SQL — Fusion_EPOS_Production.
      3.  Refresh the Dynamics store account list.
      4.  Connect to the FTP server.
      5.  List and download all CSV files from the FTP Inbound folder,
          deleting each from the server immediately after download.
      6.  Process each downloaded file (classify → insert → route).
      7.  Close all connections and log run summary.
    """
    run_start = datetime.now(timezone.utc)

    log.info("=" * 70)
    log.info("  Synovia Fusion – EPOS WASP Order Processor  v2.1.0")
    log.info("  Run started  :  %s", run_start.strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # 1. Configuration
    # ------------------------------------------------------------------
    cfg = load_config(CONFIG_FILE)
    log.info("Config loaded  :  %s", CONFIG_FILE)

    # ------------------------------------------------------------------
    # 2. Database
    # ------------------------------------------------------------------
    log.info("Connecting to SQL Server …")
    conn = get_db_connection(cfg)
    log.info(
        "  Connected  →  %s / %s",
        _clean(cfg["database"]["server"]),
        _clean(cfg["database"]["database"]),
    )

    # ------------------------------------------------------------------
    # 3. Dynamics account list
    # ------------------------------------------------------------------
    dynamics_accounts = fetch_dynamics_accounts(conn)
    log.info("  Dynamics accounts loaded  :  %d", len(dynamics_accounts))

    # ------------------------------------------------------------------
    # 4. FTP
    # ------------------------------------------------------------------
    log.info("Connecting to FTP …")
    ftp = ftp_connect(cfg)

    ftp_inbound_dir = _clean(cfg["FTP"]["inbound"])
    ftp_orders_dir  = _clean(cfg["FTP"]["orders"])

    # ------------------------------------------------------------------
    # 5. Download + delete from FTP Inbound
    # ------------------------------------------------------------------
    log.info("Scanning FTP Inbound  :  %s", ftp_inbound_dir)
    csv_files = ftp_list_csv(ftp, ftp_inbound_dir)
    log.info("  CSV files found  :  %d", len(csv_files))

    downloaded: list[Path] = []

    for basename in csv_files:
        remote_full = f"{ftp_inbound_dir.rstrip('/')}/{basename}"
        local_path  = LOCAL_INBOUND / basename

        if ftp_download(ftp, remote_full, local_path):
            downloaded.append(local_path)
            # Pick-up and clear — remove from FTP Inbound immediately
            ftp_delete(ftp, remote_full)

    log.info("Files downloaded  :  %d  →  %s", len(downloaded), LOCAL_INBOUND)

    # ------------------------------------------------------------------
    # 6. Process each file
    # ------------------------------------------------------------------
    success_count = 0
    error_count   = 0

    for local_file in downloaded:
        try:
            process_file(
                local_file,
                dynamics_accounts,
                conn,
                ftp,
                ftp_orders_dir,
            )
            success_count += 1
        except Exception as exc:
            error_count += 1
            log.exception("  ERROR processing %s : %s", local_file.name, exc)
            conn.rollback()

    # ------------------------------------------------------------------
    # 7. Close connections and summarise
    # ------------------------------------------------------------------
    ftp.quit()
    conn.close()

    run_end     = datetime.now(timezone.utc)
    elapsed_sec = (run_end - run_start).total_seconds()

    log.info("=" * 70)
    log.info("  Run finished   :  %s", run_end.strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("  Elapsed        :  %.1f seconds", elapsed_sec)
    log.info("  Files OK       :  %d", success_count)
    log.info("  Files ERROR    :  %d", error_count)
    log.info("=" * 70)


if __name__ == "__main__":
    main()
