# -*- coding: utf-8 -*-
"""
Fusion EPOS Consignment Pre-Flight Reconciliation Gate — FILE SCOPED + ORDER/ITEM ACCURATE (NO CLI)

SCOPE:
- Picks the most recent COMPLETED load from RAW.EPOS_File for Region + Supplier_Number
- Scopes RAW lines by File_ID (RAW.EPOS_LineItems links to File_ID)

RECONCILIATION (per order+item):
RAW_Pos_Qty      = SUM(Units_Sold where Units_Sold > 0)
REMOVED_Neg_Qty  = SUM(ABS(Original_Quantity) where Processed_Action='Removed' AND Original_Quantity < 0)
(Original_Quantity = 0 is noise and ignored)

KEY FIX (your failing case):
Sometimes EXC has negative removals for items with NO RAW positive qty for that order+item.
We cap removals to RAW_Pos_Qty so Expected_Stage never goes negative:
  Effective_Removed_Neg_Qty = MIN(Removed_Neg_Qty, Raw_Pos_Qty)
  Expected_Stage_Qty = Raw_Pos_Qty - Effective_Removed_Neg_Qty

PASS (per order+item):
  Stage_Qty == Expected_Stage_Qty

Hard gate:
- Exit 1 if any item fails OR any RAW item is unmapped to Dynamics item.
- Exit 1 if no eligible orders found for the selected File_ID.
"""

from __future__ import annotations

import datetime as dt
import configparser
import traceback
from dataclasses import dataclass
from pathlib import Path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import warnings

import pandas as pd
import pyodbc

# ------------------------------
# TOP CONFIG (NO CLI)
# ------------------------------

OUTDIR = Path(r"D:\madrid")

INI_PATH = Path(r"D:\Configuration\Fusion_EPOS_Production.ini")
INI_SECTION = "Fusion_EPOS_Production"

REGION = "ROI"
SUPPLIER_NUMBER = 3380

SEND_EMAIL = True
SYNOVIA_TEST = 1
DIST_LIST_KIND = "weekly"

RECIPIENTS_OVERRIDE: list[str] = [
    # "servicedesk@primeline.ie",
]

SMTP_SERVER_FALLBACK = "smtp.office365.com"
SMTP_PORT_FALLBACK = 587
SENDER_EMAIL_FALLBACK = "nexus@synoviaintegration.com"

FILE_TABLE = "RAW.EPOS_File"
RAW_TABLE = "RAW.EPOS_LineItems"
PRODUCTS_TABLE = "CFG.Products"
STAGE_TABLE = "INT.SalesOrderStage"
EXC_TABLE = "EXC.Processed_Lines"
DIST_TABLE = "CFG.Email_Distribution"

DIST_FLAG_MAP = {
    "general": "General",
    "support": "Support",
    "exception": "Exception",
    "monitoring": "Monitoring",
    "weekly": "Weekly",
}

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)


@dataclass
class DbCfg:
    server: str
    database: str
    username: str
    password: str
    driver: str = "ODBC Driver 17 for SQL Server"
    encrypt: str = "yes"
    trust_server_certificate: str = "no"


def _yn(v: str | None, default: str) -> str:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return "yes"
    if s in ("0", "false", "no", "n", "off"):
        return "no"
    return default


def load_db_cfg(ini_path: Path, section: str) -> DbCfg:
    cp = configparser.ConfigParser()
    cp.read(ini_path, encoding="utf-8")
    sec = cp[section]
    return DbCfg(
        server=sec.get("server").strip(),
        database=sec.get("database").strip(),
        username=(sec.get("username", "") or sec.get("user", "")).strip(),
        password=sec.get("password").strip(),
        driver=sec.get("driver", "ODBC Driver 17 for SQL Server").strip(),
        encrypt=_yn(sec.get("encrypt", "yes"), "yes"),
        trust_server_certificate=_yn(sec.get("trust_server_certificate", "no"), "no"),
    )


def load_smtp_cfg(ini_path: Path, section: str) -> dict:
    cp = configparser.ConfigParser()
    cp.read(ini_path, encoding="utf-8")
    sec = cp[section]

    ini_server = (sec.get("smtp_server") or "").strip()
    ini_port = sec.getint("smtp_port", SMTP_PORT_FALLBACK)
    ini_user = (sec.get("smtp_user") or "").strip()
    ini_password = (sec.get("smtp_password") or "").strip()

    server = ini_server or (SMTP_SERVER_FALLBACK or "").strip()
    port = int(ini_port or SMTP_PORT_FALLBACK)
    user = ini_user or (SENDER_EMAIL_FALLBACK or "").strip()

    return {
        "server": server,
        "port": port,
        "user": user,
        "password": ini_password,
        "source": "ini" if ini_server else ("constants" if server else "none"),
    }


def connect_db(cfg: DbCfg):
    conn_str = (
        f"DRIVER={{{cfg.driver}}};"
        f"SERVER={cfg.server};"
        f"DATABASE={cfg.database};"
        f"UID={cfg.username};"
        f"PWD={cfg.password};"
        f"Encrypt={cfg.encrypt};"
        f"TrustServerCertificate={cfg.trust_server_certificate};"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def log_line(log_path: Path, msg: str):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def try_send_email(subject: str, html_body: str, attachments: list[Path], recipients: list[str], smtp_cfg: dict, log_path: Path) -> bool:
    if not recipients:
        log_line(log_path, "WARNING: Recipient list empty; skipping email.")
        return False

    host = (smtp_cfg.get("server") or "").strip()
    port = int(smtp_cfg.get("port") or 0)
    if not host or port <= 0:
        log_line(log_path, "WARNING: SMTP not configured; skipping email.")
        return False

    msg = MIMEMultipart()
    msg["From"] = smtp_cfg.get("user", "")
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    for fp in attachments:
        if fp.exists():
            with open(fp, "rb") as f:
                part = MIMEApplication(f.read(), Name=fp.name)
            part["Content-Disposition"] = f'attachment; filename="{fp.name}"'
            msg.attach(part)

    try:
        with smtplib.SMTP(host=host, port=port, timeout=30) as server:
            code, _ = server.ehlo()
            log_line(log_path, f"SMTP ({smtp_cfg.get('source','?')}) EHLO: {code}")
            if server.has_extn("starttls"):
                server.starttls()
                code2, _ = server.ehlo()
                log_line(log_path, f"SMTP STARTTLS enabled; EHLO: {code2}")
            else:
                log_line(log_path, "SMTP server does not advertise STARTTLS; sending without TLS.")

            user = (smtp_cfg.get("user") or "").strip()
            pwd = (smtp_cfg.get("password") or "").strip()
            if user and pwd:
                server.login(user, pwd)
                log_line(log_path, "SMTP login OK")
            else:
                log_line(log_path, "SMTP login skipped (no smtp_password in INI)")

            server.send_message(msg)
        return True
    except Exception as e:
        log_line(log_path, f"WARNING: Email send failed: {e}")
        log_line(log_path, traceback.format_exc())
        return False


def get_recipients(cur) -> list[str]:
    override = [r.strip() for r in RECIPIENTS_OVERRIDE if r and r.strip()]
    if override:
        return sorted(set(override))

    cols = {
        r[0].lower()
        for r in cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'CFG'
              AND TABLE_NAME = 'Email_Distribution'
        """).fetchall()
    }
    if "email" not in cols:
        raise RuntimeError("CFG.Email_Distribution is missing required column: Email")

    flag_col = DIST_FLAG_MAP.get(DIST_LIST_KIND.lower())
    where_bits = ["SynoviaTest = ?"]
    params = [int(SYNOVIA_TEST)]
    if flag_col and flag_col.lower() in cols:
        where_bits.append(f"ISNULL([{flag_col}],0)=1")

    sql = f"""
        SELECT [Email]
        FROM {DIST_TABLE}
        WHERE {" AND ".join(where_bits)}
          AND [Email] IS NOT NULL
          AND LTRIM(RTRIM([Email])) <> ''
    """
    rows = cur.execute(sql, *params).fetchall()
    recips = sorted({r[0].strip() for r in rows if r and r[0]})
    if not recips:
        raise RuntimeError(f"No recipients found in {DIST_TABLE} for SynoviaTest={SYNOVIA_TEST} list={DIST_LIST_KIND}.")
    return recips


def resolve_latest_completed_file(conn) -> tuple[int, dict]:
    df = pd.read_sql(f"""
        SELECT TOP (1)
            [File_ID],
            [FileName],
            [Region],
            [Supplier_Number],
            [Year],
            [Week],
            [Record_Count],
            [Status],
            [Inserted_Timestamp],
            [Completed_Timestamp]
        FROM {FILE_TABLE}
        WHERE [Completed_Timestamp] IS NOT NULL
          AND [Status] IS NOT NULL
          AND LOWER(LTRIM(RTRIM([Status]))) = 'completed'
          AND [Region] = ?
          AND [Supplier_Number] = ?
        ORDER BY [Completed_Timestamp] DESC, [Inserted_Timestamp] DESC
    """, conn, params=[REGION, int(SUPPLIER_NUMBER)])
    if df.empty:
        raise RuntimeError(f"No completed file found in {FILE_TABLE} for Region={REGION} Supplier_Number={SUPPLIER_NUMBER}.")
    meta = df.loc[0].to_dict()
    return int(meta["File_ID"]), meta


def build_outputs(conn, file_id: int):
    cur = conn.cursor()

    cur.execute("IF OBJECT_ID('tempdb..#FileIDs') IS NOT NULL DROP TABLE #FileIDs;")
    cur.execute("CREATE TABLE #FileIDs (File_ID int NOT NULL PRIMARY KEY);")
    cur.execute("INSERT INTO #FileIDs (File_ID) VALUES (?);", int(file_id))

    cur.execute("IF OBJECT_ID('tempdb..#Dockets') IS NOT NULL DROP TABLE #Dockets;")
    cur.execute("CREATE TABLE #Dockets (CustomersOrderReference nvarchar(100) NOT NULL PRIMARY KEY);")
    cur.execute(f"""
        INSERT INTO #Dockets (CustomersOrderReference)
        SELECT DISTINCT CAST(r.[Docket_Reference] AS nvarchar(100))
        FROM {RAW_TABLE} r
        INNER JOIN #FileIDs f ON f.File_ID = r.File_ID
        WHERE ISNULL(r.[Units_Sold],0) > 0
          AND r.[Docket_Reference] IS NOT NULL;
    """)
    docket_count = int(cur.execute("SELECT COUNT(*) FROM #Dockets;").fetchone()[0])

    item_detail = pd.read_sql(f"""
        ;WITH RawScoped AS (
            SELECT
                CAST(r.Docket_Reference AS nvarchar(100)) AS CustomersOrderReference,
                r.Dunnes_Prod_Code,
                r.Retail_Barcode,
                SUM(CASE WHEN ISNULL(r.Units_Sold,0) > 0 THEN r.Units_Sold ELSE 0 END) AS Raw_Pos_Qty,
                COUNT(*) AS Raw_Line_Count
            FROM {RAW_TABLE} r
            INNER JOIN #FileIDs f ON f.File_ID = r.File_ID
            WHERE r.Docket_Reference IS NOT NULL
            GROUP BY CAST(r.Docket_Reference AS nvarchar(100)), r.Dunnes_Prod_Code, r.Retail_Barcode
        ),
        RawMapped AS (
            SELECT
                rs.CustomersOrderReference,
                p.Dynamics_Code AS ItemNumber,
                rs.Dunnes_Prod_Code,
                rs.Retail_Barcode,
                rs.Raw_Pos_Qty,
                rs.Raw_Line_Count
            FROM RawScoped rs
            LEFT JOIN {PRODUCTS_TABLE} p
              ON p.Dunnes_Prod_Code = rs.Dunnes_Prod_Code
        ),
        Removed AS (
            SELECT
                CAST(e.CustomersOrderReference AS nvarchar(100)) AS CustomersOrderReference,
                CAST(e.ItemNumber AS nvarchar(50)) AS ItemNumber,
                SUM(CASE WHEN ISNULL(e.Original_Quantity,0) < 0 THEN ABS(e.Original_Quantity) ELSE 0 END) AS Removed_Neg_Qty,
                SUM(CASE WHEN ISNULL(e.Original_Quantity,0) = 0 THEN 1 ELSE 0 END) AS Removed_Zero_Lines,
                COUNT(*) AS Removed_Line_Count
            FROM {EXC_TABLE} e
            WHERE e.CustomersOrderReference IN (SELECT CustomersOrderReference FROM #Dockets)
              AND LOWER(LTRIM(RTRIM(ISNULL(e.Processed_Action,'')))) = 'removed'
            GROUP BY CAST(e.CustomersOrderReference AS nvarchar(100)), CAST(e.ItemNumber AS nvarchar(50))
        ),
        Stage AS (
            SELECT
                CAST(s.CustomersOrderReference AS nvarchar(100)) AS CustomersOrderReference,
                CAST(s.ItemNumber AS nvarchar(50)) AS ItemNumber,
                SUM(ISNULL(s.OrderedSalesQuantity,0)) AS Stage_Qty,
                COUNT(*) AS Stage_Line_Count
            FROM {STAGE_TABLE} s
            WHERE s.CustomersOrderReference IN (SELECT CustomersOrderReference FROM #Dockets)
            GROUP BY CAST(s.CustomersOrderReference AS nvarchar(100)), CAST(s.ItemNumber AS nvarchar(50))
        )
        SELECT
            COALESCE(rm.CustomersOrderReference, r.CustomersOrderReference, st.CustomersOrderReference) AS CustomersOrderReference,
            COALESCE(rm.ItemNumber, r.ItemNumber, st.ItemNumber) AS ItemNumber,

            rm.Dunnes_Prod_Code,
            rm.Retail_Barcode,
            ISNULL(rm.Raw_Pos_Qty, 0) AS Raw_Pos_Qty,
            ISNULL(rm.Raw_Line_Count, 0) AS Raw_Line_Count,

            ISNULL(r.Removed_Neg_Qty, 0) AS Removed_Neg_Qty,
            ISNULL(r.Removed_Zero_Lines, 0) AS Removed_Zero_Lines,
            ISNULL(r.Removed_Line_Count, 0) AS Removed_Line_Count,

            CASE
                WHEN ISNULL(rm.Raw_Pos_Qty,0) < ISNULL(r.Removed_Neg_Qty,0) THEN ISNULL(rm.Raw_Pos_Qty,0)
                ELSE ISNULL(r.Removed_Neg_Qty,0)
            END AS Effective_Removed_Neg_Qty,

            ISNULL(st.Stage_Qty, 0) AS Stage_Qty,
            ISNULL(st.Stage_Line_Count, 0) AS Stage_Line_Count,

            (ISNULL(rm.Raw_Pos_Qty,0) -
                CASE
                    WHEN ISNULL(rm.Raw_Pos_Qty,0) < ISNULL(r.Removed_Neg_Qty,0) THEN ISNULL(rm.Raw_Pos_Qty,0)
                    ELSE ISNULL(r.Removed_Neg_Qty,0)
                END
            ) AS Expected_Stage_Qty,

            (ISNULL(st.Stage_Qty,0) -
                (ISNULL(rm.Raw_Pos_Qty,0) -
                    CASE
                        WHEN ISNULL(rm.Raw_Pos_Qty,0) < ISNULL(r.Removed_Neg_Qty,0) THEN ISNULL(rm.Raw_Pos_Qty,0)
                        ELSE ISNULL(r.Removed_Neg_Qty,0)
                    END
                )
            ) AS Delta_Qty,

            CASE
                WHEN COALESCE(rm.CustomersOrderReference, r.CustomersOrderReference, st.CustomersOrderReference) IS NULL THEN 0
                WHEN COALESCE(rm.ItemNumber, r.ItemNumber, st.ItemNumber) IS NULL THEN 0
                WHEN (ISNULL(st.Stage_Qty,0) -
                    (ISNULL(rm.Raw_Pos_Qty,0) -
                        CASE
                            WHEN ISNULL(rm.Raw_Pos_Qty,0) < ISNULL(r.Removed_Neg_Qty,0) THEN ISNULL(rm.Raw_Pos_Qty,0)
                            ELSE ISNULL(r.Removed_Neg_Qty,0)
                        END
                    )
                ) = 0 THEN 1
                ELSE 0
            END AS Pass_Item
        FROM RawMapped rm
        FULL OUTER JOIN Removed r
          ON r.CustomersOrderReference = rm.CustomersOrderReference
         AND r.ItemNumber = rm.ItemNumber
        FULL OUTER JOIN Stage st
          ON st.CustomersOrderReference = COALESCE(rm.CustomersOrderReference, r.CustomersOrderReference)
         AND st.ItemNumber = COALESCE(rm.ItemNumber, r.ItemNumber)
        WHERE COALESCE(rm.CustomersOrderReference, r.CustomersOrderReference, st.CustomersOrderReference) IS NOT NULL
    """, conn)

    if not item_detail.empty:
        item_detail["Pass_Item"] = item_detail["Pass_Item"].astype(int)

    unmapped = item_detail[(item_detail["Dunnes_Prod_Code"].notna()) & (item_detail["ItemNumber"].isna())].copy()
    diag = item_detail[(item_detail["Removed_Neg_Qty"].fillna(0) > 0) & (item_detail["Delta_Qty"].fillna(0) != 0)].copy()

    if item_detail.empty:
        order_summary = pd.DataFrame(columns=[
            "CustomersOrderReference", "Items", "Items_Failed", "Unmapped_Items",
            "Sum_Raw_Pos_Qty", "Sum_Removed_Neg_Qty", "Sum_Effective_Removed_Neg_Qty",
            "Sum_Stage_Qty", "Sum_Expected_Stage_Qty", "Sum_Delta_Qty",
            "Pass_Order",
        ])
    else:
        g = item_detail.groupby("CustomersOrderReference", dropna=False)
        order_summary = g.agg(
            Items=("ItemNumber", "count"),
            Items_Failed=("Pass_Item", lambda s: int((s == 0).sum())),
            Unmapped_Items=("ItemNumber", lambda s: int(s.isna().sum())),
            Sum_Raw_Pos_Qty=("Raw_Pos_Qty", "sum"),
            Sum_Removed_Neg_Qty=("Removed_Neg_Qty", "sum"),
            Sum_Effective_Removed_Neg_Qty=("Effective_Removed_Neg_Qty", "sum"),
            Sum_Stage_Qty=("Stage_Qty", "sum"),
            Sum_Expected_Stage_Qty=("Expected_Stage_Qty", "sum"),
            Sum_Delta_Qty=("Delta_Qty", "sum"),
        ).reset_index()
        order_summary["Pass_Order"] = (order_summary["Items_Failed"] == 0) & (order_summary["Unmapped_Items"] == 0)

    meta = {"file_id": int(file_id), "docket_count": docket_count}
    return order_summary, item_detail, unmapped, diag, meta


def render_html(file_meta: dict, meta: dict, order_summary: pd.DataFrame, item_detail: pd.DataFrame, overall_pass: bool) -> str:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    badge = "PASS ✅" if overall_pass else "FAIL ❌"
    failed_orders = order_summary[order_summary["Pass_Order"] == False].copy() if not order_summary.empty else order_summary  # noqa: E712
    n_fail = int(failed_orders.shape[0]) if not order_summary.empty else 0

    def to_tbl(dfx: pd.DataFrame, limit: int | None = None) -> str:
        if limit is not None:
            dfx = dfx.head(limit)
        return dfx.to_html(index=False, escape=True)

    scope = f"""
    <ul>
      <li><b>File_ID:</b> {file_meta.get('File_ID')}</li>
      <li><b>FileName:</b> {file_meta.get('FileName')}</li>
      <li><b>Region:</b> {file_meta.get('Region')}</li>
      <li><b>Supplier_Number:</b> {file_meta.get('Supplier_Number')}</li>
      <li><b>Year/Week:</b> {file_meta.get('Year')}/{file_meta.get('Week')}</li>
      <li><b>Completed:</b> {file_meta.get('Completed_Timestamp')}</li>
      <li><b>Orders evaluated:</b> {meta.get('docket_count', 0)}</li>
      <li><b>Failed orders:</b> {n_fail}</li>
      <li><b>Rule:</b> Stage_Qty == RAW_Pos_Qty - min(Removed_Neg_Qty, RAW_Pos_Qty) per order+item</li>
      <li><b>Removed Qty=0:</b> ignored</li>
    </ul>
    """

    return f"""
    <html>
      <head><meta charset="utf-8"/></head>
      <body style="font-family: Arial, sans-serif;">
        <h2>Fusion EPOS Consignment Gate (File scoped, Order+Item) — {badge}</h2>
        <p><b>Generated:</b> {now}</p>
        <h3>Scope</h3>
        {scope}

        {"<h3>Failed Orders (top 200)</h3>" + to_tbl(failed_orders, 200) if n_fail else "<h3>Failed Orders</h3><p>None 🎉</p>"}

        <h3>Order Summary (top 200)</h3>
        {to_tbl(order_summary, 200) if not order_summary.empty else "<p>No orders found.</p>"}

        <h3>Item Detail (top 200)</h3>
        {to_tbl(item_detail, 200) if not item_detail.empty else "<p>No item detail found.</p>"}
      </body>
    </html>
    """


def main() -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    cfg = load_db_cfg(INI_PATH, INI_SECTION)
    smtp_cfg = load_smtp_cfg(INI_PATH, INI_SECTION)

    conn = connect_db(cfg)
    try:
        file_id, file_meta = resolve_latest_completed_file(conn)

        log_path = OUTDIR / f"fusion_gate_file{file_id}_{ts}.log"
        orders_csv = OUTDIR / f"fusion_gate_file{file_id}_{ts}_orders.csv"
        items_csv = OUTDIR / f"fusion_gate_file{file_id}_{ts}_items.csv"
        unmapped_csv = OUTDIR / f"fusion_gate_file{file_id}_{ts}_unmapped.csv"
        diag_csv = OUTDIR / f"fusion_gate_file{file_id}_{ts}_diag.csv"
        html_path = OUTDIR / f"fusion_gate_file{file_id}_{ts}.html"

        log_line(log_path, f"START Gate File_ID={file_id} Region={REGION} Supplier_Number={SUPPLIER_NUMBER}")
        log_line(log_path, f"FileName={file_meta.get('FileName')} Year/Week={file_meta.get('Year')}/{file_meta.get('Week')} Completed={file_meta.get('Completed_Timestamp')}")
        log_line(log_path, f"SEND_EMAIL={SEND_EMAIL} DistList={DIST_LIST_KIND} SynoviaTest={SYNOVIA_TEST}")
        log_line(log_path, f"SMTP source={smtp_cfg.get('source')} server='{smtp_cfg.get('server')}' port={smtp_cfg.get('port')} user='{smtp_cfg.get('user')}'")

        order_summary, item_detail, unmapped, diag, meta = build_outputs(conn, file_id)

        if meta.get("docket_count", 0) == 0:
            overall_pass = False
            log_line(log_path, "FAIL: No eligible orders found in RAW for this File_ID (Units_Sold > 0).")
        else:
            overall_pass = bool(order_summary["Pass_Order"].all()) if not order_summary.empty else False

        order_summary.to_csv(orders_csv, index=False)
        item_detail.to_csv(items_csv, index=False)
        unmapped.to_csv(unmapped_csv, index=False)
        diag.to_csv(diag_csv, index=False)

        log_line(log_path, f"Wrote: {orders_csv}")
        log_line(log_path, f"Wrote: {items_csv}")
        log_line(log_path, f"Wrote: {unmapped_csv}")
        log_line(log_path, f"Wrote: {diag_csv}")

        html = render_html(file_meta, meta, order_summary, item_detail, overall_pass)
        html_path.write_text(html, encoding="utf-8")
        log_line(log_path, f"Wrote: {html_path}")

        subject = ("[PASS]" if overall_pass else "[FAIL]") + f" Fusion EPOS Gate File {file_id} {REGION}/{SUPPLIER_NUMBER} Y{file_meta.get('Year')} W{file_meta.get('Week')}"

        if SEND_EMAIL:
            cur = conn.cursor()
            recipients = get_recipients(cur)
            log_line(log_path, f"Recipients: {len(recipients)}")
            sent = try_send_email(subject, html, [orders_csv, items_csv, unmapped_csv, diag_csv, html_path, log_path], recipients, smtp_cfg, log_path)
            log_line(log_path, "Email sent." if sent else "Email skipped/failed (see warnings above).")
        else:
            log_line(log_path, "Email disabled (SEND_EMAIL=False).")

        failed_orders = int((order_summary["Pass_Order"] == False).sum()) if not order_summary.empty else 0  # noqa: E712
        log_line(log_path, f"END Gate status={'PASS' if overall_pass else 'FAIL'} Orders={meta.get('docket_count',0)} FailedOrders={failed_orders}")
        return 0 if overall_pass else 1

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
