"""
Synovia Fusion – PreFlight (Latest File) Report + Email Sender
-------------------------------------------------------------
- Finds latest COMPLETED EPOS file in RAW.EPOS_File
- Runs technical + business + postload validations scoped to that file
- Generates an Outlook-safe HTML email (no JSON-y details)
- Uses traffic-light status indicators
- Embeds logos via CID attachments (reliable for Outlook/Office365)
- Saves the HTML report copy to D:\EPOS_Mockup (created if missing)
- Sends email via Office365 SMTP

IMPORTANT:
- Do NOT log secrets.
- If Duracell logo file isn't found, the email will show a "Duracell" text badge.
"""

from __future__ import annotations

import os
import json
import smtplib
import configparser
import datetime as dt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from typing import Any, Dict, Optional, Tuple

import pyodbc
from _fusion_shared import resolve_smtp_password, connect_autocommit, ensure_dir, pick_first_existing, scalar, fmt_int, fmt_money, status_dot, status_chip, html_escape, OUTPUT_ROOT, FUSION_LOGO_CANDIDATES, RUNNING_BUNNY_PATH, SYNOVIA_FOOTER_PATH, DURACELL_LOGO_CANDIDATES, SMTP_SERVER, SMTP_PORT, SENDER_EMAIL

# =============================================================================
# SMTP SETTINGS (as provided)
# =============================================================================
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
SENDER_EMAIL = "nexus@synoviaintegration.com"

# RECIPIENTS will be loaded from CFG.Email_Distribution where Exception=1
RECIPIENTS = []


# =============================================================================
# Email distribution (CFG.Email_Distribution)
# =============================================================================

def load_exception_distribution(cur: pyodbc.Cursor) -> list[str]:
    """
    Returns list of email addresses where Exception = 1.
    """
    rows = cur.execute("""
        SELECT Email
        FROM CFG.Email_Distribution
        WHERE Exception = 1
          AND Email IS NOT NULL
          AND LTRIM(RTRIM(Email)) <> '';
    """).fetchall()
    emails = []
    for (e,) in rows:
        e = str(e).strip()
        if e and e not in emails:
            emails.append(e)
    return emails

# =============================================================================
# OUTPUT
# =============================================================================
OUTPUT_DIR = r"D:\EPOS_Mockup"

# =============================================================================
# DB BOOTSTRAP (ENV → INI → FAIL)
# =============================================================================
BOOTSTRAP = {
    "driver": "ODBC Driver 17 for SQL Server",
    "server": "futureworks-sdi-db.database.windows.net",
    "database": "Fusion_EPOS_Production",
    "username": "SynFW_DB",
    "timeout": 60,
}

PASSWORD_ENV = "FUSION_EPOS_BOOTSTRAP_PASSWORD"
INI_PATH = r"D:\Configuration\Fusion_EPOS_Production.ini"
INI_SECTION = "Fusion_EPOS_Production"

# =============================================================================
# LOGO ASSETS (CID embedded)
# =============================================================================
FUSION_LOGO_CANDIDATES = [
    r"D:\Configuration\FusionLogo.jpg",  # critical
    r"D:\Graphics\FusionLogo.jpg",
]
RUNNING_BUNNY_PATH = r"D:\Graphics\RunningBunny.jpg"
SYNOVIA_FOOTER_PATH = r"D:\Graphics\SynoviaLogoHor.jpg"

# Optional Duracell logo (best-effort). If missing, we show a text badge.
DURACELL_LOGO_CANDIDATES = [
    r"D:\Graphics\DuracellLogo.jpg",
    r"D:\Graphics\DuracellLogo.png",
    r"D:\Graphics\Duracell.jpg",
]

# =============================================================================
# VALIDATION SETTINGS
# =============================================================================
TOLERANCE_CURRENCY = float(os.environ.get("FUSION_EPOS_FOOTER_TOLERANCE", "0.01"))


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)



def scalar(cur: pyodbc.Cursor, sql: str, params: Tuple = ()) -> Any:
    row = cur.execute(sql, params).fetchone()
    return row[0] if row else None

def money(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0

def iint(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0

def fmt_money(v: float) -> str:
    return f"{v:,.2f}"

def fmt_int(v: int) -> str:
    return f"{v:,d}"

def pick_first_existing(paths: list[str]) -> Optional[str]:
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None

def load_image_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def status_dot(status: str) -> str:
    """
    Traffic light dot:
    PASS = green
    WARN = amber
    FAIL = red
    """
    st = status.upper()
    if st == "PASS":
        color = "#10b981"
    elif st == "WARN":
        color = "#f59e0b"
    else:
        color = "#ef4444"
    return f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:{color};margin-right:8px;vertical-align:middle;"></span>'

def status_chip(status: str) -> str:
    st = status.upper()
    if st == "PASS":
        bg, bd, fg = "#ecfdf5", "#a7f3d0", "#065f46"
    elif st == "WARN":
        bg, bd, fg = "#fff7ed", "#fed7aa", "#9a3412"
    else:
        bg, bd, fg = "#fef2f2", "#fecaca", "#991b1b"
    return f'<span style="display:inline-block;padding:3px 10px;border-radius:999px;background:{bg};border:1px solid {bd};color:{fg};font-weight:800;font-size:12px;">{st}</span>'

def nice_detail(label: str, a: Any, b: Any, op: str = "=") -> str:
    # renders: "Record_Count 977 = LineItems 977"
    return f"{label} {a} {op} {b}"

# -----------------------------------------------------------------------------
# Build HTML
# -----------------------------------------------------------------------------
def build_html(
    *,
    file_id: int,
    filename: str,
    region: str,
    supplier: int,
    year: int,
    week: int,
    overall_status: str,
    next_action: str,
    tiles: Dict[str, str],
    tech_rows: list[tuple[str, str, str]],
    business_rows: list[tuple[str, str, str]],
    post_rows: list[tuple[str, str, str]],
    syn_state: str,
    retail_sum: float,
    line_count: int,
    units_sum: int,
    distinct_orders: int,
    distinct_stores: int,
    distinct_products: int,
    show_duracell_logo: bool
) -> str:
    executed_utc = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # banner colors
    if overall_status == "PASS":
        banner_bg, banner_bd, banner_fg = "#ecfdf5", "#a7f3d0", "#065f46"
    elif overall_status == "WARN":
        banner_bg, banner_bd, banner_fg = "#fff7ed", "#fed7aa", "#9a3412"
    else:
        banner_bg, banner_bd, banner_fg = "#fef2f2", "#fecaca", "#991b1b"

    # Helper to render rows
    def render_section(rows: list[tuple[str, str, str]]) -> str:
        out = []
        for check, status, detail in rows:
            out.append(f"""
            <tr>
              <td style="padding:10px;border:1px solid #e5e7eb;font-size:12.5px;color:#111827;">{check}</td>
              <td style="padding:10px;border:1px solid #e5e7eb;font-size:12.5px;white-space:nowrap;">
                {status_dot(status)}{status_chip(status)}
              </td>
              <td style="padding:10px;border:1px solid #e5e7eb;font-size:12.5px;color:#475569;line-height:1.4;">{detail}</td>
            </tr>
            """)
        return "\n".join(out)

    tech_html = render_section(tech_rows)
    business_html = render_section(business_rows)
    post_html = render_section(post_rows)

    # Header logo row: FusionLogo + Duracell logo/text
    duracell_block = (
        '<img src="cid:duracell_logo" alt="Duracell" style="height:34px;width:auto;display:block;border:0;">'
        if show_duracell_logo else
        '<span style="display:inline-block;padding:6px 10px;border-radius:10px;background:#111827;color:#ffffff;font-weight:800;font-size:12px;">Duracell</span>'
    )

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width">
  <style>
    /* Montserrat request – fallbacks for email clients */
    body, table, td, div {{
      font-family: 'Montserrat', 'Segoe UI', Arial, sans-serif;
    }}
  </style>
</head>
<body style="margin:0;padding:0;background:#f6f8fb;">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">
    EPOS PreFlight – Latest File {file_id} – {overall_status}
  </div>

  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#f6f8fb;">
    <tr>
      <td align="center" style="padding:18px 12px;">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="900" style="max-width:900px;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;overflow:hidden;">

          <!-- Top header bar with logos -->
          <tr>
            <td style="padding:12px 18px;background:#0b1220;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td valign="middle" style="width:55%;">
                    <img src="cid:fusion_logo" alt="Fusion" style="height:36px;width:auto;display:block;border:0;">
                  </td>
                  <td valign="middle" align="right" style="width:45%;">
                    {duracell_block}
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Title row with bunny -->
          <tr>
            <td style="padding:16px 18px 12px 18px;background:#0b1220;color:#ffffff;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="font-size:18px;font-weight:900;">
                    EPOS – PreFlight Data Checks
                    <div style="font-size:12px;font-weight:400;opacity:0.85;margin-top:6px;line-height:1.5;">
                      System: <b>Fusion_EPOS</b> · Env: <b>PRD</b> · UTC: <b>{executed_utc}</b><br>
                      Latest File_ID: <b>{file_id}</b> · {filename}
                    </div>
                  </td>
                  <td align="right" valign="top">
                    <img src="cid:running_bunny" alt="Running Bunny" style="height:64px;width:auto;display:block;border:0;">
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Banner status -->
          <tr>
            <td style="padding:14px 18px;background:{banner_bg};border-bottom:1px solid #e5e7eb;">
              <div style="font-size:15px;font-weight:900;color:{banner_fg};">
                OVERALL RESULT: {overall_status} &nbsp; {status_chip(overall_status)}
              </div>
              <div style="margin-top:6px;font-size:12px;color:{banner_fg};line-height:1.5;">
                <b>Scope:</b> Latest completed file only &nbsp;|&nbsp;
                <b>Next action:</b> {next_action}
              </div>
            </td>
          </tr>

          <!-- Business style tiles -->
          <tr>
            <td style="padding:14px 18px;">
              <div style="font-size:14px;font-weight:900;color:#111827;margin-bottom:10px;">
                Business Impact Summary
              </div>

              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="padding:10px;border:1px solid #e5e7eb;border-radius:12px;background:#ffffff;">
                    <div style="font-size:11px;color:#6b7280;">Year / Week</div>
                    <div style="font-size:18px;font-weight:900;color:#111827;">{year} / {week}</div>
                    <div style="font-size:11px;color:#475569;margin-top:2px;">Region: {region} · Supplier: {supplier}</div>
                  </td>
                  <td width="10"></td>
                  <td style="padding:10px;border:1px solid #e5e7eb;border-radius:12px;background:#ffffff;">
                    <div style="font-size:11px;color:#6b7280;">Retail Total</div>
                    <div style="font-size:18px;font-weight:900;color:#111827;">{fmt_money(retail_sum)}</div>
                    <div style="font-size:11px;color:#475569;margin-top:2px;">Footer tolerance ±{TOLERANCE_CURRENCY}</div>
                  </td>
                  <td width="10"></td>
                  <td style="padding:10px;border:1px solid #e5e7eb;border-radius:12px;background:#ffffff;">
                    <div style="font-size:11px;color:#6b7280;">Lines / Units</div>
                    <div style="font-size:18px;font-weight:900;color:#111827;">{fmt_int(line_count)} / {fmt_int(units_sum)}</div>
                    <div style="font-size:11px;color:#475569;margin-top:2px;">
                      Orders: {fmt_int(distinct_orders)} · Stores: {fmt_int(distinct_stores)} · Products: {fmt_int(distinct_products)}
                    </div>
                  </td>
                </tr>

                <tr><td colspan="5" style="height:10px;"></td></tr>

                <tr>
                  <td style="padding:10px;border:1px solid #e5e7eb;border-radius:12px;background:#ffffff;">
                    <div style="font-size:11px;color:#6b7280;">Stores mapped</div>
                    <div style="font-size:18px;font-weight:900;color:#111827;">{tiles["stores_mapped"]}</div>
                    <div style="font-size:11px;color:#475569;margin-top:2px;">Customer delivery points ready</div>
                  </td>
                  <td width="10"></td>
                  <td style="padding:10px;border:1px solid #e5e7eb;border-radius:12px;background:#ffffff;">
                    <div style="font-size:11px;color:#6b7280;">Products mapped</div>
                    <div style="font-size:18px;font-weight:900;color:#111827;">{tiles["products_mapped"]}</div>
                    <div style="font-size:11px;color:#475569;margin-top:2px;">Dynamics item codes available</div>
                  </td>
                  <td width="10"></td>
                  <td style="padding:10px;border:1px solid #e5e7eb;border-radius:12px;background:#ffffff;">
                    <div style="font-size:11px;color:#6b7280;">Processing state</div>
                    <div style="font-size:18px;font-weight:900;color:#111827;">{syn_state}</div>
                    <div style="font-size:11px;color:#475569;margin-top:2px;">Downstream readiness indicator</div>
                  </td>
                </tr>
              </table>

              <div style="margin-top:12px;font-size:12.5px;color:#111827;line-height:1.55;">
                <b>Narrative:</b><br>
                • <b>Technical:</b> file structure is validated for year/week single and header counts.<br>
                • <b>Business:</b> store/customer mapping and product mapping are verified for this file (and uniqueness risks checked globally).<br>
                • <b>PostLoad:</b> footer totals are reconciled against line sums within tolerance.
              </div>
            </td>
          </tr>

          <!-- Checks tables -->
          <tr>
            <td style="padding:14px 18px;background:#fbfdff;border-top:1px solid #e5e7eb;border-bottom:1px solid #e5e7eb;">
              <div style="font-size:14px;font-weight:900;color:#111827;margin-bottom:10px;">
                Validation Detail (Latest File Only)
              </div>

              <div style="font-size:13px;font-weight:800;color:#111827;margin-top:10px;margin-bottom:6px;">Technical checks</div>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;background:#ffffff;">
                <tr>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Check</th>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Status</th>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Details</th>
                </tr>
                {tech_html}
              </table>

              <div style="font-size:13px;font-weight:800;color:#111827;margin-top:14px;margin-bottom:6px;">Business checks</div>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;background:#ffffff;">
                <tr>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Check</th>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Status</th>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Details</th>
                </tr>
                {business_html}
              </table>

              <div style="font-size:13px;font-weight:800;color:#111827;margin-top:14px;margin-bottom:6px;">PostLoad reconciliation checks</div>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;background:#ffffff;">
                <tr>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Check</th>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Status</th>
                  <th align="left" style="padding:10px;border:1px solid #e5e7eb;background:#f9fafb;font-size:12px;">Details</th>
                </tr>
                {post_html}
              </table>
            </td>
          </tr>

          <!-- Footer logo -->
          <tr>
            <td style="padding:0;">
              <img src="cid:synovia_footer" alt="Synovia" style="width:100%;height:auto;display:block;border:0;">
            </td>
          </tr>

          <tr>
            <td style="padding:10px 18px;font-size:11px;color:#94a3b8;">
              Generated by Synovia Fusion PreFlight engine · Email-safe HTML (no JS). If something fails, processing is paused by design.
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""
    return html


# -----------------------------------------------------------------------------
# Email sending (CID embedded images)
# -----------------------------------------------------------------------------
def send_email(html_body: str,
               *,
               subject: str,
               fusion_logo_path: str,
               bunny_path: str,
               syn_footer_path: str,
               duracell_logo_path: Optional[str]) -> None:

    msg = MIMEMultipart("related")
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(RECIPIENTS)
    msg["Subject"] = subject

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText("This email requires HTML to view the report.", "plain"))
    alt.attach(MIMEText(html_body, "html"))
    msg.attach(alt)

    # Attach images with content IDs
    def attach_img(path: str, cid: str) -> None:
        with open(path, "rb") as f:
            img = MIMEImage(f.read())
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=os.path.basename(path))
        msg.attach(img)

    attach_img(fusion_logo_path, "fusion_logo")
    attach_img(bunny_path, "running_bunny")
    attach_img(syn_footer_path, "synovia_footer")

    if duracell_logo_path and os.path.exists(duracell_logo_path):
        attach_img(duracell_logo_path, "duracell_logo")

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, resolve_smtp_password())
        server.sendmail(SENDER_EMAIL, RECIPIENTS, msg.as_string())


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    ensure_dir(OUTPUT_DIR)

    fusion_logo_path = pick_first_existing(FUSION_LOGO_CANDIDATES)
    if not fusion_logo_path:
        raise RuntimeError("Fusion logo not found. Expected D:\\Configuration\\FusionLogo.jpg or D:\\Graphics\\FusionLogo.jpg")

    if not os.path.exists(RUNNING_BUNNY_PATH):
        raise RuntimeError(f"RunningBunny.jpg not found at {RUNNING_BUNNY_PATH}")

    if not os.path.exists(SYNOVIA_FOOTER_PATH):
        raise RuntimeError(f"SynoviaLogoHor.jpg not found at {SYNOVIA_FOOTER_PATH}")

    duracell_logo_path = pick_first_existing(DURACELL_LOGO_CANDIDATES)

    with connect() as conn:
        cur = conn.cursor()

        # Load distribution list (Exception=1)
        global RECIPIENTS
        RECIPIENTS = load_exception_distribution(cur)
        if not RECIPIENTS:
            raise RuntimeError("No recipients found in CFG.Email_Distribution where Exception=1")

        # Latest completed file
        row = cur.execute("""
            SELECT TOP (1)
                File_ID, FileName, Region, Supplier_Number, [Year], [Week],
                Record_Count, Status, Inserted_Timestamp, Completed_Timestamp
            FROM RAW.EPOS_File
            WHERE Status = 'Completed'
            ORDER BY Completed_Timestamp DESC, File_ID DESC;
        """).fetchone()

        if not row:
            raise RuntimeError("No completed files found in RAW.EPOS_File.")

        file_id, filename, region, supplier, year, week, record_count, status, ins_ts, comp_ts = row

        # Footer
        footer = cur.execute("""
            SELECT TOP (1) Footer_ID, NumberOfLines, Qty_Total, Retail_Total
            FROM RAW.EPOS_FileFooter
            WHERE File_ID = ?
            ORDER BY Footer_ID DESC;
        """, (file_id,)).fetchone()

        if not footer:
            footer_id = footer_lines = footer_qty = footer_retail = None
        else:
            footer_id, footer_lines, footer_qty, footer_retail = footer

        # Line aggregates
        line_count = iint(scalar(cur, "SELECT COUNT(*) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        units_sum = iint(scalar(cur, "SELECT SUM(CAST(Units_Sold AS BIGINT)) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        retail_sum = round(money(scalar(cur, "SELECT SUM(CAST(Sales_Value_Inc_VAT AS FLOAT)) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,))), 2)

        distinct_stores = iint(scalar(cur, "SELECT COUNT(DISTINCT LTRIM(RTRIM(Store))) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        distinct_products = iint(scalar(cur, "SELECT COUNT(DISTINCT Dunnes_Prod_Code) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        distinct_orders = iint(scalar(cur, "SELECT COUNT(DISTINCT LTRIM(RTRIM(Docket_Reference))) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))

        # Year/week integrity
        distinct_years = iint(scalar(cur, "SELECT COUNT(DISTINCT [Year]) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        distinct_weeks = iint(scalar(cur, "SELECT COUNT(DISTINCT [Week]) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))

        # SynProcessed distribution
        syn_total = iint(scalar(cur, "SELECT COUNT(*) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        syn_1 = iint(scalar(cur, "SELECT SUM(CASE WHEN SynProcessed=1 THEN 1 ELSE 0 END) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        syn_0 = iint(scalar(cur, "SELECT SUM(CASE WHEN SynProcessed=0 THEN 1 ELSE 0 END) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        syn_null = iint(scalar(cur, "SELECT SUM(CASE WHEN SynProcessed IS NULL THEN 1 ELSE 0 END) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))
        syn_bad = iint(scalar(cur, "SELECT SUM(CASE WHEN SynProcessed NOT IN (0,1) AND SynProcessed IS NOT NULL THEN 1 ELSE 0 END) FROM RAW.EPOS_LineItems WHERE File_ID=?;", (file_id,)))

        if syn_null > 0 or syn_bad > 0:
            syn_state = "INVALID"
        elif syn_0 > 0 and syn_1 > 0:
            syn_state = "MIXED"
        elif syn_1 == syn_total and syn_total > 0:
            syn_state = "PROCESSED"
        elif syn_0 == syn_total and syn_total > 0:
            syn_state = "UNPROCESSED"
        else:
            syn_state = "UNKNOWN"

        # Business checks (scoped)
        unmapped_store_lines = iint(scalar(cur, """
            SELECT COUNT(*)
            FROM RAW.EPOS_LineItems l
            LEFT JOIN CFG.Dynamics_Stores s
              ON LTRIM(RTRIM(s.StoreCode)) = LTRIM(RTRIM(l.Store))
             AND ISNULL(s.Account_status,'') = 'Active'
            WHERE l.File_ID = ? AND s.StoreCode IS NULL;
        """, (file_id,)))

        unmapped_product_lines = iint(scalar(cur, """
            SELECT COUNT(*)
            FROM RAW.EPOS_LineItems l
            LEFT JOIN CFG.Products p
              ON p.Dunnes_Prod_Code = l.Dunnes_Prod_Code
             AND p.Dynamics_Code IS NOT NULL AND LTRIM(RTRIM(p.Dynamics_Code)) <> ''
            WHERE l.File_ID = ? AND p.Dunnes_Prod_Code IS NULL;
        """, (file_id,)))

        unresolved_lines = iint(scalar(cur, """
            SELECT COUNT(*)
            FROM RAW.EPOS_LineItems l
            LEFT JOIN CFG.Dynamics_Stores s
              ON LTRIM(RTRIM(s.StoreCode)) = LTRIM(RTRIM(l.Store))
             AND ISNULL(s.Account_status,'') = 'Active'
            LEFT JOIN CFG.Products p
              ON p.Dunnes_Prod_Code = l.Dunnes_Prod_Code
             AND p.Dynamics_Code IS NOT NULL AND LTRIM(RTRIM(p.Dynamics_Code)) <> ''
            WHERE l.File_ID = ?
              AND (s.StoreCode IS NULL OR p.Dunnes_Prod_Code IS NULL);
        """, (file_id,)))

        # Global uniqueness risks
        dup_active_storecodes = iint(scalar(cur, """
            SELECT COUNT(*)
            FROM (
                SELECT StoreCode
                FROM CFG.Dynamics_Stores
                WHERE ISNULL(Account_status,'') = 'Active'
                GROUP BY StoreCode
                HAVING COUNT(*) > 1
            ) d;
        """))

        dup_active_products = iint(scalar(cur, """
            SELECT COUNT(*)
            FROM (
                SELECT Dunnes_Prod_Code
                FROM CFG.Products
                WHERE Dynamics_Code IS NOT NULL AND LTRIM(RTRIM(Dynamics_Code)) <> ''
                GROUP BY Dunnes_Prod_Code
                HAVING COUNT(*) > 1
            ) d;
        """))

        # Build validation rows (human-readable details)
        tech_rows = []
        business_rows = []
        post_rows = []

        # Technical
        rc_ok = (iint(record_count) == line_count)
        tech_rows.append((
            "Header Record_Count matches LineItems count",
            "PASS" if rc_ok else "FAIL",
            nice_detail("Record_Count", iint(record_count), line_count)
        ))

        yw_ok = (distinct_years == 1 and distinct_weeks == 1)
        tech_rows.append((
            "File contains single Year and Week",
            "PASS" if yw_ok else "FAIL",
            f"Distinct Year(s): {distinct_years} · Distinct Week(s): {distinct_weeks}"
        ))

        # Business
        business_rows.append((
            "All stores mapped to active Dynamics store (latest file)",
            "PASS" if unmapped_store_lines == 0 else "FAIL",
            f"Unmapped store line items: {fmt_int(unmapped_store_lines)}"
        ))
        business_rows.append((
            "All products mapped to Dynamics_Code (latest file)",
            "PASS" if unmapped_product_lines == 0 else "FAIL",
            f"Unmapped product line items: {fmt_int(unmapped_product_lines)}"
        ))
        business_rows.append((
            "All lines resolve store + product (latest file)",
            "PASS" if unresolved_lines == 0 else "FAIL",
            f"Unresolved line items: {fmt_int(unresolved_lines)}"
        ))
        business_rows.append((
            "No duplicate ACTIVE StoreCodes (global)",
            "PASS" if dup_active_storecodes == 0 else "FAIL",
            f"Duplicate active StoreCode groups: {fmt_int(dup_active_storecodes)}"
        ))
        business_rows.append((
            "No duplicate ACTIVE Products (global)",
            "PASS" if dup_active_products == 0 else "FAIL",
            f"Duplicate active Dunnes_Prod_Code groups: {fmt_int(dup_active_products)}"
        ))

        # PostLoad
        if footer is None:
            post_rows.append(("Footer record exists for file", "FAIL", "No footer row found for this File_ID"))
            fl_ok = fq_ok = fr_ok = False
        else:
            fl_ok = (iint(footer_lines) == line_count)
            post_rows.append((
                "Footer NumberOfLines matches LineItems count",
                "PASS" if fl_ok else "FAIL",
                nice_detail("Footer lines", iint(footer_lines), line_count)
            ))

            fq_ok = (iint(footer_qty) == units_sum)
            post_rows.append((
                "Footer Qty_Total matches SUM(Units_Sold)",
                "PASS" if fq_ok else "FAIL",
                nice_detail("Footer qty", iint(footer_qty), units_sum)
            ))

            footer_retail_r2 = round(money(footer_retail), 2)
            fr_ok = abs(retail_sum - footer_retail_r2) <= TOLERANCE_CURRENCY
            post_rows.append((
                "Footer Retail_Total matches SUM(Sales_Value_Inc_VAT)",
                "PASS" if fr_ok else "FAIL",
                f"Footer retail {fmt_money(footer_retail_r2)} vs Sum {fmt_money(retail_sum)} (Tol ±{TOLERANCE_CURRENCY})"
            ))

        # Overall
        tech_ok = rc_ok and yw_ok
        business_ok = (unmapped_store_lines == 0 and unmapped_product_lines == 0 and unresolved_lines == 0 and dup_active_storecodes == 0 and dup_active_products == 0)
        post_ok = fl_ok and fq_ok and fr_ok

        blocking = []
        if not tech_ok:
            blocking.append("TECHNICAL")
        if not business_ok:
            blocking.append("BUSINESS")
        if not post_ok:
            blocking.append("POSTLOAD")

        overall_status = "PASS" if not blocking else "FAIL"
        next_action = "Proceed to the next stage (CTL/INT processing) for this file." if overall_status == "PASS" else "PAUSE processing. Resolve blocking failures for this file before continuing."

        tiles = {
            "stores_mapped": "YES" if unmapped_store_lines == 0 else "NO",
            "products_mapped": "YES" if unmapped_product_lines == 0 else "NO",
        }

        html = build_html(
            file_id=file_id,
            filename=filename,
            region=region,
            supplier=iint(supplier),
            year=iint(year),
            week=iint(week),
            overall_status=overall_status,
            next_action=next_action,
            tiles=tiles,
            tech_rows=tech_rows,
            business_rows=business_rows,
            post_rows=post_rows,
            syn_state=syn_state,
            retail_sum=retail_sum,
            line_count=line_count,
            units_sum=units_sum,
            distinct_orders=distinct_orders,
            distinct_stores=distinct_stores,
            distinct_products=distinct_products,
            show_duracell_logo=bool(duracell_logo_path),
        )

        # Save copy to disk
        ensure_dir(OUTPUT_DIR)
        out_file = os.path.join(
            OUTPUT_DIR,
            f"PreFlight_LatestFile_EmailReport_File{file_id}_{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}.html"
        )
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(html)

        # Email subject
        subject = f"EPOS - PreFlight Data Checks | File_ID {file_id} | {overall_status}"

        send_email(
            html,
            subject=subject,
            fusion_logo_path=fusion_logo_path,
            bunny_path=RUNNING_BUNNY_PATH,
            syn_footer_path=SYNOVIA_FOOTER_PATH,
            duracell_logo_path=duracell_logo_path
        )

        print(f"HTML saved: {out_file}")
        print(f"Email sent to: {', '.join(RECIPIENTS)}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
