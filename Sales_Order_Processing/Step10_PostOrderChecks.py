# -*- coding: utf-8 -*-
"""
Duracell - EPOS Control - Sales Order Reconciliation (Executive + Trend + Anomaly Insights)

Fixes:
- Product trend chart now plots HISTORY for each product (not just 1 week)
- Adds anomaly bullets:
  * Products down/up vs 4W average
  * Stores down/up vs 4W average
  * Big movers vs baseline

Dependencies:
  pip install pyodbc pandas matplotlib
"""

from __future__ import annotations

import configparser
import smtplib
from dataclasses import dataclass
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# ==========================================================
# CONFIG
# ==========================================================

INI_PATH = r"D:\Configuration\Fusion_EPOS_Production.ini"
SECTION = "Fusion_EPOS_Production"

OUTDIR = Path(r"D:\madrid")

FUSION_LOGO = Path(r"D:\Graphics\FusionLogo.jpg")
BUNNY_IMG = Path(r"D:\Graphics\RunningBunny.jpg")
SYNOVIA_LOGO = Path(r"D:\Graphics\SynoviaLogoHor.jpg")

SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

FOOTER_TEXT = (
    "Retail Value shown is EPOS retail value. "
    "This is an unmonitored mailbox; for issues raise a support desk ticket - Servicedesk@primeline.ie"
)


# ==========================================================
# UTIL
# ==========================================================

@dataclass
class Cfg:
    server: str
    database: str
    user: str
    password: str
    smtp_user: str
    smtp_password: str


def load_cfg() -> Cfg:
    cp = configparser.ConfigParser()
    cp.read(INI_PATH, encoding="utf-8")
    sec = cp[SECTION]
    return Cfg(
        server=sec["server"].strip(),
        database=sec["database"].strip(),
        user=sec["user"].strip(),
        password=sec["password"].strip(),
        smtp_user=sec.get("smtp_user", "servicedesk@primeline.ie").strip(),
        smtp_password=sec["smtp_password"].strip(),
    )


def connect_db(cfg: Cfg):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={cfg.server};"
        f"DATABASE={cfg.database};"
        f"UID={cfg.user};"
        f"PWD={cfg.password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str)


def qdf(conn, sql, params=None):
    return pd.read_sql(sql, conn, params=params or [])


def fmt_int(x) -> str:
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return "0"


def fmt_dec(x, p=2) -> str:
    try:
        return f"{float(x):,.{p}f}"
    except Exception:
        return f"{0.0:,.{p}f}"


def yes_no(v: bool) -> str:
    return "✅" if v else "❌"


def safe_float(x) -> float:
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def safe_int(x) -> int:
    try:
        return int(x) if x is not None else 0
    except Exception:
        return 0


def calendar_key_from_raw_year_week(raw_year: int, week: int) -> str:
    yy = raw_year % 100
    return f"{yy:02d}{int(week):02d}"


# ==========================================================
# SQL
# ==========================================================

SQL_LATEST_FILE = """
SELECT TOP 1 File_ID, FileName, Year, Week
FROM RAW.EPOS_File
WHERE Status='Completed'
ORDER BY Year DESC, Week DESC;
"""

SQL_RECIPIENTS = """
SELECT Email
FROM CFG.Email_Distribution
WHERE  Weekly=1;
"""

SQL_RAW_INTAKE = """
SELECT
    COUNT(*) AS Raw_Lines,
    COUNT(DISTINCT Docket_Reference) AS EPOS_Orders
FROM RAW.EPOS_LineItems
WHERE File_ID = ?;
"""

SQL_CTL_STATUS = """
SELECT
    COUNT(*) AS CTL_Lines,
    SUM(CASE WHEN Integration_Status='SO_STAGED' THEN 1 ELSE 0 END) AS Staged_Lines,
    SUM(CASE WHEN Integration_Status='REMOVED' THEN 1 ELSE 0 END) AS Removed_Lines
FROM CTL.EPOS_Line_Item
WHERE Source_File_ID = ?;
"""

SQL_AGG_COUNT = """
SELECT COUNT(*) AS Aggregated_Lines
FROM EXC.Processed_Lines e
INNER JOIN CTL.EPOS_Line_Item l
    ON l.EPOS_Line_ID = e.EPOS_Line_ID
WHERE l.Source_File_ID = ?
  AND e.Processed_Action = 'Aggregated';
"""

SQL_ORDERS_NO_LINES = """
;WITH H AS
(
    SELECT EPOS_Fusion_Key
    FROM CTL.EPOS_Header
    WHERE Source_File_ID = ?
),
NoLineOrders AS
(
    SELECT h.EPOS_Fusion_Key
    FROM H h
    LEFT JOIN CTL.EPOS_Line_Item l
        ON l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
    GROUP BY h.EPOS_Fusion_Key
    HAVING COUNT(l.EPOS_Line_ID) = 0
)
SELECT COUNT(*) AS Orders_No_Lines
FROM NoLineOrders;
"""

SQL_INT_COUNTS = """
;WITH Dockets AS
(
    SELECT DISTINCT Docket_Reference
    FROM RAW.EPOS_LineItems
    WHERE File_ID = ?
)
SELECT
    COUNT(*) AS INT_Lines,
    COUNT(DISTINCT s.CustomerRequisitionNumber) AS INT_Orders
FROM INT.SalesOrderStage s
INNER JOIN Dockets d
    ON d.Docket_Reference = s.CustomerRequisitionNumber;
"""

SQL_QTY_VARIANCE = """
;WITH Dockets AS
(
    SELECT DISTINCT Docket_Reference
    FROM RAW.EPOS_LineItems
    WHERE File_ID = ?
),
CTL_QTY AS
(
    SELECT
        h.Docket_Reference,
        l.Dynamics_Code,
        SUM(l.Units_Sold) AS CTL_Qty
    FROM CTL.EPOS_Header h
    INNER JOIN CTL.EPOS_Line_Item l
        ON l.EPOS_Fusion_Key = h.EPOS_Fusion_Key
    INNER JOIN Dockets d
        ON d.Docket_Reference = h.Docket_Reference
    WHERE h.Source_File_ID = ?
      AND l.Integration_Status = 'SO_STAGED'
    GROUP BY h.Docket_Reference, l.Dynamics_Code
),
INT_QTY AS
(
    SELECT
        s.CustomerRequisitionNumber AS Docket_Reference,
        s.ItemNumber AS Dynamics_Code,
        SUM(s.OrderedSalesQuantity) AS INT_Qty
    FROM INT.SalesOrderStage s
    INNER JOIN Dockets d
        ON d.Docket_Reference = s.CustomerRequisitionNumber
    GROUP BY s.CustomerRequisitionNumber, s.ItemNumber
)
SELECT
    SUM(ABS(c.CTL_Qty - ISNULL(i.INT_Qty,0))) AS Total_Qty_Variance
FROM CTL_QTY c
LEFT JOIN INT_QTY i
    ON i.Docket_Reference = c.Docket_Reference
   AND i.Dynamics_Code = c.Dynamics_Code;
"""

SQL_API_COUNTS = """
;WITH SOs AS
(
    SELECT DISTINCT D365_SO_Number AS SalesOrderNumber
    FROM CTL.EPOS_Header
    WHERE Source_File_ID = ?
      AND D365_SO_Number IS NOT NULL
)
SELECT
    COUNT(*) AS API_Header_Rows,
    SUM(CASE WHEN IsSuccess=1 AND HttpStatus BETWEEN 200 AND 299 THEN 1 ELSE 0 END) AS API_OK,
    SUM(CASE WHEN NOT (IsSuccess=1 AND HttpStatus BETWEEN 200 AND 299) THEN 1 ELSE 0 END) AS API_Fail
FROM API.RawCall_SalesOrderHeader h
INNER JOIN SOs s
    ON s.SalesOrderNumber = h.SalesOrderNumber;
"""

SQL_FACT_WEEKLY_WITH_DATES = """
SELECT
    f.CalendarKey,
    f.Dunnes_Year,
    f.Dunnes_Week,
    c.Start_Date,
    c.End_Date,
    SUM(f.Retail_Units) AS Units,
    SUM(f.Retail_Value) AS Value
FROM CUR.FactWeeklyRetailSales f
LEFT JOIN CFG.Calenders c
    ON c.Dunnes_Year = f.Dunnes_Year
   AND c.Dunnes_Week = f.Dunnes_Week
GROUP BY
    f.CalendarKey, f.Dunnes_Year, f.Dunnes_Week, c.Start_Date, c.End_Date
ORDER BY CAST(f.CalendarKey AS INT);
"""

SQL_FACT_PRODUCT_WEEKLY = """
SELECT
    f.CalendarKey,
    f.Dunnes_Year,
    f.Dunnes_Week,
    c.Start_Date,
    f.Dynamics_Code,
    f.Product_Description,
    SUM(f.Retail_Units) AS Units,
    SUM(f.Retail_Value) AS Value
FROM CUR.FactWeeklyRetailSales f
LEFT JOIN CFG.Calenders c
    ON c.Dunnes_Year = f.Dunnes_Year
   AND c.Dunnes_Week = f.Dunnes_Week
GROUP BY
    f.CalendarKey, f.Dunnes_Year, f.Dunnes_Week, c.Start_Date,
    f.Dynamics_Code, f.Product_Description
ORDER BY CAST(f.CalendarKey AS INT);
"""

SQL_FACT_STORE_WEEKLY = """
SELECT
    f.CalendarKey,
    c.Start_Date,
    f.Store_Name,
    SUM(f.Retail_Units) AS Units,
    SUM(f.Retail_Value) AS Value
FROM CUR.FactWeeklyRetailSales f
LEFT JOIN CFG.Calenders c
    ON c.Dunnes_Year = f.Dunnes_Year
   AND c.Dunnes_Week = f.Dunnes_Week
GROUP BY f.CalendarKey, c.Start_Date, f.Store_Name
ORDER BY CAST(f.CalendarKey AS INT);
"""


# ==========================================================
# HTML tiles (Outlook-safe)
# ==========================================================

def tile_td(label, value):
    return f"""
    <td style="padding:10px;vertical-align:top;">
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:14px;text-align:center;min-height:88px;">
        <div style="font-size:12px;color:#6b7280;">{label}</div>
        <div style="font-size:22px;font-weight:800;margin-top:6px;color:#111827;">{value}</div>
      </div>
    </td>
    """


def tiles_table(pairs, cols=4):
    rows = []
    for i in range(0, len(pairs), cols):
        chunk = pairs[i:i+cols]
        rows.append("<tr>" + "".join(tile_td(k, v) for k, v in chunk) + "</tr>")
    return f"<table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;'>{''.join(rows)}</table>"


def bullets(items):
    if not items:
        return "<div style='color:#6b7280;font-size:13px;'>No notable anomalies detected.</div>"
    lis = "".join(f"<li style='margin:6px 0;'>{x}</li>" for x in items)
    return f"<ul style='margin:8px 0 0 18px;color:#111827;font-size:14px;'>{lis}</ul>"


# ==========================================================
# Chart helpers (static PNGs)
# ==========================================================

def _apply_month_axis():
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.xticks(rotation=0)


def save_weekly_units_chart(weekly_df: pd.DataFrame, outpath: Path, weeks_back: int = 24):
    d = weekly_df.copy()
    d = d.dropna(subset=["Start_Date"]).sort_values("Start_Date").tail(weeks_back)

    plt.figure(figsize=(10.8, 3.6))
    plt.plot(d["Start_Date"], d["Units"], marker="o")
    plt.title("Weekly Units Trend (Retail units, validated/staged)")
    plt.xlabel("Week (Start date)")
    plt.ylabel("Units")
    _apply_month_axis()
    plt.tight_layout()
    plt.savefig(outpath, dpi=160)
    plt.close()


def save_product_trend(product_weekly: pd.DataFrame,
                       current_start_date,
                       outpath: Path,
                       metric_col: str,
                       top_n: int = 6,
                       weeks_back: int = 24):
    """
    Plots history for each of the top_n products.
    FIX: Select last N WEEKS by date, not last N ROWS.
    """
    df = product_weekly.copy()
    df = df.dropna(subset=["Start_Date"]).sort_values("Start_Date")

    # last N weeks by date
    week_dates = sorted(df["Start_Date"].unique())
    window_dates = week_dates[-weeks_back:] if len(week_dates) > weeks_back else week_dates
    dfw = df[df["Start_Date"].isin(window_dates)].copy()

    # Top products are chosen based on current week (latest date in window)
    latest_date = max(window_dates) if window_dates else df["Start_Date"].max()
    latest_slice = dfw[dfw["Start_Date"] == latest_date]

    top = (latest_slice.groupby(["Dynamics_Code", "Product_Description"])[metric_col]
           .sum().sort_values(ascending=False).head(top_n).reset_index())

    plt.figure(figsize=(10.8, 4.0))
    for _, r in top.iterrows():
        sku = r["Dynamics_Code"]
        label = str(r["Product_Description"])
        label_short = label[:30] + ("…" if len(label) > 30 else "")
        s = dfw[dfw["Dynamics_Code"] == sku].sort_values("Start_Date")
        plt.plot(s["Start_Date"], s[metric_col], marker="o", label=f"{sku} {label_short}")

    title_metric = "Retail value (€)" if metric_col == "Value" else "Units"
    plt.title(f"Weekly Product Trend ({title_metric}) – Top products (history)")
    plt.xlabel("Week (Start date)")
    plt.ylabel(title_metric)
    _apply_month_axis()
    plt.legend(fontsize=7, loc="upper left", ncol=1)
    plt.tight_layout()
    plt.savefig(outpath, dpi=160)
    plt.close()


# ==========================================================
# Anomaly detection
# ==========================================================

def build_anomaly_bullets(weekly: pd.DataFrame,
                          prod_weekly: pd.DataFrame,
                          store_weekly: pd.DataFrame,
                          cur_key: str,
                          lookback: int = 4,
                          top_n: int = 5) -> list[str]:
    """
    Simple, explainable anomalies:
    - Products down/up vs 4W avg (units + value)
    - Stores down/up vs 4W avg (units)
    """
    bullets_out: list[str] = []

    weekly = weekly.copy()
    weekly["KeyInt"] = weekly["CalendarKey"].astype(str).astype(int)
    cur_int = int(cur_key)

    # Determine date window (last N weeks incl current)
    w_recent = weekly[weekly["KeyInt"] <= cur_int].dropna(subset=["Start_Date"]).sort_values("Start_Date")
    recent_dates = sorted(w_recent["Start_Date"].unique())[-lookback:] if not w_recent.empty else []
    if not recent_dates:
        return bullets_out

    latest_date = recent_dates[-1]

    # ---------- Product anomalies ----------
    pw = prod_weekly.dropna(subset=["Start_Date"]).copy()
    pw = pw[pw["Start_Date"].isin(recent_dates)].copy()

    # current week values per product
    cur_p = (pw[pw["Start_Date"] == latest_date]
             .groupby(["Dynamics_Code", "Product_Description"])
             .agg(Units=("Units", "sum"), Value=("Value", "sum"))
             .reset_index())

    # 4-week avg per product (excluding current week to avoid smoothing away the signal)
    hist_dates = recent_dates[:-1] if len(recent_dates) > 1 else recent_dates
    hist_p = (pw[pw["Start_Date"].isin(hist_dates)]
              .groupby(["Dynamics_Code", "Product_Description"])
              .agg(AvgUnits=("Units", "mean"), AvgValue=("Value", "mean"))
              .reset_index())

    pcmp = cur_p.merge(hist_p, on=["Dynamics_Code", "Product_Description"], how="left")
    pcmp["AvgUnits"] = pcmp["AvgUnits"].fillna(0.0)
    pcmp["AvgValue"] = pcmp["AvgValue"].fillna(0.0)

    # unit deviation %
    def dev_pct(cur, avg):
        return ((cur - avg) / avg * 100.0) if avg else None

    pcmp["UnitsDevPct"] = pcmp.apply(lambda r: dev_pct(r["Units"], r["AvgUnits"]), axis=1)
    pcmp["ValueDevPct"] = pcmp.apply(lambda r: dev_pct(r["Value"], r["AvgValue"]), axis=1)

    # pick notable movers (abs deviation >= 20% and avg > threshold)
    p_notable = pcmp[(pcmp["AvgUnits"] >= 5) & (pcmp["UnitsDevPct"].notna())].copy()
    p_notable = p_notable.sort_values("UnitsDevPct")

    down = p_notable.head(top_n)
    up = p_notable.tail(top_n).sort_values("UnitsDevPct", ascending=False)

    for _, r in down.iterrows():
        if r["UnitsDevPct"] is None or r["UnitsDevPct"] > -20:
            continue
        bullets_out.append(
            f"Product ↓ vs avg: **{r['Dynamics_Code']}** ({str(r['Product_Description'])[:40]}) "
            f"is **{fmt_dec(abs(r['UnitsDevPct']),1)}% below** its {lookback-1}W average in units "
            f"({fmt_int(r['Units'])} vs avg {fmt_dec(r['AvgUnits'],1)})."
        )

    for _, r in up.iterrows():
        if r["UnitsDevPct"] is None or r["UnitsDevPct"] < 20:
            continue
        bullets_out.append(
            f"Product ↑ vs avg: **{r['Dynamics_Code']}** ({str(r['Product_Description'])[:40]}) "
            f"is **{fmt_dec(r['UnitsDevPct'],1)}% above** its {lookback-1}W average in units "
            f"({fmt_int(r['Units'])} vs avg {fmt_dec(r['AvgUnits'],1)})."
        )

    # ---------- Store anomalies (units) ----------
    sw = store_weekly.dropna(subset=["Start_Date"]).copy()
    sw = sw[sw["Start_Date"].isin(recent_dates)].copy()

    cur_s = (sw[sw["Start_Date"] == latest_date]
             .groupby("Store_Name")
             .agg(Units=("Units", "sum"), Value=("Value", "sum"))
             .reset_index())

    hist_s = (sw[sw["Start_Date"].isin(hist_dates)]
              .groupby("Store_Name")
              .agg(AvgUnits=("Units", "mean"))
              .reset_index())

    scmp = cur_s.merge(hist_s, on="Store_Name", how="left")
    scmp["AvgUnits"] = scmp["AvgUnits"].fillna(0.0)
    scmp["UnitsDevPct"] = scmp.apply(lambda r: dev_pct(r["Units"], r["AvgUnits"]), axis=1)

    s_notable = scmp[(scmp["AvgUnits"] >= 10) & (scmp["UnitsDevPct"].notna())].copy()
    s_notable = s_notable.sort_values("UnitsDevPct")

    s_down = s_notable.head(top_n)
    s_up = s_notable.tail(top_n).sort_values("UnitsDevPct", ascending=False)

    for _, r in s_down.iterrows():
        if r["UnitsDevPct"] is None or r["UnitsDevPct"] > -20:
            continue
        bullets_out.append(
            f"Store ↓ vs avg: **{r['Store_Name']}** is **{fmt_dec(abs(r['UnitsDevPct']),1)}% below** its {lookback-1}W avg "
            f"({fmt_int(r['Units'])} units vs avg {fmt_dec(r['AvgUnits'],1)})."
        )

    for _, r in s_up.iterrows():
        if r["UnitsDevPct"] is None or r["UnitsDevPct"] < 20:
            continue
        bullets_out.append(
            f"Store ↑ vs avg: **{r['Store_Name']}** is **{fmt_dec(r['UnitsDevPct'],1)}% above** its {lookback-1}W avg "
            f"({fmt_int(r['Units'])} units vs avg {fmt_dec(r['AvgUnits'],1)})."
        )

    # keep concise
    return bullets_out[:10]


# ==========================================================
# MAIN
# ==========================================================

def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)

    cfg = load_cfg()
    conn = connect_db(cfg)

    latest = qdf(conn, SQL_LATEST_FILE)
    if latest.empty:
        raise RuntimeError("No completed EPOS file found in RAW.EPOS_File.")

    file_id = int(latest.iloc[0]["File_ID"])
    week = int(latest.iloc[0]["Week"])
    year = int(latest.iloc[0]["Year"])
    filename = str(latest.iloc[0]["FileName"])

    cal_key = calendar_key_from_raw_year_week(year, week)

    # -------------------------
    # CONTROL
    # -------------------------
    raw = qdf(conn, SQL_RAW_INTAKE, [file_id]).iloc[0]
    ctl = qdf(conn, SQL_CTL_STATUS, [file_id]).iloc[0]
    agg = qdf(conn, SQL_AGG_COUNT, [file_id]).iloc[0]
    nol = qdf(conn, SQL_ORDERS_NO_LINES, [file_id]).iloc[0]
    intc = qdf(conn, SQL_INT_COUNTS, [file_id]).iloc[0]
    qvar = qdf(conn, SQL_QTY_VARIANCE, [file_id, file_id]).iloc[0]
    apic = qdf(conn, SQL_API_COUNTS, [file_id]).iloc[0]

    raw_lines = safe_int(raw["Raw_Lines"])
    epos_orders = safe_int(raw["EPOS_Orders"])

    ctl_lines = safe_int(ctl["CTL_Lines"])
    staged = safe_int(ctl["Staged_Lines"])
    removed = safe_int(ctl["Removed_Lines"])
    aggregated = safe_int(agg["Aggregated_Lines"])

    orders_no_lines = safe_int(nol["Orders_No_Lines"])

    int_orders = safe_int(intc["INT_Orders"])
    int_lines = safe_int(intc["INT_Lines"])

    qty_variance = safe_float(qvar["Total_Qty_Variance"])

    api_ok = safe_int(apic["API_OK"])
    api_fail = safe_int(apic["API_Fail"])
    api_total = safe_int(apic["API_Header_Rows"])

    raw_ctl_ok = (raw_lines == ctl_lines) if (raw_lines and ctl_lines) else False
    ctl_split_ok = (ctl_lines == (staged + removed)) if ctl_lines else False

    # Not all orders will have line items - we flag it
    orders_ok = (int_orders == epos_orders) if epos_orders else False

    qty_ok = abs(qty_variance) < 0.0001
    api_ok_all = (api_fail == 0)

    coverage_pct = (staged / raw_lines * 100.0) if raw_lines else 0.0

    # -------------------------
    # CUR analytics
    # -------------------------
    weekly = qdf(conn, SQL_FACT_WEEKLY_WITH_DATES)
    if weekly.empty:
        raise RuntimeError("CUR.FactWeeklyRetailSales is empty or not populated.")

    weekly["KeyInt"] = weekly["CalendarKey"].astype(str).astype(int)
    weekly = weekly.sort_values("KeyInt")

    # Current + previous based on fact
    if (weekly["CalendarKey"] == cal_key).any():
        cur_key = cal_key
    else:
        cur_key = str(weekly.iloc[-1]["CalendarKey"])

    cur_int = int(cur_key)
    prev_row = weekly[weekly["KeyInt"] < cur_int].tail(1)
    prev_key = str(prev_row.iloc[0]["CalendarKey"]) if not prev_row.empty else cur_key

    cur = weekly[weekly["CalendarKey"] == cur_key].iloc[0]
    prev = weekly[weekly["CalendarKey"] == prev_key].iloc[0]

    units_now = safe_float(cur["Units"])
    units_prev = safe_float(prev["Units"])
    value_now = safe_float(cur["Value"])
    value_prev = safe_float(prev["Value"])

    wow_units = units_now - units_prev
    wow_units_pct = (wow_units / units_prev * 100.0) if units_prev else 0.0

    wow_value = value_now - value_prev
    wow_value_pct = (wow_value / value_prev * 100.0) if value_prev else 0.0

    last4 = weekly[weekly["KeyInt"] <= cur_int].tail(4)
    avg4_units = safe_float(last4["Units"].mean())
    avg4_value = safe_float(last4["Value"].mean())

    vs4_units = units_now - avg4_units
    vs4_units_pct = (vs4_units / avg4_units * 100.0) if avg4_units else 0.0

    vs4_value = value_now - avg4_value
    vs4_value_pct = (vs4_value / avg4_value * 100.0) if avg4_value else 0.0

    prod_weekly = qdf(conn, SQL_FACT_PRODUCT_WEEKLY)
    store_weekly = qdf(conn, SQL_FACT_STORE_WEEKLY)

    # -------------------------
    # CHARTS (history!)
    # -------------------------
    units_chart = OUTDIR / f"duracell_units_trend_{cur_key}.png"
    product_value_chart = OUTDIR / f"duracell_product_value_trend_{cur_key}.png"
    product_units_chart = OUTDIR / f"duracell_product_units_trend_{cur_key}.png"

    save_weekly_units_chart(weekly, units_chart, weeks_back=24)
    save_product_trend(prod_weekly, cur.get("Start_Date"), product_value_chart, metric_col="Value", top_n=6, weeks_back=24)
    save_product_trend(prod_weekly, cur.get("Start_Date"), product_units_chart, metric_col="Units", top_n=6, weeks_back=24)

    # -------------------------
    # Anomaly bullets
    # -------------------------
    anomaly_bullets = build_anomaly_bullets(weekly, prod_weekly, store_weekly, cur_key, lookback=4, top_n=5)

    # -------------------------
    # TILES (Control first, then performance)
    # -------------------------
    control_tiles_1 = [
        ("EPOS Orders", fmt_int(epos_orders)),
        ("RAW Lines", fmt_int(raw_lines)),
        ("CTL Lines", fmt_int(ctl_lines)),
        ("Staged Lines", fmt_int(staged)),
        ("Removed Lines", fmt_int(removed)),
        ("Aggregated", fmt_int(aggregated)),
        ("RAW=CTL", yes_no(raw_ctl_ok)),
        ("CTL=(S+R)", yes_no(ctl_split_ok)),
    ]

    control_tiles_2 = [
        ("Orders w/ NO lines", fmt_int(orders_no_lines)),
        ("INT Orders", fmt_int(int_orders)),
        ("INT Lines", fmt_int(int_lines)),
        ("Orders → SO Created", yes_no(orders_ok)),
        ("Qty Variance", fmt_dec(qty_variance, 4)),
        ("Qty OK", yes_no(qty_ok)),
        ("API OK", fmt_int(api_ok)),
        ("API Fail", fmt_int(api_fail)),
    ]

    perf_tiles = [
        ("Units This Week", fmt_int(units_now)),
        ("Units WoW", f"{fmt_int(wow_units)} ({fmt_dec(wow_units_pct,1)}%)"),
        ("Units vs 4W Avg", f"{fmt_int(vs4_units)} ({fmt_dec(vs4_units_pct,1)}%)"),
        ("Coverage %", f"{fmt_dec(coverage_pct,1)}%"),
        ("Retail € This Week", fmt_dec(value_now, 2)),
        ("Retail € WoW", f"{fmt_dec(wow_value,2)} ({fmt_dec(wow_value_pct,1)}%)"),
        ("Retail € vs 4W Avg", f"{fmt_dec(vs4_value,2)} ({fmt_dec(vs4_value_pct,1)}%)"),
        ("CUR Week Key", cur_key),
    ]

    tiles_html = (
        tiles_table(control_tiles_1, cols=4)
        + "<div style='height:6px;'></div>"
        + tiles_table(control_tiles_2, cols=4)
        + "<div style='height:6px;'></div>"
        + tiles_table(perf_tiles, cols=4)
    )

    # -------------------------
    # Narrative insights (impactful)
    # -------------------------
    control_notes = []
    if orders_no_lines > 0:
        control_notes.append(f"{orders_no_lines} Sales Order(s) were created with **no line items** and have been flagged for review.")
    if raw_ctl_ok and ctl_split_ok and qty_ok and api_ok_all:
        control_notes.append("End-to-end reconciliation is clean for this run (RAW → CTL → INT → API).")
    else:
        if not raw_ctl_ok:
            control_notes.append("Mismatch: RAW line count does not equal CTL line count.")
        if not ctl_split_ok:
            control_notes.append("Mismatch: CTL line count does not equal (Staged + Removed).")
        if not qty_ok:
            control_notes.append(f"Quantity variance detected between CTL and INT: {fmt_dec(qty_variance,4)} units.")
        if not api_ok_all:
            control_notes.append("API failures detected (Sales Order header posts).")

    perf_notes = []
    if abs(wow_units_pct) >= 5:
        direction = "up" if wow_units_pct > 0 else "down"
        perf_notes.append(f"Weekly units are **{direction} {fmt_dec(abs(wow_units_pct),1)}%** vs last week.")
    else:
        perf_notes.append("Weekly units are broadly stable vs last week.")
    if abs(vs4_units_pct) >= 5:
        direction = "above" if vs4_units_pct > 0 else "below"
        perf_notes.append(f"Units are **{fmt_dec(abs(vs4_units_pct),1)}% {direction}** the 4-week average.")
    else:
        perf_notes.append("Units are in line with the 4-week average.")

    # -------------------------
    # Email
    # -------------------------
    title = "Duracell - EPOS Control - Sales Order Reconciliation"
    subtitle = f"Week {week} | Year {year} | CalendarKey {cur_key} | Source file: {filename} (File_ID={file_id})"

    html = f"""
    <html>
    <body style="font-family:Segoe UI, Arial, sans-serif; background:#f4f6fa; margin:0; padding:0;">
      <div style="max-width:1200px; margin:18px auto; background:#ffffff; padding:20px; border-radius:14px;">

        <div style="display:flex; align-items:center; justify-content:space-between;">
          <img src="cid:fusionlogo" style="height:60px;">
          <img src="cid:bunny" style="height:60px;">
        </div>

        <h2 style="margin:14px 0 6px 0;">{title}</h2>
        <div style="color:#6b7280; font-size:13px; margin-bottom:14px;">{subtitle}</div>

        {tiles_html}

        <hr style="border:none;border-top:1px solid #e5e7eb; margin:18px 0;">

        <h3 style="margin:0;">Control confirmation</h3>
        {bullets(control_notes)}

        <h3 style="margin:16px 0 0 0;">Weekly performance</h3>
        {bullets(perf_notes)}

        <h3 style="margin:16px 0 0 0;">Anomalies to review</h3>
        <div style="color:#6b7280;font-size:12px;margin:6px 0 10px 0;">
          Based on current week vs rolling {3} week baseline (approx 4-week window).
        </div>
        {bullets(anomaly_bullets)}

        <hr style="border:none;border-top:1px solid #e5e7eb; margin:18px 0;">

        <h3 style="margin:0;">Weekly Units Trend</h3>
        <div style="color:#6b7280;font-size:12px;margin:6px 0 10px 0;">
          X-axis uses Dunnes calendar Start_Date so month labels are meaningful.
        </div>
        <img src="cid:units_trend" style="width:100%; max-width:1100px; border:1px solid #e5e7eb; border-radius:10px;">

        <h3 style="margin:18px 0 0 0;">Weekly Product Trend (Retail Value) – Top products</h3>
        <img src="cid:product_value_trend" style="width:100%; max-width:1100px; border:1px solid #e5e7eb; border-radius:10px;">

        <h3 style="margin:18px 0 0 0;">Weekly Product Trend (Units) – Top products</h3>
        <img src="cid:product_units_trend" style="width:100%; max-width:1100px; border:1px solid #e5e7eb; border-radius:10px;">

        <div style="margin-top:18px;">
          <img src="cid:synovia" style="height:45px;">
        </div>

        <div style="margin-top:12px; color:#6b7280; font-size:12px;">
          {FOOTER_TEXT}
        </div>

      </div>
    </body>
    </html>
    """

    recipients = qdf(conn, SQL_RECIPIENTS)["Email"].dropna().tolist()
    if not recipients:
        raise RuntimeError("No recipients found: CFG.Email_Distribution where SynoviaTest=1 and Weekly=1.")

    msg = MIMEMultipart("related")
    msg["Subject"] = f"Duracell - Post SalesOrder Processing {week} {year}"
    msg["From"] = cfg.smtp_user
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    msg.attach(alt)
    alt.attach(MIMEText(html, "html", "utf-8"))

    def attach_inline(cid: str, path: Path):
        if not path.exists():
            raise FileNotFoundError(f"Missing asset: {path}")
        with open(path, "rb") as f:
            img = MIMEImage(f.read())
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=path.name)
        msg.attach(img)

    attach_inline("fusionlogo", FUSION_LOGO)
    attach_inline("bunny", BUNNY_IMG)
    attach_inline("synovia", SYNOVIA_LOGO)
    attach_inline("units_trend", units_chart)
    attach_inline("product_value_trend", product_value_chart)
    attach_inline("product_units_trend", product_units_chart)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(cfg.smtp_user, cfg.smtp_password)
        server.sendmail(cfg.smtp_user, recipients, msg.as_string())

    print("Report sent successfully.")
    print(f"File_ID={file_id} Week={week} Year={year} CalendarKey={cur_key} PrevKey={prev_key}")


if __name__ == "__main__":
    main()
