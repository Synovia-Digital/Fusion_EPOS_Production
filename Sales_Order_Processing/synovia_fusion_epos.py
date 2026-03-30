"""
Synovia Fusion - EPOS parsing and validation rules (Dunnes Stores)

This module is intentionally self-contained and importable by other scripts
that need consistent EPOS file discipline.
"""

from __future__ import annotations

import re
import csv
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd


FILENAME_REGEX = re.compile(
    r"^SalesReport_(?P<region>[^_]+)_(?P<supplier>\d+)_(?P<yyww>\d{4})(?:_.*)?\.csv$",
    re.IGNORECASE
)

EXPECTED_HEADERS = [
    "Year", "Week", "Day", "Region", "Store", "Store_Name",
    "Docket_Reference", "Retail_Barcode", "Dunnes_Prod_Code",
    "Product_Description", "Supplier", "Suppl_Name",
    "Units_Sold", "Sales_Value_Inc_VAT", "Supplier_Product_Code"
]

FOOTER_PHRASE = "lines in this file excluding header and this footer"


def parse_filename(filename: str) -> Tuple[str, int, int, int]:
    """
    Returns (region, supplier, year_yy, week) extracted from filename.

    Raises ValueError if filename is invalid.
    """
    m = FILENAME_REGEX.match(filename)
    if not m:
        raise ValueError(f"Invalid filename: {filename}")

    supplier = int(m.group("supplier"))
    year = int(m.group("yyww")[:2])
    week = int(m.group("yyww")[2:])
    region = m.group("region")
    return region, supplier, year, week


def parse_footer(filepath: str) -> Tuple[Optional[int], Optional[int], Optional[float], Optional[str], bool]:
    """
    Footer discipline:
    - The last non-empty line is expected to be a CSV record containing totals and a
      sentence including FOOTER_PHRASE.

    Returns:
      (footer_lines, footer_qty, footer_sales, footer_text, footer_recognised)
    """
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            lines = [l.strip() for l in f if l.strip()]
    except Exception:
        return None, None, None, None, False

    if not lines:
        return None, None, None, None, False

    footer = lines[-1]
    try:
        fields = next(csv.reader([footer]))
    except Exception:
        return None, None, None, footer, False

    try:
        if len(fields) >= 14 and fields[0].isdigit():
            if FOOTER_PHRASE in (fields[1] or "").lower():
                if (fields[10] or "").strip().upper() == "TOTALS":
                    footer_lines = int(fields[0])
                    footer_qty = int(float(fields[12])) if fields[12] else None
                    footer_sales = float(fields[13]) if fields[13] else None
                    return footer_lines, footer_qty, footer_sales, footer, True
    except Exception:
        pass

    return None, None, None, footer, False


def normalise_headers(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Case/whitespace tolerant header alignment to EXPECTED_HEADERS.
    Returns (df_aligned, missing_headers).
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    by_lower = {str(c).strip().lower(): str(c).strip() for c in df.columns}
    missing = [h for h in EXPECTED_HEADERS if h.lower() not in by_lower]
    if missing:
        return df, missing
    rename_map = {by_lower[h.lower()]: h for h in EXPECTED_HEADERS}
    df = df.rename(columns=rename_map)
    df = df.loc[:, EXPECTED_HEADERS]
    return df, []


def filter_footer_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Remove any row containing the footer phrase in ANY column.
    """
    if df.empty:
        return df, 0

    mask = df.astype(str).apply(
        lambda s: s.str.contains(FOOTER_PHRASE, case=False, na=False)
    ).any(axis=1)

    removed = int(mask.sum())
    return df.loc[~mask].copy(), removed


def coerce_numeric_fields(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Coerce Units_Sold and Sales_Value_Inc_VAT robustly, tracking coercion failures.
    """
    df = df.copy()

    units = pd.to_numeric(df["Units_Sold"].astype(str).str.replace(",", "", regex=False), errors="coerce")
    units_bad = int(units.isna().sum())
    df["Units_Sold"] = units.astype("Int64")  # allows <NA>

    sales = pd.to_numeric(df["Sales_Value_Inc_VAT"].astype(str).str.replace(",", "", regex=False), errors="coerce")
    sales_bad = int(sales.isna().sum())
    df["Sales_Value_Inc_VAT"] = sales.round(2).fillna(0.0)

    return df, {"Units_Sold_bad": units_bad, "Sales_Value_bad": sales_bad}
