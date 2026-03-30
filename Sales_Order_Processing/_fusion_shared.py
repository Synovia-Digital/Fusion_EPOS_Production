"""
_fusion_shared.py  –  Synovia Fusion EPOS Shared Bootstrap
===========================================================
Co-deployed with all step scripts in the same flat directory:
  D:\\Applications\\Fusion_Release_4\\Fusion_Duracell_EPOS
  \\Fusion_EPOS_Production\\Sales_Order_Processing\\

No path injection. No sys.path manipulation.
All SDK modules (synovia_fusion_*) live in the same folder and import directly.

Provides:
  - Bootstrap constants (server, db, credentials env keys)
  - Secret resolution: resolve_db_password(), resolve_smtp_password()
  - Connection helpers: connect_autocommit(), connect_meta_data()
  - Thin wrappers re-exporting SDK helpers for convenience
  - Formatters: fmt_int(), fmt_money(), fmt_qty(), safe_float(), safe_int()
  - HTML helpers: status_dot(), status_chip(), html_escape()
  - Filesystem: ensure_dir(), pick_first_existing()
  - Path constants: LOG_DIR, OUTPUT_ROOT, logo paths, etc.
"""

from __future__ import annotations

import os
from typing import Any, Optional, Tuple

import pyodbc

# SDK classes are co-located in the same directory — plain imports
from synovia_fusion_config import IniEnvContext
from synovia_fusion_db import (
    build_sqlserver_conn_str,
    connect_meta_data as _sdk_connect_meta_data,
    object_exists,
)

# =============================================================================
# DEPLOY ROOT + PATHS
# =============================================================================
DEPLOY_ROOT = (
    r"D:\Applications\Fusion_Release_4\Fusion_Duracell_EPOS\Fusion_EPOS_Production"
)
LOG_DIR     = r"D:\Logs"
OUTPUT_ROOT = r"D:\EPOS_Outputs"

# =============================================================================
# DB BOOTSTRAP
# =============================================================================
BOOTSTRAP = {
    "driver":   "ODBC Driver 17 for SQL Server",
    "server":   "futureworks-sdi-db.database.windows.net",
    "database": "Fusion_EPOS_Production",
    "username": "SynFW_DB",
    "timeout":  60,
}

DB_PASSWORD_ENV   = "FUSION_EPOS_BOOTSTRAP_PASSWORD"
SMTP_PASSWORD_ENV = "FUSION_EPOS_SMTP_PASSWORD"
D365_SECRET_ENV   = "FUSION_D365_CLIENT_SECRET"

INI_PATH    = r"D:\Configuration\Fusion_EPOS_Production.ini"
INI_SECTION = "Fusion_EPOS_Production"

# =============================================================================
# SMTP
# =============================================================================
SMTP_SERVER  = "smtp.office365.com"
SMTP_PORT    = 587
SENDER_EMAIL = "nexus@synoviaintegration.com"

# =============================================================================
# LOGO ASSETS
# =============================================================================
FUSION_LOGO_CANDIDATES = [
    r"D:\Configuration\FusionLogo.jpg",
    r"D:\Graphics\FusionLogo.jpg",
]
RUNNING_BUNNY_PATH  = r"D:\Graphics\RunningBunny.jpg"
SYNOVIA_FOOTER_PATH = r"D:\Graphics\SynoviaLogoHor.jpg"
DURACELL_LOGO_CANDIDATES = [
    r"D:\Graphics\DuracellLogo.jpg",
    r"D:\Graphics\DuracellLogo.png",
    r"D:\Graphics\Duracell.jpg",
]


# =============================================================================
# SECRET RESOLUTION  (delegates to IniEnvContext)
# =============================================================================

def _ctx() -> IniEnvContext:
    return IniEnvContext(ini_path=INI_PATH, ini_section=INI_SECTION)


def resolve_db_password() -> str:
    """ENV FUSION_EPOS_BOOTSTRAP_PASSWORD → INI password= → RuntimeError."""
    return _ctx().get_secret(env_key=DB_PASSWORD_ENV, ini_key="password", required=True)


def resolve_smtp_password() -> str:
    """ENV FUSION_EPOS_SMTP_PASSWORD → INI smtp_password= → RuntimeError."""
    return _ctx().get_secret(env_key=SMTP_PASSWORD_ENV, ini_key="smtp_password", required=True)


def resolve_d365_secret(ini_path: str, ini_section: str = "AUTH") -> str:
    """ENV FUSION_D365_CLIENT_SECRET → Production_365.ini [AUTH] client_secret=."""
    import configparser
    pw = os.environ.get(D365_SECRET_ENV, "").strip()
    if pw:
        return pw
    cfg = configparser.ConfigParser()
    if os.path.exists(ini_path):
        cfg.read(ini_path, encoding="utf-8")
    if ini_section in cfg and "client_secret" in cfg[ini_section]:
        v = cfg[ini_section]["client_secret"].strip()
        if v:
            return v
    raise RuntimeError(
        f"D365 client secret not found. Set ENV '{D365_SECRET_ENV}' "
        f"or add client_secret= to {ini_path} [{ini_section}]."
    )


# =============================================================================
# CONNECTION HELPERS
# =============================================================================

def _conn_str(app_name: str) -> str:
    return build_sqlserver_conn_str(
        driver=BOOTSTRAP["driver"],
        server=BOOTSTRAP["server"],
        database=BOOTSTRAP["database"],
        username=BOOTSTRAP["username"],
        password=resolve_db_password(),
        timeout_sec=int(BOOTSTRAP["timeout"]),
        encrypt=True,
        trust_server_certificate=False,
        app_name=app_name,
    )


def connect_autocommit(app_name: str = "Fusion_EPOS") -> pyodbc.Connection:
    """Single autocommit connection — for SP calls, meta writes, audit logging."""
    return pyodbc.connect(_conn_str(app_name), autocommit=True)


def connect_meta_data(app_name: str = "Fusion_EPOS") -> Tuple[pyodbc.Connection, pyodbc.Connection]:
    """Returns (conn_meta=autocommit, conn_data=manual-commit)."""
    return _sdk_connect_meta_data(_conn_str(app_name))


# =============================================================================
# DB HELPERS
# =============================================================================

def scalar(cur: pyodbc.Cursor, sql: str, params: Tuple = ()) -> Any:
    row = cur.execute(sql, params).fetchone()
    return row[0] if row else None


def has_col(cur: pyodbc.Cursor, schema: str, table: str, col: str) -> bool:
    row = cur.execute("""
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
    """, schema, table, col).fetchone()
    return bool(row)


# =============================================================================
# FORMATTERS
# =============================================================================

def fmt_int(v: Any) -> str:
    try:
        return f"{int(v or 0):,d}"
    except Exception:
        return "0"


def fmt_money(v: Any) -> str:
    try:
        return f"{float(v or 0.0):,.2f}"
    except Exception:
        return "0.00"


def fmt_qty(v: Any) -> str:
    try:
        x = float(v or 0)
        if abs(x - round(x)) < 1e-9:
            return f"{int(round(x)):,d}"
        return f"{x:,.4f}".rstrip("0").rstrip(".")
    except Exception:
        return "0"


def safe_float(v: Any) -> float:
    try:
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def safe_int(v: Any) -> int:
    try:
        return int(v) if v is not None else 0
    except Exception:
        return 0


def html_escape(s: Any) -> str:
    s = "" if s is None else str(s)
    return (s.replace("&", "&amp;").replace("<", "&lt;")
             .replace(">", "&gt;").replace('"', "&quot;"))


# =============================================================================
# OUTLOOK-SAFE HTML HELPERS
# =============================================================================

def status_dot(status: str) -> str:
    st = (status or "").upper()
    if st in ("PASS", "READY", "INFO", "SUCCESS"):
        color = "#10b981"
    elif st in ("WARN", "NO_LINES", "STAGED"):
        color = "#f59e0b"
    else:
        color = "#ef4444"
    return (
        f'<span style="display:inline-block;width:10px;height:10px;'
        f'border-radius:50%;background:{color};margin-right:8px;vertical-align:middle;"></span>'
    )


def status_chip(status: str) -> str:
    st = (status or "").upper()
    if st in ("PASS", "READY", "INFO", "SUCCESS"):
        bg, bd, fg = "#ecfdf5", "#a7f3d0", "#065f46"
    elif st in ("WARN", "NO_LINES", "STAGED"):
        bg, bd, fg = "#fff7ed", "#fed7aa", "#9a3412"
    else:
        bg, bd, fg = "#fef2f2", "#fecaca", "#991b1b"
    return (
        f'<span style="display:inline-block;padding:3px 10px;border-radius:999px;'
        f'background:{bg};border:1px solid {bd};color:{fg};font-weight:800;font-size:12px;">{st}</span>'
    )


# =============================================================================
# FILESYSTEM HELPERS
# =============================================================================

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def pick_first_existing(paths: list) -> Optional[str]:
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None
