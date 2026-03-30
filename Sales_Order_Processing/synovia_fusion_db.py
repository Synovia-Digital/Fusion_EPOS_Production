"""
Synovia Fusion - SQL Server Utilities (Enterprise Standard)

Key standards:
- Explicit separation of META (autocommit) vs DATA (transactional) connections
- Capability detection for optional objects (tables/views/synonyms)
- No dynamic SQL beyond safe table targeting for known schemas
"""

from __future__ import annotations

import pyodbc
from typing import Optional, Dict, Tuple, Any


def build_sqlserver_conn_str(
    *,
    driver: str,
    server: str,
    database: str,
    username: str,
    password: str,
    timeout_sec: int = 60,
    encrypt: bool = True,
    trust_server_certificate: bool = False,
    app_name: Optional[str] = None,
) -> str:
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        f"UID={username}",
        f"PWD={password}",
        f"Encrypt={'yes' if encrypt else 'no'}",
        f"TrustServerCertificate={'yes' if trust_server_certificate else 'no'}",
        f"Connection Timeout={int(timeout_sec)}",
    ]
    if app_name:
        parts.append(f"APP={app_name}")
    return ";".join(parts) + ";"


def connect_meta_data(conn_str: str) -> Tuple[pyodbc.Connection, pyodbc.Connection]:
    """
    Returns:
      conn_meta (autocommit=True)  - for EXC/CFG/control writes and durable logging
      conn_data (autocommit=False) - for transactional data loads
    """
    conn_meta = pyodbc.connect(conn_str, autocommit=True)
    conn_data = pyodbc.connect(conn_str, autocommit=False)
    return conn_meta, conn_data


def object_exists(conn: pyodbc.Connection, schema: str, name: str) -> bool:
    """
    True if object exists as table/view/synonym in the given schema.
    """
    cur = conn.cursor()
    row = cur.execute("""
        SELECT TOP (1) 1
        FROM (
            SELECT 1 AS x
            FROM sys.objects o
            INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ? AND o.type IN ('U','V')
            UNION ALL
            SELECT 1
            FROM sys.synonyms sn
            INNER JOIN sys.schemas s ON s.schema_id = sn.schema_id
            WHERE s.name = ? AND sn.name = ?
        ) q;
    """, schema, name, schema, name).fetchone()
    return row is not None


def try_select_cfg_file_locations(conn_meta: pyodbc.Connection) -> Optional[Dict[str, str]]:
    """
    Best-effort read of CFG.Fusion_EPOS_File_Locations.

    Returns dict {Location_Key: UNC_Location} or None if table/view doesn't exist or query fails.
    """
    try:
        cur = conn_meta.cursor()
        rows = cur.execute("""
            SELECT Location_Key, UNC_Location
            FROM CFG.Fusion_EPOS_File_Locations
            WHERE IsActive = 1;
        """).fetchall()
        if not rows:
            return None
        return {r.Location_Key: r.UNC_Location for r in rows}
    except Exception:
        return None


def resolve_file_locations(
    *,
    conn_meta: pyodbc.Connection,
    env_map: Dict[str, str],
    ini_values: Dict[str, str],
) -> Dict[str, str]:
    """
    Resolve file locations with robust fallbacks.

    Priority:
      1) CFG.Fusion_EPOS_File_Locations (if present)
      2) Environment variables (env_map)
      3) INI values (ini_values)

    Returns dict with required keys:
      Inbound_Folder, Processed_Folder, Duplicate_Folder, Error_Folder
    """
    loc = try_select_cfg_file_locations(conn_meta) or {}

    # ENV fallback
    for k, env_key in env_map.items():
        if k not in loc:
            v = __import__("os").environ.get(env_key)
            if v and str(v).strip():
                loc[k] = str(v).strip()

    # INI fallback
    for k, v in ini_values.items():
        if k not in loc and str(v).strip():
            loc[k] = str(v).strip()

    required = ["Inbound_Folder", "Processed_Folder", "Duplicate_Folder", "Error_Folder"]
    missing = [k for k in required if k not in loc or not str(loc[k]).strip()]
    if missing:
        raise RuntimeError(f"Missing file locations: {missing}. Resolved keys: {sorted(loc.keys())}")

    return {k: loc[k] for k in required}
