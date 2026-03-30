"""
Synovia Fusion - File Operations (Enterprise Standard)

Standards:
- No overwrite moves
- Retry for UNC / transient IO
- Staging into _processing to avoid mid-copy reads and parallel runs
"""

from __future__ import annotations

import os
import time
import uuid
import shutil
import hashlib
from datetime import datetime
from typing import Optional, Tuple


def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except Exception:
        return 0


def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def wait_for_file_ready(path: str, *, stable_sec: int = 3, timeout_sec: int = 120) -> bool:
    """
    Wait until:
      - file exists
      - size is stable for stable_sec
      - can be opened for read
    """
    start = time.time()
    last_size = -1
    stable_start = None

    while time.time() - start <= timeout_sec:
        if not os.path.exists(path):
            time.sleep(0.5)
            continue

        sz = file_size(path)
        if sz == last_size and sz > 0:
            if stable_start is None:
                stable_start = time.time()
            elif time.time() - stable_start >= stable_sec:
                try:
                    with open(path, "rb"):
                        return True
                except Exception:
                    pass
        else:
            stable_start = None
            last_size = sz

        time.sleep(0.5)

    return False


def safe_move(
    src: str,
    dest_dir: str,
    dest_name: str,
    *,
    retries: int = 6,
    delay: float = 1.0,
) -> str:
    """
    Move without overwrite. If destination exists, append timestamp+uuid.
    Retries transient errors (UNC/network).
    """
    ensure_dir(dest_dir)

    base, ext = os.path.splitext(dest_name)
    dest_path = os.path.join(dest_dir, dest_name)

    if os.path.exists(dest_path):
        stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        dest_path = os.path.join(dest_dir, f"{base}_{stamp}_{uuid.uuid4().hex}{ext}")

    last = None
    for _ in range(retries):
        try:
            shutil.move(src, dest_path)
            return dest_path
        except Exception as e:
            last = e
            time.sleep(delay)

    raise RuntimeError(f"Failed to move after retries: {src} -> {dest_path}. Last error: {last}")


def stage_inbound_file(
    inbound_dir: str,
    processing_dir: str,
    fname: str,
    *,
    ready_stable_sec: int = 3,
    ready_timeout_sec: int = 120,
) -> Optional[Tuple[str, str]]:
    """
    Stages file from inbound_dir to processing_dir using an atomic move.

    Returns (staged_path, original_filename) or None if not ready or cannot be staged.
    """
    src = os.path.join(inbound_dir, fname)
    if not os.path.isfile(src):
        return None

    if not wait_for_file_ready(src, stable_sec=ready_stable_sec, timeout_sec=ready_timeout_sec):
        return None

    ensure_dir(processing_dir)
    staged_name = f"{fname}.processing.{uuid.uuid4().hex}"
    staged_path = os.path.join(processing_dir, staged_name)

    try:
        safe_move(src, processing_dir, staged_name)
        return staged_path, fname
    except Exception:
        return None


def list_orphan_staged(processing_dir: str) -> Tuple[str, ...]:
    """
    Finds orphan staged files (e.g., after crash) in processing_dir.
    """
    if not os.path.isdir(processing_dir):
        return tuple()
    out = []
    for n in sorted(os.listdir(processing_dir)):
        if ".processing." in n:
            p = os.path.join(processing_dir, n)
            if os.path.isfile(p):
                out.append(p)
    return tuple(out)


def original_name_from_staged(staged_path: str) -> str:
    """
    Convert '<name>.processing.<guid>' back to '<name>'.
    """
    base = os.path.basename(staged_path)
    return base.split(".processing.")[0]
