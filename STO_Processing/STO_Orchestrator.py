#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
#  STO_Orchestrator.py
#  Sequentially executes all 6 STO Pipeline steps, captures output and
#  exit codes, then generates and emails an HTML summary report.
#
#  Pipeline Steps:
#    1. 1.Load_Wasp_Order.py            – CSV Ingestion
#    2. 2.STO_Delivery_Scheduling.py    – Delivery Date Scheduling
#    3. 3.Stage_Transfer_Orders_D365.py – D365 Staging (SP wrapper)
#    4. 4.Create_Dynamics_TO's.py       – D365 Transfer Order Creation
#    5. 5.Run_SAP_Staging.py            – SAP Staging (SP wrapper)
#    6. 6.Transmit_SAP_TO.py            – SAP CPI Transmission
#
#  Usage:
#    python STO_Orchestrator.py                 # Run all steps
#    python STO_Orchestrator.py --steps 1,2,3   # Run specific steps
#    python STO_Orchestrator.py --report-only   # Generate report from last run log
#    python STO_Orchestrator.py --no-email      # Run without sending email
#
#  Location:
#    \\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4\
#      Fusion_Duracell_EPOS\Fusion_EPOS_Production\STO_Processing\STO_Orchestrator.py
#
#  Report Script:
#    \\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4\
#      Fusion_Duracell_EPOS\Fusion_EPOS_Production\Reporting\STO_Pipeline_Report.py
#
# =============================================================================

import argparse
import json
import os
import signal
import subprocess
import sys
import socket
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# =============================================================================
#  PATHS
# =============================================================================

SCRIPT_DIR = Path(
    r"\\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4"
    r"\Fusion_Duracell_EPOS\Fusion_EPOS_Production\STO_Processing"
)

REPORTING_DIR = Path(
    r"\\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4"
    r"\Fusion_Duracell_EPOS\Fusion_EPOS_Production\Reporting"
)

LOG_DIR = Path(
    r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub"
    r"\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs"
)

ORCH_LOG_DIR = LOG_DIR / "Orchestrator"
ORCH_LOG_DIR.mkdir(parents=True, exist_ok=True)

MACHINE_NAME = socket.gethostname()

# =============================================================================
#  PIPELINE DEFINITION
# =============================================================================

PIPELINE_STEPS = [
    {
        "step":        1,
        "name":        "CSV Ingestion",
        "script":      "1.Load_Wasp_Order.py",
        "interactive": False,
        "timeout":     600,     # 10 min
        "critical":    True,    # Pipeline stops if this fails
    },
    {
        "step":        2,
        "name":        "Delivery Scheduling",
        "script":      "2.STO_Delivery_Scheduling.py",
        "interactive": False,
        "timeout":     600,
        "critical":    True,
    },
    {
        "step":        3,
        "name":        "D365 Staging (SP)",
        "script":      "3.Stage_Transfer_Orders_D365.py",
        "interactive": True,     # Has input() confirmation prompt
        "timeout":     600,
        "critical":    True,
    },
    {
        "step":        4,
        "name":        "D365 Transfer Order Creation",
        "script":      "4.Create_Dynamics_TO's.py",
        "interactive": False,
        "timeout":     1800,     # 30 min — API calls
        "critical":    True,
    },
    {
        "step":        5,
        "name":        "SAP Staging (SP)",
        "script":      "5.Run_SAP_Staging.py",
        "interactive": True,     # Has input() confirmation prompt
        "timeout":     600,
        "critical":    True,
    },
    {
        "step":        6,
        "name":        "SAP CPI Transmission",
        "script":      "6.Transmit_SAP_TO.py",
        "interactive": False,
        "timeout":     1800,     # 30 min — API calls
        "critical":    False,
    },
]


# =============================================================================
#  CONSOLE HELPERS
# =============================================================================

class C:
    RST  = "\033[0m";   BOLD = "\033[1m";  DIM  = "\033[2m"
    RED  = "\033[31m";  GRN  = "\033[32m"; YLW  = "\033[33m"
    BLU  = "\033[34m";  CYN  = "\033[36m"
    BGRN = "\033[92m";  BYLW = "\033[93m"; BCYN = "\033[96m"; BWHT = "\033[97m"
    BGRED = "\033[41m"; BGGRN = "\033[42m"; BGYLW = "\033[43m"
    BGBLU = "\033[44m"; BGMAG = "\033[45m"

_W = 76


def _ts():
    return datetime.now().strftime("%H:%M:%S")


def banner(text, bg=C.BGMAG, fg=C.BWHT):
    pad = max(0, _W - len(text) - 4)
    l, r = pad // 2, pad - pad // 2
    print(f"\n{bg}{fg}{C.BOLD}  {'─'*l}  {text}  {'─'*r}  {C.RST}")


def section(text):
    print(f"\n{C.BOLD}{C.BCYN}{'═'*_W}{C.RST}")
    print(f"{C.BOLD}{C.BCYN}  ◆  {text}{C.RST}")
    print(f"{C.BOLD}{C.BCYN}{'═'*_W}{C.RST}")


def divider():
    print(f"  {C.DIM}{'─'*(_W-2)}{C.RST}")


def kv(label, val, vc=C.BWHT):
    print(f"  {C.DIM}{label:<34}{C.RST}{vc}{val}{C.RST}")


def con_info(m):  print(f"  {C.BCYN}[{_ts()}]{C.RST}  {C.BCYN}ℹ{C.RST}  {m}")
def con_ok(m):    print(f"  {C.BGRN}[{_ts()}]{C.RST}  {C.BGRN}✔{C.RST}  {m}")
def con_warn(m):  print(f"  {C.BYLW}[{_ts()}]{C.RST}  {C.BYLW}⚠{C.RST}  {m}")
def con_err(m):   print(f"  {C.RED}[{_ts()}]{C.RST}  {C.RED}✘{C.RST}  {m}")


def step_banner(step_num, total, name, script):
    pct = int((step_num - 1) / total * 100) if total else 0
    bar = f"{C.BGRN}{'█' * int(pct/4)}{C.DIM}{'░' * (25 - int(pct/4))}{C.RST}"
    print(f"\n{C.BGBLU}{C.BWHT}{C.BOLD}"
          f"  STEP {step_num}/{total}  │  {name}  │  {script}  {C.RST}")
    print(f"  {bar}  {C.DIM}{pct}%{C.RST}")


def step_result(step_num, name, status, duration_s, exit_code, error=""):
    if status == "SUCCESS":
        bg, ico = C.BGGRN + C.BOLD, "✅"
    elif status == "SKIPPED":
        bg, ico = C.BGYLW + C.BOLD, "⏭️"
    elif status == "NOTHING_TO_DO":
        bg, ico = C.BGYLW + C.BOLD, "💤"
    else:
        bg, ico = C.BGRED + C.BWHT + C.BOLD, "❌"
    print(f"\n  {bg}  {ico}  Step {step_num} — {name} — {status}  {C.RST}")
    kv("Duration", f"{duration_s:.1f}s")
    kv("Exit Code", str(exit_code),
       C.BGRN if exit_code == 0 else C.RED)
    if error:
        kv("Error", error[:200], C.RED)


# =============================================================================
#  STEP RUNNER
# =============================================================================

def run_step(step_def: dict, python_exe: str = sys.executable) -> dict:
    """
    Execute a single pipeline step as a subprocess.

    Returns a dict with:
      step, name, script, status, exit_code, duration_s,
      stdout, stderr, started_at, finished_at, error
    """
    step_num = step_def["step"]
    name     = step_def["name"]
    script   = step_def["script"]
    timeout  = step_def["timeout"]
    interactive = step_def["interactive"]

    script_path = SCRIPT_DIR / script

    result = {
        "step":        step_num,
        "name":        name,
        "script":      script,
        "status":      "PENDING",
        "exit_code":   -1,
        "duration_s":  0.0,
        "stdout":      "",
        "stderr":      "",
        "started_at":  datetime.now(timezone.utc).isoformat(),
        "finished_at": "",
        "error":       "",
    }

    if not script_path.exists():
        result["status"]  = "ERROR"
        result["error"]   = f"Script not found: {script_path}"
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        return result

    step_banner(step_num, 6, name, script)
    con_info(f"Launching: {python_exe} \"{script_path}\"")

    start = time.time()

    try:
        # For interactive scripts (3 and 5), pipe "y\n" to stdin
        # to auto-confirm the proceed prompt
        stdin_data = "y\n" if interactive else None

        proc = subprocess.run(
            [python_exe, str(script_path)],
            input=stdin_data,
            capture_output=True,
            text=True,
            encoding="utf-8",          # FIX: explicit UTF-8 instead of cp1252 default
            errors="replace",          # FIX: replace undecodable bytes with '�'
            timeout=timeout,
            cwd=str(SCRIPT_DIR),
        )

        duration = time.time() - start
        result["exit_code"]   = proc.returncode
        result["stdout"]      = proc.stdout[-10000:] if proc.stdout else ""
        result["stderr"]      = proc.stderr[-5000:]  if proc.stderr else ""
        result["duration_s"]  = round(duration, 2)
        result["finished_at"] = datetime.now(timezone.utc).isoformat()

        # Determine status from exit code and output
        if proc.returncode == 0:
            # Check if the script exited cleanly but with nothing to process
            stdout_lower = (proc.stdout or "").lower()
            nothing_patterns = [
                "no csv files found",
                "no orders pending",
                "nothing to do",
                "no orders with masterstatus",
                "no pending headers",
                "no new orders to stage",
                "all eligible orders already staged",
            ]
            if any(p in stdout_lower for p in nothing_patterns):
                result["status"] = "NOTHING_TO_DO"
                con_warn(f"Step {step_num} completed — nothing to process")
            else:
                result["status"] = "SUCCESS"
                con_ok(f"Step {step_num} completed successfully")
        else:
            result["status"] = "ERROR"
            # Extract last meaningful error line
            err_lines = [l for l in (proc.stderr or "").strip().splitlines()
                         if l.strip()]
            result["error"] = err_lines[-1][:500] if err_lines else \
                              f"Exit code {proc.returncode}"
            con_err(f"Step {step_num} failed: exit code {proc.returncode}")

    except subprocess.TimeoutExpired:
        duration = time.time() - start
        result["status"]      = "TIMEOUT"
        result["duration_s"]  = round(duration, 2)
        result["error"]       = f"Timed out after {timeout}s"
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        con_err(f"Step {step_num} TIMED OUT after {timeout}s")

    except Exception as ex:
        duration = time.time() - start
        result["status"]      = "ERROR"
        result["duration_s"]  = round(duration, 2)
        result["error"]       = str(ex)[:500]
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        con_err(f"Step {step_num} failed: {ex}")

    # Print stdout tail for visibility
    if result["stdout"]:
        tail_lines = result["stdout"].strip().splitlines()[-15:]
        print(f"\n  {C.DIM}  ┌─ OUTPUT (last {len(tail_lines)} lines) "
              f"{'─'*40}{C.RST}")
        for line in tail_lines:
            print(f"  {C.DIM}  │ {line}{C.RST}")
        print(f"  {C.DIM}  └{'─'*60}{C.RST}")

    step_result(step_num, name, result["status"],
                result["duration_s"], result["exit_code"],
                result["error"])

    return result


# =============================================================================
#  RUN LOG  (JSON file for reporting)
# =============================================================================

def save_run_log(results: list, run_start: datetime, run_end: datetime):
    """Save the orchestration results to a JSON file for reporting."""
    run_data = {
        "run_id":       run_start.strftime("%Y%m%d_%H%M%S"),
        "machine":      MACHINE_NAME,
        "started_at":   run_start.isoformat(),
        "finished_at":  run_end.isoformat(),
        "duration_s":   round((run_end - run_start).total_seconds(), 2),
        "total_steps":  len(results),
        "success":      sum(1 for r in results if r["status"] == "SUCCESS"),
        "failed":       sum(1 for r in results
                           if r["status"] in ("ERROR", "TIMEOUT")),
        "skipped":      sum(1 for r in results if r["status"] == "SKIPPED"),
        "nothing":      sum(1 for r in results
                           if r["status"] == "NOTHING_TO_DO"),
        "overall":      "SUCCESS" if all(
                            r["status"] in ("SUCCESS", "NOTHING_TO_DO", "SKIPPED")
                            for r in results
                        ) else "FAILED",
        "steps":        results,
    }

    # Save to log directory
    log_file = ORCH_LOG_DIR / f"STO_Run_{run_data['run_id']}.json"
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(run_data, f, indent=2, ensure_ascii=False, default=str)

    # Also save as "latest" for the report script
    latest_file = ORCH_LOG_DIR / "STO_Run_Latest.json"
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(run_data, f, indent=2, ensure_ascii=False, default=str)

    con_ok(f"Run log saved: {log_file}")
    return run_data


# =============================================================================
#  HTML REPORT BUILDER
# =============================================================================

def build_report_html(run_data: dict) -> str:
    """
    Build an HTML email report from the run data.
    Uses the same styling conventions as reporting_shared.py.
    """

    # ── Determine logo ────────────────────────────────────
    logo_path = Path(
        r"\\pl-az-int-prd\D_Drive\Configuration\fusionlogo.jpg"
    )
    logo_src = ""
    if logo_path.exists():
        import base64
        with open(logo_path, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf-8")
        logo_src = f"data:image/jpeg;base64,{data}"

    # ── KPI values ────────────────────────────────────────
    overall  = run_data["overall"]
    total    = run_data["total_steps"]
    success  = run_data["success"]
    failed   = run_data["failed"]
    nothing  = run_data["nothing"]
    skipped  = run_data["skipped"]
    duration = run_data["duration_s"]
    run_time = run_data["started_at"][:19].replace("T", " ")

    overall_colour = "#43A047" if overall == "SUCCESS" else "#E53935"
    overall_icon   = "✅" if overall == "SUCCESS" else "❌"

    # ── Overall banner ────────────────────────────────────
    banner_bg = "#E8F5E9" if overall == "SUCCESS" else "#FFEBEE"
    banner_border = "#43A047" if overall == "SUCCESS" else "#E53935"
    banner_text = "#2E7D32" if overall == "SUCCESS" else "#C62828"
    banner_msg = (
        f"{overall_icon} Pipeline completed successfully — "
        f"all {total} steps passed"
        if overall == "SUCCESS"
        else f"{overall_icon} Pipeline completed with failures — "
             f"{failed} step(s) failed"
    )

    alert_html = (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        f'<tr><td style="padding:12px 20px 0">'
        f'<table width="100%" cellpadding="14" cellspacing="0" border="0" '
        f'style="background:{banner_bg};border-radius:8px;'
        f'border-left:4px solid {banner_border};'
        f'font-family:\'Montserrat\',\'Segoe UI\',Arial,sans-serif;'
        f'font-size:13px;font-weight:700;color:{banner_text}">'
        f'<tr><td>{banner_msg}</td></tr>'
        f'</table></td></tr></table>'
    )

    # ── KPI tiles ─────────────────────────────────────────
    tiles_html = f"""
<table width="100%" cellpadding="0" cellspacing="0" border="0"
       style="padding:20px 20px 0">
  <tr>
    <td width="20%" style="padding:4px">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background:#fff;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.07)">
        <tr><td style="height:3px;background:{overall_colour};border-radius:10px 10px 0 0;font-size:0">&nbsp;</td></tr>
        <tr><td style="padding:18px 12px;text-align:center">
          <div style="font-size:30px;font-weight:800;color:{overall_colour}">{overall_icon}</div>
          <div style="font-size:10px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.7px">Overall</div>
          <div style="font-size:11px;color:#9CA3AF;margin-top:4px">{overall}</div>
        </td></tr>
      </table>
    </td>
    <td width="20%" style="padding:4px">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background:#fff;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.07)">
        <tr><td style="height:3px;background:#43A047;border-radius:10px 10px 0 0;font-size:0">&nbsp;</td></tr>
        <tr><td style="padding:18px 12px;text-align:center">
          <div style="font-size:30px;font-weight:800;color:#2E7D32">{success}</div>
          <div style="font-size:10px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.7px">Success</div>
        </td></tr>
      </table>
    </td>
    <td width="20%" style="padding:4px">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background:#fff;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.07)">
        <tr><td style="height:3px;background:#E53935;border-radius:10px 10px 0 0;font-size:0">&nbsp;</td></tr>
        <tr><td style="padding:18px 12px;text-align:center">
          <div style="font-size:30px;font-weight:800;color:#C62828">{failed}</div>
          <div style="font-size:10px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.7px">Failed</div>
        </td></tr>
      </table>
    </td>
    <td width="20%" style="padding:4px">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background:#fff;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.07)">
        <tr><td style="height:3px;background:#FFC107;border-radius:10px 10px 0 0;font-size:0">&nbsp;</td></tr>
        <tr><td style="padding:18px 12px;text-align:center">
          <div style="font-size:30px;font-weight:800;color:#795548">{nothing}</div>
          <div style="font-size:10px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.7px">Nothing To Do</div>
        </td></tr>
      </table>
    </td>
    <td width="20%" style="padding:4px">
      <table width="100%" cellpadding="0" cellspacing="0" border="0"
             style="background:#fff;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.07)">
        <tr><td style="height:3px;background:#1E88E5;border-radius:10px 10px 0 0;font-size:0">&nbsp;</td></tr>
        <tr><td style="padding:18px 12px;text-align:center">
          <div style="font-size:30px;font-weight:800;color:#0D1B2A">{duration:.0f}s</div>
          <div style="font-size:10px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.7px">Duration</div>
        </td></tr>
      </table>
    </td>
  </tr>
</table>"""

    # ── Steps table ───────────────────────────────────────
    status_badges = {
        "SUCCESS":       ("badge-green",  "Success"),
        "NOTHING_TO_DO": ("badge-amber",  "Nothing To Do"),
        "SKIPPED":       ("badge-blue",   "Skipped"),
        "ERROR":         ("badge-red",    "Error"),
        "TIMEOUT":       ("badge-red",    "Timeout"),
        "PENDING":       ("badge-blue",   "Pending"),
    }

    step_rows = ""
    for s in run_data["steps"]:
        badge_class, badge_text = status_badges.get(
            s["status"], ("badge-blue", s["status"]))
        error_html = ""
        if s.get("error"):
            error_html = (
                f'<div style="font-size:10px;color:#C62828;margin-top:4px;'
                f'font-style:italic">{s["error"][:200]}</div>'
            )
        step_rows += f"""
        <tr>
          <td style="font-weight:700;color:#1B3A5C;width:40px;text-align:center">{s["step"]}</td>
          <td>
            <strong>{s["name"]}</strong>
            <div style="font-size:10px;color:#9CA3AF">{s["script"]}</div>
            {error_html}
          </td>
          <td style="text-align:center">
            <span class="badge {badge_class}">{badge_text}</span>
          </td>
          <td class="num">{s["exit_code"]}</td>
          <td class="num">{s["duration_s"]:.1f}s</td>
        </tr>"""

    steps_html = f"""
<div style="padding:22px 20px 8px">
  <h2 style="font-size:12px;font-weight:700;color:#1B3A5C;text-transform:uppercase;
             letter-spacing:1px;border-left:3px solid #1E88E5;padding-left:10px;margin:0">
    Pipeline Step Results
  </h2>
</div>
<div style="padding:0 20px">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="border-collapse:collapse;background:#fff;border-radius:10px;
                overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.07);font-size:12px;
                font-family:'Montserrat','Segoe UI',Arial,sans-serif">
    <thead>
      <tr style="background:#1B3A5C;color:#fff">
        <th style="padding:11px 14px;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;text-align:center;width:40px">#</th>
        <th style="padding:11px 14px;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;text-align:left">Step</th>
        <th style="padding:11px 14px;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;text-align:center">Status</th>
        <th style="padding:11px 14px;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;text-align:right">Exit</th>
        <th style="padding:11px 14px;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;text-align:right">Duration</th>
      </tr>
    </thead>
    <tbody>{step_rows}</tbody>
  </table>
</div>"""

    # ── Run metadata ──────────────────────────────────────
    meta_html = f"""
<div style="padding:22px 20px 8px">
  <h2 style="font-size:12px;font-weight:700;color:#1B3A5C;text-transform:uppercase;
             letter-spacing:1px;border-left:3px solid #1E88E5;padding-left:10px;margin:0">
    Run Information
  </h2>
</div>
<div style="padding:0 20px">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="border-collapse:collapse;background:#fff;border-radius:10px;
                overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.07);font-size:12px;
                font-family:'Montserrat','Segoe UI',Arial,sans-serif">
    <tbody>
      <tr style="border-bottom:1px solid #F3F4F6">
        <td style="padding:10px 14px;color:#6B7280;font-weight:600;width:180px">Run ID</td>
        <td style="padding:10px 14px;color:#374151">{run_data['run_id']}</td>
      </tr>
      <tr style="border-bottom:1px solid #F3F4F6;background:#FAFAFA">
        <td style="padding:10px 14px;color:#6B7280;font-weight:600">Machine</td>
        <td style="padding:10px 14px;color:#374151">{run_data['machine']}</td>
      </tr>
      <tr style="border-bottom:1px solid #F3F4F6">
        <td style="padding:10px 14px;color:#6B7280;font-weight:600">Started</td>
        <td style="padding:10px 14px;color:#374151">{run_data['started_at'][:19].replace('T',' ')}</td>
      </tr>
      <tr style="border-bottom:1px solid #F3F4F6;background:#FAFAFA">
        <td style="padding:10px 14px;color:#6B7280;font-weight:600">Finished</td>
        <td style="padding:10px 14px;color:#374151">{run_data['finished_at'][:19].replace('T',' ')}</td>
      </tr>
      <tr>
        <td style="padding:10px 14px;color:#6B7280;font-weight:600">Total Duration</td>
        <td style="padding:10px 14px;color:#374151;font-weight:700">{run_data['duration_s']:.1f} seconds</td>
      </tr>
    </tbody>
  </table>
</div>
<div style="height:16px"></div>"""

    # ── Assemble body ─────────────────────────────────────
    body = alert_html + tiles_html + steps_html + meta_html

    # ── Wrap in full HTML shell (matching reporting_shared style) ──
    logo_tag = (
        f"<img src='{logo_src}' alt='Fusion' "
        f"style='height:44px;width:auto;display:block'>"
        if logo_src
        else '<span style="font-size:22px;font-weight:800;color:#ffffff;'
             'letter-spacing:-0.5px">FUSION</span>'
    )

    title    = "STO Pipeline Report"
    subtitle = f"{run_time}  ·  {MACHINE_NAME}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap"
      rel="stylesheet">
<style>
  body{{margin:0;padding:0;background:#F0F2F5;
       font-family:'Montserrat','Segoe UI',Arial,sans-serif;
       -webkit-font-smoothing:antialiased}}
  .badge{{display:inline-block;padding:2px 9px;border-radius:20px;font-size:10px;
          font-weight:700;text-transform:uppercase;letter-spacing:.4px}}
  .badge-green{{background:#E8F5E9;color:#2E7D32}}
  .badge-amber{{background:#FFF3E0;color:#E65100}}
  .badge-blue{{background:#E3F2FD;color:#1565C0}}
  .badge-red{{background:#FFEBEE;color:#C62828}}
  .badge-purple{{background:#F3E5F5;color:#6A1B9A}}
  .num{{text-align:right;font-weight:600}}
</style>
</head>
<body>
<table width="100%" cellpadding="0" cellspacing="0" border="0"
       style="background:#F0F2F5;min-width:600px">
<tr><td align="center">
<table width="760" cellpadding="0" cellspacing="0" border="0"
       style="max-width:760px;width:100%">

  <!-- HEADER -->
  <tr>
    <td style="background:linear-gradient(135deg,#0D1B2A 0%,#1B3A5C 60%,#1E5F99 100%);
               padding:28px 28px 0;border-radius:0 0 6px 6px">
      <table width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td style="vertical-align:middle">{logo_tag}</td>
          <td style="vertical-align:middle;text-align:right">
            <div style="font-size:19px;font-weight:800;color:#ffffff;line-height:1.2;
                        font-family:'Montserrat','Segoe UI',Arial,sans-serif">{title}</div>
            <div style="font-size:11px;font-weight:600;color:#7EB8E8;margin-top:4px;
                        text-transform:uppercase;letter-spacing:.7px;
                        font-family:'Montserrat','Segoe UI',Arial,sans-serif">{subtitle}</div>
          </td>
        </tr>
        <tr><td colspan="2" height="20">&nbsp;</td></tr>
        <tr>
          <td colspan="2" height="3"
              style="background:linear-gradient(90deg,#1E88E5,#42A5F5,rgba(255,255,255,0));
                     font-size:0;line-height:0">&nbsp;</td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- BODY -->
  <tr><td>{body}</td></tr>

  <!-- FOOTER -->
  <tr>
    <td style="padding:20px 20px 28px;text-align:center;font-size:10px;color:#9CA3AF;
               line-height:1.7;font-family:'Montserrat','Segoe UI',Arial,sans-serif">
      <strong style="color:#6B7280">Synovia Fusion – STO Pipeline Orchestrator</strong><br>
      Automated report &nbsp;·&nbsp; Do not reply
      &nbsp;·&nbsp; nexus@synoviaintegration.com
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# =============================================================================
#  EMAIL SENDER
# =============================================================================

def send_report(run_data: dict, recipients: list = None):
    """
    Send the pipeline report via email using the shared config.
    Falls back to reporting_shared.send_email if available.
    """
    import configparser
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    CONFIG_FILE = (
        r"\\pl-az-int-prd\D_Drive\Configuration"
        r"\Fusion_EPOS_Production.ini"
    )

    if recipients is None:
        recipients = ["aidan.harrington@synoviadigital.com"]

    html = build_report_html(run_data)

    # Load email config
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)

    def _clean(v): return v.strip().strip('"')

    em = cfg["Nexus_Email"]
    host = _clean(em["smtp_host"])
    port = int(_clean(em["smtp_port"]))
    user = _clean(em["smtp_user"])
    pwd  = _clean(em["smtp_password"])

    overall_icon = "✅" if run_data["overall"] == "SUCCESS" else "❌"
    subject = (
        f"{overall_icon} STO Pipeline {run_data['overall']} — "
        f"{run_data['success']}/{run_data['total_steps']} steps OK — "
        f"{datetime.now().strftime('%d %b %Y %H:%M')}"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = user
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html, "html", "utf-8"))

    con_info(f"Sending report → {recipients} via {host}:{port}")

    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(user, pwd)
        smtp.sendmail(user, recipients, msg.as_bytes())

    con_ok("Report email sent successfully")


# =============================================================================
#  MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="STO Pipeline Orchestrator — runs all 6 steps "
                    "and sends an HTML summary report."
    )
    parser.add_argument(
        "--steps", type=str, default=None,
        help="Comma-separated step numbers to run (e.g. 1,2,3). "
             "Default: all steps."
    )
    parser.add_argument(
        "--no-email", action="store_true",
        help="Skip sending the email report."
    )
    parser.add_argument(
        "--report-only", action="store_true",
        help="Generate report from the last run log without executing steps."
    )
    parser.add_argument(
        "--recipients", type=str, default=None,
        help="Comma-separated email addresses. "
             "Default: aidan.harrington@synoviadigital.com"
    )
    parser.add_argument(
        "--continue-on-error", action="store_true",
        help="Continue executing subsequent steps even if a critical "
             "step fails."
    )
    args = parser.parse_args()

    recipients = (
        [r.strip() for r in args.recipients.split(",")]
        if args.recipients
        else ["aidan.harrington@synoviadigital.com"]
    )

    # ── Report-only mode ──────────────────────────────────
    if args.report_only:
        banner("STO PIPELINE — REPORT ONLY")
        latest = ORCH_LOG_DIR / "STO_Run_Latest.json"
        if not latest.exists():
            con_err(f"No run log found at {latest}")
            sys.exit(1)
        with open(latest, "r", encoding="utf-8") as f:
            run_data = json.load(f)
        con_ok(f"Loaded run log: {run_data['run_id']}")
        if not args.no_email:
            send_report(run_data, recipients)
        else:
            # Save HTML locally
            html_path = ORCH_LOG_DIR / f"STO_Report_{run_data['run_id']}.html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(build_report_html(run_data))
            con_ok(f"Report saved: {html_path}")
        return

    # ── Determine which steps to run ──────────────────────
    if args.steps:
        selected = set(int(s.strip()) for s in args.steps.split(","))
    else:
        selected = set(range(1, 7))

    steps_to_run = [s for s in PIPELINE_STEPS if s["step"] in selected]

    # ── Start ─────────────────────────────────────────────
    run_start = datetime.now(timezone.utc)

    banner("STO PIPELINE ORCHESTRATOR")
    section("STARTUP")
    kv("Machine",    MACHINE_NAME)
    kv("Script Dir", str(SCRIPT_DIR))
    kv("Log Dir",    str(ORCH_LOG_DIR))
    kv("Steps",      ", ".join(str(s["step"]) for s in steps_to_run))
    kv("Recipients", ", ".join(recipients))
    kv("Started",    run_start.strftime("%Y-%m-%d %H:%M:%S UTC"))

    # Verify scripts exist
    section("PRE-FLIGHT CHECKS")
    all_found = True
    for s in steps_to_run:
        path = SCRIPT_DIR / s["script"]
        if path.exists():
            con_ok(f"Step {s['step']}: {s['script']}")
        else:
            con_warn(f"Step {s['step']}: {s['script']} — NOT FOUND (will skip)")
            all_found = False

    if not all_found:
        con_warn("One or more scripts not found — those steps will be skipped.")
        steps_to_run = [s for s in steps_to_run
                        if (SCRIPT_DIR / s["script"]).exists()]
        if not steps_to_run:
            con_err("No runnable steps remaining. Exiting.")
            sys.exit(1)

    # ── Execute pipeline ──────────────────────────────────
    section("EXECUTING PIPELINE")
    results = []
    pipeline_halted = False

    for step_def in steps_to_run:
        if pipeline_halted:
            skip_result = {
                "step":        step_def["step"],
                "name":        step_def["name"],
                "script":      step_def["script"],
                "status":      "SKIPPED",
                "exit_code":   -1,
                "duration_s":  0.0,
                "stdout":      "",
                "stderr":      "",
                "started_at":  "",
                "finished_at": "",
                "error":       "Skipped — pipeline halted by prior failure",
            }
            results.append(skip_result)
            con_warn(f"Step {step_def['step']} SKIPPED — "
                     f"pipeline halted by prior failure")
            continue

        result = run_step(step_def)
        results.append(result)

        # Check if we should halt
        if (result["status"] in ("ERROR", "TIMEOUT")
                and step_def["critical"]
                and not args.continue_on_error):
            con_err(f"Critical step {step_def['step']} failed — "
                    f"halting pipeline")
            pipeline_halted = True

    # ── Summary ───────────────────────────────────────────
    run_end  = datetime.now(timezone.utc)
    run_data = save_run_log(results, run_start, run_end)

    section("PIPELINE COMPLETE")
    overall_bg = C.BGGRN if run_data["overall"] == "SUCCESS" else C.BGRED
    print(f"\n  {overall_bg}{C.BOLD}{C.BWHT}"
          f"  {'✅' if run_data['overall'] == 'SUCCESS' else '❌'}  "
          f"OVERALL: {run_data['overall']}  {C.RST}")
    kv("Total Steps",    str(run_data["total_steps"]))
    kv("Success",        str(run_data["success"]),
       C.BGRN + C.BOLD)
    kv("Failed",         str(run_data["failed"]),
       C.RED + C.BOLD if run_data["failed"] else C.DIM)
    kv("Nothing To Do",  str(run_data["nothing"]),
       C.BYLW if run_data["nothing"] else C.DIM)
    kv("Skipped",        str(run_data["skipped"]),
       C.DIM)
    kv("Total Duration", f"{run_data['duration_s']:.1f}s")

    # ── Send report ───────────────────────────────────────
    if not args.no_email:
        section("SENDING REPORT")
        try:
            send_report(run_data, recipients)
        except Exception as ex:
            con_err(f"Failed to send report: {ex}")
            # Save HTML locally as fallback
            html_path = ORCH_LOG_DIR / f"STO_Report_{run_data['run_id']}.html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(build_report_html(run_data))
            con_warn(f"Report saved locally: {html_path}")
    else:
        # Save HTML locally
        html_path = ORCH_LOG_DIR / f"STO_Report_{run_data['run_id']}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(build_report_html(run_data))
        con_ok(f"Report saved: {html_path} (email skipped)")

    # Exit with non-zero if pipeline failed
    if run_data["overall"] != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
