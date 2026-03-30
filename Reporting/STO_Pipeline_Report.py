#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
#  STO_Pipeline_Report.py
#  Standalone report generator for the STO Pipeline Orchestrator.
#
#  Can be run manually from the Reporting directory to regenerate
#  and resend the pipeline report from the latest (or a specific) run.
#
#  Usage:
#    python STO_Pipeline_Report.py                     # Send latest run report
#    python STO_Pipeline_Report.py --run-id 20260401_140000
#    python STO_Pipeline_Report.py --save-only         # Save HTML, don't email
#    python STO_Pipeline_Report.py --recipients a@b.com,c@d.com
#
#  Location:
#    \\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4\
#      Fusion_Duracell_EPOS\Fusion_EPOS_Production\Reporting\STO_Pipeline_Report.py
#
#  Depends on:
#    reporting_shared.py  (same directory — for email config and HTML helpers)
#    STO_Orchestrator.py  (STO_Processing directory — for run log JSON files)
#
# =============================================================================

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# ── Import the shared reporting library ───────────────────
# This script lives alongside reporting_shared.py in the Reporting directory.
# Add current dir to path so the import works regardless of CWD.
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

try:
    from reporting_shared import (
        load_config, send_email, logo_base64,
        html_wrapper, tile_row, alert_ok, alert_warn,
        log, CONFIG_FILE,
    )
    HAS_SHARED = True
except ImportError:
    HAS_SHARED = False
    print("WARNING: reporting_shared.py not found — "
          "falling back to built-in email sender.")

# Also add the orchestrator directory so we can import its report builder
ORCHESTRATOR_DIR = Path(
    r"\\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4"
    r"\Fusion_Duracell_EPOS\Fusion_EPOS_Production\STO_Processing"
)
sys.path.insert(0, str(ORCHESTRATOR_DIR))

try:
    from STO_Orchestrator import build_report_html, send_report as orch_send
    HAS_ORCHESTRATOR = True
except ImportError:
    HAS_ORCHESTRATOR = False

# =============================================================================
#  PATHS
# =============================================================================

ORCH_LOG_DIR = Path(
    r"\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub"
    r"\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs\Orchestrator"
)

REPORT_OUTPUT_DIR = Path(
    r"\\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4"
    r"\Fusion_Duracell_EPOS\Fusion_EPOS_Production\Reporting\Reports"
)
REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_RECIPIENTS = ["aidan.harrington@synoviadigital.com"]


# =============================================================================
#  BUILT-IN REPORT BUILDER  (fallback if orchestrator import fails)
# =============================================================================

def _build_report_html_fallback(run_data: dict) -> str:
    """
    Minimal HTML report builder used if STO_Orchestrator can't be imported.
    Uses reporting_shared.html_wrapper if available.
    """
    overall  = run_data["overall"]
    steps    = run_data["steps"]
    duration = run_data["duration_s"]

    overall_icon = "✅" if overall == "SUCCESS" else "❌"

    # Build step rows
    step_rows = ""
    for s in steps:
        colour = "#2E7D32" if s["status"] == "SUCCESS" else \
                 "#E65100" if s["status"] == "NOTHING_TO_DO" else \
                 "#C62828" if s["status"] in ("ERROR", "TIMEOUT") else "#1565C0"
        error_line = (
            f'<br><small style="color:#C62828">{s["error"][:200]}</small>'
            if s.get("error") else ""
        )
        step_rows += f"""
        <tr style="border-bottom:1px solid #eee">
          <td style="padding:8px;text-align:center;font-weight:700">{s["step"]}</td>
          <td style="padding:8px"><strong>{s["name"]}</strong>
              <br><small style="color:#999">{s["script"]}</small>{error_line}</td>
          <td style="padding:8px;text-align:center">
            <span style="color:{colour};font-weight:700">{s["status"]}</span></td>
          <td style="padding:8px;text-align:right">{s["exit_code"]}</td>
          <td style="padding:8px;text-align:right">{s["duration_s"]:.1f}s</td>
        </tr>"""

    body = f"""
    <div style="padding:20px;text-align:center">
      <div style="font-size:48px">{overall_icon}</div>
      <div style="font-size:24px;font-weight:800;color:{'#2E7D32' if overall=='SUCCESS' else '#C62828'}">
        {overall}</div>
      <div style="color:#666;margin-top:8px">
        {run_data['success']}/{run_data['total_steps']} steps OK &nbsp;·&nbsp;
        {duration:.0f}s total &nbsp;·&nbsp;
        {run_data['started_at'][:19].replace('T',' ')}
      </div>
    </div>
    <table style="width:100%;border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px">
      <thead>
        <tr style="background:#1B3A5C;color:#fff">
          <th style="padding:10px;width:40px">#</th>
          <th style="padding:10px;text-align:left">Step</th>
          <th style="padding:10px">Status</th>
          <th style="padding:10px;text-align:right">Exit</th>
          <th style="padding:10px;text-align:right">Duration</th>
        </tr>
      </thead>
      <tbody>{step_rows}</tbody>
    </table>
    <div style="padding:20px;color:#999;font-size:11px;text-align:center">
      Machine: {run_data['machine']} &nbsp;·&nbsp;
      Run ID: {run_data['run_id']}
    </div>"""

    if HAS_SHARED:
        logo = logo_base64()
        title = "STO Pipeline Report"
        subtitle = (f"{run_data['started_at'][:19].replace('T',' ')} · "
                    f"{run_data['machine']}")
        return html_wrapper(logo, title, subtitle, body)
    else:
        return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>body{{font-family:Arial,sans-serif;background:#f5f5f5;margin:0;padding:20px}}
.container{{max-width:700px;margin:0 auto;background:#fff;border-radius:8px;
            overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1)}}</style>
</head><body><div class="container">{body}</div></body></html>"""


# =============================================================================
#  SEND EMAIL (using reporting_shared if available)
# =============================================================================

def send_pipeline_report(run_data: dict, recipients: list):
    """Send the pipeline report email."""

    html = (build_report_html(run_data) if HAS_ORCHESTRATOR
            else _build_report_html_fallback(run_data))

    overall_icon = "✅" if run_data["overall"] == "SUCCESS" else "❌"
    subject = (
        f"{overall_icon} STO Pipeline {run_data['overall']} — "
        f"{run_data['success']}/{run_data['total_steps']} steps OK — "
        f"{datetime.now().strftime('%d %b %Y %H:%M')}"
    )

    if HAS_SHARED:
        # Use reporting_shared's config and sender
        import configparser
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        cfg  = load_config(CONFIG_FILE)
        em   = cfg["Nexus_Email"]

        def _c(v): return v.strip().strip('"')
        host = _c(em["smtp_host"])
        port = int(_c(em["smtp_port"]))
        user = _c(em["smtp_user"])
        pwd  = _c(em["smtp_password"])

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = user
        msg["To"]      = ", ".join(recipients)
        msg.attach(MIMEText(html, "html", "utf-8"))

        print(f"Sending report → {recipients} via {host}:{port}")
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(user, pwd)
            smtp.sendmail(user, recipients, msg.as_bytes())
        print("✔ Report sent successfully")

    elif HAS_ORCHESTRATOR:
        orch_send(run_data, recipients)

    else:
        print("ERROR: Neither reporting_shared nor STO_Orchestrator "
              "could be imported for email sending.")
        sys.exit(1)


# =============================================================================
#  MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="STO Pipeline Report — generate and send the pipeline "
                    "execution report."
    )
    parser.add_argument(
        "--run-id", type=str, default=None,
        help="Specific run ID (e.g. 20260401_140000). "
             "Default: latest run."
    )
    parser.add_argument(
        "--save-only", action="store_true",
        help="Save HTML report to disk without sending email."
    )
    parser.add_argument(
        "--recipients", type=str, default=None,
        help="Comma-separated email recipients. "
             "Default: aidan.harrington@synoviadigital.com"
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Directory to save HTML report. Default: Reporting/Reports/"
    )
    args = parser.parse_args()

    recipients = (
        [r.strip() for r in args.recipients.split(",")]
        if args.recipients
        else DEFAULT_RECIPIENTS
    )

    output_dir = Path(args.output_dir) if args.output_dir else REPORT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Load run data ─────────────────────────────────────
    if args.run_id:
        log_file = ORCH_LOG_DIR / f"STO_Run_{args.run_id}.json"
    else:
        log_file = ORCH_LOG_DIR / "STO_Run_Latest.json"

    if not log_file.exists():
        print(f"ERROR: Run log not found: {log_file}")
        print("\nAvailable run logs:")
        for f in sorted(ORCH_LOG_DIR.glob("STO_Run_*.json")):
            if f.name != "STO_Run_Latest.json":
                print(f"  {f.name}")
        sys.exit(1)

    with open(log_file, "r", encoding="utf-8") as f:
        run_data = json.load(f)

    print(f"Loaded run: {run_data['run_id']}  "
          f"({run_data['overall']}  "
          f"{run_data['success']}/{run_data['total_steps']} OK)")

    # ── Generate report ───────────────────────────────────
    html = (build_report_html(run_data) if HAS_ORCHESTRATOR
            else _build_report_html_fallback(run_data))

    # Always save a copy
    html_path = output_dir / f"STO_Pipeline_Report_{run_data['run_id']}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✔ Report saved: {html_path}")

    # ── Send email ────────────────────────────────────────
    if not args.save_only:
        try:
            send_pipeline_report(run_data, recipients)
        except Exception as ex:
            print(f"ERROR sending email: {ex}")
            print(f"Report is available at: {html_path}")
            sys.exit(1)
    else:
        print("Email skipped (--save-only)")


if __name__ == "__main__":
    main()
