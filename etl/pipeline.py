import os
import re
import sys
import logging
import subprocess
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import email_alert

# ── Paths ──────────────────────────────────────────────────────────────────
PYTHON_EXE   = r"C:\Users\Adedayo\AppData\Local\python-embed\python.exe"
DBT_RUNNER   = r"C:\Users\Adedayo\AppData\Local\python-embed\dbt_runner.py"
ETL_SCRIPT   = Path(__file__).parent / "incremental_load.py"
DBT_PROJ_DIR = Path(__file__).parent.parent / "revenue_pipeline"

# ── Logging ────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def run_process(cmd, cwd=None):
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    return result.returncode, result.stdout, result.stderr


def parse_etl(stdout, stderr):
    combined = stdout + "\n" + stderr
    stats = {"at_rows": None, "bt_rows": None, "rsn_rows": None, "cp_rows": None,
             "enum_rows": None, "mu_rows": None, "elapsed": None, "errors": []}

    for line in combined.splitlines():
        if "agent_transactions:" in line and "rows loaded" in line:
            m = re.search(r"agent_transactions:\s+([\d,]+)\s+rows loaded", line)
            if m:
                stats["at_rows"] = m.group(1)
        elif "bus_ticketing_transaction:" in line and "rows loaded" in line:
            m = re.search(r"bus_ticketing_transaction:\s+([\d,]+)\s+rows loaded", line)
            if m:
                stats["bt_rows"] = m.group(1)
        elif "rev_sub_newcode:" in line and "rows reloaded" in line:
            m = re.search(r"rev_sub_newcode:\s+([\d,]+)\s+rows reloaded", line)
            if m:
                stats["rsn_rows"] = m.group(1)
        elif "concession_permit:" in line and "rows reloaded" in line:
            m = re.search(r"concession_permit:\s+([\d,]+)\s+rows reloaded", line)
            if m:
                stats["cp_rows"] = m.group(1)
        elif "enumeration:" in line and "rows loaded" in line:
            m = re.search(r"enumeration:\s+([\d,]+)\s+rows loaded", line)
            if m:
                stats["enum_rows"] = m.group(1)
        elif "merchant_user:" in line and "rows loaded" in line:
            m = re.search(r"merchant_user:\s+([\d,]+)\s+rows loaded", line)
            if m:
                stats["mu_rows"] = m.group(1)
        elif "ETL completed in" in line:
            m = re.search(r"ETL completed in (.+)", line)
            if m:
                stats["elapsed"] = m.group(1).strip()
        elif "ERROR" in line:
            stats["errors"].append(line.strip())

    return stats


def parse_dbt(stdout, stderr):
    combined = stdout + "\n" + stderr
    models_pass, models_fail, errors = [], [], []

    for line in combined.splitlines():
        if re.search(r"\d+ of \d+ OK", line):
            m = re.search(r"OK created .+?model (\S+)", line)
            if m:
                models_pass.append(m.group(1))
        elif re.search(r"\d+ of \d+ ERROR", line):
            m = re.search(r"ERROR creating .+?model (\S+)", line)
            if m:
                models_fail.append(m.group(1))
            errors.append(line.strip())

    elapsed = None
    m = re.search(r"\((\d+\.\d+s)\)\s*$", combined, re.MULTILINE)
    if m:
        elapsed = m.group(1)

    summary = None
    m = re.search(r"PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) TOTAL=(\d+)", combined)
    if m:
        summary = f"PASS={m.group(1)} WARN={m.group(2)} ERROR={m.group(3)} SKIP={m.group(4)} TOTAL={m.group(5)}"

    return {
        "models_pass": models_pass,
        "models_fail": models_fail,
        "errors":      errors,
        "elapsed":     elapsed,
        "summary":     summary,
        "success":     "Completed successfully" in combined and not models_fail,
    }


def _error_block(errors):
    if not errors:
        return ""
    lines = "<br>".join(errors[:20])
    return (
        '<div style="background:#fef2f2;border-left:4px solid #dc2626;'
        'padding:10px 14px;margin-top:8px;font-family:monospace;font-size:12px;'
        f'color:#7f1d1d;word-break:break-all">{lines}</div>'
    )


def build_html(run_date, etl_ok, etl_stats, dbt_ok, dbt_stats):
    ok_badge  = ('<span style="background:#16a34a;color:#fff;padding:2px 10px;'
                 'border-radius:4px;font-size:12px;font-weight:bold">SUCCESS</span>')
    err_badge = ('<span style="background:#dc2626;color:#fff;padding:2px 10px;'
                 'border-radius:4px;font-size:12px;font-weight:bold">FAILED</span>')

    def badge(ok):
        return ok_badge if ok else err_badge

    at_rows   = etl_stats.get("at_rows")   or "0 (no new data)"
    bt_rows   = etl_stats.get("bt_rows")   or "0 (no new data)"
    rsn_rows  = etl_stats.get("rsn_rows")  or "—"
    cp_rows   = etl_stats.get("cp_rows")   or "—"
    enum_rows = etl_stats.get("enum_rows") or "0 (no new data)"
    mu_rows   = etl_stats.get("mu_rows")   or "0 (no new data)"
    etl_time  = etl_stats.get("elapsed")   or "—"

    model_rows = ""
    for m in dbt_stats["models_pass"]:
        model_rows += (
            f'<tr><td style="padding:5px 12px">{m}</td>'
            '<td style="padding:5px 12px;color:#16a34a;font-weight:bold">OK</td></tr>'
        )
    for m in dbt_stats["models_fail"]:
        model_rows += (
            f'<tr><td style="padding:5px 12px">{m}</td>'
            '<td style="padding:5px 12px;color:#dc2626;font-weight:bold">ERROR</td></tr>'
        )

    dbt_footer = ""
    if dbt_stats.get("summary"):
        dbt_footer += (
            f'<tr style="background:#f9fafb"><td colspan="2" style="padding:5px 12px">'
            f'<em>{dbt_stats["summary"]}</em></td></tr>'
        )
    if dbt_stats.get("elapsed"):
        dbt_footer += (
            f'<tr><td style="padding:5px 12px;font-weight:bold">Elapsed</td>'
            f'<td style="padding:5px 12px">{dbt_stats["elapsed"]}</td></tr>'
        )

    return f"""<html><body style="font-family:Arial,sans-serif;color:#111827;max-width:640px;margin:0 auto;padding:16px">
<h2 style="border-bottom:2px solid #e5e7eb;padding-bottom:10px;margin-bottom:4px">
  CentricApps BI &mdash; Daily Pipeline Report
</h2>
<p style="color:#6b7280;margin-top:4px;margin-bottom:24px">{run_date}</p>

<h3 style="margin-bottom:6px">ETL Load &nbsp;{badge(etl_ok)}</h3>
<table style="border-collapse:collapse;width:100%;margin-bottom:4px">
  <tr style="background:#f9fafb">
    <td style="padding:5px 12px;font-weight:bold;width:220px">agent_transactions</td>
    <td style="padding:5px 12px">{at_rows} rows</td>
  </tr>
  <tr>
    <td style="padding:5px 12px;font-weight:bold">bus_ticketing_transaction</td>
    <td style="padding:5px 12px">{bt_rows} rows</td>
  </tr>
  <tr style="background:#f9fafb">
    <td style="padding:5px 12px;font-weight:bold">rev_sub_newcode</td>
    <td style="padding:5px 12px">{rsn_rows} rows</td>
  </tr>
  <tr>
    <td style="padding:5px 12px;font-weight:bold">concession_permit</td>
    <td style="padding:5px 12px">{cp_rows} rows</td>
  </tr>
  <tr style="background:#f9fafb">
    <td style="padding:5px 12px;font-weight:bold">enumeration</td>
    <td style="padding:5px 12px">{enum_rows} rows</td>
  </tr>
  <tr>
    <td style="padding:5px 12px;font-weight:bold">merchant_user</td>
    <td style="padding:5px 12px">{mu_rows} rows</td>
  </tr>
  <tr style="background:#f9fafb">
    <td style="padding:5px 12px;font-weight:bold">Elapsed</td>
    <td style="padding:5px 12px">{etl_time}</td>
  </tr>
</table>
{_error_block(etl_stats['errors'])}

<h3 style="margin-top:28px;margin-bottom:6px">dbt Transformation &nbsp;{badge(dbt_ok)}</h3>
<table style="border-collapse:collapse;width:100%;margin-bottom:4px">
  <thead>
    <tr style="background:#f3f4f6">
      <th style="padding:6px 12px;text-align:left">Model</th>
      <th style="padding:6px 12px;text-align:left">Status</th>
    </tr>
  </thead>
  <tbody>
    {model_rows}
    {dbt_footer}
  </tbody>
</table>
{_error_block(dbt_stats['errors'])}

<p style="color:#9ca3af;font-size:11px;margin-top:32px;border-top:1px solid #e5e7eb;padding-top:10px">
  Sent automatically by CentricApps BI pipeline &middot; {run_date}
</p>
</body></html>"""


def main():
    run_start = datetime.now()
    run_date  = run_start.strftime("%Y-%m-%d %H:%M")
    log.info("=" * 60)
    log.info("Pipeline started")

    # ── Step 1: ETL ───────────────────────────────────────────────
    log.info("Running ETL...")
    etl_rc, etl_out, etl_err = run_process(
        [PYTHON_EXE, str(ETL_SCRIPT)],
        cwd=str(ETL_SCRIPT.parent),
    )
    etl_stats = parse_etl(etl_out, etl_err)
    etl_ok    = etl_rc == 0 and not etl_stats["errors"]
    log.info(f"ETL {'succeeded' if etl_ok else 'FAILED'} (exit {etl_rc})")

    # ── Step 2: dbt ───────────────────────────────────────────────
    if etl_ok:
        log.info("Running dbt...")
        dbt_rc, dbt_out, dbt_err = run_process(
            [PYTHON_EXE, DBT_RUNNER, "run", "--project-dir", str(DBT_PROJ_DIR)],
            cwd=str(DBT_PROJ_DIR),
        )
        dbt_stats = parse_dbt(dbt_out, dbt_err)
        if dbt_rc != 0:
            dbt_stats["success"] = False
        dbt_ok = dbt_stats["success"]
        log.info(f"dbt {'succeeded' if dbt_ok else 'FAILED'} (exit {dbt_rc})")
    else:
        dbt_ok    = False
        dbt_stats = {
            "models_pass": [], "models_fail": [],
            "errors":  ["dbt was skipped because the ETL step failed."],
            "elapsed": None, "summary": None, "success": False,
        }

    # ── Step 3: Email ─────────────────────────────────────────────
    overall = "SUCCESS" if (etl_ok and dbt_ok) else "FAILED"
    subject = f"[CentricApps BI] Pipeline {overall} — {run_date}"
    html    = build_html(run_date, etl_ok, etl_stats, dbt_ok, dbt_stats)

    try:
        email_alert.send_alert(subject, html)
        log.info("Alert email sent.")
    except Exception as exc:
        log.error(f"Failed to send alert email: {exc}")

    log.info(f"Pipeline finished in {datetime.now() - run_start}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
