import os
import sys
import csv
import io
import json
import base64
import http.client
import ssl
import logging
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Recipients ────────────────────────────────────────────────────────────────
# Add more emails to this list as needed
RECIPIENTS = [
    "adedayoiyanda@gmail.com",
    "ryanosakweogo@gmail.com",
]

SENDER_EMAIL = os.getenv("ALERT_EMAIL", "adedayoprojects@gmail.com")
SENDER_NAME  = "CentricApps BI"

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"csv_report_{date.today().strftime('%Y%m%d')}.log"

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

try:
    import psycopg2
except ImportError as e:
    log.error(f"Missing package: {e}")
    sys.exit(1)

pg_cfg = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD"),
    "dbname":   os.getenv("PG_DATABASE"),
}

QUERY_MDA = """
    SELECT
        mda,
        SUM(transaction_count)::bigint  AS transaction_count,
        SUM(total_amount)               AS total_amount
    FROM public_marts.agg_revenue_daily
    WHERE payment_day >= %s
      AND payment_day <= %s
      AND payment_day IS NOT NULL
    GROUP BY mda
    ORDER BY total_amount DESC
"""

QUERY_ETICKET = """
    SELECT
        revenue_item                AS "Ticket type",
        COUNT(*)::bigint            AS "Ticket sold",
        SUM(amount)                 AS "Revenue"
    FROM public_marts.fct_transactions
    WHERE payment_day >= %s
      AND payment_day <= %s
      AND is_completed = true
      AND (payment_period IS NOT NULL AND payment_period != '')
    GROUP BY revenue_item
    ORDER BY SUM(amount) DESC
"""

QUERY_BUS = """
    SELECT
        bus_id,
        COUNT(ticket_id)::bigint    AS trips,
        SUM(amount)                 AS revenue
    FROM public_staging.stg_bus_ticketing_transaction
    WHERE transaction_day >= %s
      AND transaction_day <= %s
    GROUP BY bus_id
    ORDER BY SUM(amount) DESC
"""

QUERY_AGENT_ENUMERATION = """
    SELECT
        agent_email,
        first_name,
        last_name,
        bank,
        bank_account,
        SUM(enumeration_count)::bigint  AS total_enumeration_count
    FROM public_marts.fct_agent_enumeration
    WHERE agent_status = 'Verified'
      AND create_day >= %s
      AND create_day <= %s
    GROUP BY agent_email, first_name, last_name, bank, bank_account
    ORDER BY SUM(enumeration_count) DESC
"""


# ── Date range ────────────────────────────────────────────────────────────────
def get_date_range():
    today      = date.today()          # Saturday
    end_date   = today - timedelta(days=1)   # last Friday
    start_date = today - timedelta(days=7)   # last Saturday
    return start_date, end_date


# ── CSV builder ───────────────────────────────────────────────────────────────
def build_csv(rows, columns):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    writer.writerows(rows)
    return buf.getvalue()


# ── Email ─────────────────────────────────────────────────────────────────────
def build_html(start_date, end_date, mda_count, eticket_count, bus_count, agent_count):
    period = f"{start_date.strftime('%b %d, %Y')} – {end_date.strftime('%b %d, %Y')}"
    return f"""<html><body style="font-family:Arial,sans-serif;color:#111827;max-width:640px;margin:0 auto;padding:16px">
<h2 style="border-bottom:2px solid #e5e7eb;padding-bottom:10px;margin-bottom:4px">
  CentricApps BI &mdash; Weekly Revenue Reports
</h2>
<p style="color:#6b7280;margin-top:4px;margin-bottom:24px">{period}</p>
<p>Please find the weekly reports attached as CSV files.</p>
<table style="border-collapse:collapse;width:100%;margin-top:12px">
  <tr style="background:#f9fafb">
    <td style="padding:5px 12px;font-weight:bold;width:200px">Period</td>
    <td style="padding:5px 12px">{period}</td>
  </tr>
  <tr>
    <td style="padding:5px 12px;font-weight:bold">MDA Breakdown</td>
    <td style="padding:5px 12px">{mda_count} MDAs</td>
  </tr>
  <tr style="background:#f9fafb">
    <td style="padding:5px 12px;font-weight:bold">eTicket Type Breakdown</td>
    <td style="padding:5px 12px">{eticket_count} ticket types</td>
  </tr>
  <tr>
    <td style="padding:5px 12px;font-weight:bold">Bus Performance Summary</td>
    <td style="padding:5px 12px">{bus_count} buses</td>
  </tr>
  <tr style="background:#f9fafb">
    <td style="padding:5px 12px;font-weight:bold">Agent Enumeration</td>
    <td style="padding:5px 12px">{agent_count} agents</td>
  </tr>
</table>
<p style="color:#9ca3af;font-size:11px;margin-top:32px;border-top:1px solid #e5e7eb;padding-top:10px">
  Sent by CentricApps BI &middot; {date.today().strftime('%Y-%m-%d')}
</p>
</body></html>"""


def send_email(subject, html_body, attachments):
    # attachments: list of (csv_content, csv_filename) tuples
    api_key = os.getenv("BREVO_API_KEY", "")
    if not api_key:
        raise RuntimeError("BREVO_API_KEY not set in .env")

    payload = json.dumps({
        "sender":      {"name": SENDER_NAME, "email": SENDER_EMAIL},
        "to":          [{"email": r} for r in RECIPIENTS],
        "subject":     subject,
        "htmlContent": html_body,
        "attachment":  [
            {
                "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                "name":    filename,
            }
            for content, filename in attachments
        ],
    }).encode()

    ctx  = ssl.create_default_context()
    conn = http.client.HTTPSConnection("api.brevo.com", context=ctx)
    conn.request(
        "POST",
        "/v3/smtp/email",
        body=payload,
        headers={
            "Content-Type": "application/json",
            "api-key":      api_key,
        },
    )
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()

    if resp.status not in (200, 201):
        raise RuntimeError(f"Brevo API error {resp.status}: {body}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start_date, end_date = get_date_range()
    log.info(f"Report period: {start_date} to {end_date}")

    try:
        conn = psycopg2.connect(**pg_cfg)
        cur  = conn.cursor()
    except Exception as e:
        log.error(f"PostgreSQL connection failed: {e}")
        sys.exit(1)

    try:
        log.info("Querying MDA breakdown...")
        cur.execute(QUERY_MDA, (start_date, end_date))
        mda_rows = cur.fetchall()
        mda_cols = [desc[0] for desc in cur.description]
        log.info(f"MDA rows: {len(mda_rows)}")

        log.info("Querying eTicket type breakdown...")
        cur.execute(QUERY_ETICKET, (start_date, end_date))
        eticket_rows = cur.fetchall()
        eticket_cols = [desc[0] for desc in cur.description]
        log.info(f"eTicket rows: {len(eticket_rows)}")

        log.info("Querying bus performance summary...")
        cur.execute(QUERY_BUS, (start_date, end_date))
        bus_rows = cur.fetchall()
        bus_cols = [desc[0] for desc in cur.description]
        log.info(f"Bus rows: {len(bus_rows)}")

        log.info("Querying agent enumeration...")
        cur.execute(QUERY_AGENT_ENUMERATION, (start_date, end_date))
        agent_rows = cur.fetchall()
        agent_cols = [desc[0] for desc in cur.description]
        log.info(f"Agent rows: {len(agent_rows)}")
    except Exception as e:
        log.error(f"Query failed: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    if not mda_rows and not eticket_rows and not bus_rows:
        log.warning("No data found for the period — email not sent.")
        sys.exit(0)

    attachments = [
        (
            build_csv(mda_rows, mda_cols),
            f"mda_collections_{start_date}_to_{end_date}.csv",
        ),
        (
            build_csv(eticket_rows, eticket_cols),
            f"eticket_type_breakdown_{start_date}_to_{end_date}.csv",
        ),
        (
            build_csv(bus_rows, bus_cols),
            f"bus_performance_summary_{start_date}_to_{end_date}.csv",
        ),
        (
            build_csv(agent_rows, agent_cols),
            f"agent_enumeration_{start_date}_to_{end_date}.csv",
        ),
    ]

    subject = (
        f"CentricApps BI — Weekly Revenue Reports "
        f"({start_date.strftime('%b %d')} – {end_date.strftime('%b %d, %Y')})"
    )
    html = build_html(start_date, end_date, len(mda_rows), len(eticket_rows), len(bus_rows), len(agent_rows))

    log.info(f"Sending to: {', '.join(RECIPIENTS)}")
    try:
        send_email(subject, html, attachments)
        log.info("Email sent successfully.")
    except Exception as e:
        log.error(f"Failed to send email: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
