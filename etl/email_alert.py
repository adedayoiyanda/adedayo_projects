import http.client
import json
import os
import ssl
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

_API_KEY     = os.getenv("BREVO_API_KEY", "")
_ALERT_EMAIL = os.getenv("ALERT_EMAIL", "adedayoprojects@gmail.com")
_SENDER_NAME = "CentricApps BI"


def send_alert(subject: str, html_body: str) -> None:
    if not _API_KEY:
        raise RuntimeError("BREVO_API_KEY not set in .env")

    payload = json.dumps({
        "sender": {"name": _SENDER_NAME, "email": _ALERT_EMAIL},
        "to":     [{"email": _ALERT_EMAIL}],
        "subject":     subject,
        "htmlContent": html_body,
    }).encode()

    ctx  = ssl.create_default_context()
    conn = http.client.HTTPSConnection("api.brevo.com", context=ctx)
    conn.request(
        "POST",
        "/v3/smtp/email",
        body=payload,
        headers={
            "Content-Type": "application/json",
            "api-key":      _API_KEY,
        },
    )
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()

    if resp.status not in (200, 201):
        raise RuntimeError(f"Brevo API error {resp.status}: {body}")
