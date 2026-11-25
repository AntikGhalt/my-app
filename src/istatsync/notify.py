"""E-mail sent when a run publishes new data.

`istatsync run --summary FILE` records what happened; `istatsync notify
--summary FILE` turns it into one short message, only if something was
updated. Failures are not mailed here: GitHub already e-mails the owner of
a failed scheduled run.

Settings (environment):
    ISTATSYNC_NOTIFY_TO      recipient(s), comma separated
    ISTATSYNC_SMTP_USER      sending account, e.g. a Gmail address
    ISTATSYNC_SMTP_PASSWORD  its app password
    ISTATSYNC_SMTP_HOST      default smtp.gmail.com
    ISTATSYNC_SMTP_PORT      default 465 (SSL)
"""

from __future__ import annotations

import json
import os
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path


def write_summary(path: Path, outcomes, titles: dict[str, str], destination: str) -> None:
    rows = [
        {
            "dataset": o.dataset,
            "title": titles.get(o.dataset, o.dataset),
            "status": o.status,
            "edition": o.edition,
            "last_period": o.last_period,
            "detail": o.detail,
        }
        for o in outcomes
    ]
    path.write_text(
        json.dumps({"destination": destination, "outcomes": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_message(
    summary: dict, sender: str, recipients: list[str], folder_url: str | None, run_url: str | None
) -> EmailMessage | None:
    """None when nothing new was published."""
    rows = summary.get("outcomes", [])
    data = [r for r in rows if r["status"] == "updated" and not r["dataset"].endswith(".xlsx")]
    tables = [r for r in rows if r["status"] == "updated" and r["dataset"].endswith(".xlsx")]
    if not data:
        return None

    editions = sorted({r["edition"] for r in data if r["edition"]})
    subject = "Nuovi dati ISTAT e Banca d'Italia pubblicati"
    if editions:
        subject += f" (edizione {', '.join(editions)})"

    lines = ["Sono stati pubblicati dati nuovi o rivisti:", ""]
    for r in data:
        when = f"edizione {r['edition']}" if r["edition"] else "nuovi dati o revisioni"
        until = f", dati fino a {r['last_period']}" if r["last_period"] else ""
        lines.append(f"- {r['title']}: {when}{until}")
    if tables:
        lines += ["", "Tabelle di sintesi ricalcolate:"]
        lines += [f"- {r['dataset']}" for r in tables]
    if folder_url:
        lines += ["", f"Cartella: {folder_url}"]
    lines += ["", "Le versioni precedenti sono nella cartella archivio."]
    if run_url:
        lines += ["", f"Dettagli dell'esecuzione: {run_url}"]
    lines += ["", "--", "Messaggio automatico di istatsync."]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content("\n".join(lines))
    return msg


def send(msg: EmailMessage, host: str, port: int, user: str, password: str) -> None:
    with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=60) as smtp:
        smtp.login(user, password)
        smtp.send_message(msg)


def notify_from_env(summary_path: Path, drive_folder_id: str | None) -> str:
    """Send the message if there is one and the mail settings exist.
    Returns a one-line description of what happened."""
    env = os.environ.get
    recipients = [a.strip() for a in (env("ISTATSYNC_NOTIFY_TO") or "").split(",") if a.strip()]
    user = env("ISTATSYNC_SMTP_USER") or ""
    password = env("ISTATSYNC_SMTP_PASSWORD") or ""
    if not (recipients and user and password):
        return "e-mail non configurata: nessun messaggio"
    if not summary_path.is_file():
        return "nessun riepilogo dell'esecuzione: nessun messaggio"

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    folder = (
        f"https://drive.google.com/drive/folders/{drive_folder_id}" if drive_folder_id else None
    )
    run_url = None
    if env("GITHUB_RUN_ID"):
        repo = f"{env('GITHUB_SERVER_URL')}/{env('GITHUB_REPOSITORY')}"
        run_url = f"{repo}/actions/runs/{env('GITHUB_RUN_ID')}"

    msg = build_message(summary, user, recipients, folder, run_url)
    if msg is None:
        return "nessun dato nuovo: nessun messaggio"
    host = env("ISTATSYNC_SMTP_HOST") or "smtp.gmail.com"
    port = int(env("ISTATSYNC_SMTP_PORT") or 465)
    send(msg, host, port, user, password)
    return f"e-mail inviata a {len(recipients)} destinatari: {msg['Subject']}"
