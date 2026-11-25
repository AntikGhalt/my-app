import json

import pytest

from istatsync import notify
from istatsync.runner import Outcome


def summary(*outcomes):
    return {"destination": "gdrive:x", "outcomes": list(outcomes)}


def row(dataset, status, edition=None, last="2026-Q2", title=None):
    return {
        "dataset": dataset,
        "title": title or dataset,
        "status": status,
        "edition": edition,
        "last_period": last,
        "detail": "",
    }


def test_no_message_when_nothing_changed():
    s = summary(row("a", "unchanged", "2026M10"), row("b", "failed"))
    assert notify.build_message(s, "me@x", ["you@y"], None, None) is None


def test_table_rebuild_alone_is_not_news():
    s = summary(row("tabelle/t.xlsx", "updated"))
    assert notify.build_message(s, "me@x", ["you@y"], None, None) is None


def test_message_lists_updates_editions_and_links():
    s = summary(
        row("conto", "updated", "2026M10", title="Sequenza dei conti"),
        row("wf", "updated", None, title="Ricchezza finanziaria"),
        row("pop", "unchanged", "2026M10"),
        row("tabelle/tabella_reddito_trimestrale.xlsx", "updated"),
    )
    msg = notify.build_message(s, "me@x", ["you@y"], "https://drive/folder", "https://github/run/1")
    body = msg.get_content()
    assert msg["Subject"].endswith("(edizione 2026M10)")
    assert msg["To"] == "you@y"
    assert "Sequenza dei conti: edizione 2026M10, dati fino a 2026-Q2" in body
    assert "Ricchezza finanziaria: nuovi dati o revisioni" in body
    assert "pop" not in body
    assert "tabella_reddito_trimestrale.xlsx" in body
    assert "https://drive/folder" in body and "https://github/run/1" in body


def test_summary_roundtrip(tmp_path):
    path = tmp_path / "s.json"
    notify.write_summary(
        path, [Outcome("conto", "updated", "2026M10", "2026-Q2")], {"conto": "Conto"}, "here"
    )
    data = json.loads(path.read_text())
    assert data["outcomes"][0]["title"] == "Conto"


def test_unconfigured_mail_is_skipped_quietly(tmp_path, monkeypatch):
    for name in ("ISTATSYNC_NOTIFY_TO", "ISTATSYNC_SMTP_USER", "ISTATSYNC_SMTP_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    assert "non configurata" in notify.notify_from_env(tmp_path / "none.json", None)


def test_sends_through_smtp(tmp_path, monkeypatch):
    path = tmp_path / "s.json"
    path.write_text(json.dumps(summary(row("conto", "updated", "2026M10"))))
    monkeypatch.setenv("ISTATSYNC_NOTIFY_TO", "a@x, b@y")
    monkeypatch.setenv("ISTATSYNC_SMTP_USER", "me@gmail.com")
    monkeypatch.setenv("ISTATSYNC_SMTP_PASSWORD", "app-password")
    sent = []
    monkeypatch.setattr(notify, "send", lambda msg, *a: sent.append((msg, a)))
    result = notify.notify_from_env(path, "FOLDER")
    msg, (host, port, user, password) = sent[0]
    assert (host, port, user) == ("smtp.gmail.com", 465, "me@gmail.com")
    assert msg["To"] == "a@x, b@y"
    assert "drive.google.com/drive/folders/FOLDER" in msg.get_content()
    assert "inviata a 2" in result


@pytest.mark.parametrize("status", ["failed", "skipped", "unchanged"])
def test_other_statuses_never_mail(status):
    assert notify.build_message(summary(row("x", status)), "a", ["b"], None, None) is None
