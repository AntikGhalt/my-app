"""DriveSink against an in-memory stand-in for the Drive v3 `files` resource."""

import itertools
import re

import pytest

from istatsync.sinks.drive import FOLDER_MIME, DriveSink

_Q = re.compile(
    r"name = '((?:[^'\\]|\\.)*)' and '([^']+)' in parents and trashed = false"
    r" and mimeType (!?=) '([^']+)'"
)


class _Call:
    def __init__(self, fn):
        self._fn = fn

    def execute(self):
        return self._fn()


class FakeFiles:
    def __init__(self, root="ROOT"):
        self.items: dict[str, dict] = {root: {"name": "root", "parents": [], "mime": FOLDER_MIME}}
        self._ids = (f"id{i}" for i in itertools.count())

    def list(self, q, **_):
        m = _Q.fullmatch(q)
        assert m, q
        name = re.sub(r"\\(.)", r"\1", m.group(1))
        parent, op = m.group(2), m.group(3)

        def run():
            hits = [
                {"id": fid}
                for fid, f in self.items.items()
                if f["name"] == name
                and parent in f["parents"]
                and ((f["mime"] == FOLDER_MIME) == (op == "="))
            ]
            return {"files": hits}

        return _Call(run)

    def create(self, body, media_body=None, **_):
        def run():
            fid = next(self._ids)
            self.items[fid] = {
                "name": body["name"],
                "parents": body["parents"],
                "mime": body.get("mimeType", "file"),
                "content": _bytes(media_body),
            }
            return {"id": fid}

        return _Call(run)

    def update(self, fileId, media_body, **_):
        return _Call(lambda: self.items[fileId].update(content=_bytes(media_body)) or {})

    def copy(self, fileId, body, **_):
        def run():
            fid = next(self._ids)
            self.items[fid] = {
                **self.items[fileId],
                "name": body["name"],
                "parents": body["parents"],
            }
            return {"id": fid}

        return _Call(run)

    def get_media(self, fileId, **_):
        return _Call(lambda: self.items[fileId]["content"])

    def by_name(self, name):
        return [(fid, f) for fid, f in self.items.items() if f["name"] == name]


def _bytes(media):
    return media.getbytes(0, media.size()) if media is not None else None


class FakeService:
    def __init__(self):
        self._files = FakeFiles()

    def files(self):
        return self._files


@pytest.fixture
def drive():
    service = FakeService()
    return DriveSink("ROOT", service=service), service.files()


def test_write_creates_nested_folders_once(drive):
    sink, files = drive
    sink.write("trimestrali/a.csv", b"1")
    sink.write("trimestrali/b.csv", b"2")
    assert len(files.by_name("trimestrali")) == 1
    assert sink.read("trimestrali/b.csv") == b"2"


def test_overwrite_keeps_file_id(drive):
    sink, files = drive
    sink.write("annuali/x.xlsx", b"v1")
    [(fid, _)] = files.by_name("x.xlsx")
    sink.write("annuali/x.xlsx", b"v2")
    [(same, f)] = files.by_name("x.xlsx")
    assert same == fid and f["content"] == b"v2"


def test_missing_file_reads_none(drive):
    sink, _ = drive
    assert sink.read("nope/manifest.json") is None


def test_copy_to_archive(drive):
    sink, files = drive
    sink.write("mensili/nic.csv", b"old")
    sink.copy("mensili/nic.csv", "archivio/nic/nic_20251101_al2025-10.csv")
    assert sink.read("archivio/nic/nic_20251101_al2025-10.csv") == b"old"
    assert sink.read("mensili/nic.csv") == b"old"


def test_names_with_quotes_are_escaped(drive):
    sink, _ = drive
    sink.write("l'archivio/it's.csv", b"ok")
    assert sink.read("l'archivio/it's.csv") == b"ok"


def test_folder_and_file_with_same_name_do_not_clash(drive):
    sink, _ = drive
    sink.write("log", b"a file called log")
    sink.write("log/run_log.csv", b"x")
    assert sink.read("log") == b"a file called log"
    assert sink.read("log/run_log.csv") == b"x"
