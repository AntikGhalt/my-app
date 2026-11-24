"""Runs the catalogue: download, compare with what was published, publish."""

from __future__ import annotations

import csv
import io
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

from .catalog import Dataset
from .periods import edition_key, latest_edition
from .sdmx import RateLimited, SdmxClient, Structure, build_key
from .sinks import Sink
from .tables import build_tables
from .transform import build_table, to_csv, to_excel

log = logging.getLogger(__name__)

MANIFEST = "manifest.json"
RUN_LOG = "log/run_log.csv"
ARCHIVE = "archivio"
PROBE_YEARS = 3


@dataclass
class Outcome:
    dataset: str
    status: str  # updated | unchanged | failed | skipped
    edition: str | None = None
    last_period: str | None = None
    detail: str = ""


class Runner:
    def __init__(
        self,
        clients: dict[str, SdmxClient],
        sink: Sink,
        tz: str = "Europe/Rome",
        force: bool = False,
    ):
        self.clients = clients
        self.sink = sink
        self.tz = ZoneInfo(tz)
        self.force = force
        raw = sink.read(MANIFEST)
        self.manifest: dict[str, dict] = json.loads(raw) if raw else {}

    def now(self) -> datetime:
        return datetime.now(self.tz).replace(microsecond=0)

    # ------------------------------------------------------------------

    def run(self, datasets: list[Dataset]) -> list[Outcome]:
        outcomes: list[Outcome] = []
        throttled: set[str] = set()
        for ds in datasets:
            if ds.source in throttled:
                detail = f"{ds.source} is throttling, not attempted"
                outcomes.append(Outcome(ds.id, "skipped", detail=detail))
                continue
            log.info("[%s] start (%s)", ds.id, ds.dataflow)
            try:
                outcome = self.run_one(ds)
            except RateLimited as exc:
                # Stop only this provider: the ECB does not care about ISTAT's ban.
                throttled.add(ds.source)
                outcome = Outcome(ds.id, "failed", detail=str(exc))
            except Exception as exc:  # one broken dataset must not stop the others
                log.exception("[%s] failed", ds.id)
                outcome = Outcome(ds.id, "failed", detail=f"{type(exc).__name__}: {exc}")
            log.info("[%s] %s %s", ds.id, outcome.status, outcome.detail)
            outcomes.append(outcome)
            # Persist after every dataset: a crash later on must not make us
            # forget what was already published.
            self._save_manifest()

        outcomes += self.rebuild_tables()
        self._save_manifest()
        self._append_log(outcomes)
        return outcomes

    def rebuild_tables(self, force: bool = False) -> list[Outcome]:
        """Summary workbooks, rebuilt only when one of their inputs changed."""
        try:
            written = build_tables(self.sink, self.manifest, force=force or self.force)
        except Exception as exc:
            log.exception("summary tables failed")
            return [Outcome("tabelle", "failed", detail=f"{type(exc).__name__}: {exc}")]
        return [Outcome(path, "updated", detail="tabella ricostruita") for path in written]

    def run_one(self, ds: Dataset) -> Outcome:
        client = self.clients[ds.source]
        structure = client.structure(ds.dataflow)
        key = self._key(ds, structure)
        previous = self.manifest.get(ds.id, {})
        same_definition = previous.get("fingerprint") == ds.fingerprint

        edition = None
        if "EDITION" in structure.dimensions:
            edition = ds.edition or self._probe_edition(client, ds, structure, key)
            if edition is None:
                return Outcome(ds.id, "failed", detail="no edition with data found")
            if not self.force and same_definition and previous.get("edition") == edition:
                return self._unchanged(ds, previous, "edition already published")
            key["EDITION"] = edition

        raw = client.data(ds.dataflow, structure.dimensions, key, start=ds.start)
        table = build_table(raw, ds, structure, edition)
        digest = table.content_hash()

        if not self.force and same_definition and previous.get("sha256") == digest:
            return self._unchanged(ds, previous, "same data as last run")

        stamp = self.now()
        base = f"{ds.folder}/{ds.id}"
        if previous:
            self._archive(ds, previous)

        key_str, _ = build_key(structure.dimensions, key)
        source = client.data_url(ds.dataflow, key_str)
        self.sink.write(f"{base}.xlsx", to_excel(table, stamp.isoformat(), source))
        self.sink.write(f"{base}.csv", to_csv(table))

        self.manifest[ds.id] = {
            "title": ds.title,
            "dataflow": ds.dataflow,
            "files": [f"{base}.xlsx", f"{base}.csv"],
            "edition": edition,
            "first_period": table.first_period,
            "last_period": table.last_period,
            "series": table.n_series,
            "rows": len(table.long),
            "sha256": digest,
            "fingerprint": ds.fingerprint,
            "updated_at": stamp.isoformat(),
            "checked_at": stamp.isoformat(),
        }
        what = f"edition {edition}" if edition else f"data up to {table.last_period}"
        return Outcome(ds.id, "updated", edition, table.last_period, what)

    # ------------------------------------------------------------------

    def _key(self, ds: Dataset, structure: Structure) -> dict:
        key: dict = {}
        if "FREQ" in structure.dimensions:
            key["FREQ"] = ds.frequency
        key.update(ds.filter)
        unknown = set(key) - set(structure.dimensions)
        if unknown:
            raise ValueError(
                f"filter uses {sorted(unknown)}, not in {ds.dataflow}: {structure.dimensions}"
            )
        return key

    def _probe_edition(
        self, client: SdmxClient, ds: Dataset, structure: Structure, key: dict
    ) -> str | None:
        """One small request over recent years with EDITION left empty: ISTAT
        then returns every edition at once, and we pick the newest complete one."""
        start = str(date.today().year - PROBE_YEARS)
        probe = client.data(ds.dataflow, structure.dimensions, {**key, "EDITION": []}, start=start)
        if probe.empty:
            return None
        series_dims = [d for d in structure.dimensions if d in probe and d != "EDITION"]
        counts = probe.groupby("EDITION")[series_dims].apply(lambda g: len(g.drop_duplicates()))
        chosen = latest_edition(counts.to_dict())
        newest = max(counts.index, key=edition_key)
        if chosen != newest:
            log.warning("[%s] edition %s looks partial, using %s", ds.id, newest, chosen)
        return chosen

    def _unchanged(self, ds: Dataset, previous: dict, why: str) -> Outcome:
        previous["checked_at"] = self.now().isoformat()
        return Outcome(
            ds.id, "unchanged", previous.get("edition"), previous.get("last_period"), why
        )

    def _archive(self, ds: Dataset, previous: dict) -> None:
        when = previous.get("updated_at", "")[:10].replace("-", "")
        label = previous.get("edition") or f"al{previous.get('last_period', '')}"
        for path in previous.get("files", []):
            ext = path.rsplit(".", 1)[-1]
            self.sink.copy(path, f"{ARCHIVE}/{ds.id}/{ds.id}_{when}_{label}.{ext}")

    def _save_manifest(self) -> None:
        body = json.dumps(self.manifest, indent=2, ensure_ascii=False, sort_keys=True)
        self.sink.write(MANIFEST, body.encode())

    def _append_log(self, outcomes: list[Outcome]) -> None:
        existing = (self.sink.read(RUN_LOG) or b"").decode("utf-8")
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        if not existing:
            writer.writerow(["timestamp", "dataset", "status", "edition", "last_period", "detail"])
        ts = self.now().isoformat()
        for o in outcomes:
            writer.writerow(
                [ts, o.dataset, o.status, o.edition or "", o.last_period or "", o.detail]
            )
        self.sink.write(RUN_LOG, (existing + out.getvalue()).encode("utf-8"))
