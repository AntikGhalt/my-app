import json
from dataclasses import replace

import pandas as pd
import pytest
from conftest import make_rows

from istatsync.runner import MANIFEST, RUN_LOG, Runner
from istatsync.sdmx import RateLimited, build_key
from istatsync.sinks import LocalSink


class FakeClient:
    """Serves a fixed set of editions; records every data request."""

    def __init__(self, structure, editions: dict[str, pd.DataFrame]):
        self._structure = structure
        self.editions = editions
        self.calls: list[dict] = []
        self.requests_made = 0
        self.fail_with: Exception | None = None

    def structure(self, dataflow):
        return self._structure

    def data(self, dataflow, dimensions, key, start=None, end=None):
        if self.fail_with:
            raise self.fail_with
        self.calls.append(dict(key))
        edition = key.get("EDITION")
        if edition:
            return self.editions[edition].copy()
        return pd.concat(self.editions.values(), ignore_index=True)  # probe: all editions

    def data_url(self, dataflow, key):
        return f"https://example.org/{dataflow}/{key}"


@pytest.fixture
def sink(tmp_path):
    return LocalSink(tmp_path)


def run(client, sink, *datasets, force=False):
    return Runner({"istat": client}, sink, force=force).run(list(datasets))


def test_first_run_publishes_files_and_manifest(structure, dataset, sink, tmp_path):
    client = FakeClient(structure, {"2025M9": make_rows("2025M9"), "2025M10": make_rows("2025M10")})
    [outcome] = run(client, sink, dataset)

    assert outcome.status == "updated"
    assert outcome.edition == "2025M10"
    assert (tmp_path / "trimestrali/consumi_test.xlsx").is_file()
    assert (tmp_path / "trimestrali/consumi_test.csv").is_file()
    manifest = json.loads((tmp_path / MANIFEST).read_text())
    assert manifest["consumi_test"]["edition"] == "2025M10"
    assert "updated" in (tmp_path / RUN_LOG).read_text()


def test_probe_then_single_download_with_frequency_in_key(structure, dataset, sink):
    client = FakeClient(structure, {"2025M10": make_rows("2025M10")})
    run(client, sink, dataset)
    probe, download = client.calls
    assert probe["EDITION"] == [] and probe["FREQ"] == "Q"
    assert download["EDITION"] == "2025M10"


def test_same_edition_skips_the_download(structure, dataset, sink):
    client = FakeClient(structure, {"2025M10": make_rows("2025M10")})
    run(client, sink, dataset)
    client.calls.clear()

    [outcome] = run(client, sink, dataset)
    assert outcome.status == "unchanged"
    assert len(client.calls) == 1  # the probe only


def test_new_edition_archives_the_previous_files(structure, dataset, sink, tmp_path):
    client = FakeClient(structure, {"2025M10": make_rows("2025M10")})
    run(client, sink, dataset)

    client.editions["2025M11"] = make_rows("2025M11", periods=("2024-Q1", "2024-Q2", "2024-Q3"))
    [outcome] = run(client, sink, dataset)

    assert outcome.status == "updated"
    assert outcome.last_period == "2024-Q3"
    archived = sorted(p.name for p in (tmp_path / "archivio/consumi_test").iterdir())
    assert len(archived) == 2 and all("2025M10" in name for name in archived)


def test_changed_definition_forces_refresh(structure, dataset, sink):
    client = FakeClient(structure, {"2025M10": make_rows("2025M10")})
    run(client, sink, dataset)
    [outcome] = run(client, sink, replace(dataset, start="2024"))
    assert outcome.status == "updated"


def test_dataflow_without_edition_compares_content(structure, dataset, sink):
    flat = replace(structure, dimensions=[d for d in structure.dimensions if d != "EDITION"])
    rows = make_rows("x").drop(columns="EDITION")
    client = FakeClient(flat, {"": rows})
    client.data = lambda *a, **k: rows.copy()

    assert run(client, sink, dataset)[0].status == "updated"
    assert run(client, sink, dataset)[0].status == "unchanged"
    rows.loc[0, "OBS_VALUE"] += 1  # ISTAT revised one value
    assert run(client, sink, dataset)[0].status == "updated"


def test_one_failure_does_not_stop_the_others(structure, dataset, sink):
    bad = replace(dataset, id="broken", filter={"NOT_A_DIMENSION": "X"})
    client = FakeClient(structure, {"2025M10": make_rows("2025M10")})
    outcomes = run(client, sink, bad, dataset)
    assert [o.status for o in outcomes] == ["failed", "updated"]
    assert "NOT_A_DIMENSION" in outcomes[0].detail


def test_throttling_stops_the_run(structure, dataset, sink):
    client = FakeClient(structure, {})
    client.fail_with = RateLimited("HTTP 429")
    second = replace(dataset, id="second")
    outcomes = run(client, sink, dataset, second)
    assert [o.status for o in outcomes] == ["failed", "skipped"]


def test_force_republishes(structure, dataset, sink):
    client = FakeClient(structure, {"2025M10": make_rows("2025M10")})
    run(client, sink, dataset)
    assert run(client, sink, dataset, force=True)[0].status == "updated"


def test_source_url_matches_key(structure):
    key, _ = build_key(structure.dimensions, {"FREQ": "Q", "EDITION": "2025M10"})
    assert key == "Q...2025M10"


def test_throttling_is_per_source(structure, dataset, sink):
    istat = FakeClient(structure, {})
    istat.fail_with = RateLimited("HTTP 429")
    ecb = FakeClient(structure, {"2025M10": make_rows("2025M10")})
    later_istat = replace(dataset, id="later_istat")
    wf = replace(dataset, id="wf", source="ecb")
    outcomes = Runner({"istat": istat, "ecb": ecb}, sink).run([dataset, later_istat, wf])
    assert [o.status for o in outcomes] == ["failed", "skipped", "updated"]
