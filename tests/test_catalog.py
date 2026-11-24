from pathlib import Path

import pytest

from istatsync.catalog import load_catalog
from istatsync.sinks import LocalSink

ROOT = Path(__file__).parents[1]


def test_shipped_catalogue_is_valid():
    datasets = load_catalog(ROOT / "datasets.toml")
    assert datasets
    assert {d.frequency for d in datasets} == {"A", "Q", "M"}


def write(tmp_path, body: str) -> Path:
    p = tmp_path / "c.toml"
    p.write_text(body)
    return p


def test_defaults_are_merged(tmp_path):
    p = write(
        tmp_path,
        """
[defaults]
start = "1995"

[[dataset]]
id = "a"
title = "A"
dataflow = "X"
frequency = "Q"
""",
    )
    assert load_catalog(p)[0].start == "1995"


@pytest.mark.parametrize(
    "body, message",
    [
        ('id = "Bad-Id"\ntitle="t"\ndataflow="X"\nfrequency="Q"', "lowercase"),
        ('id = "a"\ntitle="t"\ndataflow="X"\nfrequency="W"', "frequency"),
        ('id = "a"\ntitle="t"\ndataflow="X"\nfrequency="Q"\nfilter = { FREQ = "A" }', "FREQ"),
    ],
)
def test_invalid_entries(tmp_path, body, message):
    with pytest.raises(ValueError, match=message):
        load_catalog(write(tmp_path, "[[dataset]]\n" + body))


def test_duplicate_ids(tmp_path):
    entry = '[[dataset]]\nid="a"\ntitle="t"\ndataflow="X"\nfrequency="Q"\n'
    with pytest.raises(ValueError, match="duplicate"):
        load_catalog(write(tmp_path, entry * 2))


def test_fingerprint_tracks_definition(dataset):
    from dataclasses import replace

    assert dataset.fingerprint == replace(dataset).fingerprint
    assert dataset.fingerprint != replace(dataset, start="2000").fingerprint


def test_local_sink_refuses_paths_outside_root(tmp_path):
    with pytest.raises(ValueError):
        LocalSink(tmp_path).write("../evil.txt", b"x")
