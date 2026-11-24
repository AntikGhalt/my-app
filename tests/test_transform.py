import io
from dataclasses import replace

import pandas as pd
import pytest
from conftest import make_rows

from istatsync.transform import build_table, to_csv, to_excel, wide_frames


def test_long_table_has_labels_and_calendar(dataset, structure):
    t = build_table(make_rows("2025M10"), dataset, structure, "2025M10")
    row = t.long.iloc[0]
    assert row["DATA_TYPE_AGGR_LABEL"] in {"Spesa famiglie", "Imposte correnti"}
    assert {"YEAR", "QUARTER", "DATE"} <= set(t.long.columns)
    assert t.series_dims == ["DATA_TYPE_AGGR", "ADJUSTMENT"]
    assert t.n_series == 4
    assert (t.first_period, t.last_period) == ("2024-Q1", "2024-Q2")


def test_missing_values_are_dropped(dataset, structure):
    raw = make_rows("2025M10")
    raw.loc[0, "OBS_VALUE"] = None
    assert len(build_table(raw, dataset, structure, None).long) == len(raw) - 1


def test_duplicates_are_an_error_not_silently_dropped(dataset, structure):
    raw = make_rows("2025M10")
    raw = pd.concat([raw, raw.iloc[[0]]])
    with pytest.raises(ValueError, match="duplicate"):
        build_table(raw, dataset, structure, None)


def test_empty_download_is_an_error(dataset, structure):
    with pytest.raises(ValueError, match="no observations"):
        build_table(make_rows("x").iloc[0:0], dataset, structure, None)


def test_wide_split_by_sheet(dataset, structure):
    t = build_table(make_rows("2025M10"), dataset, structure, "2025M10")
    frames = wide_frames(t)
    assert list(frames) == ["dati grezzi", "dati destagionalizzati"]
    wide = frames["dati grezzi"]
    assert list(wide.columns[:4]) == ["TIME_PERIOD", "DATE", "YEAR", "QUARTER"]
    assert set(wide.columns[4:]) == {"Spesa famiglie", "Imposte correnti"}
    assert len(wide) == 2


def test_negate_only_touches_excel(dataset, structure):
    ds = replace(dataset, negate=["D5_*"])
    t = build_table(make_rows("2025M10"), ds, structure, "2025M10")
    assert wide_frames(t)["dati grezzi"]["Imposte correnti"].iloc[0] == -50.0
    assert (t.long["OBS_VALUE"] > 0).all()


def test_hash_ignores_labels_but_not_values(dataset, structure):
    raw = make_rows("2025M10")
    a = build_table(raw, dataset, structure, "2025M10").content_hash()
    relabelled = replace(structure, labels={})
    assert build_table(raw, dataset, relabelled, "2025M10").content_hash() == a
    assert build_table(make_rows("2025M10", bump=0.1), dataset, structure, None).content_hash() != a


def test_excel_and_csv_are_readable(dataset, structure):
    t = build_table(make_rows("2025M10"), dataset, structure, "2025M10")
    book = pd.read_excel(
        io.BytesIO(to_excel(t, "2025-11-01T08:00:00+01:00", "https://x")), sheet_name=None
    )
    assert list(book) == ["Info", "Serie", "dati grezzi", "dati destagionalizzati"]
    info = dict(zip(book["Info"]["campo"], book["Info"]["valore"], strict=True))
    assert info["edizione"] == "2025M10"
    assert info["ultimo_periodo"] == "2024-Q2"

    csv = pd.read_csv(io.BytesIO(to_csv(t)), encoding="utf-8-sig")
    assert len(csv) == len(t.long)
    assert csv.columns[0] == "FREQ"
