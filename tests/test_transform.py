import io
from dataclasses import replace

import pandas as pd
import pytest
from conftest import make_rows

from istatsync.transform import _distinct_names, build_table, to_csv, to_excel, wide_frames


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
    assert list(wide.columns[4:]) == ["Impieghi - Spesa famiglie", "Impieghi - Imposte correnti"]
    assert len(wide) == 2


def test_negate_only_touches_excel(dataset, structure):
    ds = replace(dataset, negate=["D5_*"])
    t = build_table(make_rows("2025M10"), ds, structure, "2025M10")
    assert wide_frames(t)["dati grezzi"]["Impieghi - Imposte correnti"].iloc[0] == -50.0
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


def test_resources_and_uses_with_the_same_label_get_distinct_columns(dataset, structure):
    # Real case from QNA1_1: ISTAT labels D5_C_W0 and D5_D_W0 identically.
    labels = {**structure.labels, "DATA_TYPE_AGGR": {"D5_C_W0": "imposte", "D5_D_W0": "imposte"}}
    st = replace(structure, labels=labels)
    raw = make_rows("2025M10")
    raw["DATA_TYPE_AGGR"] = raw["DATA_TYPE_AGGR"].map(
        {"P31_D_W0_S14": "D5_C_W0", "D5_D_W0": "D5_D_W0"}
    )
    t = build_table(raw, dataset, st, None)
    assert set(t.long["FLUSSO"]) == {"risorse", "impieghi"}
    wide = wide_frames(t)["dati grezzi"]
    assert list(wide.columns[4:]) == ["Risorse - imposte", "Impieghi - imposte"]


def test_remaining_label_clashes_get_codes(dataset, structure):
    labels = {**structure.labels, "DATA_TYPE_AGGR": {"AAA": "uguale", "BBB": "uguale"}}
    st = replace(structure, labels=labels)
    raw = make_rows("2025M10")
    raw["DATA_TYPE_AGGR"] = raw["DATA_TYPE_AGGR"].map({"P31_D_W0_S14": "AAA", "D5_D_W0": "BBB"})
    t = build_table(raw, dataset, st, None)
    assert "FLUSSO" not in t.long
    assert list(wide_frames(t)["dati grezzi"].columns[4:]) == ["uguale [AAA]", "uguale [BBB]"]


def test_series_follow_codelist_order(dataset, structure):
    t = build_table(make_rows("2025M10"), dataset, structure, None)
    # The fixture codelist lists P31_D_W0_S14 before D5_D_W0.
    assert t.long["DATA_TYPE_AGGR"].iloc[0] == "P31_D_W0_S14"


def test_negated_zero_is_plain_zero(dataset, structure):
    raw = make_rows("2025M10")
    raw["OBS_VALUE"] = 0.0
    t = build_table(raw, replace(dataset, negate=["*"]), structure, None)
    values = wide_frames(t)["dati grezzi"].iloc[:, 4:].to_numpy().ravel()
    assert all(str(v) == "0.0" for v in values)


def test_sheet_names_keep_the_part_that_differs():
    labels = [
        f"indice dei prezzi al consumo per l'intera collettività (base {y}=100) - dati mensili"
        for y in (1995, 2010, 2015, 2025)
    ]
    names = _distinct_names(labels)
    assert names[labels[0]] == "(base 1995=100)"
    assert len(set(names.values())) == 4


def test_short_distinct_labels_are_left_alone():
    assert _distinct_names(["dati grezzi", "dati destagionalizzati"]) == {
        "dati grezzi": "dati grezzi",
        "dati destagionalizzati": "dati destagionalizzati",
    }
