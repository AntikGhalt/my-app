import io
import shutil
import subprocess
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import load_workbook

from istatsync.sinks import LocalSink
from istatsync.tables import TABLES, _default_semesters, build_tables, build_workbook, read_inputs

QUARTERS = [f"{y}-Q{q}" for y in (2024, 2025) for q in (1, 2, 3, 4)] + ["2026-Q1", "2026-Q2"]
SPEC = next(t for t in TABLES if not t.annual)


def fake_raw() -> pd.DataFrame:
    """Simple numbers so expected results can be worked out by hand."""
    n = len(QUARTERS)
    cols = {i.col: [0.0] * n for i in SPEC.inputs}
    cols["D1_C"] = [100.0 + i for i in range(n)]
    cols["B6G"] = [100.0 + i for i in range(n)]  # all income is wages
    cols["C_V"] = [80.0] * n
    cols["C_L"] = [80.0] * (n - 2) + [40.0, 40.0]  # prices double in 2026
    cols["ULA"] = [10.0, 30.0] * (n // 2)
    cols["POP"] = [1000.0] * n
    cols["WF_A"] = [500.0 + 10 * i for i in range(n)]
    cols["WF_L"] = [100.0] * n
    return pd.DataFrame(cols, index=QUARTERS)


def test_default_selection_follows_last_year_layout():
    semesters = [f"{y}-S{h}" for y in range(2005, 2027) for h in (1, 2)][:-1]
    assert _default_semesters(semesters) == ["2007-S1", "2019-S1", "2025-S1", "2026-S1"]
    assert _default_semesters(["2025-S1", "2025-S2"]) == ["2025-S1", "2025-S2"] + ["2025-S2"] * 2


def test_read_inputs_picks_one_series_per_column():
    rows = []
    for item in SPEC.inputs:
        for period in ("2025-Q1", "2025-Q2"):
            rows.append(
                {**item.filter, "TIME_PERIOD": period, "OBS_VALUE": 1.0, "_ds": item.dataset}
            )
    df = pd.DataFrame(rows)
    raw = read_inputs(SPEC, lambda ds: df[df["_ds"] == ds].drop(columns="_ds"))
    assert list(raw.index) == ["2025-Q1", "2025-Q2"]
    assert set(raw.columns) == {i.col for i in SPEC.inputs}


def test_ambiguous_filter_is_an_error():
    df = pd.DataFrame(
        {"DATA_TYPE_AGGR": ["D1_C_W0"] * 2, "TIME_PERIOD": ["2025-Q1"] * 2, "OBS_VALUE": [1, 2]}
    )
    with pytest.raises(ValueError):
        read_inputs(SPEC, lambda ds: df)


def test_workbook_structure():
    wb = load_workbook(io.BytesIO(build_workbook(SPEC, fake_raw(), {})))
    assert wb.sheetnames[:4] == ["Tabella", "Serie", "Dati", "Note"]
    ws = wb["Tabella"]
    assert ws["B3"].value == "Semestre"
    assert [ws[f"{c}5"].value for c in "CDEF"] == ["2024-S2", "2025-S1", "2025-S2", "2026-S1"]
    assert str(ws["C9"].value).startswith("=IFERROR(")
    assert "lista_Semestre" in wb.defined_names


@pytest.mark.skipif(shutil.which("soffice") is None, reason="LibreOffice not installed")
def test_formulas_compute_in_a_spreadsheet(tmp_path: Path):
    wb = load_workbook(io.BytesIO(build_workbook(SPEC, fake_raw(), {})))
    ws = wb["Tabella"]
    ws["B3"] = "Trimestre"
    for col, key in zip("CDEF", ["2024-T1", "2025-T1", "2025-T4", "2026-T2"], strict=True):
        ws[f"{col}5"] = key
    src = tmp_path / "t.xlsx"
    wb.save(src)
    subprocess.run(
        [
            "soffice",
            "--headless",
            "--calc",
            "--convert-to",
            "xlsx",
            "--outdir",
            str(tmp_path / "o"),
            str(src),
        ],
        check=True,
        capture_output=True,
        timeout=180,
    )
    values = load_workbook(tmp_path / "o" / "t.xlsx", data_only=True)["Tabella"]
    rows = {values.cell(r, 1).value: r for r in range(1, values.max_row + 1)}

    # Nominal wages in billions: quarters 1, 5, 8, 10 of the fake series.
    wages = [values[f"{c}8"].value for c in "CDEF"]
    assert wages == pytest.approx([0.100, 0.104, 0.107, 0.109])
    # Net financial wealth at the end of 2026-Q2: 500 + 90 - 100.
    wf_row = next(
        r for lab, r in rows.items() if lab and lab.startswith("Ricchezza finanziaria netta")
    )
    assert values[f"F{wf_row}"].value == pytest.approx(0.49)
    # Prices double in 2026, so the 2024 deflator is half the 2026-T2 one.
    defl_row = rows["Deflatore dei consumi (ultimo periodo = 100)"]
    assert values[f"C{defl_row}"].value == pytest.approx(50.0)
    # Wages per ULA in a single quarter: 109 * 1000 / 30.
    ula_row = rows["Redditi da lavoro dipendente per ULA dipendente"]
    assert values[f"F{ula_row}"].value == pytest.approx(109 * 1000 / 30)
    # The check row has no percentage changes.
    check_row = rows["Controllo: componenti meno reddito disponibile"]
    assert values[f"G{check_row}"].value is None


def test_tables_rebuild_only_when_inputs_change(tmp_path):
    sink = LocalSink(tmp_path)
    raw = fake_raw()
    manifest = {}
    for item in SPEC.inputs:
        frame = pd.DataFrame(
            [{**item.filter, "TIME_PERIOD": p, "OBS_VALUE": raw.at[p, item.col]} for p in raw.index]
        )
        path = f"trimestrali/{item.dataset}__{item.col}.csv"
        sink.write(path, frame.to_csv(index=False).encode("utf-8-sig"))
        entry = manifest.setdefault(
            item.dataset, {"files": [], "sha256": "a", "edition": "2026M10"}
        )
        entry["files"].append(path)

    # One CSV per dataset is what build_tables expects: merge per dataset.
    for ds, entry in manifest.items():
        frames = [
            pd.read_csv(io.BytesIO(sink.read(p)), encoding="utf-8-sig", dtype=str)
            for p in entry["files"]
        ]
        merged = f"trimestrali/{ds}.csv"
        sink.write(merged, pd.concat(frames).to_csv(index=False).encode("utf-8-sig"))
        entry["files"] = [merged]

    first = build_tables(sink, manifest)
    assert first == ["tabelle/tabella_reddito_trimestrale.xlsx"]  # annual inputs missing
    assert build_tables(sink, manifest) == []
    manifest["ula_trimestrali"]["sha256"] = "b"
    assert build_tables(sink, manifest) == first
    assert any((tmp_path / "archivio" / "tabelle").iterdir())
