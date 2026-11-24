"""Turn raw SDMX rows into the published tables (long CSV and wide Excel)."""

from __future__ import annotations

import fnmatch
import hashlib
import io
import re
from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

from .catalog import Dataset
from .periods import period_columns
from .sdmx import Structure

# Dimensions that never describe *what* a series is, so they are left out of
# column headers.
_NOT_DESCRIPTIVE = {"FREQ", "EDITION"}
_ATTRIBUTES = ["OBS_STATUS", "UNIT_MEAS", "UNIT_MULT", "BASE_PER"]


@dataclass
class Table:
    dataset: Dataset
    structure: Structure
    edition: str | None
    long: pd.DataFrame
    series_dims: list[str]

    @property
    def first_period(self) -> str:
        return str(self.long["TIME_PERIOD"].min())

    @property
    def last_period(self) -> str:
        return str(self.long["TIME_PERIOD"].max())

    @property
    def n_series(self) -> int:
        if not self.series_dims:
            return 1
        return int(self.long[self.series_dims].drop_duplicates().shape[0])

    def content_hash(self) -> str:
        """Hash of codes, periods and values only, so that a relabelled code
        list or a different download time does not count as new data."""
        dims = [d for d in self.structure.dimensions if d in self.long.columns]
        cols = [*dims, "TIME_PERIOD", "OBS_VALUE"]
        canonical = self.long[cols].sort_values(cols[:-1]).to_csv(index=False)
        return hashlib.sha256(canonical.encode()).hexdigest()


def build_table(
    raw: pd.DataFrame, dataset: Dataset, structure: Structure, edition: str | None
) -> Table:
    if raw.empty:
        raise ValueError(f"{dataset.id}: ISTAT returned no observations")

    dims = [d for d in structure.dimensions if d in raw.columns]
    df = raw.dropna(subset=["OBS_VALUE"]).copy()

    duplicated = df.duplicated(subset=[*dims, "TIME_PERIOD"], keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, [*dims, "TIME_PERIOD"]].head(3).to_dict("records")
        raise ValueError(f"{dataset.id}: duplicate observations, e.g. {sample}")

    for dim in dims:
        df[f"{dim}_LABEL"] = [structure.label(dim, code) for code in df[dim]]

    df = pd.concat([df, period_columns(df["TIME_PERIOD"])], axis=1)

    series_dims = [d for d in dims if d not in _NOT_DESCRIPTIVE and df[d].nunique() > 1]
    ordered = [c for d in dims for c in (d, f"{d}_LABEL")]
    time_cols = [c for c in ("TIME_PERIOD", "DATE", "YEAR", "QUARTER", "MONTH") if c in df]
    attrs = [c for c in _ATTRIBUTES if c in df]
    df = df[[*ordered, *time_cols, "OBS_VALUE", *attrs]]
    df = df.sort_values([*series_dims, "TIME_PERIOD"]).reset_index(drop=True)
    return Table(dataset, structure, edition, df, series_dims)


def to_csv(table: Table) -> bytes:
    # utf-8-sig so that Excel opens the accents correctly on double-click.
    return table.long.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def wide_frames(table: Table) -> dict[str, pd.DataFrame]:
    """One wide frame per sheet: periods down, series across."""
    ds = table.dataset
    long = table.long.copy()
    if ds.negate and "DATA_TYPE_AGGR" in long:
        flip = long["DATA_TYPE_AGGR"].map(
            lambda code: any(fnmatch.fnmatchcase(code, pat) for pat in ds.negate)
        )
        long.loc[flip, "OBS_VALUE"] = -long.loc[flip, "OBS_VALUE"]

    split = ds.sheets_by if ds.sheets_by in table.series_dims else None
    header_dims = [d for d in table.series_dims if d != split]

    if header_dims:
        long["_series"] = _join_labels(long, header_dims)
    else:
        long["_series"] = ds.title

    time_cols = [c for c in ("TIME_PERIOD", "DATE", "YEAR", "QUARTER", "MONTH") if c in long]
    groups: Iterable = long.groupby(f"{split}_LABEL", sort=False) if split else [("Dati", long)]

    frames = {}
    for name, part in groups:
        # Columns keep the order of the long table (sorted by codes), which
        # follows the ISTAT hierarchy, rather than alphabetical label order.
        order = list(dict.fromkeys(part["_series"]))
        wide = part.pivot(index=time_cols, columns="_series", values="OBS_VALUE")
        wide = wide[order].reset_index()
        wide.columns.name = None
        frames[_sheet_name(str(name), frames)] = wide
    return frames


def series_catalogue(table: Table) -> pd.DataFrame:
    cols = [c for d in table.series_dims for c in (d, f"{d}_LABEL")]
    if not cols:
        return pd.DataFrame({"serie": [table.dataset.title]})
    out = table.long[cols].drop_duplicates().reset_index(drop=True)
    return out


def to_excel(table: Table, downloaded_at: str, source_url: str) -> bytes:
    ds = table.dataset
    info = pd.DataFrame(
        [
            ("dataset", ds.id),
            ("titolo", ds.title),
            ("dataflow", ds.dataflow),
            ("dataflow_nome", table.structure.name),
            ("edizione", table.edition or "-"),
            ("primo_periodo", table.first_period),
            ("ultimo_periodo", table.last_period),
            ("numero_serie", table.n_series),
            ("filtro", ", ".join(f"{k}={_fmt(v)}" for k, v in ds.filter.items()) or "-"),
            ("segno_invertito", ", ".join(ds.negate) or "-"),
            ("scaricato_il", downloaded_at),
            ("fonte", source_url),
            ("note", ds.notes or "-"),
        ],
        columns=["campo", "valore"],
    )

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        info.to_excel(xl, sheet_name="Info", index=False)
        _style(xl.sheets["Info"], widths=[18, 90], wrap=True)

        catalogue = series_catalogue(table)
        catalogue.to_excel(xl, sheet_name="Serie", index=False)
        _style(xl.sheets["Serie"], widths=[16, 60] * (len(catalogue.columns) // 2 or 1))

        for sheet, frame in wide_frames(table).items():
            frame.to_excel(xl, sheet_name=sheet, index=False)
            n_time = sum(c in frame for c in ("TIME_PERIOD", "DATE", "YEAR", "QUARTER", "MONTH"))
            ws = xl.sheets[sheet]
            _style(ws, widths=[12] * n_time + [22] * (frame.shape[1] - n_time), wrap=True)
            ws.freeze_panes = ws.cell(row=2, column=n_time + 1)
            for row in ws.iter_rows(min_row=2, min_col=2, max_col=2):
                row[0].number_format = "yyyy-mm-dd"
    return buf.getvalue()


def _join_labels(df: pd.DataFrame, dims: list[str]) -> pd.Series:
    parts = [df[f"{d}_LABEL"].astype(str) for d in dims]
    joined = parts[0]
    for p in parts[1:]:
        joined = joined + " | " + p
    return joined


def _sheet_name(name: str, taken: dict) -> str:
    clean = re.sub(r"[\[\]:*?/\\]", " ", name).strip()[:31] or "Dati"
    candidate, n = clean, 2
    while candidate in taken or candidate in ("Info", "Serie"):
        suffix = f" ({n})"
        candidate = clean[: 31 - len(suffix)] + suffix
        n += 1
    return candidate


def _style(ws, widths: list[int], wrap: bool = False) -> None:
    for i, width in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = width
    for cell in ws[1]:
        cell.font = Font(bold=True)
        if wrap:
            cell.alignment = Alignment(wrap_text=True, vertical="top")


def _fmt(value) -> str:
    return "+".join(value) if isinstance(value, list) else str(value)
