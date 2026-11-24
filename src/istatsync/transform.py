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

# National-accounts codes carry the ESA accounting entry: D5_C_W0 is a
# resource (credit), D5_D_W0 a use (debit), B6G_B_W0 a balancing item. ISTAT
# gives both sides the same label, so the side has to be spelled out.
_ENTRY = re.compile(r"_([CDB])_")
_ENTRY_LABEL = {"C": "risorse", "D": "impieghi", "B": "saldi"}


def accounting_entry(code: str) -> str:
    m = _ENTRY.search(code)
    return _ENTRY_LABEL[m.group(1)] if m else ""


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
    if "DATA_TYPE_AGGR" in series_dims:
        entry = df["DATA_TYPE_AGGR"].map(accounting_entry)
        if entry.astype(bool).any():
            df["FLUSSO"] = entry
            ordered.insert(ordered.index("DATA_TYPE_AGGR"), "FLUSSO")
    attrs = [c for c in _ATTRIBUTES if c in df]
    df = df[[*ordered, *time_cols, "OBS_VALUE", *attrs]]

    # Order series as ISTAT lists the codes (the account sequence, the COICOP
    # tree), not alphabetically; unknown codes go last.
    sort_keys = []
    for dim in series_dims:
        position = {code: i for i, code in enumerate(structure.labels.get(dim, {}))}
        df[f"_pos_{dim}"] = df[dim].map(position).fillna(len(position))
        sort_keys += [f"_pos_{dim}", dim]
    df = df.sort_values([*sort_keys, "TIME_PERIOD"]).reset_index(drop=True)
    df = df.drop(columns=[c for c in df if c.startswith("_pos_")])
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
        # "+ 0.0" turns -0.0 into 0.0, which Excel would otherwise show as "-0".
        long.loc[flip, "OBS_VALUE"] = -long.loc[flip, "OBS_VALUE"] + 0.0

    split = ds.sheets_by if ds.sheets_by in table.series_dims else None
    header_dims = [d for d in table.series_dims if d != split]

    long["_series"] = _series_headers(long, header_dims) if header_dims else ds.title

    time_cols = [c for c in ("TIME_PERIOD", "DATE", "YEAR", "QUARTER", "MONTH") if c in long]
    if split:
        names = _distinct_names(list(dict.fromkeys(long[f"{split}_LABEL"])))
        groups: Iterable = (
            (names[label], part) for label, part in long.groupby(f"{split}_LABEL", sort=False)
        )
    else:
        groups = [("Dati", long)]

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
    if "FLUSSO" in table.long:
        cols.insert(cols.index("DATA_TYPE_AGGR"), "FLUSSO")
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


def _series_headers(df: pd.DataFrame, dims: list[str]) -> pd.Series:
    """Column header per series: labels joined with ' | '. Codes are appended
    where two different series would otherwise get the same header."""
    cols = [*dims, *(f"{d}_LABEL" for d in dims)]
    if "FLUSSO" in df:
        cols.append("FLUSSO")
    headers: dict[tuple, str] = {}
    for row in df[cols].drop_duplicates().to_dict("records"):
        parts = []
        for dim in dims:
            label = str(row[f"{dim}_LABEL"])
            if dim == "DATA_TYPE_AGGR" and row.get("FLUSSO"):
                label = f"{row['FLUSSO'].capitalize()} - {label}"
            parts.append(label)
        headers[tuple(row[d] for d in dims)] = " | ".join(parts)

    counts: dict[str, int] = {}
    for header in headers.values():
        counts[header] = counts.get(header, 0) + 1
    for codes, header in headers.items():
        if counts[header] > 1:
            headers[codes] = f"{header} [{'.'.join(codes)}]"

    keys = df[dims].itertuples(index=False, name=None)
    return pd.Series([headers[k] for k in keys], index=df.index)


def _distinct_names(labels: list[str], limit: int = 31) -> dict[str, str]:
    """Short sheet names that still tell the labels apart.

    ISTAT labels often differ only at the end ("indice dei prezzi ... (base
    1995=100) - dati mensili" vs "(base 2015=100)"), which Excel's 31-character
    limit would cut off. In that case the words shared by every label are
    dropped, keeping one word of context before the part that differs.
    """
    if len({label[:limit] for label in labels}) == len(labels):
        return {label: label for label in labels}
    words = [label.split() for label in labels]
    head = 0
    while all(len(w) > head + 1 for w in words) and len({w[head] for w in words}) == 1:
        head += 1
    tail = 0
    while all(len(w) > head + tail + 1 for w in words) and len({w[-1 - tail] for w in words}) == 1:
        tail += 1
    start = max(head - 1, 0)
    return {
        label: " ".join(w[start : len(w) - tail]) for label, w in zip(labels, words, strict=True)
    }


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
