"""Dataset definitions, loaded from datasets.toml."""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .sdmx import SOURCES

FOLDERS = {"A": "annuali", "Q": "trimestrali", "M": "mensili"}
_ID = re.compile(r"^[a-z0-9_]+$")


@dataclass(frozen=True)
class Dataset:
    id: str
    title: str
    dataflow: str
    frequency: str
    filter: dict[str, str | list[str]] = field(default_factory=dict)
    start: str | None = None
    # None = pick the latest complete edition when the dataflow has an
    # EDITION dimension. A value such as "2025M10" pins that edition.
    edition: str | None = None
    # Dimension whose values become separate Excel sheets (e.g. ADJUSTMENT).
    sheets_by: str | None = None
    # DATA_TYPE_AGGR codes (glob patterns allowed, e.g. "*_D_*") shown with a
    # minus sign in the Excel file, so that uses subtract from resources in
    # stacked charts. The CSV always keeps ISTAT's signs.
    negate: list[str] = field(default_factory=list)
    notes: str = ""
    # "istat" or "ecb" (Banca d'Italia financial accounts via the ECB).
    source: str = "istat"
    # Disabled datasets stay in the catalogue but are skipped by `run`
    # unless named explicitly.
    enabled: bool = True

    @property
    def folder(self) -> str:
        return FOLDERS[self.frequency]

    @property
    def fingerprint(self) -> str:
        """Changes whenever the definition changes, forcing a re-download."""
        # `enabled` and `notes` do not change the data, so they are left out.
        fields = {k: v for k, v in asdict(self).items() if k not in ("enabled", "notes")}
        payload = json.dumps(fields, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode()).hexdigest()[:12]


def load_catalog(path: Path) -> list[Dataset]:
    with path.open("rb") as fh:
        raw = tomllib.load(fh)

    defaults = raw.get("defaults", {})
    datasets: list[Dataset] = []
    seen: set[str] = set()
    for entry in raw.get("dataset", []):
        merged = {**defaults, **entry}
        ds = Dataset(**merged)
        if not _ID.match(ds.id):
            raise ValueError(f"dataset id {ds.id!r}: use lowercase letters, digits and _")
        if ds.id in seen:
            raise ValueError(f"duplicate dataset id {ds.id!r}")
        if ds.source not in SOURCES:
            raise ValueError(f"{ds.id}: source must be one of {sorted(SOURCES)}")
        if ds.frequency not in FOLDERS:
            raise ValueError(f"{ds.id}: frequency must be one of {sorted(FOLDERS)}")
        if ds.filter.get("FREQ", ds.frequency) != ds.frequency:
            raise ValueError(f"{ds.id}: filter FREQ disagrees with frequency")
        seen.add(ds.id)
        datasets.append(ds)
    return datasets
