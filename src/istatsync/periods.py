"""ISTAT editions and SDMX time periods."""

from __future__ import annotations

import re

import pandas as pd

_EDITION = re.compile(r"^(\d{4})M(\d{1,2})$")


def edition_key(edition: str) -> tuple[int, int]:
    """Sortable key for an ISTAT edition such as '2025M9' or '2025M10'.

    ISTAT does not zero-pad the month, so plain string comparison puts
    '2025M9' after '2025M10'. Unknown formats sort first.
    """
    m = _EDITION.match(edition or "")
    if not m:
        return (0, 0)
    return int(m.group(1)), int(m.group(2))


def latest_edition(series_per_edition: dict[str, int], min_share: float = 0.8) -> str | None:
    """Pick the newest edition that carries a complete set of series.

    ISTAT sometimes publishes an edition with only part of the series (a flash
    release, or one table refreshed before the others). Taking such an edition
    blindly would make series vanish from the output, so an edition is skipped
    if it has fewer than `min_share` of the series found in the best-covered one.
    """
    if not series_per_edition:
        return None
    best = max(series_per_edition.values())
    for edition in sorted(series_per_edition, key=edition_key, reverse=True):
        if series_per_edition[edition] >= min_share * best:
            return edition
    return None


def period_columns(periods: pd.Series) -> pd.DataFrame:
    """Calendar helper columns for SDMX periods ('2024', '2024-Q3', '2024-07')."""
    out = pd.DataFrame(index=periods.index)
    p = periods.astype(str)
    out["YEAR"] = p.str[:4].astype(int)

    quarterly = p.str.contains("-Q", regex=False)
    monthly = p.str.match(r"^\d{4}-\d{2}$")

    quarter = pd.Series(pd.NA, index=p.index, dtype="Int64")
    quarter[quarterly] = p[quarterly].str[-1].astype(int)
    month = pd.Series(pd.NA, index=p.index, dtype="Int64")
    month[monthly] = p[monthly].str[5:7].astype(int)
    quarter[monthly] = (month[monthly] - 1) // 3 + 1

    if quarterly.any() or monthly.any():
        out["QUARTER"] = quarter
    if monthly.any():
        out["MONTH"] = month

    # First day of the period: Tableau and Excel both handle real dates better
    # than '2024-Q3' strings.
    start_month = month.fillna((quarter.fillna(1) - 1) * 3 + 1).astype(int)
    out["DATE"] = pd.to_datetime({"year": out["YEAR"], "month": start_month, "day": 1}).dt.date
    return out
