import datetime as dt

import pandas as pd

from istatsync.periods import edition_key, latest_edition, period_columns


def test_unpadded_months_sort_numerically():
    editions = ["2025M10", "2025M9", "2024M12", "2025M1"]
    assert sorted(editions, key=edition_key) == ["2024M12", "2025M1", "2025M9", "2025M10"]


def test_garbage_edition_sorts_first():
    assert edition_key("provvisoria") < edition_key("1990M1")


def test_latest_edition_prefers_newest():
    assert latest_edition({"2025M9": 14, "2025M10": 14}) == "2025M10"


def test_latest_edition_skips_partial_release():
    # The October edition only carries 3 of the 14 series: keep September.
    assert latest_edition({"2025M9": 14, "2025M10": 3}) == "2025M9"


def test_latest_edition_empty():
    assert latest_edition({}) is None


def test_period_columns_quarterly():
    out = period_columns(pd.Series(["2024-Q1", "2024-Q4"]))
    assert out["YEAR"].tolist() == [2024, 2024]
    assert out["QUARTER"].tolist() == [1, 4]
    assert out["DATE"].tolist() == [dt.date(2024, 1, 1), dt.date(2024, 10, 1)]
    assert "MONTH" not in out


def test_period_columns_monthly():
    out = period_columns(pd.Series(["2026-01", "2026-08"]))
    assert out["MONTH"].tolist() == [1, 8]
    assert out["QUARTER"].tolist() == [1, 3]
    assert out["DATE"].tolist() == [dt.date(2026, 1, 1), dt.date(2026, 8, 1)]


def test_period_columns_annual():
    out = period_columns(pd.Series(["1995", "2024"]))
    assert list(out.columns) == ["YEAR", "DATE"]
    assert out["DATE"].tolist() == [dt.date(1995, 1, 1), dt.date(2024, 1, 1)]
