from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from istatsync.catalog import Dataset
from istatsync.sdmx import Structure

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def structure() -> Structure:
    return Structure(
        dataflow="TEST_FLOW",
        name="Spesa per i consumi finali delle famiglie",
        dimensions=["FREQ", "DATA_TYPE_AGGR", "ADJUSTMENT", "EDITION"],
        labels={
            "DATA_TYPE_AGGR": {"P31_D_W0_S14": "Spesa famiglie", "D5_D_W0": "Imposte correnti"},
            "ADJUSTMENT": {"N": "dati grezzi", "Y": "dati destagionalizzati"},
        },
    )


@pytest.fixture
def dataset() -> Dataset:
    return Dataset(
        id="consumi_test",
        title="Consumi test",
        dataflow="TEST_FLOW",
        frequency="Q",
        filter={"DATA_TYPE_AGGR": ["P31_D_W0_S14", "D5_D_W0"]},
        sheets_by="ADJUSTMENT",
    )


def make_rows(edition: str, periods=("2024-Q1", "2024-Q2"), bump: float = 0.0) -> pd.DataFrame:
    rows = []
    for aggr, base in (("P31_D_W0_S14", 300.0), ("D5_D_W0", 50.0)):
        for adj in ("N", "Y"):
            for i, period in enumerate(periods):
                rows.append(
                    {
                        "FREQ": "Q",
                        "DATA_TYPE_AGGR": aggr,
                        "ADJUSTMENT": adj,
                        "EDITION": edition,
                        "TIME_PERIOD": period,
                        "OBS_VALUE": base + i + bump,
                        "UNIT_MEAS": "EURO",
                        "UNIT_MULT": "6",
                    }
                )
    return pd.DataFrame(rows)


class FakeResponse:
    def __init__(self, status: int, content: bytes = b"", url: str = "http://x"):
        self.status_code = status
        self.content = content
        self.text = content.decode("utf-8", "replace")
        self.url = url


class FakeSession:
    """Returns queued responses (or raises queued exceptions) in order."""

    def __init__(self, *responses):
        self.queue = list(responses)
        self.calls: list[tuple[str, dict | None]] = []
        self.headers: dict = {}

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params))
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item
