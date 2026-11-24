from dataclasses import replace

import pytest
import requests
from conftest import FIXTURES, FakeResponse, FakeSession

from istatsync.sdmx import (
    SOURCES,
    RateLimited,
    RequestPacer,
    SdmxClient,
    SdmxError,
    Structure,
    build_key,
    parse_structure,
    read_sdmx_csv,
)
from istatsync.sinks import LocalSink

DIMS = ["FREQ", "REF_AREA", "DATA_TYPE_AGGR", "ADJUSTMENT", "EDITION"]

CSV = (
    b"DATAFLOW,FREQ,REF_AREA,DATA_TYPE_AGGR,ADJUSTMENT,EDITION,TIME_PERIOD,OBS_VALUE,OBS_STATUS,"
    b"NOTE_DS,UNIT_MULT\n"
    b"IT1:X(1.0),Q,IT,A,N,2026M9,2026-Q1,321932,,,6\n"
    b"IT1:X(1.0),Q,IT,A,N,2026M9,2026-Q2,,,,6\n"
    b"IT1:X(1.0),Q,IT,B,N,2026M9,2026-Q1,10.5,,,6\n"
)


TEST_SOURCE = replace(SOURCES["istat"], base_url="https://example.org/rest", min_interval=0)


def client(*responses, source=TEST_SOURCE, **kw):
    return SdmxClient(source, session=FakeSession(*responses), sleep=lambda s: None, **kw)


# -- keys --------------------------------------------------------------------


def test_key_is_positional_with_empty_wildcards():
    key, local = build_key(DIMS, {"FREQ": "Q", "DATA_TYPE_AGGR": ["A", "B"]})
    assert key == "Q..A+B.."
    assert local == {}


def test_key_rejects_unknown_dimension():
    with pytest.raises(ValueError, match="COICOP"):
        build_key(DIMS, {"COICOP": "CP01"})


def test_long_key_falls_back_to_local_filter():
    many = [f"CODE{i:03d}" for i in range(60)]
    key, local = build_key(DIMS, {"FREQ": "Q", "DATA_TYPE_AGGR": many}, base_len=100)
    assert key == "Q...."
    assert local == {"DATA_TYPE_AGGR": many}


# -- pacing ------------------------------------------------------------------


def test_pacer_spaces_requests():
    now = [0.0]
    slept = []

    def sleep(s):
        slept.append(s)
        now[0] += s

    pacer = RequestPacer(13, clock=lambda: now[0], sleep=sleep)
    pacer.wait()
    now[0] += 5
    pacer.wait()
    assert slept == [pytest.approx(8)]


# -- HTTP behaviour ------------------------------------------------------------


def test_data_parses_csv_and_drops_notes():
    c = client(FakeResponse(200, CSV))
    df = c.data("X", DIMS, {"FREQ": "Q"})
    assert "DATAFLOW" not in df and "NOTE_DS" not in df
    assert df["OBS_VALUE"].isna().sum() == 1
    assert df.loc[2, "OBS_VALUE"] == 10.5
    assert df["EDITION"].iloc[0] == "2026M9"


def test_data_applies_local_filter_for_long_keys():
    many = ["A"] + [f"Z{i:03d}" for i in range(80)]
    c = client(FakeResponse(200, CSV))
    df = c.data("X", DIMS, {"DATA_TYPE_AGGR": many})
    assert set(df["DATA_TYPE_AGGR"]) == {"A"}
    url, _ = c.session.calls[0]
    assert "Z001" not in url


def test_404_means_no_data():
    c = client(FakeResponse(404, b"NoRecordsFound"))
    assert c.data("X", DIMS, {}).empty


@pytest.mark.parametrize("status", [403, 429])
def test_throttling_stops_immediately(status):
    c = client(FakeResponse(status), FakeResponse(200, CSV))
    with pytest.raises(RateLimited):
        c.data("X", DIMS, {})
    assert len(c.session.calls) == 1


def test_server_errors_are_retried():
    c = client(FakeResponse(503), requests.ConnectionError("reset"), FakeResponse(200, CSV))
    assert len(c.data("X", DIMS, {})) == 3
    assert c.requests_made == 3


def test_gives_up_after_max_retries():
    c = client(FakeResponse(503), FakeResponse(503), FakeResponse(503), max_retries=3)
    with pytest.raises(SdmxError, match="503"):
        c.data("X", DIMS, {})


def test_bad_request_is_not_retried():
    c = client(FakeResponse(400, b"Semantic error"))
    with pytest.raises(SdmxError, match="400"):
        c.data("X", DIMS, {})


# -- structure -----------------------------------------------------------------


def test_parse_structure_orders_dimensions_and_prefers_italian():
    s = parse_structure((FIXTURES / "structure.xml").read_bytes(), "TEST_FLOW")
    assert s.name == "Spesa per i consumi finali delle famiglie"
    assert s.dimensions == ["FREQ", "DATA_TYPE_AGGR", "ADJUSTMENT", "EDITION"]
    assert s.label("DATA_TYPE_AGGR", "P31_D_W0_S14") == "Spesa delle famiglie residenti"
    assert s.label("DATA_TYPE_AGGR", "D5_D_W0") == "Current taxes"  # no Italian name
    assert s.label("EDITION", "2025M10") == "2025M10"  # no codelist


def test_structure_is_cached_in_the_sink(tmp_path):
    xml = (FIXTURES / "structure.xml").read_bytes()
    sink = LocalSink(tmp_path)
    client(FakeResponse(200, xml), cache=sink).structure("TEST_FLOW")
    assert (tmp_path / ".cache/structure_TEST_FLOW.json").is_file()
    second = client(cache=sink)  # no responses queued: must not hit the network
    assert second.structure("TEST_FLOW").dimensions[0] == "FREQ"


def test_stale_cache_is_refreshed(tmp_path, structure):
    sink = LocalSink(tmp_path)
    sink.write(".cache/structure_TEST_FLOW.json", structure.to_json().encode())  # fetched_at=0
    xml = (FIXTURES / "structure.xml").read_bytes()
    c = client(FakeResponse(200, xml), cache=sink)
    fresh = c.structure("TEST_FLOW")
    assert c.requests_made == 1
    assert fresh.fetched_at > 0
    assert fresh.dimensions == ["FREQ", "DATA_TYPE_AGGR", "ADJUSTMENT", "EDITION"]


def test_structure_json_roundtrip(structure):
    assert Structure.from_json(structure.to_json()) == structure


def test_read_sdmx_csv_keeps_codes_as_text():
    df = read_sdmx_csv(b"DATAFLOW,REF_AREA,TIME_PERIOD,OBS_VALUE\nX,001,2024,1\n")
    assert df["REF_AREA"].iloc[0] == "001"


def test_ecb_urls_and_csv_layout():
    ecb = replace(SOURCES["ecb"], min_interval=0)
    body = (
        b"KEY,FREQ,REF_AREA,ACCOUNTING_ENTRY,TIME_PERIOD,OBS_VALUE,TITLE\n"
        b"QSA.Q.IT.A,Q,IT,A,2026-Q2,6608813,Italy\n"
    )
    c = client(FakeResponse(200, body), source=ecb)
    df = c.data("QSA", ["FREQ", "REF_AREA", "ACCOUNTING_ENTRY"], {"REF_AREA": "IT"})
    url, _ = c.session.calls[0]
    assert url == "https://data-api.ecb.europa.eu/service/data/QSA/.IT."
    assert "KEY" not in df and df["OBS_VALUE"].iloc[0] == 6608813


def test_cache_paths_do_not_collide_between_sources(tmp_path, structure):
    sink = LocalSink(tmp_path)
    xml = (FIXTURES / "structure.xml").read_bytes()
    client(FakeResponse(200, xml), cache=sink).structure("TEST_FLOW")
    ecb = replace(SOURCES["ecb"], min_interval=0)
    client(FakeResponse(200, xml), cache=sink, source=ecb).structure("TEST_FLOW")
    names = sorted(p.name for p in (tmp_path / ".cache").iterdir())
    assert names == ["structure_TEST_FLOW.json", "structure_ecb_TEST_FLOW.json"]
