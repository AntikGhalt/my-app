"""Thin client for the ISTAT SDMX REST endpoint.

Things the ISTAT service does that a generic SDMX client does not expect:

* 5 requests per minute per IP. Going over gets the IP banned for one or two
  days, and the ban shows up as 403/429 or as connections hanging. We space
  requests out and stop the whole run on the first sign of throttling rather
  than retrying into a ban.
* URLs longer than a few hundred characters fail with HTTP 400. When a key
  would be too long we leave the longest dimension empty (= all codes) and
  filter the rows locally.
* "No data" is reported as HTTP 404 with a NoRecordsFound body, not as an
  empty dataset.
* The structure endpoint is slow (a minute or more per dataflow) and big, so
  it is cached in the output sink: a Cloud Run job starts from an empty disk
  every time and would otherwise re-download every structure on every run.
"""

from __future__ import annotations

import io
import json
import logging
import time
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

import pandas as pd
import requests

log = logging.getLogger(__name__)

AGENCY = "IT1"
CSV_ACCEPT = "application/vnd.sdmx.data+csv;version=1.0.0"
STRUCTURE_ACCEPT = "application/vnd.sdmx.structure+xml;version=2.1"
MAX_URL_LENGTH = 340

_NS = {
    "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}
_XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


class IstatError(RuntimeError):
    pass


class RateLimited(IstatError):
    """ISTAT is throttling us. Continuing would risk an IP ban."""


Key = Mapping[str, str | Sequence[str]]


@dataclass
class Structure:
    """What we need from a data structure definition."""

    dataflow: str
    name: str
    dimensions: list[str]
    # dimension id -> {code -> label}
    labels: dict[str, dict[str, str]] = field(default_factory=dict)
    fetched_at: float = 0.0

    def label(self, dimension: str, code: str) -> str:
        return self.labels.get(dimension, {}).get(code, code)

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False)

    @classmethod
    def from_json(cls, text: str) -> Structure:
        return cls(**json.loads(text))


class Cache(Protocol):
    def read(self, path: str) -> bytes | None: ...
    def write(self, path: str, data: bytes) -> None: ...


class RequestPacer:
    """Keeps at least `interval` seconds between the *start* of two requests."""

    def __init__(self, interval: float, clock=time.monotonic, sleep=time.sleep):
        self.interval = interval
        self._clock = clock
        self._sleep = sleep
        self._last: float | None = None

    def wait(self) -> None:
        if self._last is not None:
            remaining = self.interval - (self._clock() - self._last)
            if remaining > 0:
                log.debug("pacing: sleeping %.1fs", remaining)
                self._sleep(remaining)
        self._last = self._clock()


class IstatClient:
    def __init__(
        self,
        base_url: str,
        *,
        cache: Cache | None = None,
        min_interval: float = 13.0,
        timeout: float = 300.0,
        max_retries: int = 3,
        structure_ttl_days: int = 7,
        session: requests.Session | None = None,
        sleep=time.sleep,
    ):
        self.base_url = base_url.rstrip("/")
        self.cache = cache
        self.timeout = timeout
        self.max_retries = max_retries
        self.structure_ttl = structure_ttl_days * 86400
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "istatsync/2 (+python-requests)")
        self.pacer = RequestPacer(min_interval, sleep=sleep)
        self._sleep = sleep
        self.requests_made = 0

    # -- HTTP -------------------------------------------------------------

    def _get(self, url: str, *, params: dict | None, accept: str) -> requests.Response | None:
        """GET with pacing and retries. Returns None for ISTAT's 'no records' 404."""
        for attempt in range(1, self.max_retries + 1):
            self.pacer.wait()
            self.requests_made += 1
            try:
                resp = self.session.get(
                    url, params=params, headers={"Accept": accept}, timeout=self.timeout
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                log.warning("attempt %d/%d failed: %s", attempt, self.max_retries, exc)
                if attempt == self.max_retries:
                    raise IstatError(f"ISTAT unreachable: {exc}") from exc
                self._sleep(30 * attempt)
                continue

            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            if resp.status_code in (403, 429):
                raise RateLimited(
                    f"HTTP {resp.status_code} from ISTAT: the IP is probably throttled. "
                    "Stop and retry tomorrow; do not loop."
                )
            if resp.status_code == 400:
                raise IstatError(f"HTTP 400 for {resp.url}: {resp.text[:300]}")
            if resp.status_code >= 500 and attempt < self.max_retries:
                log.warning(
                    "HTTP %d, retrying (%d/%d)", resp.status_code, attempt, self.max_retries
                )
                self._sleep(30 * attempt)
                continue
            raise IstatError(f"HTTP {resp.status_code} for {resp.url}: {resp.text[:300]}")
        raise AssertionError("unreachable")

    # -- structure --------------------------------------------------------

    def structure(self, dataflow: str) -> Structure:
        cached = self._read_cache(dataflow)
        if cached:
            return cached
        url = f"{self.base_url}/dataflow/{AGENCY}/{dataflow}/latest"
        resp = self._get(
            url, params={"references": "all", "detail": "full"}, accept=STRUCTURE_ACCEPT
        )
        if resp is None:
            raise IstatError(f"dataflow {dataflow} not found")
        structure = parse_structure(resp.content, dataflow)
        structure.fetched_at = time.time()
        if self.cache is not None:
            self.cache.write(_cache_path(dataflow), structure.to_json().encode("utf-8"))
        return structure

    def _read_cache(self, dataflow: str) -> Structure | None:
        raw = self.cache.read(_cache_path(dataflow)) if self.cache is not None else None
        if not raw:
            return None
        structure = Structure.from_json(raw.decode("utf-8"))
        if time.time() - structure.fetched_at > self.structure_ttl:
            return None
        return structure

    # -- data -------------------------------------------------------------

    def data(
        self,
        dataflow: str,
        dimensions: Sequence[str],
        key: Key,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Download observations as a DataFrame with one column per dimension,
        plus TIME_PERIOD and OBS_VALUE. Returns an empty frame if ISTAT has no rows."""
        key_str, local_filter = build_key(
            dimensions, key, base_len=len(self.data_url(dataflow, ""))
        )
        url = self.data_url(dataflow, key_str)
        params = {"startPeriod": start, "endPeriod": end}
        params = {k: v for k, v in params.items() if v}
        resp = self._get(url, params=params, accept=CSV_ACCEPT)
        if resp is None:
            return pd.DataFrame(columns=[*dimensions, "TIME_PERIOD", "OBS_VALUE"])
        df = read_sdmx_csv(resp.content)
        for dim, codes in local_filter.items():
            df = df[df[dim].isin(codes)]
        return df.reset_index(drop=True)

    def data_url(self, dataflow: str, key: str) -> str:
        return f"{self.base_url}/data/{AGENCY},{dataflow},1.0/{key}/ALL/"


def _cache_path(dataflow: str) -> str:
    return f".cache/structure_{dataflow}.json"


def _codes(value: str | Sequence[str] | None) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def build_key(
    dimensions: Sequence[str], key: Key, *, base_len: int = 0, max_len: int = MAX_URL_LENGTH
) -> tuple[str, dict[str, list[str]]]:
    """Build the positional SDMX key ("Q.IT.A+B..").

    Dimensions not listed in `key` are left empty (all codes). If the URL
    would get too long, the dimensions with the most codes are dropped from
    the key, longest first, and returned as a filter to apply locally.
    """
    unknown = set(key) - set(dimensions)
    if unknown:
        raise ValueError(f"unknown dimensions {sorted(unknown)}; valid: {list(dimensions)}")

    selected = {d: _codes(key.get(d)) for d in dimensions}
    local: dict[str, list[str]] = {}

    def render() -> str:
        return ".".join("+".join(selected[d]) for d in dimensions)

    while base_len + len(render()) > max_len:
        longest = max(selected, key=lambda d: len("+".join(selected[d])))
        if not selected[longest]:
            break
        local[longest] = selected[longest]
        selected[longest] = []
        log.info("key too long for ISTAT, filtering %s locally", longest)
    return render(), local


def read_sdmx_csv(content: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    df = df.drop(columns=[c for c in df.columns if c == "DATAFLOW" or c.startswith("NOTE_")])
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"].replace("", None), errors="coerce")
    return df


def parse_structure(content: bytes, dataflow: str) -> Structure:
    root = ET.fromstring(content)

    flow = root.find(f".//s:Dataflow[@id='{dataflow}']", _NS)
    name = _pick_name(flow) if flow is not None else dataflow

    dsd = root.find(".//s:DataStructure", _NS)
    if dsd is None:
        raise IstatError(f"no DataStructure for {dataflow}")

    codelists = {
        cl.get("id"): {code.get("id"): _pick_name(code) for code in cl.findall("s:Code", _NS)}
        for cl in root.iterfind(".//s:Codelist", _NS)
    }

    dims: list[tuple[int, str]] = []
    labels: dict[str, dict[str, str]] = {}
    for dim in dsd.iterfind(".//s:DimensionList/s:Dimension", _NS):
        dim_id = dim.get("id")
        dims.append((int(dim.get("position", len(dims) + 1)), dim_id))
        ref = dim.find("s:LocalRepresentation/s:Enumeration/Ref", _NS)
        if ref is not None and ref.get("id") in codelists:
            labels[dim_id] = codelists[ref.get("id")]

    return Structure(
        dataflow=dataflow,
        name=name,
        dimensions=[d for _, d in sorted(dims)],
        labels=labels,
    )


def _pick_name(element: ET.Element) -> str:
    """Italian name if present, otherwise English, otherwise the id."""
    names = {n.get(_XML_LANG): (n.text or "").strip() for n in element.findall("c:Name", _NS)}
    return names.get("it") or names.get("en") or element.get("id", "")
