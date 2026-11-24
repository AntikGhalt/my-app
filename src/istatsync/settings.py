"""Runtime settings, read from environment variables (optionally via a .env file).

Nothing infrastructure-specific lives in the code: folder ids, buckets and
project names are supplied by whoever deploys the job.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .sdmx import SOURCES

DEFAULT_BASE_URL = SOURCES["istat"].base_url


def _load_dotenv(path: Path) -> None:
    """Minimal .env reader: KEY=VALUE lines, no interpolation. Real env wins."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(f"ISTATSYNC_{name}")
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    catalog_path: Path
    output_dir: Path
    base_url: str = DEFAULT_BASE_URL
    # ISTAT allows 5 queries/minute per IP and bans offenders for 1-2 days.
    # 13 s keeps us safely under the limit even with clock jitter.
    min_request_interval: float = 13.0
    request_timeout: float = 300.0
    max_retries: int = 3
    structure_ttl_days: int = 30
    drive_folder_id: str | None = None
    timezone: str = "Europe/Rome"

    @classmethod
    def from_env(cls, dotenv: Path | None = Path(".env")) -> Settings:
        if dotenv is not None:
            _load_dotenv(dotenv)
        output_dir = Path(_env("OUTPUT_DIR", "data"))
        return cls(
            catalog_path=Path(_env("CATALOG", "datasets.toml")),
            output_dir=output_dir,
            base_url=_env("BASE_URL", DEFAULT_BASE_URL).rstrip("/"),
            min_request_interval=float(_env("MIN_REQUEST_INTERVAL", "13")),
            request_timeout=float(_env("REQUEST_TIMEOUT", "300")),
            max_retries=int(_env("MAX_RETRIES", "3")),
            structure_ttl_days=int(_env("STRUCTURE_TTL_DAYS", "30")),
            drive_folder_id=_env("DRIVE_FOLDER_ID"),
            timezone=_env("TIMEZONE", "Europe/Rome"),
        )
