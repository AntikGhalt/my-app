"""Where published files end up.

A sink only needs to read, write and copy files addressed by a relative
POSIX path ("trimestrali/consumi_famiglie.xlsx"). Versioning and the
manifest are handled by the runner, so every sink behaves the same.
"""

from __future__ import annotations

from typing import Protocol

from .local import LocalSink


class Sink(Protocol):
    def describe(self) -> str: ...
    def read(self, path: str) -> bytes | None: ...
    def write(self, path: str, data: bytes) -> None: ...
    def copy(self, src: str, dst: str) -> None: ...


__all__ = ["LocalSink", "Sink"]
