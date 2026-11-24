from __future__ import annotations

import os
import shutil
from pathlib import Path


class LocalSink:
    def __init__(self, root: Path):
        self.root = Path(root)

    def describe(self) -> str:
        return str(self.root.resolve())

    def _path(self, path: str) -> Path:
        full = (self.root / path).resolve()
        if self.root.resolve() not in full.parents and full != self.root.resolve():
            raise ValueError(f"path escapes output dir: {path}")
        return full

    def read(self, path: str) -> bytes | None:
        p = self._path(path)
        return p.read_bytes() if p.is_file() else None

    def write(self, path: str, data: bytes) -> None:
        p = self._path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        # Write next to the target and rename, so a crash never leaves a
        # half-written file where Tableau or Excel expect a good one.
        tmp = p.with_name(p.name + ".part")
        tmp.write_bytes(data)
        os.replace(tmp, p)

    def copy(self, src: str, dst: str) -> None:
        s, d = self._path(src), self._path(dst)
        if not s.is_file():
            return
        d.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(s, d)
