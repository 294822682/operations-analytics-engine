from __future__ import annotations

from pathlib import Path


def runs_dir(workspace: Path) -> Path:
    return Path(workspace) / "artifacts" / "runs"


__all__ = ["runs_dir"]
