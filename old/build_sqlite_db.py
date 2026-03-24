#!/usr/bin/env python3
"""Compatibility shell for SQLite loading.

Round 5:
- core SQLite load logic now lives under `src/oae/storage/sqlite_loader.py`
- CLI/runtime logic now lives under `src/oae/cli/build_sqlite_db.py`
- this file remains only to preserve the historical command entrypoint
"""

from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oae.cli.build_sqlite_db import main


if __name__ == "__main__":
    raise SystemExit(main())
