#!/usr/bin/env python3
"""Compatibility shell for target daily export / performance snapshots.

Round 5:
- core target daily logic now lives under `src/oae/performance/target_daily.py`
- CLI/runtime logic now lives under `src/oae/cli/export_target_daily.py`
- this file remains only to preserve the historical command entrypoint
"""

from __future__ import annotations

import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oae.cli.export_target_daily import main


if __name__ == "__main__":
    raise SystemExit(main())
