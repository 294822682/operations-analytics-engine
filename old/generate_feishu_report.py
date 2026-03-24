#!/usr/bin/env python3
"""Compatibility shell for Feishu report exports.

Round 6:
- core export logic now lives under `src/oae/exports/feishu_report.py`
- CLI/runtime logic now lives under `src/oae/cli/export_feishu_report.py`
- this file remains only to preserve the historical command entrypoint
"""

from __future__ import annotations

import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oae.cli.export_feishu_report import main


if __name__ == "__main__":
    raise SystemExit(main())
