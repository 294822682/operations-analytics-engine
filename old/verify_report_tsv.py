#!/usr/bin/env python3
"""Compatibility shell for TSV verification.

Round 6:
- core verification logic now lives under `src/oae/quality/tsv_verify.py`
- CLI/runtime logic now lives under `src/oae/cli/verify_report_tsv.py`
- this file remains only to preserve the historical command entrypoint
"""

from __future__ import annotations

import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oae.cli.verify_report_tsv import main


if __name__ == "__main__":
    raise SystemExit(main())
