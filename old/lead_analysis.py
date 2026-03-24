#!/usr/bin/env python3
"""Compatibility shell for analysis CLI.

Round 8:
- official analysis CLI now lives under `src/oae/cli/run_analysis.py`
- this file remains only to preserve the historical command entrypoint
"""

from __future__ import annotations

import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oae.cli.run_analysis import main


if __name__ == "__main__":
    raise SystemExit(main())
