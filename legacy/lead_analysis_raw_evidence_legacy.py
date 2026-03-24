#!/usr/bin/env python3
"""Compatibility shell for raw-evidence analysis.

Round 4:
- raw-evidence execution now lives in `src/oae/analysis/raw_pipeline.py`
- this file remains only for historical entrypoint compatibility
"""

from __future__ import annotations

import sys
from pathlib import Path


WORKSPACE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = WORKSPACE_DIR / "src"
if str(WORKSPACE_DIR) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_DIR))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oae.analysis.raw_pipeline import main


# TODO(round4-compat): remove this shell after all operator docs and historical
# automation references switch to `python -m oae.analysis.raw_pipeline` or
# `python lead_analysis.py --analysis-mode raw-evidence`.

if __name__ == "__main__":
    raise SystemExit(main())
