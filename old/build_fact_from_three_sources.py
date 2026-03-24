#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compatibility shell for fact-layer generation.

Round 3:
- core fact logic now lives under `src/oae/facts`
- CLI/runtime logic now lives under `src/oae/cli/build_fact.py`
- this file remains only to preserve the historical command entrypoint
"""

from __future__ import annotations

import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oae.cli.build_fact import main


# TODO(round3-compat): retain this shell until all schedulers and operator docs
# switch to `python -m oae.cli.build_fact`.

if __name__ == "__main__":
    raise SystemExit(main())
