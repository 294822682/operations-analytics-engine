"""Thin coordinator for target daily export / performance snapshots.

Round 6:
- runtime orchestration now lives under `src/oae/performance/runtime.py`
- IO helpers are exposed via `src/oae/performance/io.py`
- panel builders are exposed via `src/oae/performance/panels.py`
- this module stays as the stable import/CLI bridge
"""

from __future__ import annotations

from oae.performance.runtime import main


if __name__ == "__main__":
    raise SystemExit(main())
