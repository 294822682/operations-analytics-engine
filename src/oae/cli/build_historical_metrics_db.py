"""CLI entrypoint for historical metrics SQLite builder."""

from __future__ import annotations

import argparse
from pathlib import Path

from oae.storage.historical_metrics import build_historical_metrics_db


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build historical metrics SQLite registry")
    parser.add_argument("--workspace", default=".", help="Repository workspace")
    parser.add_argument("--start-month", required=True, help="Start month, YYYY-MM")
    parser.add_argument("--end-month", required=True, help="End month, YYYY-MM")
    parser.add_argument("--history-dir", default="历史文件", help="Historical source directory")
    parser.add_argument("--db", default="output/historical_metrics.db", help="Target SQLite db path")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    workspace = Path(args.workspace).expanduser().resolve()
    db_path = Path(args.db).expanduser()
    if not db_path.is_absolute():
        db_path = workspace / db_path
    records = build_historical_metrics_db(
        start_month=args.start_month,
        end_month=args.end_month,
        history_dir=args.history_dir,
        workspace=workspace,
        db_path=db_path,
    )
    print(f"HISTORICAL_METRICS_DB={db_path.resolve()}")
    print(f"HIST_SOURCE_REGISTRY_ROWS={len(records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
