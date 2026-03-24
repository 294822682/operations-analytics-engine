"""Official CLI for unified/raw analysis orchestration."""

from __future__ import annotations

import argparse
from pathlib import Path

from oae.analysis.runtime import build_analysis_metadata, resolve_analysis_mode, run_analysis_mode
from oae.version import METRIC_VERSION, SCHEMA_VERSION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="经营分析协调入口")
    parser.add_argument(
        "--analysis-mode",
        choices=["auto", "unified-fact", "raw-evidence"],
        default="auto",
        help="auto=优先统一事实层，否则显式退回 raw-evidence",
    )
    parser.add_argument("--fact-csv", default="output/fact_attribution.csv", help="统一事实层 fact_attribution.csv")
    parser.add_argument("--manual-override-file", default="config/manual_attribution_overrides.csv", help="专项人工确认归属配置")
    parser.add_argument("--input-file", default="", help="raw-evidence 模式的原始线索文件")
    parser.add_argument("--sheet-name", default="", help="raw-evidence 模式的 Excel sheet")
    parser.add_argument("--output-dir", default="./全量分析", help="分析输出目录")
    parser.add_argument("--snapshot-dir", default="artifacts/snapshots", help="分析快照目录")
    parser.add_argument("--manifest-dir", default="artifacts/exports/analysis", help="分析 manifest 目录")
    parser.add_argument("--run-id", default="", help="运行编号")
    parser.add_argument("--schema-version", default=SCHEMA_VERSION, help="analysis schema 版本")
    parser.add_argument("--metric-version", default=METRIC_VERSION, help="经营口径版本")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workspace = Path(__file__).resolve().parents[3]
    fact_path = Path(args.fact_csv).expanduser().resolve()
    manual_override_path = Path(args.manual_override_file).expanduser().resolve() if str(args.manual_override_file).strip() else None
    output_dir = Path(args.output_dir).expanduser().resolve()
    snapshot_dir = Path(args.snapshot_dir).expanduser().resolve()
    manifest_dir = Path(args.manifest_dir).expanduser().resolve()
    analysis_mode = resolve_analysis_mode(args.analysis_mode, fact_path)
    metadata = build_analysis_metadata(
        run_id=args.run_id,
        schema_version=args.schema_version,
        metric_version=args.metric_version,
    )
    print(
        run_analysis_mode(
            workspace=workspace,
            analysis_mode=analysis_mode,
            fact_path=fact_path,
            manual_override_path=manual_override_path,
            input_file=args.input_file,
            sheet_name=args.sheet_name,
            output_dir=output_dir,
            snapshot_dir=snapshot_dir,
            manifest_dir=manifest_dir,
            metadata=metadata,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
