"""Manifest integration for Feishu exports."""

from __future__ import annotations

from pathlib import Path

from oae.contracts.models import RunMetadata
from oae.exports.manifest import write_export_manifest


def write_feishu_manifests(
    *,
    export_dir: Path,
    report_date: str,
    metadata: RunMetadata,
    row_count: int,
    snapshot_path: Path | None,
    ledger_path: Path | None,
    analysis_snapshot_path: Path | None,
    fact_path: Path,
    md_path: Path,
    tsv_path: Path,
) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    export_sources = [
        "daily_performance_snapshot" if snapshot_path and snapshot_path.exists() else "daily_goal_legacy_csv",
        "compensation_ledger" if ledger_path and ledger_path.exists() else "",
        "analysis_snapshot" if analysis_snapshot_path and analysis_snapshot_path.exists() else "",
        "fact_attribution",
    ]
    export_sources = [item for item in export_sources if item]

    write_export_manifest(
        manifest_path=export_dir / f"feishu_table_latest_{report_date}.manifest.json",
        export_name="feishu_table_latest",
        snapshot_date=report_date,
        metadata=metadata,
        source_tables=export_sources,
        row_count=row_count,
        consumer="excel_template_v1",
        output_path=tsv_path,
    )
    write_export_manifest(
        manifest_path=export_dir / f"feishu_report_latest_{report_date}.manifest.json",
        export_name="feishu_report_latest",
        snapshot_date=report_date,
        metadata=metadata,
        source_tables=[
            "daily_performance_snapshot" if snapshot_path and snapshot_path.exists() else "daily_goal_legacy_csv",
            "fact_attribution",
        ],
        row_count=row_count,
        consumer="feishu_markdown_reader",
        output_path=md_path,
    )
