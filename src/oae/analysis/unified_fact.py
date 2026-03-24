"""Unified-fact analysis entry used to move lead_analysis off the raw-only path."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

from oae.analysis.naming import evaluate_analysis_naming_status, load_analysis_naming_policy, write_analysis_naming_status
from oae.analysis.snapshot import build_analysis_snapshot_frame
from oae.analysis.channel import build_theme as build_channel_theme
from oae.analysis.funnel import build_theme as build_funnel_theme
from oae.analysis.host_anchor import build_theme as build_host_anchor_theme
from oae.analysis.ops_review import build_theme as build_ops_review_theme
from oae.analysis.quality import build_theme as build_quality_theme
from oae.analysis.raw_evidence import RAW_EVIDENCE_TOPICS, summarize_raw_evidence_topics, write_raw_evidence_manifest
from oae.analysis.sla import build_theme as build_sla_theme
from oae.contracts.models import RunMetadata
from oae.exports.manifest import write_export_manifest
from oae.overrides import load_fact_with_manual_overrides


def run_unified_fact_analysis(
    *,
    fact_path: Path,
    manual_override_path: Path | None,
    output_dir: Path,
    snapshot_dir: Path,
    manifest_dir: Path,
    metadata: RunMetadata,
) -> dict[str, Path]:
    fact = load_fact_with_manual_overrides(fact_path, manual_override_path=manual_override_path)
    manual_override_summary = fact.attrs.get("manual_override_summary", {})
    fact["date"] = pd.to_datetime(fact["date"], errors="coerce").dt.normalize()
    latest_date = fact["date"].max()
    if pd.isna(latest_date):
        raise ValueError("fact_attribution 中没有可用 date")

    subject_key_col = "business_subject_key" if "business_subject_key" in fact.columns else "_lead_key"
    themes = [
        ("funnel", build_funnel_theme),
        ("sla", build_sla_theme),
        ("quality", build_quality_theme),
        ("host_anchor", build_host_anchor_theme),
        ("channel", build_channel_theme),
        ("ops_review", build_ops_review_theme),
    ]
    workbook_tables: list[tuple[str, pd.DataFrame]] = []
    snapshot_rows: list[dict[str, object]] = []
    migrated_topics = []
    for theme_name, builder in themes:
        tables, rows = builder(fact, latest_date, subject_key_col)
        migrated_topics.append(theme_name)
        for sheet_name, table in tables.items():
            workbook_tables.append((sheet_name, table))
        snapshot_rows.extend(rows)

    analysis_snapshot = _build_analysis_snapshot(snapshot_rows=snapshot_rows, metadata=metadata)

    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    workbook_canonical_path = output_dir / f"analysis_workbook_unified-fact_latest_{latest_date.strftime('%Y-%m-%d')}.xlsx"
    snapshot_canonical_path = snapshot_dir / f"analysis_snapshot_unified-fact_latest_{latest_date.strftime('%Y-%m-%d')}.csv"
    workbook_path = output_dir / "analysis_tables.xlsx"
    snapshot_path = snapshot_dir / f"analysis_snapshot_latest_{latest_date.strftime('%Y-%m-%d')}.csv"
    theme_manifest_path = output_dir / "analysis_theme_manifest.json"
    raw_manifest_path = write_raw_evidence_manifest(output_dir)
    snapshot_manifest_canonical_path = manifest_dir / f"analysis_snapshot_unified-fact_latest_{latest_date.strftime('%Y-%m-%d')}.manifest.json"
    workbook_manifest_canonical_path = manifest_dir / f"analysis_workbook_unified-fact_latest_{latest_date.strftime('%Y-%m-%d')}.manifest.json"
    snapshot_manifest_path = manifest_dir / f"analysis_snapshot_latest_{latest_date.strftime('%Y-%m-%d')}.manifest.json"
    workbook_manifest_path = manifest_dir / f"analysis_workbook_latest_{latest_date.strftime('%Y-%m-%d')}.manifest.json"
    naming_status_path = manifest_dir / f"analysis_output_naming_status_unified-fact_latest_{latest_date.strftime('%Y-%m-%d')}.json"
    workspace_dir = output_dir.parent.resolve()
    policy = load_analysis_naming_policy(workspace_dir)

    with pd.ExcelWriter(workbook_canonical_path) as writer:
        for sheet_name, table in workbook_tables:
            table.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    analysis_snapshot.to_csv(snapshot_canonical_path, index=False, encoding="utf-8-sig")
    write_export_manifest(
        manifest_path=snapshot_manifest_canonical_path,
        export_name="analysis_snapshot",
        snapshot_date=latest_date.strftime("%Y-%m-%d"),
        metadata=metadata,
        source_tables=["analysis_snapshot"],
        row_count=len(analysis_snapshot),
        consumer="analysis-snapshot.unified-fact",
        output_path=snapshot_canonical_path,
    )
    write_export_manifest(
        manifest_path=workbook_manifest_canonical_path,
        export_name="analysis_workbook",
        snapshot_date=latest_date.strftime("%Y-%m-%d"),
        metadata=metadata,
        source_tables=["analysis_tables"],
        row_count=len(workbook_tables),
        consumer="excel_workbook.unified-fact",
        output_path=workbook_canonical_path,
    )
    naming_status = evaluate_analysis_naming_status(
        workspace_dir=workspace_dir,
        policy=policy,
        analysis_mode="unified-fact",
        snapshot_date=latest_date.strftime("%Y-%m-%d"),
        default_outputs={"workbook": workbook_canonical_path, "snapshot": snapshot_canonical_path},
        compatibility_outputs={"workbook": workbook_path, "snapshot": snapshot_path},
        default_manifests={"workbook": workbook_manifest_canonical_path, "snapshot": snapshot_manifest_canonical_path},
        compatibility_manifests={"workbook": workbook_manifest_path, "snapshot": snapshot_manifest_path},
        pipeline_uses_canonical=True,
        quality_uses_canonical=True,
    )
    compatibility_write_effective = bool(naming_status["compatibility_write_effective"])
    if compatibility_write_effective:
        shutil.copyfile(workbook_canonical_path, workbook_path)
        analysis_snapshot.to_csv(snapshot_path, index=False, encoding="utf-8-sig")
        write_export_manifest(
            manifest_path=snapshot_manifest_path,
            export_name="analysis_snapshot",
            snapshot_date=latest_date.strftime("%Y-%m-%d"),
            metadata=metadata,
            source_tables=["analysis_snapshot"],
            row_count=len(analysis_snapshot),
            consumer="analysis-snapshot.unified-fact",
            output_path=snapshot_path,
        )
        write_export_manifest(
            manifest_path=workbook_manifest_path,
            export_name="analysis_workbook",
            snapshot_date=latest_date.strftime("%Y-%m-%d"),
            metadata=metadata,
            source_tables=["analysis_tables"],
            row_count=len(workbook_tables),
            consumer="excel_workbook.unified-fact",
            output_path=workbook_path,
        )
    else:
        for stale_path in [workbook_path, snapshot_path, workbook_manifest_path, snapshot_manifest_path]:
            if stale_path.exists():
                stale_path.unlink()
    naming_status = {
        **naming_status,
        "compatibility_write_effective": compatibility_write_effective,
        "default_output_keys": {
            "workbook": str(workbook_canonical_path),
            "snapshot": str(snapshot_canonical_path),
            "workbook_manifest": str(workbook_manifest_canonical_path),
            "snapshot_manifest": str(snapshot_manifest_canonical_path),
        },
    }
    write_analysis_naming_status(naming_status_path, naming_status)
    theme_manifest_path.write_text(
        json.dumps(
            {
                "analysis_mode": "unified-fact",
                "migrated_topics": migrated_topics,
                "raw_evidence_topics": RAW_EVIDENCE_TOPICS,
                "raw_evidence_groups": summarize_raw_evidence_topics(),
                "default_outputs": naming_status["default_output_keys"],
                "manual_consumer_default": {
                    "workbook": str(workbook_canonical_path),
                    "snapshot": str(snapshot_canonical_path),
                },
                "compatibility_outputs": {
                    "workbook": str(workbook_path),
                    "snapshot": str(snapshot_path),
                },
                "compatibility_outputs_deprecated": True,
                "compatibility_write_effective": compatibility_write_effective,
                "naming_status_path": str(naming_status_path),
                "manual_override_summary": manual_override_summary,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "workbook": workbook_path if compatibility_write_effective else workbook_canonical_path,
        "workbook_canonical": workbook_canonical_path,
        "default_workbook": workbook_canonical_path,
        "workbook_compatibility": workbook_path,
        "snapshot": snapshot_path if compatibility_write_effective else snapshot_canonical_path,
        "snapshot_canonical": snapshot_canonical_path,
        "default_snapshot": snapshot_canonical_path,
        "snapshot_compatibility": snapshot_path,
        "snapshot_manifest": snapshot_manifest_path if compatibility_write_effective else snapshot_manifest_canonical_path,
        "snapshot_manifest_canonical": snapshot_manifest_canonical_path,
        "default_snapshot_manifest": snapshot_manifest_canonical_path,
        "snapshot_manifest_compatibility": snapshot_manifest_path,
        "workbook_manifest": workbook_manifest_path if compatibility_write_effective else workbook_manifest_canonical_path,
        "workbook_manifest_canonical": workbook_manifest_canonical_path,
        "default_workbook_manifest": workbook_manifest_canonical_path,
        "workbook_manifest_compatibility": workbook_manifest_path,
        "theme_manifest": theme_manifest_path,
        "raw_evidence_manifest": raw_manifest_path,
        "naming_status": naming_status_path,
        "compatibility_write_effective": compatibility_write_effective,
        "manual_override_summary": manual_override_summary,
    }


def _build_analysis_snapshot(*, snapshot_rows: list[dict[str, object]], metadata: RunMetadata) -> pd.DataFrame:
    snapshot_date = str(snapshot_rows[0].get("snapshot_date", "")) if snapshot_rows else ""
    return build_analysis_snapshot_frame(
        snapshot_rows=snapshot_rows,
        snapshot_date=snapshot_date,
        metadata=metadata,
        analysis_mode="unified-fact",
        evidence_mode="unified",
        default_source_scope="fact_attribution",
        default_raw_evidence_required=False,
        default_migration_status="unified_managed",
    )
