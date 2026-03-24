"""Writers for raw-analysis workbook/report/snapshot outputs."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

from oae.analysis.naming import evaluate_analysis_naming_status, load_analysis_naming_policy, write_analysis_naming_status
from oae.contracts.models import RunMetadata
from oae.exports.manifest import write_export_manifest


def write_raw_analysis_outputs(
    *,
    output_dir: Path,
    snapshot_dir: Path,
    manifest_dir: Path,
    snapshot_date: str,
    metadata: RunMetadata,
    workbook_tables: list[tuple[str, pd.DataFrame]],
    anomaly_tables: list[tuple[str, pd.DataFrame]],
    raw_snapshot: pd.DataFrame,
    topic_manifest: dict[str, object],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    workbook_canonical_path = output_dir / f"analysis_workbook_raw-evidence_latest_{snapshot_date}.xlsx"
    anomaly_report_canonical_path = output_dir / f"analysis_anomaly_report_raw-evidence_latest_{snapshot_date}.xlsx"
    raw_snapshot_canonical_path = snapshot_dir / f"analysis_snapshot_raw-evidence_latest_{snapshot_date}.csv"
    workbook_path = output_dir / "analysis_tables.xlsx"
    anomaly_report_path = output_dir / "time_chain_anomaly_report.xlsx"
    raw_snapshot_path = snapshot_dir / f"raw_analysis_snapshot_latest_{snapshot_date}.csv"
    topic_manifest_path = output_dir / "raw_analysis_theme_manifest.json"
    snapshot_manifest_canonical_path = manifest_dir / f"analysis_snapshot_raw-evidence_latest_{snapshot_date}.manifest.json"
    workbook_manifest_canonical_path = manifest_dir / f"analysis_workbook_raw-evidence_latest_{snapshot_date}.manifest.json"
    anomaly_manifest_canonical_path = manifest_dir / f"analysis_anomaly_report_raw-evidence_latest_{snapshot_date}.manifest.json"
    snapshot_manifest_path = manifest_dir / f"raw_analysis_snapshot_latest_{snapshot_date}.manifest.json"
    workbook_manifest_path = manifest_dir / f"raw_analysis_workbook_latest_{snapshot_date}.manifest.json"
    anomaly_manifest_path = manifest_dir / f"raw_anomaly_report_latest_{snapshot_date}.manifest.json"
    naming_status_path = manifest_dir / f"analysis_output_naming_status_raw-evidence_latest_{snapshot_date}.json"
    workspace_dir = output_dir.parent.resolve()
    policy = load_analysis_naming_policy(workspace_dir)

    with pd.ExcelWriter(workbook_canonical_path) as writer:
        for sheet_name, table in workbook_tables:
            table.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    with pd.ExcelWriter(anomaly_report_canonical_path) as writer:
        for sheet_name, table in anomaly_tables:
            table.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    raw_snapshot.to_csv(raw_snapshot_canonical_path, index=False, encoding="utf-8-sig")
    topic_manifest_path.write_text(json.dumps(topic_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    write_export_manifest(
        manifest_path=snapshot_manifest_canonical_path,
        export_name="analysis_snapshot",
        snapshot_date=snapshot_date,
        metadata=metadata,
        source_tables=["analysis_snapshot"],
        row_count=len(raw_snapshot),
        consumer="analysis-snapshot.raw-evidence",
        output_path=raw_snapshot_canonical_path,
    )
    write_export_manifest(
        manifest_path=workbook_manifest_canonical_path,
        export_name="analysis_workbook",
        snapshot_date=snapshot_date,
        metadata=metadata,
        source_tables=["raw_evidence_tables"],
        row_count=len(workbook_tables),
        consumer="excel_workbook.raw-evidence",
        output_path=workbook_canonical_path,
    )
    write_export_manifest(
        manifest_path=anomaly_manifest_canonical_path,
        export_name="analysis_anomaly_report",
        snapshot_date=snapshot_date,
        metadata=metadata,
        source_tables=["raw_time_anomaly"],
        row_count=len(anomaly_tables),
        consumer="excel_workbook.raw-evidence",
        output_path=anomaly_report_canonical_path,
    )
    naming_status = evaluate_analysis_naming_status(
        workspace_dir=workspace_dir,
        policy=policy,
        analysis_mode="raw-evidence",
        snapshot_date=snapshot_date,
        default_outputs={
            "workbook": workbook_canonical_path,
            "anomaly_report": anomaly_report_canonical_path,
            "snapshot": raw_snapshot_canonical_path,
        },
        compatibility_outputs={
            "workbook": workbook_path,
            "anomaly_report": anomaly_report_path,
            "snapshot": raw_snapshot_path,
        },
        default_manifests={
            "workbook": workbook_manifest_canonical_path,
            "anomaly_report": anomaly_manifest_canonical_path,
            "snapshot": snapshot_manifest_canonical_path,
        },
        compatibility_manifests={
            "workbook": workbook_manifest_path,
            "anomaly_report": anomaly_manifest_path,
            "snapshot": snapshot_manifest_path,
        },
        pipeline_uses_canonical=True,
        quality_uses_canonical=True,
    )
    compatibility_write_effective = bool(naming_status["compatibility_write_effective"])
    if compatibility_write_effective:
        shutil.copyfile(workbook_canonical_path, workbook_path)
        shutil.copyfile(anomaly_report_canonical_path, anomaly_report_path)
        raw_snapshot.to_csv(raw_snapshot_path, index=False, encoding="utf-8-sig")
        write_export_manifest(
            manifest_path=snapshot_manifest_path,
            export_name="analysis_snapshot",
            snapshot_date=snapshot_date,
            metadata=metadata,
            source_tables=["analysis_snapshot"],
            row_count=len(raw_snapshot),
            consumer="analysis-snapshot.raw-evidence",
            output_path=raw_snapshot_path,
        )
        write_export_manifest(
            manifest_path=workbook_manifest_path,
            export_name="analysis_workbook",
            snapshot_date=snapshot_date,
            metadata=metadata,
            source_tables=["raw_evidence_tables"],
            row_count=len(workbook_tables),
            consumer="excel_workbook.raw-evidence",
            output_path=workbook_path,
        )
        write_export_manifest(
            manifest_path=anomaly_manifest_path,
            export_name="analysis_anomaly_report",
            snapshot_date=snapshot_date,
            metadata=metadata,
            source_tables=["raw_time_anomaly"],
            row_count=len(anomaly_tables),
            consumer="excel_workbook.raw-evidence",
            output_path=anomaly_report_path,
        )
    else:
        for stale_path in [
            workbook_path,
            anomaly_report_path,
            raw_snapshot_path,
            workbook_manifest_path,
            anomaly_manifest_path,
            snapshot_manifest_path,
        ]:
            if stale_path.exists():
                stale_path.unlink()
    naming_status = {
        **naming_status,
        "compatibility_write_effective": compatibility_write_effective,
        "default_output_keys": {
            "workbook": str(workbook_canonical_path),
            "anomaly_report": str(anomaly_report_canonical_path),
            "snapshot": str(raw_snapshot_canonical_path),
            "workbook_manifest": str(workbook_manifest_canonical_path),
            "anomaly_manifest": str(anomaly_manifest_canonical_path),
            "snapshot_manifest": str(snapshot_manifest_canonical_path),
        },
    }
    write_analysis_naming_status(naming_status_path, naming_status)
    topic_manifest_path.write_text(
        json.dumps(
            {
                **topic_manifest,
                "manual_consumer_default": {
                    "workbook": str(workbook_canonical_path),
                    "anomaly_report": str(anomaly_report_canonical_path),
                    "snapshot": str(raw_snapshot_canonical_path),
                },
                "compatibility_outputs": {
                    "workbook": str(workbook_path),
                    "anomaly_report": str(anomaly_report_path),
                    "snapshot": str(raw_snapshot_path),
                },
                "compatibility_outputs_deprecated": True,
                "naming_status_path": str(naming_status_path),
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
        "anomaly_report": anomaly_report_path if compatibility_write_effective else anomaly_report_canonical_path,
        "anomaly_report_canonical": anomaly_report_canonical_path,
        "default_anomaly_report": anomaly_report_canonical_path,
        "anomaly_report_compatibility": anomaly_report_path,
        "raw_snapshot": raw_snapshot_path if compatibility_write_effective else raw_snapshot_canonical_path,
        "raw_snapshot_canonical": raw_snapshot_canonical_path,
        "default_snapshot": raw_snapshot_canonical_path,
        "raw_snapshot_compatibility": raw_snapshot_path,
        "snapshot_manifest": snapshot_manifest_path if compatibility_write_effective else snapshot_manifest_canonical_path,
        "snapshot_manifest_canonical": snapshot_manifest_canonical_path,
        "default_snapshot_manifest": snapshot_manifest_canonical_path,
        "snapshot_manifest_compatibility": snapshot_manifest_path,
        "workbook_manifest": workbook_manifest_path if compatibility_write_effective else workbook_manifest_canonical_path,
        "workbook_manifest_canonical": workbook_manifest_canonical_path,
        "default_workbook_manifest": workbook_manifest_canonical_path,
        "workbook_manifest_compatibility": workbook_manifest_path,
        "anomaly_manifest": anomaly_manifest_path if compatibility_write_effective else anomaly_manifest_canonical_path,
        "anomaly_manifest_canonical": anomaly_manifest_canonical_path,
        "default_anomaly_manifest": anomaly_manifest_canonical_path,
        "anomaly_manifest_compatibility": anomaly_manifest_path,
        "theme_manifest": topic_manifest_path,
        "naming_status": naming_status_path,
        "compatibility_write_effective": compatibility_write_effective,
    }
