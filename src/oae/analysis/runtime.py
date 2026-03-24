"""Analysis runtime coordinator shared by CLI entrypoints."""

from __future__ import annotations

import json
from pathlib import Path

from oae.analysis import RAW_EVIDENCE_TOPICS, run_unified_fact_analysis
from oae.analysis.raw_evidence import summarize_raw_evidence_topics
from oae.analysis.raw_pipeline import run_raw_evidence_analysis
from oae.contracts.models import RunMetadata
from oae.quality import build_quality_report, run_raw_contract_checks
from oae.version import build_run_id


def _naming_lines(status_path: Path) -> str:
    status = json.loads(status_path.read_text(encoding="utf-8"))
    dry_run = status.get("dry_run_result", {})
    return (
        f"analysis_naming_status={status_path}\n"
        f"compatibility_write_effective={status.get('compatibility_write_effective')}\n"
        f"can_disable_now={status.get('can_disable_now')}\n"
        f"dry_run_disable_requested={status.get('dry_run_disable_compatibility')}\n"
        f"dry_run_disable_status={dry_run.get('status', '')}\n"
        f"disable_blockers_summary={status.get('disable_blockers_summary', '')}"
    )


def resolve_analysis_mode(requested_mode: str, fact_path: Path) -> str:
    if requested_mode != "auto":
        return requested_mode
    return "unified-fact" if fact_path.exists() else "raw-evidence"


def build_analysis_metadata(*, run_id: str, schema_version: str, metric_version: str) -> RunMetadata:
    return RunMetadata(
        run_id=run_id or build_run_id(),
        schema_version=schema_version,
        metric_version=metric_version,
    )


def run_analysis_mode(
    *,
    workspace: Path,
    analysis_mode: str,
    fact_path: Path,
    manual_override_path: Path | None,
    input_file: str,
    sheet_name: str,
    output_dir: Path,
    snapshot_dir: Path,
    manifest_dir: Path,
    metadata: RunMetadata,
) -> str:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    if analysis_mode == "unified-fact":
        outputs = run_unified_fact_analysis(
            fact_path=fact_path,
            manual_override_path=manual_override_path,
            output_dir=output_dir,
            snapshot_dir=snapshot_dir,
            manifest_dir=manifest_dir,
            metadata=metadata,
        )
        return (
            "analysis_mode=unified-fact\n"
            "migrated_topics=funnel,sla,quality,host_anchor,channel,ops_review\n"
            f"raw_evidence_topics={','.join(item['topic'] for item in RAW_EVIDENCE_TOPICS)}\n"
            f"raw_topic_groups={','.join(f'{key}:{len(value)}' for key, value in summarize_raw_evidence_topics().items())}\n"
            f"workbook={outputs['default_workbook']}\n"
            f"workbook_compatibility={outputs['workbook_compatibility']}\n"
            f"analysis_snapshot={outputs['default_snapshot']}\n"
            f"analysis_snapshot_compatibility={outputs['snapshot_compatibility']}\n"
            f"snapshot_manifest={outputs['default_snapshot_manifest']}\n"
            f"snapshot_manifest_compatibility={outputs['snapshot_manifest_compatibility']}\n"
            f"theme_manifest={outputs['theme_manifest']}\n"
            f"manual_override_count={outputs['manual_override_summary'].get('applied_override_count', 0)}\n"
            f"manual_override_rows={outputs['manual_override_summary'].get('applied_row_count', 0)}\n"
            f"{_naming_lines(outputs['naming_status'])}"
        )

    outputs = run_raw_evidence_analysis(
        workspace_dir=workspace,
        input_file=input_file,
        sheet_name=sheet_name,
        output_dir=output_dir,
        snapshot_dir=snapshot_dir,
        manifest_dir=manifest_dir,
        metadata=metadata,
    )
    raw_contract_checks = run_raw_contract_checks(
        raw_snapshot_path=outputs["default_snapshot"],
        snapshot_manifest_path=outputs["default_snapshot_manifest"],
        workbook_manifest_path=outputs["default_workbook_manifest"],
        anomaly_manifest_path=outputs["default_anomaly_manifest"],
        workbook_path=outputs["default_workbook"],
        anomaly_report_path=outputs["default_anomaly_report"],
        theme_manifest_path=outputs["theme_manifest"],
        expected_schema_version=metadata.schema_version,
        expected_metric_version=metadata.metric_version,
        expected_run_id=metadata.run_id,
    )
    raw_contract_report = build_quality_report(
        run_id=metadata.run_id,
        output_files=[outputs["default_snapshot"], outputs["default_workbook"], outputs["default_anomaly_report"]],
        baseline_result={},
        extra_checks=raw_contract_checks,
        report_type="quality_report",
        report_scope="raw-evidence-contract",
    )
    raw_contract_report_path = manifest_dir / f"raw_contract_report_{metadata.run_id}.json"
    raw_contract_report_canonical_path = manifest_dir / f"quality_report_raw-evidence-contract_{metadata.run_id}.json"
    raw_contract_report_canonical_path.write_text(json.dumps(raw_contract_report, ensure_ascii=False, indent=2), encoding="utf-8")
    if outputs.get("compatibility_write_effective", False):
        raw_contract_report_path.write_text(json.dumps(raw_contract_report, ensure_ascii=False, indent=2), encoding="utf-8")
    elif raw_contract_report_path.exists():
        raw_contract_report_path.unlink()
    return (
        "analysis_mode=raw-evidence\n"
        f"raw_evidence_topics={','.join(item['topic'] for item in RAW_EVIDENCE_TOPICS)}\n"
        f"raw_topic_groups={','.join(f'{key}:{len(value)}' for key, value in summarize_raw_evidence_topics().items())}\n"
        f"workbook={outputs['default_workbook']}\n"
        f"workbook_compatibility={outputs['workbook_compatibility']}\n"
        f"anomaly_report={outputs['default_anomaly_report']}\n"
        f"anomaly_report_compatibility={outputs['anomaly_report_compatibility']}\n"
        f"analysis_snapshot={outputs['default_snapshot']}\n"
        f"analysis_snapshot_compatibility={outputs['raw_snapshot_compatibility']}\n"
        f"snapshot_manifest={outputs['default_snapshot_manifest']}\n"
        f"snapshot_manifest_compatibility={outputs['snapshot_manifest_compatibility']}\n"
        f"theme_manifest={outputs['theme_manifest']}\n"
        f"quality_report={raw_contract_report_canonical_path}\n"
        f"quality_report_compatibility={raw_contract_report_path if outputs.get('compatibility_write_effective', False) else 'DISABLED'}\n"
        f"{_naming_lines(outputs['naming_status'])}\n"
        f"legacy_entry={workspace / 'legacy' / 'lead_analysis_raw_evidence_legacy.py'}"
    )
