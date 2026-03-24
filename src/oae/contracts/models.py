"""Formal product objects introduced during productization round 1."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class RunMetadata:
    run_id: str
    schema_version: str
    metric_version: str
    template_version: str = ""
    freeze_id: str = ""


@dataclass(frozen=True)
class ExportManifestRecord:
    export_name: str
    snapshot_date: str
    schema_version: str
    metric_version: str
    run_id: str
    template_version: str
    freeze_id: str
    source_tables: list[str]
    row_count: int
    generated_at: str
    consumer: str
    output_path: str

    @classmethod
    def build(
        cls,
        *,
        export_name: str,
        snapshot_date: str,
        metadata: RunMetadata,
        source_tables: list[str],
        row_count: int,
        consumer: str,
        output_path: str | Path,
    ) -> "ExportManifestRecord":
        return cls(
            export_name=export_name,
            snapshot_date=snapshot_date,
            schema_version=metadata.schema_version,
            metric_version=metadata.metric_version,
            run_id=metadata.run_id,
            template_version=metadata.template_version,
            freeze_id=metadata.freeze_id,
            source_tables=source_tables,
            row_count=int(row_count),
            generated_at=datetime.now().isoformat(timespec="seconds"),
            consumer=consumer,
            output_path=str(Path(output_path)),
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ManualAttributionOverride:
    override_id: str
    business_subject_key: str
    phone: str
    lead_id: str
    override_scope: str
    target_account: str
    target_host: str
    reason: str
    evidence_note: str
    confirmed_by: str
    confirmed_at: str
    effective_from: str
    effective_to: str
    status: str
    metric_version: str
    run_id: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ManualOverrideIssue:
    issue_id: str
    issue_type: str
    severity: str
    override_id: str
    business_subject_key: str
    phone: str
    lead_id: str
    detected_stage: str
    message_cn: str
    suggested_action: str
    status: str = "open"
    run_id: str = ""
    matched_rows: int = 0
    conflict_override_ids: list[str] = field(default_factory=list)
    target_account: str = ""
    target_host: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class OverrideIssueDailyDigest:
    run_id: str
    summary_status: str
    blocking_count: int
    warning_count: int
    info_count: int
    top_priority_issues: list[dict[str, object]]
    account_impact_summary: list[str]
    host_impact_summary: list[str]
    latest_panel_risk_summary: list[str]
    suggested_actions: list[str]
    generated_at: str
    applied_override_count: int = 0
    applied_row_count: int = 0
    changed_final_consumer_scope: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
