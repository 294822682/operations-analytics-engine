"""Load manual attribution overrides from formal config input."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from oae.contracts.models import ManualAttributionOverride, ManualOverrideIssue
from oae.rules.io_utils import read_table_auto
from oae.rules.common import normalize_text

from .override_validator import validate_manual_override_frame


MANUAL_OVERRIDE_COLUMNS = [
    "override_id",
    "business_subject_key",
    "phone",
    "lead_id",
    "override_scope",
    "target_account",
    "target_host",
    "reason",
    "evidence_note",
    "confirmed_by",
    "confirmed_at",
    "effective_from",
    "effective_to",
    "status",
    "metric_version",
    "run_id",
]


def inspect_manual_attribution_overrides(path: Path, *, run_id: str = "") -> dict[str, object]:
    source_path = str(path)
    issues: list[dict[str, object]] = []

    if not path.exists():
        issue = ManualOverrideIssue(
            issue_id="manual-override-load-missing-file",
            issue_type="missing_override_file",
            severity="blocking",
            override_id="",
            business_subject_key="",
            phone="",
            lead_id="",
            detected_stage="load",
            message_cn=f"专项人工确认归属文件不存在：{path}",
            suggested_action="确认固定路径 config/manual_attribution_overrides.csv 存在，或先创建空模板。",
            status="open",
            run_id=run_id,
        )
        issues.append(issue.to_dict())
        return _inspection_result(
            source_path=source_path,
            overrides=[],
            normalized=pd.DataFrame(columns=MANUAL_OVERRIDE_COLUMNS),
            warnings=[],
            counts={"configured": 0, "active": 0, "inactive": 0, "revoked": 0},
            issues=issues,
            summary="专项人工确认归属文件不存在",
        )

    raw = read_table_auto(path)
    raw.columns = [str(col).strip() for col in raw.columns]
    missing_cols = [col for col in MANUAL_OVERRIDE_COLUMNS if col not in raw.columns]
    if missing_cols:
        issue = ManualOverrideIssue(
            issue_id="manual-override-load-missing-columns",
            issue_type="missing_columns",
            severity="blocking",
            override_id="",
            business_subject_key="",
            phone="",
            lead_id="",
            detected_stage="load",
            message_cn=f"专项人工确认归属文件缺少字段：{missing_cols}",
            suggested_action="按模板补齐字段后再执行正式口径。",
            status="open",
            run_id=run_id,
        )
        issues.append(issue.to_dict())
        return _inspection_result(
            source_path=source_path,
            overrides=[],
            normalized=pd.DataFrame(columns=MANUAL_OVERRIDE_COLUMNS),
            warnings=[],
            counts={"configured": 0, "active": 0, "inactive": 0, "revoked": 0},
            issues=issues,
            summary="专项人工确认归属文件缺少字段",
        )

    frame = raw[MANUAL_OVERRIDE_COLUMNS].copy()
    frame = frame.fillna("")
    frame = frame.map(normalize_text)
    frame = frame.loc[frame.apply(lambda row: any(str(value).strip() for value in row.tolist()), axis=1)].copy()

    validation = validate_manual_override_frame(frame, run_id=run_id)
    issues.extend(validation["issues"])
    normalized = validation["normalized"]
    blocking_count = sum(1 for item in issues if str(item.get("severity")) == "blocking")
    overrides = [] if blocking_count else [ManualAttributionOverride(**row) for row in normalized.to_dict(orient="records")]
    return _inspection_result(
        source_path=source_path,
        overrides=overrides,
        normalized=normalized,
        warnings=validation["warnings"],
        counts=validation["counts"],
        issues=issues,
        summary=validation["summary"],
    )


def load_manual_attribution_overrides(path: Path, *, run_id: str = "") -> tuple[list[ManualAttributionOverride], dict[str, object]]:
    inspection = inspect_manual_attribution_overrides(path, run_id=run_id)
    issue_summary = inspection["issue_summary"]
    if issue_summary["blocking_count"] > 0:
        top_messages = [item["message_cn"] for item in inspection["issues"] if item.get("severity") == "blocking"][:5]
        raise SystemExit(
            f"[ERROR] 专项人工确认归属文件校验失败：文件={path}；"
            f"阻断问题={top_messages}"
        )

    summary = {
        "source_path": str(path),
        "summary": inspection["summary"],
        "warnings": inspection["warnings"],
        "counts": inspection["counts"],
        "columns": MANUAL_OVERRIDE_COLUMNS,
        "issues": inspection["issues"],
        "issue_summary": issue_summary,
    }
    return inspection["overrides"], summary


def _inspection_result(
    *,
    source_path: str,
    overrides: list[ManualAttributionOverride],
    normalized: pd.DataFrame,
    warnings: list[str],
    counts: dict[str, int],
    issues: list[dict[str, object]],
    summary: str,
) -> dict[str, object]:
    return {
        "source_path": source_path,
        "summary": summary,
        "warnings": warnings,
        "counts": counts,
        "columns": MANUAL_OVERRIDE_COLUMNS,
        "issues": issues,
        "issue_summary": _issue_summary(issues),
        "normalized": normalized,
        "overrides": overrides,
    }


def _issue_summary(issues: list[dict[str, object]]) -> dict[str, object]:
    blocking = [item for item in issues if str(item.get("severity")) == "blocking"]
    warning = [item for item in issues if str(item.get("severity")) == "warning"]
    info = [item for item in issues if str(item.get("severity")) == "info"]
    return {
        "issue_count": len(issues),
        "blocking_count": len(blocking),
        "warning_count": len(warning),
        "info_count": len(info),
        "blocking_issue_ids": [str(item.get("issue_id", "")) for item in blocking],
    }
