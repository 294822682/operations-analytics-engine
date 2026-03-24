"""Quality-report builders for pipeline runs."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path


def build_quality_report(
    *,
    run_id: str,
    output_files: list[Path],
    baseline_result: dict[str, object] | None = None,
    extra_checks: list[dict[str, object]] | None = None,
    report_type: str = "quality_report",
    report_scope: str = "pipeline",
) -> dict[str, object]:
    files = []
    for path in output_files:
        files.append(
            {
                "path": str(path),
                "exists": path.exists(),
                "size_bytes": path.stat().st_size if path.exists() else 0,
            }
        )

    checks = extra_checks or []
    category_counts: dict[str, int] = {}
    for item in baseline_result.get("details", []) if baseline_result else []:
        category = str(item.get("category", "uncategorized"))
        category_counts[category] = category_counts.get(category, 0) + 1
    for item in checks:
        category = str(item.get("category", "uncategorized"))
        category_counts[category] = category_counts.get(category, 0) + 1

    provisional_status = "pass"
    baseline_details = baseline_result.get("details", []) if baseline_result else []
    has_unsafe_baseline_warning = any(
        str(item.get("category", "")) not in {"pass", "metadata-only change", "safe structural change"}
        and str(item.get("status", "")) not in {"match", "metadata-only change", "safe structural change"}
        for item in baseline_details
    )
    if baseline_result and baseline_result.get("status") == "fail":
        provisional_status = "fail"
    if any(item.get("status") == "fail" for item in checks):
        provisional_status = "fail"
    elif (baseline_result and baseline_result.get("status") == "warning" and has_unsafe_baseline_warning) or any(item.get("status") == "warning" for item in checks):
        provisional_status = "warning"

    summary = _build_summary(overall_status=provisional_status, baseline_result=baseline_result or {}, checks=checks)
    overall_status = provisional_status
    if provisional_status == "warning" and _is_safe_only_summary(summary):
        overall_status = "pass"
        summary = _build_summary(overall_status=overall_status, baseline_result=baseline_result or {}, checks=checks)
    return {
        "report_type": report_type,
        "report_scope": report_scope,
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": overall_status,
        "summary": summary,
        "contract_checks": [item for item in checks if str(item.get("category")) == "contract violation"],
        "metric_checks": [item for item in checks if str(item.get("category")) == "metric drift"],
        "structural_checks": [item for item in checks if str(item.get("category")) == "structural change"],
        "pass_through_checks": [item for item in checks if str(item.get("category")) == "pass"],
        "files": files,
        "baseline": baseline_result or {},
        "checks": checks,
        "categories": category_counts,
    }


def _build_summary(
    *,
    overall_status: str,
    baseline_result: dict[str, object],
    checks: list[dict[str, object]],
) -> dict[str, object]:
    check_map = {str(item.get("name")): item for item in checks}
    key_alerts: list[str] = []
    safe_changes: list[str] = []
    attention_items: list[str] = []
    threshold_breach_count = 0
    configured_threshold_alerts: list[str] = []
    threshold_sources: set[str] = set()
    threshold_profiles: set[str] = set()

    for item in checks:
        name = str(item.get("name", ""))
        status = str(item.get("status", "pass"))
        details = item.get("details", {})
        breaches = details.get("threshold_breaches", []) if isinstance(details, dict) else []
        threshold_source = details.get("threshold_source") if isinstance(details, dict) else None
        threshold_profile = details.get("threshold_profile") if isinstance(details, dict) else None
        if threshold_source:
            threshold_sources.add(str(threshold_source))
        if threshold_profile:
            threshold_profiles.add(str(threshold_profile))
        threshold_breach_count += len(breaches)
        if status == "fail":
            key_alerts.extend([f"{name}: {msg}" for msg in breaches] or [f"{name}: 校验失败"])
        elif status == "warning":
            attention_items.extend([f"{name}: {msg}" for msg in breaches] or [f"{name}: 需要关注"])
        if threshold_source and breaches:
            configured_threshold_alerts.extend([f"{name}: {msg}" for msg in breaches])

    for item in baseline_result.get("details", []):
        status = str(item.get("status", ""))
        filename = str(item.get("name", ""))
        if status == "metadata-only change":
            safe_changes.append(f"{filename}: 仅元数据变化，业务结果安全")
            continue
        if status == "safe structural change":
            safe_changes.append(f"{filename}: 结构扩展但核心数据/消费契约稳定")
            continue
        if status == "match":
            continue
        if status == "structural change":
            related = _related_business_checks(filename, check_map)
            if related and all(check.get("status") == "pass" for check in related):
                safe_changes.append(f"{filename}: 结构变化，但关键经营指标稳定")
            else:
                attention_items.append(f"{filename}: 结构变化，需关注字段/模板兼容")
        elif status in {"missing-generated", "new-file"}:
            key_alerts.append(f"{filename}: 输出契约异常（{status}）")

    metric_drift_summary = {
        "fact": _format_metric_summary(check_map.get("fact.baseline_metrics")),
        "snapshot": _format_metric_summary(check_map.get("snapshot.baseline_metrics")),
        "ledger": _format_ledger_summary(
            check_map.get("ledger.unique_scope"),
            check_map.get("ledger.required_fields"),
            check_map.get("ledger.snapshot_reconcile"),
        ),
        "analysis": _format_analysis_summary(
            check_map.get("analysis.subject_areas"),
            check_map.get("analysis.row_count"),
            check_map.get("analysis.raw_evidence_topics"),
        ),
    }
    override_issue_summary = _format_override_issue_summary(check_map.get("manual_override.issues"))

    operational_decision = "safe"
    if overall_status == "fail":
        operational_decision = "block"
    elif overall_status == "warning":
        operational_decision = "investigate"

    return {
        "overall_status": overall_status,
        "operational_decision": operational_decision,
        "threshold_breach_count": threshold_breach_count,
        "threshold_profile": sorted(threshold_profiles)[0] if len(threshold_profiles) == 1 else "",
        "threshold_profiles": sorted(threshold_profiles),
        "threshold_sources": sorted(threshold_sources),
        "configured_threshold_alerts": configured_threshold_alerts,
        "key_alerts": key_alerts,
        "attention_items": attention_items,
        "safe_changes": safe_changes,
        "metric_drift_summary": metric_drift_summary,
        "override_issue_summary": override_issue_summary,
    }


def _is_safe_only_summary(summary: dict[str, object]) -> bool:
    key_alerts = summary.get("key_alerts", [])
    attention_items = summary.get("attention_items", [])
    configured_threshold_alerts = summary.get("configured_threshold_alerts", [])
    threshold_breach_count = int(summary.get("threshold_breach_count", 0) or 0)
    safe_changes = summary.get("safe_changes", [])
    return (
        not key_alerts
        and not attention_items
        and not configured_threshold_alerts
        and threshold_breach_count == 0
        and bool(safe_changes)
    )


def _related_business_checks(filename: str, check_map: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    if filename == "fact_attribution.csv":
        keys = ["fact.structural", "fact.baseline_metrics"]
    elif filename.startswith("daily_goal_account_latest_"):
        keys = ["snapshot.account_total_reconcile", "snapshot.anchor_rollup", "snapshot.baseline_metrics"]
    elif filename == "analysis_tables.xlsx" or filename.startswith("analysis_workbook_unified-fact_latest_"):
        keys = ["analysis.subject_areas", "analysis.row_count"]
    elif filename.startswith("feishu_table_latest_"):
        keys = [key for key in check_map if key.startswith("contract.")]
    else:
        keys = []
    return [check_map[key] for key in keys if key in check_map]


def _format_metric_summary(check: dict[str, object] | None) -> dict[str, object]:
    if not check:
        return {}
    details = check.get("details", {})
    if not isinstance(details, dict):
        return {}
    return {
        "status": check.get("status", "pass"),
        "summary": details.get("summary", ""),
        "threshold_breaches": details.get("threshold_breaches", []),
        "threshold_profile": details.get("threshold_profile", ""),
        "threshold_source": details.get("threshold_source", ""),
        "threshold_rule": details.get("threshold_rule", {}),
        "drift_metrics": details.get("drift_metrics", {}),
        "current_metrics": details.get("current_metrics", details.get("metrics", {})),
        "baseline_metrics": details.get("baseline_metrics", {}),
    }


def _format_ledger_summary(
    unique_scope_check: dict[str, object] | None,
    required_fields_check: dict[str, object] | None,
    reconcile_check: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "unique_scope_status": unique_scope_check.get("status", "unknown") if unique_scope_check else "unknown",
        "required_fields_status": required_fields_check.get("status", "unknown") if required_fields_check else "unknown",
        "reconcile_status": reconcile_check.get("status", "unknown") if reconcile_check else "unknown",
        "duplicate_count": _nested(unique_scope_check, "details", "metrics", "duplicate_count"),
        "null_fields": _nested(required_fields_check, "details", "null_fields"),
        "mismatch_count": _nested(reconcile_check, "details", "mismatch_count"),
        "threshold_profile": _nested(unique_scope_check, "details", "threshold_profile") or _nested(required_fields_check, "details", "threshold_profile") or _nested(reconcile_check, "details", "threshold_profile"),
        "threshold_source": _nested(unique_scope_check, "details", "threshold_source") or _nested(required_fields_check, "details", "threshold_source") or _nested(reconcile_check, "details", "threshold_source"),
        "threshold_rule": {
            "unique_scope": _nested(unique_scope_check, "details", "threshold_rule"),
            "required_fields": _nested(required_fields_check, "details", "threshold_rule"),
            "snapshot_reconcile": _nested(reconcile_check, "details", "threshold_rule"),
        },
        "threshold_breaches": (
            (_nested(unique_scope_check, "details", "threshold_breaches") or [])
            + (_nested(required_fields_check, "details", "threshold_breaches") or [])
            + (_nested(reconcile_check, "details", "threshold_breaches") or [])
        ),
    }


def _format_analysis_summary(
    subject_check: dict[str, object] | None,
    row_check: dict[str, object] | None,
    raw_check: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "subject_area_status": subject_check.get("status", "unknown") if subject_check else "unknown",
        "row_count_status": row_check.get("status", "unknown") if row_check else "unknown",
        "missing_subject_areas": _nested(subject_check, "details", "missing_subject_areas"),
        "subject_area_counts": _nested(row_check, "details", "subject_area_counts"),
        "raw_evidence_groups": _nested(raw_check, "details", "raw_evidence_groups"),
        "threshold_profile": _nested(subject_check, "details", "threshold_profile") or _nested(row_check, "details", "threshold_profile"),
        "threshold_source": _nested(subject_check, "details", "threshold_source") or _nested(row_check, "details", "threshold_source"),
        "threshold_rule": {
            "subject_areas": _nested(subject_check, "details", "threshold_rule"),
            "row_count": _nested(row_check, "details", "threshold_rule"),
        },
        "threshold_breaches": (
            (_nested(subject_check, "details", "threshold_breaches") or [])
            + (_nested(row_check, "details", "threshold_breaches") or [])
        ),
    }


def _format_override_issue_summary(check: dict[str, object] | None) -> dict[str, object]:
    if not check:
        return {}
    details = check.get("details", {})
    if not isinstance(details, dict):
        return {}
    return {
        "status": check.get("status", "pass"),
        "issue_count": details.get("issue_count", 0),
        "blocking_count": details.get("blocking_count", 0),
        "warning_count": details.get("warning_count", 0),
        "info_count": details.get("info_count", 0),
        "conflict_count": details.get("conflict_count", 0),
        "unmatched_count": details.get("unmatched_count", 0),
        "unmatched_not_in_current_run_count": details.get("unmatched_not_in_current_run_count", 0),
        "unmatched_probable_misconfig_count": details.get("unmatched_probable_misconfig_count", 0),
        "unmatched_outside_effective_window_count": details.get("unmatched_outside_effective_window_count", 0),
        "unmatched_insufficient_locator_count": details.get("unmatched_insufficient_locator_count", 0),
        "unmatched_needs_manual_review_count": details.get("unmatched_needs_manual_review_count", 0),
        "partial_apply_count": details.get("partial_apply_count", 0),
        "invalid_override_count": details.get("invalid_override_count", 0),
        "risk_count": details.get("risk_count", 0),
        "top_issues_cn": details.get("top_issues_cn", []),
    }


def _nested(item: dict[str, object] | None, *keys: str):
    current = item
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
