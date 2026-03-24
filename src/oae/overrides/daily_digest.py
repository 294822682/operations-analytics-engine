"""Business-readable daily digest for manual override issues."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from oae.contracts.models import OverrideIssueDailyDigest


PRIORITY_SCORES = {
    "conflict_override": 100,
    "duplicate_override_id": 95,
    "metric_version_mismatch": 95,
    "missing_override_file": 95,
    "missing_columns": 95,
    "invalid_status": 92,
    "invalid_scope": 92,
    "missing_locator": 92,
    "missing_target": 92,
    "missing_required_field": 92,
    "missing_metric_version": 92,
    "invalid_confirmed_at": 90,
    "invalid_effective_from": 90,
    "invalid_effective_to": 90,
    "invalid_effective_window": 90,
    "unmatched_probable_misconfig": 82,
    "unmatched_insufficient_locator": 78,
    "unmatched_needs_manual_review": 74,
    "unmatched_not_in_current_run": 48,
    "unmatched_outside_effective_window": 42,
    "partial_apply": 70,
    "historical_account_host_risk": 45,
    "historical_host_latest_explain_risk": 42,
    "historical_account_cumulative_risk": 38,
    "historical_general_risk": 35,
}


def build_manual_override_daily_digest(
    *,
    run_id: str,
    issue_manifest: dict[str, object],
    override_manifest: dict[str, object] | None = None,
) -> dict[str, object]:
    override_manifest = override_manifest or {}
    issue_summary = issue_manifest.get("issue_summary", {}) if isinstance(issue_manifest, dict) else {}
    issues = list(issue_manifest.get("issues", [])) if isinstance(issue_manifest, dict) else []
    top_priority_issues = _build_top_priority_issues(issues)
    latest_panel_risk_summary = _build_latest_panel_risk_summary(issues)
    account_impact_summary = _build_account_impact_summary(issues, override_manifest)
    host_impact_summary = _build_host_impact_summary(issues, override_manifest)
    suggested_actions = _build_suggested_actions(issue_summary, top_priority_issues, latest_panel_risk_summary)
    digest = OverrideIssueDailyDigest(
        run_id=run_id,
        summary_status=_summary_status(issue_summary),
        blocking_count=int(issue_summary.get("blocking_count", 0) or 0),
        warning_count=int(issue_summary.get("warning_count", 0) or 0),
        info_count=int(issue_summary.get("info_count", 0) or 0),
        top_priority_issues=top_priority_issues,
        account_impact_summary=account_impact_summary,
        host_impact_summary=host_impact_summary,
        latest_panel_risk_summary=latest_panel_risk_summary,
        suggested_actions=suggested_actions,
        generated_at=datetime.now().isoformat(timespec="seconds"),
        applied_override_count=int(override_manifest.get("applied_override_count", 0) or 0),
        applied_row_count=int(override_manifest.get("applied_row_count", 0) or 0),
        changed_final_consumer_scope=bool(override_manifest.get("applied_override_count", 0)),
    ).to_dict()
    digest["configured_override_count"] = int(override_manifest.get("configured_rows", 0) or 0)
    digest["active_override_count"] = int(override_manifest.get("active_rows", 0) or 0)
    digest["affected_accounts"] = list(override_manifest.get("affected_accounts", []) or [])
    digest["affected_hosts"] = list(override_manifest.get("affected_hosts", []) or [])
    digest["final_consumer_scope"] = list(override_manifest.get("final_consumer_scope", []) or [])
    digest["issue_counts"] = {
        "issue_count": int(issue_summary.get("issue_count", 0) or 0),
        "conflict_count": int(issue_summary.get("conflict_count", 0) or 0),
        "unmatched_total": int(issue_summary.get("unmatched_count", 0) or 0),
        "unmatched_not_in_current_run_count": int(issue_summary.get("unmatched_not_in_current_run_count", 0) or 0),
        "unmatched_probable_misconfig_count": int(issue_summary.get("unmatched_probable_misconfig_count", 0) or 0),
        "unmatched_outside_effective_window_count": int(issue_summary.get("unmatched_outside_effective_window_count", 0) or 0),
        "unmatched_insufficient_locator_count": int(issue_summary.get("unmatched_insufficient_locator_count", 0) or 0),
        "unmatched_needs_manual_review_count": int(issue_summary.get("unmatched_needs_manual_review_count", 0) or 0),
        "partial_apply_count": int(issue_summary.get("partial_apply_count", 0) or 0),
        "invalid_override_count": int(issue_summary.get("invalid_override_count", 0) or 0),
        "risk_count": int(issue_summary.get("risk_count", 0) or 0),
    }
    digest["application_summary"] = {
        "description_cn": _application_summary_cn(override_manifest),
        "changes_final_consumer_scope": bool(override_manifest.get("applied_override_count", 0)),
        "affected_accounts": list(override_manifest.get("affected_accounts", []) or []),
        "affected_hosts": list(override_manifest.get("affected_hosts", []) or []),
    }
    return digest


def build_manual_override_daily_digest_view(digest: dict[str, object]) -> dict[str, object]:
    return {
        "summary_status": digest.get("summary_status", "clear"),
        "blocking_count": digest.get("blocking_count", 0),
        "warning_count": digest.get("warning_count", 0),
        "info_count": digest.get("info_count", 0),
        "issue_counts": dict(digest.get("issue_counts", {}) or {}),
        "today_blocking_issue_exists": bool(int(digest.get("blocking_count", 0) or 0) > 0),
        "top_priority_issues": [
            {
                "rank": item.get("rank", 0),
                "priority_bucket_cn": item.get("priority_bucket_cn", ""),
                "override_id": item.get("override_id", ""),
                "target_account": item.get("target_account", ""),
                "target_host": item.get("target_host", ""),
                "message_cn": item.get("message_cn", ""),
            }
            for item in (digest.get("top_priority_issues", []) or [])[:3]
            if isinstance(item, dict)
        ],
        "suggested_actions": list(digest.get("suggested_actions", []) or []),
        "latest_panel_risk_summary": list(digest.get("latest_panel_risk_summary", []) or []),
        "application_summary": digest.get("application_summary", {}),
    }


def _summary_status(issue_summary: dict[str, object]) -> str:
    if int(issue_summary.get("blocking_count", 0) or 0) > 0:
        return "blocking"
    if int(issue_summary.get("warning_count", 0) or 0) > 0:
        return "warning"
    if int(issue_summary.get("info_count", 0) or 0) > 0:
        return "info_only"
    return "clear"


def _build_top_priority_issues(issues: list[dict[str, object]]) -> list[dict[str, object]]:
    ranked = sorted(
        issues,
        key=lambda item: (
            _severity_rank(str(item.get("severity", ""))),
            PRIORITY_SCORES.get(str(item.get("issue_type", "")), 0),
            int(item.get("matched_rows", 0) or 0),
        ),
        reverse=True,
    )
    top_items = []
    for rank, item in enumerate(ranked[:3], start=1):
        top_items.append(
            {
                "rank": rank,
                "priority_bucket_cn": _priority_bucket_cn(item),
                "issue_type": str(item.get("issue_type", "")),
                "severity": str(item.get("severity", "")),
                "override_id": str(item.get("override_id", "")),
                "phone": str(item.get("phone", "")),
                "lead_id": str(item.get("lead_id", "")),
                "target_account": str(item.get("target_account", "")),
                "target_host": str(item.get("target_host", "")),
                "matched_rows": int(item.get("matched_rows", 0) or 0),
                "impact_scope_cn": _impact_scope_cn(item),
                "message_cn": str(item.get("message_cn", "")),
                "suggested_action": str(item.get("suggested_action", "")),
            }
        )
    return top_items


def _build_account_impact_summary(
    issues: list[dict[str, object]],
    override_manifest: dict[str, object],
) -> list[str]:
    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"applied": 0, "blocking": 0, "warning": 0, "account_risk": 0, "latest_risk": 0})
    for item in override_manifest.get("applied_details", []) or []:
        if not isinstance(item, dict):
            continue
        account = str(item.get("target_account", "") or "").strip()
        if account:
            stats[account]["applied"] += 1
    for issue in issues:
        account = str(issue.get("target_account", "") or "").strip()
        if not account:
            continue
        severity = str(issue.get("severity", ""))
        issue_type = str(issue.get("issue_type", ""))
        if severity == "blocking":
            stats[account]["blocking"] += 1
        elif severity == "warning":
            stats[account]["warning"] += 1
        if issue_type in {"historical_account_cumulative_risk", "historical_account_host_risk"}:
            stats[account]["account_risk"] += 1
        if issue_type in {"historical_host_latest_explain_risk", "historical_account_host_risk"}:
            stats[account]["latest_risk"] += 1
    if not stats:
        return ["今天没有命中任何专项归属账号，账号侧无需额外处理。"]
    lines: list[str] = []
    for account, item in sorted(stats.items(), key=lambda kv: (kv[1]["blocking"], kv[1]["warning"], kv[1]["applied"]), reverse=True):
        parts = [f"{account}：已命中 {item['applied']} 条专项归属"]
        if item["blocking"] > 0:
            parts.append(f"{item['blocking']} 条会阻断正式口径")
        elif item["warning"] > 0:
            parts.append(f"{item['warning']} 条建议今天优先修")
        if item["account_risk"] > 0:
            parts.append("更偏累计口径影响")
        if item["latest_risk"] > 0:
            parts.append("同时要注意主播 latest 解释")
        lines.append("，".join(parts) + "。")
    return lines[:5]


def _build_host_impact_summary(
    issues: list[dict[str, object]],
    override_manifest: dict[str, object],
) -> list[str]:
    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"applied": 0, "blocking": 0, "warning": 0, "latest_risk": 0})
    for item in override_manifest.get("applied_details", []) or []:
        if not isinstance(item, dict):
            continue
        host = str(item.get("target_host", "") or "").strip()
        if host:
            stats[host]["applied"] += 1
    for issue in issues:
        host = str(issue.get("target_host", "") or "").strip()
        if not host:
            continue
        severity = str(issue.get("severity", ""))
        issue_type = str(issue.get("issue_type", ""))
        if severity == "blocking":
            stats[host]["blocking"] += 1
        elif severity == "warning":
            stats[host]["warning"] += 1
        if issue_type in {"historical_host_latest_explain_risk", "historical_account_host_risk"}:
            stats[host]["latest_risk"] += 1
    if not stats:
        return ["今天没有命中任何专项归属主播，主播侧无需额外处理。"]
    lines: list[str] = []
    for host, item in sorted(stats.items(), key=lambda kv: (kv[1]["blocking"], kv[1]["warning"], kv[1]["applied"]), reverse=True):
        parts = [f"{host}：已命中 {item['applied']} 条专项归属"]
        if item["blocking"] > 0:
            parts.append(f"{item['blocking']} 条会阻断正式口径")
        elif item["warning"] > 0:
            parts.append(f"{item['warning']} 条建议今天优先修")
        if item["latest_risk"] > 0:
            parts.append("需要注意主播 latest 标签解释")
        lines.append("，".join(parts) + "。")
    return lines[:5]


def _build_latest_panel_risk_summary(issues: list[dict[str, object]]) -> list[str]:
    host_counts: dict[str, int] = defaultdict(int)
    general_count = 0
    for issue in issues:
        issue_type = str(issue.get("issue_type", ""))
        if issue_type not in {"historical_host_latest_explain_risk", "historical_account_host_risk", "historical_general_risk"}:
            continue
        host = str(issue.get("target_host", "") or "").strip()
        if host:
            host_counts[host] += 1
        else:
            general_count += 1
    if not host_counts and general_count == 0:
        return ["今天没有明显的主播 latest 标签解释风险。"]
    lines = [
        f"{host}：{count} 条专项只命中历史样本，累计口径已修正，但今天解释主播 latest 面板时建议顺手复核。"
        for host, count in sorted(host_counts.items(), key=lambda kv: kv[1], reverse=True)
    ]
    if general_count:
        lines.append(f"另有 {general_count} 条历史专项目前只能做一般性提示，暂时无法进一步判断更偏账号还是主播解释风险。")
    return lines[:5]


def _build_suggested_actions(
    issue_summary: dict[str, object],
    top_priority_issues: list[dict[str, object]],
    latest_panel_risk_summary: list[str],
) -> list[str]:
    actions: list[str] = []
    if int(issue_summary.get("blocking_count", 0) or 0) > 0:
        actions.append("今天先处理阻断项，再继续使用正式专项归属口径。")
    elif int(issue_summary.get("warning_count", 0) or 0) > 0:
        actions.append("今天优先修正 warning 级专项归属问题，再看 info 提示。")
    elif int(issue_summary.get("info_count", 0) or 0) > 0:
        actions.append("今天没有阻断项和高优先级错误，专项归属可以继续使用，重点是解释历史样本风险。")
    else:
        actions.append("今天专项归属没有高优先级问题，可按现有正式口径继续出数。")

    if int(issue_summary.get("unmatched_probable_misconfig_count", 0) or 0) > 0:
        actions.append("今天优先修正高概率误填的 unmatched 配置，这类问题最可能直接影响专项归属是否命中。")
    elif int(issue_summary.get("unmatched_needs_manual_review_count", 0) or 0) > 0:
        actions.append("今天有 unmatched 需要人工核实，建议先回看登记表或业务确认记录。")
    elif int(issue_summary.get("unmatched_not_in_current_run_count", 0) or 0) > 0:
        actions.append("今天存在本轮本来不存在的 unmatched，可先忽略，不必当天打断正式出数。")

    for item in top_priority_issues[:2]:
        message = str(item.get("suggested_action", "")).strip()
        if message and message not in actions:
            actions.append(message)

    if latest_panel_risk_summary and latest_panel_risk_summary[0] != "今天没有明显的主播 latest 标签解释风险。":
        extra = "如果今天要解释主播 latest 面板，请优先核对摘要里提到的历史专项样本。"
        if extra not in actions:
            actions.append(extra)
    return actions[:5]


def _priority_bucket_cn(issue: dict[str, object]) -> str:
    severity = str(issue.get("severity", ""))
    if severity == "blocking":
        return "今日阻断问题"
    if severity == "warning":
        return "今日高优先级修复问题"
    return "今日可延后关注问题"


def _impact_scope_cn(issue: dict[str, object]) -> str:
    issue_type = str(issue.get("issue_type", ""))
    if issue_type == "conflict_override":
        return "会阻断正式口径"
    if issue_type == "unmatched_probable_misconfig":
        return "高概率配置误填，建议当天修"
    if issue_type == "unmatched_insufficient_locator":
        return "定位键不足，建议补充后再跑"
    if issue_type == "unmatched_needs_manual_review":
        return "需要当天人工核实"
    if issue_type == "unmatched_not_in_current_run":
        return "本轮样本本来不存在，可延后处理"
    if issue_type == "unmatched_outside_effective_window":
        return "当前样本不在 override 生效期内"
    if issue_type == "partial_apply":
        return "只部分影响最终口径"
    if issue_type == "historical_account_cumulative_risk":
        return "更偏账号累计口径影响"
    if issue_type == "historical_host_latest_explain_risk":
        return "更偏主播 latest 标签解释风险"
    if issue_type == "historical_account_host_risk":
        return "同时影响累计口径和主播 latest 解释"
    if issue_type == "historical_general_risk":
        return "历史样本一般性风险提示"
    return "需人工复核"


def _severity_rank(severity: str) -> int:
    if severity == "blocking":
        return 3
    if severity == "warning":
        return 2
    if severity == "info":
        return 1
    return 0


def _application_summary_cn(override_manifest: dict[str, object]) -> str:
    applied_override_count = int(override_manifest.get("applied_override_count", 0) or 0)
    applied_row_count = int(override_manifest.get("applied_row_count", 0) or 0)
    if applied_override_count <= 0:
        return "今天没有专项人工确认归属实际改变最终消费口径。"
    affected_accounts = list(override_manifest.get("affected_accounts", []) or [])
    affected_hosts = list(override_manifest.get("affected_hosts", []) or [])
    account_text = "、".join(affected_accounts) if affected_accounts else "无"
    host_text = "、".join(affected_hosts) if affected_hosts else "无"
    return (
        f"今天共有 {applied_override_count} 条专项人工确认归属生效，影响 {applied_row_count} 行样本；"
        f"主要影响账号={account_text}，主播={host_text}。"
    )
