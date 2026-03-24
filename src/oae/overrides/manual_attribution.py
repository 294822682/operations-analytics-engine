"""Consumer-layer manual attribution override application and governance."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

from oae.contracts.models import ManualAttributionOverride, ManualOverrideIssue
from oae.rules.account_mapping import normalize_account
from oae.rules.common import normalize_text

from .override_loader import inspect_manual_attribution_overrides, load_manual_attribution_overrides


TRACE_COLUMN_DEFAULTS = {
    "自动标准账号": "",
    "自动本场主播": "",
    "自动归属状态": "",
    "自动无匹配原因": "",
    "最终标准账号": "",
    "最终本场主播": "",
    "最终归属状态": "",
    "最终无匹配原因": "",
    "最终归属来源": "auto",
    "专项归属是否生效": False,
    "专项归属ID": "",
    "专项归属范围": "",
    "专项归属原因": "",
    "专项归属依据": "",
    "专项归属确认人": "",
    "专项归属确认时间": "",
    "专项归属目标账号": "",
    "专项归属目标主播": "",
}


def load_fact_with_manual_overrides(
    fact_path: Path,
    manual_override_path: Path | None = None,
) -> pd.DataFrame:
    fact = pd.read_csv(fact_path)
    fact["线索ID_norm"] = _lead_id_series(fact)
    fact["手机号"] = _phone_series(fact)
    if manual_override_path is None or not str(manual_override_path).strip():
        result = _initialize_trace_columns(fact)
        summary = _empty_summary(source_path="")
        result.attrs["manual_override_summary"] = summary
        return result

    overrides, override_source_summary = load_manual_attribution_overrides(Path(manual_override_path).expanduser().resolve())
    result, summary = apply_manual_attribution_overrides(
        fact=fact,
        overrides=overrides,
        source_path=override_source_summary["source_path"],
    )
    result.attrs["manual_override_summary"] = summary
    return result


def apply_manual_attribution_overrides(
    *,
    fact: pd.DataFrame,
    overrides: list[ManualAttributionOverride],
    source_path: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    out = _initialize_trace_columns(fact)
    if not overrides:
        return out, _empty_summary(source_path=source_path)

    out["_manual_override_effective_date"] = _effective_date_series(out)
    governance = inspect_manual_override_application(fact=out, overrides=overrides, source_path=source_path)
    issue_summary = governance["issue_summary"]
    issues = governance["issues"]
    if int(issue_summary.get("blocking_count", 0) or 0) > 0:
        blocking_messages = [item["message_cn"] for item in issues if item.get("severity") == "blocking"][:5]
        raise SystemExit(
            "[ERROR] 专项人工确认归属存在阻断问题，请先修复 override 配置；"
            f"问题={blocking_messages}"
        )

    applied_details: list[dict[str, object]] = []
    affected_accounts: set[str] = set()
    affected_hosts: set[str] = set()
    active_count = 0
    inactive_count = 0
    revoked_count = 0
    applied_override_count = 0
    applied_row_count = 0

    for override in overrides:
        row = override.to_dict()
        status = normalize_text(row.get("status", "")).lower()
        if status == "inactive":
            inactive_count += 1
            continue
        if status == "revoked":
            revoked_count += 1
            continue
        active_count += 1

        matched_indexes = governance["matched_index_map"].get(str(row.get("override_id", "")), [])
        mask = _mask_from_indexes(out.index, matched_indexes)
        matched = out.loc[mask].copy()
        if matched.empty:
            continue

        target_account = normalize_account(row.get("target_account", "")) if normalize_text(row.get("target_account", "")) else ""
        target_host = normalize_text(row.get("target_host", ""))

        final_account = out.loc[mask, "自动标准账号"].astype(str)
        if target_account:
            final_account = target_account
        final_host = out.loc[mask, "自动本场主播"].astype(str)
        if target_host:
            final_host = target_host

        out.loc[mask, "最终标准账号"] = final_account
        out.loc[mask, "最终本场主播"] = final_host
        out.loc[mask, "最终归属状态"] = "匹配成功"
        out.loc[mask, "最终无匹配原因"] = ""
        out.loc[mask, "最终归属来源"] = "manual_override"
        out.loc[mask, "专项归属是否生效"] = True
        out.loc[mask, "专项归属ID"] = row["override_id"]
        out.loc[mask, "专项归属范围"] = row["override_scope"]
        out.loc[mask, "专项归属原因"] = row["reason"]
        out.loc[mask, "专项归属依据"] = row["evidence_note"]
        out.loc[mask, "专项归属确认人"] = row["confirmed_by"]
        out.loc[mask, "专项归属确认时间"] = row["confirmed_at"]
        out.loc[mask, "专项归属目标账号"] = target_account
        out.loc[mask, "专项归属目标主播"] = target_host

        applied_override_count += 1
        applied_row_count += int(mask.sum())
        if target_account:
            affected_accounts.add(target_account)
        else:
            affected_accounts.update({item for item in out.loc[mask, "最终标准账号"].astype(str).tolist() if item})
        if target_host:
            affected_hosts.add(target_host)
        else:
            affected_hosts.update(
                {item for item in out.loc[mask, "最终本场主播"].astype(str).tolist() if item and item != "【无主线索】"}
            )
        applied_details.append(
            {
                **_override_brief(row, matched_rows=int(mask.sum())),
                "matched_business_subject_keys": sorted(matched["business_subject_key"].dropna().astype(str).unique().tolist())[:10],
                "matched_lead_ids": sorted(matched["线索ID_norm"].dropna().astype(str).unique().tolist())[:10],
                "original_auto": {
                    "standard_accounts": sorted(matched["自动标准账号"].dropna().astype(str).unique().tolist())[:10],
                    "hosts": sorted(matched["自动本场主播"].dropna().astype(str).unique().tolist())[:10],
                    "statuses": sorted(matched["自动归属状态"].dropna().astype(str).unique().tolist())[:10],
                    "unmatched_reasons": sorted(matched["自动无匹配原因"].dropna().astype(str).unique().tolist())[:10],
                },
                "final_consumer": {
                    "target_account": target_account or "",
                    "target_host": target_host or "",
                    "final_status": "匹配成功",
                    "consumers": ["daily_snapshot", "analysis_snapshot", "feishu_report", "tsv_verify"],
                },
            }
        )

    out["标准账号"] = out["最终标准账号"]
    out["本场主播"] = out["最终本场主播"]
    out["归属状态"] = out["最终归属状态"]
    out["无匹配原因"] = out["最终无匹配原因"]
    out = out.drop(columns=["_manual_override_effective_date"])

    summary = {
        "source_path": source_path,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "configured_rows": len(overrides),
        "active_rows": active_count,
        "inactive_rows": inactive_count,
        "revoked_rows": revoked_count,
        "applied_override_count": applied_override_count,
        "applied_row_count": applied_row_count,
        "applied_business_subject_count": int(
            out.loc[out["专项归属是否生效"].astype(bool), "business_subject_key"].dropna().astype(str).nunique()
        ) if "business_subject_key" in out.columns else 0,
        "affected_accounts": sorted(affected_accounts),
        "affected_hosts": sorted(affected_hosts),
        "unmatched_active_overrides": [
            item
            for item in issues
            if str(item.get("issue_type", "")).startswith("unmatched_")
        ],
        "applied_details": applied_details,
        "final_consumer_scope": ["daily_snapshot", "analysis_snapshot", "feishu_report", "tsv_verify"],
        "issues": issues,
        "issue_summary": issue_summary,
    }
    return out, summary


def build_manual_override_manifest(
    *,
    fact_path: Path,
    manual_override_path: Path,
    run_id: str,
) -> dict[str, object]:
    fact = load_fact_with_manual_overrides(fact_path, manual_override_path)
    summary = fact.attrs.get("manual_override_summary", _empty_summary(source_path=str(manual_override_path)))
    return {
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        **summary,
    }


def build_manual_override_issue_manifest(
    *,
    fact_path: Path,
    manual_override_path: Path,
    run_id: str,
) -> dict[str, object]:
    inspection = inspect_manual_attribution_overrides(manual_override_path, run_id=run_id)
    issues = list(inspection.get("issues", []))
    if fact_path.exists() and int(inspection["issue_summary"].get("blocking_count", 0) or 0) == 0:
        fact = pd.read_csv(fact_path)
        governance = inspect_manual_override_application(
            fact=fact,
            overrides=inspection["overrides"],
            source_path=inspection["source_path"],
            run_id=run_id,
        )
        issues.extend(governance["issues"])
        issue_summary = _merge_issue_summary(inspection["issue_summary"], governance["issue_summary"])
        runtime_summary = governance["runtime_summary"]
    else:
        issue_summary = inspection["issue_summary"]
        runtime_summary = _empty_issue_runtime_summary()

    return {
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_path": inspection["source_path"],
        "issue_summary": issue_summary,
        "issues": issues,
        "validation_summary": {
            "summary": inspection.get("summary", ""),
            "warnings": inspection.get("warnings", []),
            "counts": inspection.get("counts", {}),
        },
        "runtime_summary": runtime_summary,
    }


def dump_manual_override_manifest(path: Path, manifest: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def build_manual_override_check(summary: dict[str, object]) -> dict[str, object]:
    issue_summary = summary.get("issue_summary", {}) if isinstance(summary, dict) else {}
    issues = summary.get("issues", []) if isinstance(summary, dict) else []
    blocking = [item for item in issues if str(item.get("severity")) == "blocking"]
    warnings = [item for item in issues if str(item.get("severity")) == "warning"]
    infos = [item for item in issues if str(item.get("severity")) == "info"]
    status = "pass"
    category = "pass"
    if blocking:
        status = "fail"
        category = "contract violation"
    elif warnings:
        status = "warning"
        category = "structural change"
    breaches = [f"{item.get('override_id', '')}: {item.get('message_cn', '')}" for item in blocking + warnings]
    return {
        "name": "manual_override.issues",
        "category": category,
        "status": status,
        "details": {
            "summary": "专项人工确认归属异常治理摘要",
            "source_path": summary.get("source_path", ""),
            "configured_rows": summary.get("configured_rows", 0),
            "active_rows": summary.get("active_rows", 0),
            "applied_override_count": summary.get("applied_override_count", 0),
            "applied_row_count": summary.get("applied_row_count", 0),
            "affected_accounts": summary.get("affected_accounts", []),
            "affected_hosts": summary.get("affected_hosts", []),
            "threshold_breaches": breaches,
            "issue_count": issue_summary.get("issue_count", 0),
            "blocking_count": issue_summary.get("blocking_count", 0),
            "warning_count": issue_summary.get("warning_count", 0),
            "info_count": issue_summary.get("info_count", 0),
            "conflict_count": issue_summary.get("conflict_count", 0),
            "unmatched_count": issue_summary.get("unmatched_count", 0),
            "unmatched_not_in_current_run_count": issue_summary.get("unmatched_not_in_current_run_count", 0),
            "unmatched_probable_misconfig_count": issue_summary.get("unmatched_probable_misconfig_count", 0),
            "unmatched_outside_effective_window_count": issue_summary.get("unmatched_outside_effective_window_count", 0),
            "unmatched_insufficient_locator_count": issue_summary.get("unmatched_insufficient_locator_count", 0),
            "unmatched_needs_manual_review_count": issue_summary.get("unmatched_needs_manual_review_count", 0),
            "partial_apply_count": issue_summary.get("partial_apply_count", 0),
            "invalid_override_count": issue_summary.get("invalid_override_count", 0),
            "risk_count": issue_summary.get("risk_count", 0),
            "override_issue_summary": issue_summary,
            "top_issues_cn": [item.get("message_cn", "") for item in blocking + warnings + infos[:3]],
            "issues": issues,
        },
    }


def inspect_manual_override_application(
    *,
    fact: pd.DataFrame,
    overrides: list[ManualAttributionOverride],
    source_path: str,
    run_id: str = "",
) -> dict[str, object]:
    out = _initialize_trace_columns(fact)
    out["_manual_override_effective_date"] = _effective_date_series(out)
    latest_effective_date = out["_manual_override_effective_date"].max()
    metric_version = _metric_version_from_fact(out)
    issues: list[dict[str, object]] = []
    matched_index_map: dict[str, list[int]] = {}
    matched_briefs: dict[str, dict[str, object]] = {}
    sample_to_override_ids: defaultdict[str, set[str]] = defaultdict(set)

    for override in overrides:
        row = override.to_dict()
        status = normalize_text(row.get("status", "")).lower()
        if status != "active":
            continue
        override_id = str(row.get("override_id", ""))

        if normalize_text(row.get("metric_version", "")) and normalize_text(row.get("metric_version", "")) != metric_version:
            issues.append(
                _issue_dict(
                    issue_type="metric_version_mismatch",
                    severity="blocking",
                    override_id=override_id,
                    row=row,
                    detected_stage="application",
                    message_cn=f"override 的 metric_version={row.get('metric_version', '')} 与当前事实层口径 {metric_version} 不一致。",
                    suggested_action="确认这条专项归属是否仍适用于当前经营口径；不适用则停用或修正 metric_version。",
                    run_id=run_id,
                )
            )
            continue

        base_mask = _locator_mask(out, row, include_effective_window=False)
        effective_mask = _locator_mask(out, row, include_effective_window=True)
        matched_rows = int(effective_mask.sum())
        matched_index_map[override_id] = out.index[effective_mask].tolist()
        matched_briefs[override_id] = _override_brief(row, matched_rows=matched_rows)

        if matched_rows == 0:
            if bool(base_mask.sum()):
                issues.append(
                    _issue_dict(
                        issue_type="unmatched_outside_effective_window",
                        severity="info",
                        override_id=override_id,
                        row=row,
                        detected_stage="application",
                        message_cn="当前主链中能找到对应样本，但样本日期不在该 override 生效区间内，本轮未生效。",
                        suggested_action="如果本轮也要生效，请检查 effective_from / effective_to；如果只是历史专项，可忽略。",
                        run_id=run_id,
                    )
                )
            else:
                issues.append(_build_unmatched_issue(fact=out, row=row, run_id=run_id))
            continue

        matched = out.loc[effective_mask].copy()
        for sample_key in _sample_key_series(matched).astype(str).tolist():
            sample_to_override_ids[sample_key].add(override_id)

        partial_issue = _partial_apply_issue(row=row, run_id=run_id)
        if partial_issue is not None:
            issues.append(partial_issue)

        if pd.notna(latest_effective_date):
            matched_max = matched["_manual_override_effective_date"].max()
            if pd.notna(matched_max) and matched_max < latest_effective_date:
                issues.append(
                    _historical_risk_issue(
                        row=row,
                        run_id=run_id,
                        matched_rows=matched_rows,
                    )
                )

    for sample_key, override_ids in sample_to_override_ids.items():
        if len(override_ids) <= 1:
            continue
        conflict_ids = sorted(override_ids)
        for override_id in conflict_ids:
            row = matched_briefs.get(override_id, {})
            issues.append(
                _issue_dict(
                    issue_type="conflict_override",
                    severity="blocking",
                    override_id=override_id,
                    row=row,
                    detected_stage="application",
                    message_cn=f"override 与其他 active override 同时命中同一样本：{sample_key}，冲突项={conflict_ids}。",
                    suggested_action="保留唯一一条有效 override，或改细定位条件，避免多条配置覆盖同一样本。",
                    run_id=run_id,
                    matched_rows=int(row.get("matched_rows", 0) or 0),
                    conflict_override_ids=[item for item in conflict_ids if item != override_id],
                )
            )

    issue_summary = _issue_summary(issues)
    issue_summary["source_path"] = source_path
    runtime_summary = {
        "matched_override_count": int(sum(1 for indexes in matched_index_map.values() if indexes)),
        "matched_sample_count": int(sum(len(indexes) for indexes in matched_index_map.values())),
        "latest_effective_date": latest_effective_date.strftime("%Y-%m-%d") if pd.notna(latest_effective_date) else "",
    }
    return {
        "issues": issues,
        "issue_summary": issue_summary,
        "matched_index_map": matched_index_map,
        "runtime_summary": runtime_summary,
    }


def _initialize_trace_columns(fact: pd.DataFrame) -> pd.DataFrame:
    out = fact.copy()
    out["线索ID_norm"] = _lead_id_series(out)
    out["手机号"] = _phone_series(out)
    out["自动标准账号"] = _normalize_account_series(out.get("标准账号", pd.Series("", index=out.index)))
    out["自动本场主播"] = _text_series(out.get("本场主播", pd.Series("", index=out.index)))
    out["自动归属状态"] = _text_series(out.get("归属状态", pd.Series("", index=out.index)))
    out["自动无匹配原因"] = _text_series(out.get("无匹配原因", pd.Series("", index=out.index)))
    out["最终标准账号"] = out["自动标准账号"]
    out["最终本场主播"] = out["自动本场主播"]
    out["最终归属状态"] = out["自动归属状态"]
    out["最终无匹配原因"] = out["自动无匹配原因"]
    for column, default in TRACE_COLUMN_DEFAULTS.items():
        if column not in out.columns:
            out[column] = default
    out["最终归属来源"] = "auto"
    out["专项归属是否生效"] = False
    out["标准账号"] = out["最终标准账号"]
    out["本场主播"] = out["最终本场主播"]
    out["归属状态"] = out["最终归属状态"]
    out["无匹配原因"] = out["最终无匹配原因"]
    return out


def _locator_mask(fact: pd.DataFrame, row: dict[str, object], *, include_effective_window: bool = True) -> pd.Series:
    mask = pd.Series(True, index=fact.index)
    has_locator = False

    business_subject_key = normalize_text(row.get("business_subject_key", ""))
    if business_subject_key:
        has_locator = True
        mask &= fact.get("business_subject_key", pd.Series("", index=fact.index)).astype(str).str.strip().eq(business_subject_key)

    phone = _normalize_phone(row.get("phone", ""))
    if phone:
        has_locator = True
        mask &= _phone_series(fact).eq(phone)

    lead_id = normalize_text(row.get("lead_id", ""))
    if lead_id:
        has_locator = True
        mask &= _lead_id_series(fact).eq(lead_id)

    if not has_locator:
        return pd.Series(False, index=fact.index)

    if include_effective_window:
        effective_from = normalize_text(row.get("effective_from", ""))
        if effective_from:
            mask &= fact["_manual_override_effective_date"] >= pd.to_datetime(effective_from, errors="coerce")
        effective_to = normalize_text(row.get("effective_to", ""))
        if effective_to:
            mask &= fact["_manual_override_effective_date"] <= pd.to_datetime(effective_to, errors="coerce")
    return mask.fillna(False)


def _effective_date_series(fact: pd.DataFrame) -> pd.Series:
    if "线索创建时间" in fact.columns:
        created = pd.to_datetime(fact["线索创建时间"], errors="coerce").dt.normalize()
    else:
        created = pd.Series(pd.NaT, index=fact.index)
    if "date" in fact.columns:
        fallback = pd.to_datetime(fact["date"], errors="coerce").dt.normalize()
        return created.fillna(fallback)
    return created


def _override_brief(row: dict[str, object], *, matched_rows: int) -> dict[str, object]:
    return {
        "override_id": row.get("override_id", ""),
        "override_scope": row.get("override_scope", ""),
        "reason": row.get("reason", ""),
        "evidence_note": row.get("evidence_note", ""),
        "confirmed_by": row.get("confirmed_by", ""),
        "confirmed_at": row.get("confirmed_at", ""),
        "business_subject_key": row.get("business_subject_key", ""),
        "phone": row.get("phone", ""),
        "lead_id": row.get("lead_id", ""),
        "target_account": row.get("target_account", ""),
        "target_host": row.get("target_host", ""),
        "matched_rows": matched_rows,
    }


def _empty_summary(*, source_path: str) -> dict[str, object]:
    return {
        "source_path": source_path,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "configured_rows": 0,
        "active_rows": 0,
        "inactive_rows": 0,
        "revoked_rows": 0,
        "applied_override_count": 0,
        "applied_row_count": 0,
        "applied_business_subject_count": 0,
        "affected_accounts": [],
        "affected_hosts": [],
        "unmatched_active_overrides": [],
        "applied_details": [],
        "final_consumer_scope": ["daily_snapshot", "analysis_snapshot", "feishu_report", "tsv_verify"],
        "issues": [],
        "issue_summary": _empty_issue_summary(),
    }


def _lead_id_series(frame: pd.DataFrame) -> pd.Series:
    if "线索ID_norm" in frame.columns:
        return frame["线索ID_norm"].astype(str).str.strip()
    if "线索ID" in frame.columns:
        return frame["线索ID"].astype(str).str.strip()
    return pd.Series("", index=frame.index)


def _phone_series(frame: pd.DataFrame) -> pd.Series:
    if "手机号" not in frame.columns:
        return pd.Series("", index=frame.index)
    return frame["手机号"].map(_normalize_phone)


def _normalize_phone(value: object) -> str:
    text = normalize_text(value)
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or text


def _normalize_account_series(series: pd.Series) -> pd.Series:
    return series.map(lambda value: normalize_account(value) if normalize_text(value) else "")


def _text_series(series: pd.Series) -> pd.Series:
    return series.map(normalize_text)


def _sample_key_series(frame: pd.DataFrame) -> pd.Series:
    if "business_subject_key" in frame.columns:
        business_subject = frame["business_subject_key"].astype(str).str.strip()
        if business_subject.ne("").any():
            return business_subject
    return _lead_id_series(frame)


def _metric_version_from_fact(frame: pd.DataFrame) -> str:
    if "metric_version" not in frame.columns:
        return ""
    values = frame["metric_version"].dropna().astype(str).str.strip()
    values = values[values.ne("")]
    return values.iloc[0] if not values.empty else ""


def _partial_apply_issue(*, row: dict[str, object], run_id: str) -> dict[str, object] | None:
    target_account = normalize_text(row.get("target_account", ""))
    target_host = normalize_text(row.get("target_host", ""))
    scope = normalize_text(row.get("override_scope", "")).lower()
    if target_account and target_host:
        return None
    if not target_account and not target_host:
        return None
    severity = "warning" if scope == "account_host" else "info"
    if target_account and not target_host:
        message = "该 override 只覆盖账号，主播仍沿用自动归因结果。"
        action = "如果业务上主播也已明确，请补充 target_host；否则该提示可保留。"
    else:
        message = "该 override 只覆盖主播，账号仍沿用自动归因结果。"
        action = "如果业务上账号也已明确，请补充 target_account；否则该提示可保留。"
    return _issue_dict(
        issue_type="partial_apply",
        severity=severity,
        override_id=str(row.get("override_id", "")),
        row=row,
        detected_stage="application",
        message_cn=message,
        suggested_action=action,
        run_id=run_id,
    )


def _historical_risk_issue(
    *,
    row: dict[str, object],
    run_id: str,
    matched_rows: int,
) -> dict[str, object]:
    target_account = normalize_account(row.get("target_account", "")) if normalize_text(row.get("target_account", "")) else ""
    target_host = normalize_text(row.get("target_host", ""))
    override_id = str(row.get("override_id", ""))
    if target_account and target_host:
        return _issue_dict(
            issue_type="historical_account_host_risk",
            severity="info",
            override_id=override_id,
            row=row,
            detected_stage="application",
            message_cn="该 override 只命中历史样本，会改动账号/主播累计口径，同时需要注意主播 latest 标签解释风险。",
            suggested_action="如果今天需要解释账号累计变化和主播 latest 标签，请同步核对最新日期是否也需要补专项归属。",
            run_id=run_id,
            matched_rows=matched_rows,
        )
    if target_host:
        return _issue_dict(
            issue_type="historical_host_latest_explain_risk",
            severity="info",
            override_id=override_id,
            row=row,
            detected_stage="application",
            message_cn="该 override 只命中历史样本，主要影响主播累计口径，并可能让今日主播 latest 标签解释不完全一致。",
            suggested_action="如果今天需要解释主播 latest 面板，请核对最新日期是否也需要补这条专项归属。",
            run_id=run_id,
            matched_rows=matched_rows,
        )
    if target_account:
        return _issue_dict(
            issue_type="historical_account_cumulative_risk",
            severity="info",
            override_id=override_id,
            row=row,
            detected_stage="application",
            message_cn="该 override 只命中历史样本，主要影响账号累计口径，对今日 latest 标签影响有限。",
            suggested_action="如只看累计可延后处理；如今天要解释账号累计变化，请核对最新日期是否也需要补专项归属。",
            run_id=run_id,
            matched_rows=matched_rows,
        )
    return _issue_dict(
        issue_type="historical_general_risk",
        severity="info",
        override_id=override_id,
        row=row,
        detected_stage="application",
        message_cn="该 override 只命中历史样本，会影响累计结果，但当前无法进一步判断更偏账号还是主播解释风险。",
        suggested_action="如业务上需要解释今天结果，请补充更明确的目标账号/主播，或人工核对最新日期是否也要生效。",
        run_id=run_id,
        matched_rows=matched_rows,
    )


def _build_unmatched_issue(
    *,
    fact: pd.DataFrame,
    row: dict[str, object],
    run_id: str,
) -> dict[str, object]:
    diagnostics = _locator_diagnostics(fact, row)
    locator_fields = diagnostics["locator_fields"]
    bad_fields = diagnostics["malformed_locator_fields"]
    partially_matched_fields = diagnostics["individually_matched_fields"]

    if locator_fields == ["business_subject_key"]:
        return _issue_dict(
            issue_type="unmatched_insufficient_locator",
            severity="warning",
            override_id=str(row.get("override_id", "")),
            row=row,
            detected_stage="application",
            message_cn="当前只提供 business_subject_key 且本轮未命中，定位信息不足，无法可靠判断样本本轮不存在还是配置写错。",
            suggested_action="优先补手机号或唯一线索ID，再决定是否继续保留这条专项归属。",
            run_id=run_id,
        )

    if bad_fields:
        field_text = "、".join(bad_fields)
        return _issue_dict(
            issue_type="unmatched_probable_misconfig",
            severity="warning",
            override_id=str(row.get("override_id", "")),
            row=row,
            detected_stage="application",
            message_cn=f"当前未命中且 {field_text} 格式可疑，高概率是专项归属配置误填。",
            suggested_action=f"优先检查 {field_text} 的填写格式是否正确，修正后再跑正式口径。",
            run_id=run_id,
        )

    if partially_matched_fields:
        field_text = "、".join(partially_matched_fields)
        return _issue_dict(
            issue_type="unmatched_probable_misconfig",
            severity="warning",
            override_id=str(row.get("override_id", "")),
            row=row,
            detected_stage="application",
            message_cn=f"当前未命中，但 {field_text} 单独看能在本轮找到，高概率是多定位键之间有一项写错。",
            suggested_action="优先核对能命中的定位键与不能命中的定位键，通常是手机号 / 唯一线索ID / business_subject_key 其中一项不一致。",
            run_id=run_id,
        )

    if diagnostics["single_reliable_locator"]:
        return _issue_dict(
            issue_type="unmatched_not_in_current_run",
            severity="info",
            override_id=str(row.get("override_id", "")),
            row=row,
            detected_stage="application",
            message_cn="当前未命中，但定位字段格式正常，且本轮看起来就是没有这条业务样本。",
            suggested_action="如果这条专项本来就是历史样本，可暂不处理；如果业务上确认本轮应该出现，再人工核查源数据。",
            run_id=run_id,
        )

    if diagnostics["all_locator_fields_valid"] and not diagnostics["any_individual_match"]:
        return _issue_dict(
            issue_type="unmatched_not_in_current_run",
            severity="info",
            override_id=str(row.get("override_id", "")),
            row=row,
            detected_stage="application",
            message_cn="当前未命中，且所有定位字段都像是正常值，本轮大概率本来就没有这条业务样本。",
            suggested_action="如果这是历史专项或未来专项，可先忽略；如果业务确认本轮应该存在，再回头核数据来源。",
            run_id=run_id,
        )

    return _issue_dict(
        issue_type="unmatched_needs_manual_review",
        severity="warning",
        override_id=str(row.get("override_id", "")),
        row=row,
        detected_stage="application",
        message_cn="当前未命中，系统无法自动判断是样本本轮不存在还是专项配置有误，建议当天人工核实。",
        suggested_action="优先核对手机号、唯一线索ID、business_subject_key 与业务日期；必要时回看登记表或业务确认记录。",
        run_id=run_id,
    )


def _locator_diagnostics(fact: pd.DataFrame, row: dict[str, object]) -> dict[str, object]:
    business_subject_key = normalize_text(row.get("business_subject_key", ""))
    phone = _normalize_phone(row.get("phone", ""))
    lead_id = normalize_text(row.get("lead_id", ""))

    locator_fields = [
        field
        for field, value in [
            ("business_subject_key", business_subject_key),
            ("phone", phone),
            ("lead_id", lead_id),
        ]
        if value
    ]

    matched_fields: list[str] = []
    if business_subject_key:
        business_subject_series = fact.get("business_subject_key", pd.Series("", index=fact.index)).astype(str).str.strip()
        if bool(business_subject_series.eq(business_subject_key).any()):
            matched_fields.append("business_subject_key")
    if phone and bool(_phone_series(fact).eq(phone).any()):
        matched_fields.append("phone")
    if lead_id and bool(_lead_id_series(fact).eq(lead_id).any()):
        matched_fields.append("lead_id")

    malformed_fields: list[str] = []
    if phone and not _looks_like_valid_phone(phone):
        malformed_fields.append("手机号")
    if lead_id and not _looks_like_valid_lead_id(lead_id):
        malformed_fields.append("唯一线索ID")
    if business_subject_key and not _looks_like_valid_business_subject_key(business_subject_key):
        malformed_fields.append("business_subject_key")

    single_reliable_locator = len(locator_fields) == 1 and locator_fields[0] in {"phone", "lead_id"} and not malformed_fields
    return {
        "locator_fields": locator_fields,
        "individually_matched_fields": matched_fields,
        "any_individual_match": bool(matched_fields),
        "malformed_locator_fields": malformed_fields,
        "single_reliable_locator": single_reliable_locator,
        "all_locator_fields_valid": bool(locator_fields) and not malformed_fields,
    }


def _looks_like_valid_phone(phone: str) -> bool:
    return phone.isdigit() and len(phone) == 11


def _looks_like_valid_lead_id(lead_id: str) -> bool:
    lead_id = lead_id.strip()
    if not lead_id:
        return False
    return lead_id.upper().startswith("ID") and len(lead_id) >= 10


def _looks_like_valid_business_subject_key(value: str) -> bool:
    value = value.strip()
    if not value:
        return False
    return ":" in value or value.upper().startswith("PHONE")


def _issue_dict(
    *,
    issue_type: str,
    severity: str,
    override_id: str,
    row: dict[str, object],
    detected_stage: str,
    message_cn: str,
    suggested_action: str,
    run_id: str,
    matched_rows: int = 0,
    conflict_override_ids: list[str] | None = None,
) -> dict[str, object]:
    issue = ManualOverrideIssue(
        issue_id=f"manual-override-{detected_stage}-{issue_type}-{override_id or 'unknown'}",
        issue_type=issue_type,
        severity=severity,
        override_id=override_id,
        business_subject_key=normalize_text(row.get("business_subject_key", "")),
        phone=_normalize_phone(row.get("phone", "")),
        lead_id=normalize_text(row.get("lead_id", "")),
        detected_stage=detected_stage,
        message_cn=message_cn,
        suggested_action=suggested_action,
        status="open",
        run_id=run_id,
        matched_rows=int(matched_rows),
        conflict_override_ids=conflict_override_ids or [],
        target_account=normalize_account(row.get("target_account", "")) if normalize_text(row.get("target_account", "")) else "",
        target_host=normalize_text(row.get("target_host", "")),
    )
    return issue.to_dict()


def _issue_summary(issues: list[dict[str, object]]) -> dict[str, object]:
    by_type: defaultdict[str, int] = defaultdict(int)
    for item in issues:
        by_type[str(item.get("issue_type", ""))] += 1
    blocking = [item for item in issues if str(item.get("severity")) == "blocking"]
    warning = [item for item in issues if str(item.get("severity")) == "warning"]
    info = [item for item in issues if str(item.get("severity")) == "info"]
    return {
        "issue_count": len(issues),
        "blocking_count": len(blocking),
        "warning_count": len(warning),
        "info_count": len(info),
        "conflict_count": by_type.get("conflict_override", 0),
        "unmatched_count": (
            by_type.get("unmatched_not_in_current_run", 0)
            + by_type.get("unmatched_probable_misconfig", 0)
            + by_type.get("unmatched_outside_effective_window", 0)
            + by_type.get("unmatched_insufficient_locator", 0)
            + by_type.get("unmatched_needs_manual_review", 0)
        ),
        "unmatched_not_in_current_run_count": by_type.get("unmatched_not_in_current_run", 0),
        "unmatched_probable_misconfig_count": by_type.get("unmatched_probable_misconfig", 0),
        "unmatched_outside_effective_window_count": by_type.get("unmatched_outside_effective_window", 0),
        "unmatched_insufficient_locator_count": by_type.get("unmatched_insufficient_locator", 0),
        "unmatched_needs_manual_review_count": by_type.get("unmatched_needs_manual_review", 0),
        "partial_apply_count": by_type.get("partial_apply", 0),
        "invalid_override_count": (
            by_type.get("missing_override_file", 0)
            + by_type.get("missing_columns", 0)
            + by_type.get("invalid_status", 0)
            + by_type.get("invalid_scope", 0)
            + by_type.get("missing_locator", 0)
            + by_type.get("missing_target", 0)
            + by_type.get("missing_required_field", 0)
            + by_type.get("missing_metric_version", 0)
            + by_type.get("invalid_confirmed_at", 0)
            + by_type.get("invalid_effective_from", 0)
            + by_type.get("invalid_effective_to", 0)
            + by_type.get("invalid_effective_window", 0)
            + by_type.get("duplicate_override_id", 0)
            + by_type.get("metric_version_mismatch", 0)
        ),
        "risk_count": (
            by_type.get("historical_account_cumulative_risk", 0)
            + by_type.get("historical_host_latest_explain_risk", 0)
            + by_type.get("historical_account_host_risk", 0)
            + by_type.get("historical_general_risk", 0)
        ),
        "top_blocking_messages": [str(item.get("message_cn", "")) for item in blocking[:5]],
        "top_warning_messages": [str(item.get("message_cn", "")) for item in warning[:5]],
    }


def _empty_issue_summary() -> dict[str, object]:
    return {
        "issue_count": 0,
        "blocking_count": 0,
        "warning_count": 0,
        "info_count": 0,
        "conflict_count": 0,
        "unmatched_count": 0,
        "unmatched_not_in_current_run_count": 0,
        "unmatched_probable_misconfig_count": 0,
        "unmatched_outside_effective_window_count": 0,
        "unmatched_insufficient_locator_count": 0,
        "unmatched_needs_manual_review_count": 0,
        "partial_apply_count": 0,
        "invalid_override_count": 0,
        "risk_count": 0,
        "top_blocking_messages": [],
        "top_warning_messages": [],
    }


def _empty_issue_runtime_summary() -> dict[str, object]:
    return {
        "matched_override_count": 0,
        "matched_sample_count": 0,
        "latest_effective_date": "",
    }


def _merge_issue_summary(base: dict[str, object], extra: dict[str, object]) -> dict[str, object]:
    merged = _empty_issue_summary()
    for key in [
        "issue_count",
        "blocking_count",
        "warning_count",
        "info_count",
        "conflict_count",
        "unmatched_count",
        "unmatched_not_in_current_run_count",
        "unmatched_probable_misconfig_count",
        "unmatched_outside_effective_window_count",
        "unmatched_insufficient_locator_count",
        "unmatched_needs_manual_review_count",
        "partial_apply_count",
        "invalid_override_count",
        "risk_count",
    ]:
        merged[key] = int(base.get(key, 0) or 0) + int(extra.get(key, 0) or 0)
    merged["top_blocking_messages"] = list(
        dict.fromkeys((base.get("top_blocking_messages", []) or []) + (extra.get("top_blocking_messages", []) or []))
    )[:5]
    merged["top_warning_messages"] = list(
        dict.fromkeys((base.get("top_warning_messages", []) or []) + (extra.get("top_warning_messages", []) or []))
    )[:5]
    return merged


def _mask_from_indexes(index: pd.Index, matched_indexes: list[int]) -> pd.Series:
    return pd.Series(index.isin(matched_indexes), index=index)
