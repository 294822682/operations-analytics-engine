"""Validate manual attribution override rows."""

from __future__ import annotations

from typing import Any

import pandas as pd

from oae.contracts.models import ManualOverrideIssue


VALID_OVERRIDE_SCOPES = {"account", "host", "account_host", "channel", "other"}
VALID_STATUSES = {"active", "inactive", "revoked"}


def validate_manual_override_frame(frame: pd.DataFrame, *, run_id: str = "") -> dict[str, object]:
    rows = frame.copy()
    errors: list[str] = []
    warnings: list[str] = []
    issues: list[ManualOverrideIssue] = []

    if rows.empty:
        return {
            "status": "pass",
            "summary": "专项人工确认归属为空模板",
            "warnings": [],
            "errors": [],
            "issues": [],
            "normalized": rows,
            "counts": {"configured": 0, "active": 0, "inactive": 0, "revoked": 0},
        }

    for idx, row in rows.iterrows():
        row_no = idx + 2
        override_id = _normalize_text(row.get("override_id", "")) or f"row-{row_no}"
        status = _normalize_text(row.get("status", "")).lower() or "active"
        if not _normalize_text(row.get("status", "")):
            warnings.append(f"第 {row_no} 行 status 为空，按 active 处理")
            issues.append(
                _issue(
                    issue_type="status_defaulted",
                    severity="info",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn="status 为空，系统已按 active 处理。",
                    suggested_action="如需停用或撤销，请明确填写 active / inactive / revoked。",
                    run_id=run_id,
                )
            )
        if status not in VALID_STATUSES:
            errors.append(f"第 {row_no} 行 status 非法：{status}，可选 {sorted(VALID_STATUSES)}")
            issues.append(
                _issue(
                    issue_type="invalid_status",
                    severity="blocking",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn=f"status 非法：{status}，可选值为 {sorted(VALID_STATUSES)}。",
                    suggested_action="把 status 改成 active / inactive / revoked 之一。",
                    run_id=run_id,
                )
            )
        rows.at[idx, "status"] = status

        target_account = _normalize_text(row.get("target_account", ""))
        target_host = _normalize_text(row.get("target_host", ""))
        inferred_scope = _normalize_scope(_normalize_text(row.get("override_scope", "")), target_account, target_host)
        if inferred_scope not in VALID_OVERRIDE_SCOPES:
            errors.append(f"第 {row_no} 行 override_scope 非法：{row.get('override_scope', '')}")
            issues.append(
                _issue(
                    issue_type="invalid_scope",
                    severity="blocking",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn=f"override_scope 非法：{row.get('override_scope', '')}。",
                    suggested_action="把 override_scope 改成 account / host / account_host / channel / other。",
                    run_id=run_id,
                )
            )
        rows.at[idx, "override_scope"] = inferred_scope

        if not any(
            _normalize_text(row.get(col, ""))
            for col in ["business_subject_key", "phone", "lead_id"]
        ):
            errors.append(f"第 {row_no} 行缺少定位字段：business_subject_key / phone / lead_id 至少填一个")
            issues.append(
                _issue(
                    issue_type="missing_locator",
                    severity="blocking",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn="缺少定位字段：business_subject_key / phone / lead_id 至少填一个。",
                    suggested_action="至少补充手机号、唯一线索ID、或 business_subject_key 之一。",
                    run_id=run_id,
                )
            )

        if not target_account and not target_host:
            errors.append(f"第 {row_no} 行缺少目标归属：target_account / target_host 至少填一个")
            issues.append(
                _issue(
                    issue_type="missing_target",
                    severity="blocking",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn="缺少目标归属：target_account / target_host 至少填一个。",
                    suggested_action="至少补充目标账号或目标主播其一。",
                    run_id=run_id,
                )
            )

        if status == "active":
            for field in ["override_id", "reason", "confirmed_by", "confirmed_at"]:
                if not _normalize_text(row.get(field, "")):
                    errors.append(f"第 {row_no} 行缺少必填字段：{field}")
                    issues.append(
                        _issue(
                            issue_type="missing_required_field",
                            severity="blocking",
                            row_no=row_no,
                            override_id=override_id,
                            row=row,
                            detected_stage="validation",
                            message_cn=f"缺少必填字段：{field}。",
                            suggested_action=f"补充 {field} 后再执行正式口径。",
                            run_id=run_id,
                        )
                    )
            if not _normalize_text(row.get("metric_version", "")):
                errors.append(f"第 {row_no} 行缺少必填字段：metric_version")
                issues.append(
                    _issue(
                        issue_type="missing_metric_version",
                        severity="blocking",
                        row_no=row_no,
                        override_id=override_id,
                        row=row,
                        detected_stage="validation",
                        message_cn="active override 缺少 metric_version。",
                        suggested_action="补充当前经营口径版本，避免把旧专项配置误用到新口径。",
                        run_id=run_id,
                    )
                )

        confirmed_at = _parse_datetime(row.get("confirmed_at", ""))
        if _normalize_text(row.get("confirmed_at", "")) and confirmed_at is None:
            errors.append(f"第 {row_no} 行 confirmed_at 无法识别：{row.get('confirmed_at', '')}")
            issues.append(
                _issue(
                    issue_type="invalid_confirmed_at",
                    severity="blocking",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn=f"confirmed_at 无法识别：{row.get('confirmed_at', '')}。",
                    suggested_action="改成 YYYY-MM-DD HH:MM:SS 或 Excel 可识别时间。",
                    run_id=run_id,
                )
            )
        if confirmed_at is not None:
            rows.at[idx, "confirmed_at"] = confirmed_at.strftime("%Y-%m-%d %H:%M:%S")

        effective_from = _parse_date(row.get("effective_from", ""))
        if _normalize_text(row.get("effective_from", "")) and effective_from is None:
            errors.append(f"第 {row_no} 行 effective_from 无法识别：{row.get('effective_from', '')}")
            issues.append(
                _issue(
                    issue_type="invalid_effective_from",
                    severity="blocking",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn=f"effective_from 无法识别：{row.get('effective_from', '')}。",
                    suggested_action="改成 YYYY-MM-DD 或 Excel 可识别日期。",
                    run_id=run_id,
                )
            )
        if effective_from is not None:
            rows.at[idx, "effective_from"] = effective_from.strftime("%Y-%m-%d")
        else:
            rows.at[idx, "effective_from"] = _normalize_text(row.get("effective_from", ""))

        effective_to = _parse_date(row.get("effective_to", ""))
        if _normalize_text(row.get("effective_to", "")) and effective_to is None:
            errors.append(f"第 {row_no} 行 effective_to 无法识别：{row.get('effective_to', '')}")
            issues.append(
                _issue(
                    issue_type="invalid_effective_to",
                    severity="blocking",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn=f"effective_to 无法识别：{row.get('effective_to', '')}。",
                    suggested_action="改成 YYYY-MM-DD 或 Excel 可识别日期。",
                    run_id=run_id,
                )
            )
        if effective_to is not None:
            rows.at[idx, "effective_to"] = effective_to.strftime("%Y-%m-%d")
        else:
            rows.at[idx, "effective_to"] = _normalize_text(row.get("effective_to", ""))

        if effective_from is not None and effective_to is not None and effective_to < effective_from:
            errors.append(f"第 {row_no} 行 effective_to 早于 effective_from")
            issues.append(
                _issue(
                    issue_type="invalid_effective_window",
                    severity="blocking",
                    row_no=row_no,
                    override_id=override_id,
                    row=row,
                    detected_stage="validation",
                    message_cn="effective_to 早于 effective_from。",
                    suggested_action="调整生效起止日期，确保结束日期不早于开始日期。",
                    run_id=run_id,
                )
            )

    duplicate_ids = (
        rows.loc[rows["status"] == "active", "override_id"]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
    )
    duplicated = duplicate_ids[duplicate_ids.duplicated()].unique().tolist()
    if duplicated:
        errors.append(f"存在重复的 active override_id：{duplicated}")
        for override_id in duplicated:
            issues.append(
                ManualOverrideIssue(
                    issue_id=f"manual-override-validation-duplicate-id-{override_id}",
                    issue_type="duplicate_override_id",
                    severity="blocking",
                    override_id=override_id,
                    business_subject_key="",
                    phone="",
                    lead_id="",
                    detected_stage="validation",
                    message_cn=f"存在重复的 active override_id：{override_id}。",
                    suggested_action="给每条 active override 配置唯一的 override_id。",
                    status="open",
                    run_id=run_id,
                    target_account="",
                    target_host="",
                )
            )

    counts = {
        "configured": int(len(rows)),
        "active": int((rows["status"] == "active").sum()),
        "inactive": int((rows["status"] == "inactive").sum()),
        "revoked": int((rows["status"] == "revoked").sum()),
    }
    return {
        "status": "fail" if errors else "pass",
        "summary": "专项人工确认归属校验通过" if not errors else "专项人工确认归属校验失败",
        "warnings": warnings,
        "errors": errors,
        "issues": [item.to_dict() for item in issues],
        "normalized": rows,
        "counts": counts,
    }


def _normalize_scope(raw_scope: str, target_account: str, target_host: str) -> str:
    if raw_scope:
        return raw_scope.lower()
    if target_account and target_host:
        return "account_host"
    if target_account:
        return "account"
    if target_host:
        return "host"
    return "other"


def _parse_datetime(value: Any) -> pd.Timestamp | None:
    text = _normalize_text(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    return None if pd.isna(parsed) else parsed


def _parse_date(value: Any) -> pd.Timestamp | None:
    parsed = _parse_datetime(value)
    return None if parsed is None else parsed.normalize()


def _normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _issue(
    *,
    issue_type: str,
    severity: str,
    row_no: int,
    override_id: str,
    row: pd.Series,
    detected_stage: str,
    message_cn: str,
    suggested_action: str,
    run_id: str,
) -> ManualOverrideIssue:
    return ManualOverrideIssue(
        issue_id=f"manual-override-{detected_stage}-{issue_type}-row-{row_no}",
        issue_type=issue_type,
        severity=severity,
        override_id=override_id,
        business_subject_key=_normalize_text(row.get("business_subject_key", "")),
        phone=_normalize_text(row.get("phone", "")),
        lead_id=_normalize_text(row.get("lead_id", "")),
        detected_stage=detected_stage,
        message_cn=message_cn,
        suggested_action=suggested_action,
        status="open",
        run_id=run_id,
        target_account=_normalize_text(row.get("target_account", "")),
        target_host=_normalize_text(row.get("target_host", "")),
    )
