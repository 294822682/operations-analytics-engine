"""Business-level reconciliation checks with operator-friendly summaries."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from oae.analysis.raw_evidence import RAW_EVIDENCE_TOPICS, summarize_raw_evidence_topics
from oae.contracts import CONTRACT_SCHEMAS
from oae.quality.config import load_quality_thresholds
from oae.quality.contracts import check_export_manifest_contracts


def run_business_quality_checks(
    *,
    fact_path: Path,
    snapshot_path: Path,
    ledger_path: Path,
    analysis_snapshot_path: Path,
    export_manifest_paths: list[Path],
    baseline_reference_dir: Path,
    expected_schema_version: str,
    expected_metric_version: str,
    expected_template_version: str,
    expected_run_id: str,
    expected_freeze_id: str,
    quality_thresholds_path: Path | None = None,
    quality_threshold_profile: str = "operational",
) -> list[dict[str, object]]:
    thresholds, threshold_source, threshold_profile = load_quality_thresholds(
        quality_thresholds_path,
        profile=quality_threshold_profile,
    )
    checks = []
    checks.extend(check_fact_layer(fact_path, baseline_reference_dir / "fact_attribution.csv", thresholds=thresholds["fact"], threshold_source=threshold_source, threshold_profile=threshold_profile))
    checks.extend(check_snapshot_layer(snapshot_path, baseline_reference_dir / "daily_goal_account_latest_2026-03-12.csv", thresholds=thresholds["snapshot"], threshold_source=threshold_source, threshold_profile=threshold_profile))
    checks.extend(check_ledger_layer(ledger_path, snapshot_path, thresholds=thresholds["ledger"], threshold_source=threshold_source, threshold_profile=threshold_profile))
    checks.extend(
        check_export_contracts(
            export_manifest_paths,
            expected_schema_version=expected_schema_version,
            expected_metric_version=expected_metric_version,
            expected_template_version=expected_template_version,
            expected_run_id=expected_run_id,
            expected_freeze_id=expected_freeze_id,
        )
    )
    checks.extend(check_analysis_snapshot(analysis_snapshot_path, thresholds=thresholds["analysis"], threshold_source=threshold_source, threshold_profile=threshold_profile))
    return checks


def check_fact_layer(
    fact_path: Path,
    baseline_fact_path: Path,
    *,
    thresholds: dict[str, dict[str, object]],
    threshold_source: str,
    threshold_profile: str,
) -> list[dict[str, object]]:
    fact = pd.read_csv(fact_path)
    checks = []
    row_count = int(len(fact))
    unique_keys = int(fact["_lead_key"].nunique()) if "_lead_key" in fact.columns else 0
    current_metrics = {
        "row_count": row_count,
        "unique_lead_key_count": unique_keys,
        "business_subject_key_coverage": _nonempty_ratio(fact.get("business_subject_key")),
        "phone_missing_rate": _blank_ratio(fact.get("手机号")),
        "unowned_ratio": _ratio((fact.get("归属状态", pd.Series(dtype="object")) == "无主线索").sum(), row_count),
        "attribution_success_rate": _ratio((fact.get("归属状态", pd.Series(dtype="object")) == "匹配成功").sum(), row_count),
    }
    structural_status = "pass" if row_count > 0 and unique_keys == row_count else "fail"
    structural_alerts = []
    if row_count <= 0:
        structural_alerts.append("事实层为空")
    if unique_keys != row_count:
        structural_alerts.append("手机号优先主键不唯一")
    checks.append(
        _check(
            "fact.structural",
            "structural change",
            structural_status,
            {
                "summary": "事实层结构完整性检查",
                "metrics": current_metrics,
                "threshold_breaches": structural_alerts,
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
            },
        )
    )

    if baseline_fact_path.exists():
        baseline = pd.read_csv(baseline_fact_path)
        baseline_metrics = {
            "row_count": int(len(baseline)),
            "phone_missing_rate": _blank_ratio(baseline.get("手机号")),
            "unowned_ratio": _ratio((baseline.get("归属状态", pd.Series(dtype="object")) == "无主线索").sum(), len(baseline)),
            "attribution_success_rate": _ratio((baseline.get("归属状态", pd.Series(dtype="object")) == "匹配成功").sum(), len(baseline)),
        }
        drift, breaches, status = _evaluate_thresholds(current_metrics, baseline_metrics, thresholds)
        checks.append(
            _check(
                "fact.baseline_metrics",
                "metric drift",
                status,
                {
                    "summary": "事实层核心经营指标对比 baseline",
                    "current_metrics": current_metrics,
                    "baseline_metrics": baseline_metrics,
                    "drift_metrics": drift,
                    "threshold_breaches": breaches,
                    "threshold_source": threshold_source,
                    "threshold_profile": threshold_profile,
                    "threshold_rule": thresholds,
                },
            )
        )
    return checks


def check_snapshot_layer(
    snapshot_path: Path,
    baseline_account_latest_path: Path,
    *,
    thresholds: dict[str, dict[str, object]],
    threshold_source: str,
    threshold_profile: str,
) -> list[dict[str, object]]:
    snapshot = pd.read_csv(snapshot_path)
    account = snapshot[snapshot["scope_type"].isin(["account", "account_total"])].copy()
    account_rows = account[account["scope_type"] == "account"].copy()
    target_accounts = account_rows[pd.to_numeric(account_rows["lead_target_month"], errors="coerce").fillna(0) > 0].copy()
    anchor = snapshot[snapshot["scope_type"] == "anchor"].copy()
    total_row = account[account["scope_name"] == "线索组汇总"]
    total = total_row.iloc[0] if not total_row.empty else None

    checks = []
    snapshot_metrics = {
        "total_mtd_leads": float(total["mtd_leads"]) if total is not None else float(target_accounts["mtd_leads"].sum()),
        "total_mtd_deals": float(total["mtd_deals"]) if total is not None else float(target_accounts["mtd_deals"].sum()),
        "total_mtd_spend": float(total["mtd_spend"]) if total is not None else float(target_accounts["mtd_spend"].sum()),
        "target_account_mtd_leads_sum": float(target_accounts["mtd_leads"].sum()),
        "target_account_mtd_deals_sum": float(target_accounts["mtd_deals"].sum()),
        "target_account_mtd_spend_sum": float(target_accounts["mtd_spend"].sum()),
        "anchor_mtd_leads_sum": float(anchor["mtd_leads"].sum()),
        "anchor_mtd_deals_sum": float(anchor["mtd_deals"].sum()),
        "anchor_mtd_spend_sum": float(anchor["mtd_spend"].sum()),
        "mtd_lead_attain": float(total["mtd_lead_attain"]) if total is not None and "mtd_lead_attain" in total else 0.0,
        "mtd_deal_attain": float(total["mtd_deal_attain"]) if total is not None and "mtd_deal_attain" in total else 0.0,
    }
    total_matches = (
        _close_enough(snapshot_metrics["total_mtd_leads"], snapshot_metrics["target_account_mtd_leads_sum"], tolerance=1e-6)
        and _close_enough(snapshot_metrics["total_mtd_deals"], snapshot_metrics["target_account_mtd_deals_sum"], tolerance=1e-6)
        and _close_enough(snapshot_metrics["total_mtd_spend"], snapshot_metrics["target_account_mtd_spend_sum"], tolerance=1e-6)
    )
    checks.append(
        _check(
            "snapshot.account_total_reconcile",
            "structural change",
            "pass" if total_matches else "fail",
            {
                "summary": "账号层汇总与总计行对账",
                "metrics": snapshot_metrics,
                "threshold_breaches": [] if total_matches else ["账号层汇总与总计行不一致"],
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
            },
        )
    )

    coverage = {
        "anchor_lead_coverage": _ratio(snapshot_metrics["anchor_mtd_leads_sum"], snapshot_metrics["total_mtd_leads"]),
        "anchor_deal_coverage": _ratio(snapshot_metrics["anchor_mtd_deals_sum"], snapshot_metrics["total_mtd_deals"]),
        "anchor_spend_coverage": _ratio(snapshot_metrics["anchor_mtd_spend_sum"], snapshot_metrics["total_mtd_spend"]),
    }
    coverage_alerts = []
    if coverage["anchor_lead_coverage"] < 0.5 or coverage["anchor_lead_coverage"] > 1.05:
        coverage_alerts.append("主播 leads 汇总覆盖率异常")
    if coverage["anchor_deal_coverage"] < 0.5 or coverage["anchor_deal_coverage"] > 1.05:
        coverage_alerts.append("主播 deals 汇总覆盖率异常")
    if coverage["anchor_spend_coverage"] > 1.05:
        coverage_alerts.append("主播 spend 汇总覆盖率异常")
    checks.append(
        _check(
            "snapshot.anchor_rollup",
            "structural change",
            "warning" if coverage_alerts else "pass",
            {
                "summary": "主播层覆盖率检查",
                "metrics": coverage,
                "threshold_breaches": coverage_alerts,
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
            },
        )
    )

    if baseline_account_latest_path.exists() and total is not None:
        baseline = pd.read_csv(baseline_account_latest_path)
        baseline_total = baseline[baseline["scope_name"] == "线索组汇总"].iloc[0]
        baseline_metrics = {
            "total_mtd_leads": float(baseline_total["mtd_leads"]),
            "total_mtd_deals": float(baseline_total["mtd_deals"]),
            "total_mtd_spend": float(baseline_total["mtd_spend"]),
            "mtd_lead_attain": float(baseline_total["mtd_lead_attain"]) if "mtd_lead_attain" in baseline_total else 0.0,
            "mtd_deal_attain": float(baseline_total["mtd_deal_attain"]) if "mtd_deal_attain" in baseline_total else 0.0,
        }
        compare_metrics = {
            "total_mtd_leads": snapshot_metrics["total_mtd_leads"],
            "total_mtd_deals": snapshot_metrics["total_mtd_deals"],
            "total_mtd_spend": snapshot_metrics["total_mtd_spend"],
            "mtd_lead_attain": snapshot_metrics["mtd_lead_attain"],
            "mtd_deal_attain": snapshot_metrics["mtd_deal_attain"],
        }
        drift, breaches, status = _evaluate_thresholds(compare_metrics, baseline_metrics, thresholds)
        checks.append(
            _check(
                "snapshot.baseline_metrics",
                "metric drift",
                status,
                {
                    "summary": "日报快照核心经营指标对比 baseline",
                    "current_metrics": compare_metrics,
                    "baseline_metrics": baseline_metrics,
                    "drift_metrics": drift,
                    "threshold_breaches": breaches,
                    "threshold_source": threshold_source,
                    "threshold_profile": threshold_profile,
                    "threshold_rule": thresholds,
                },
            )
        )
    return checks


def check_ledger_layer(
    ledger_path: Path,
    snapshot_path: Path,
    *,
    thresholds: dict[str, dict[str, object]],
    threshold_source: str,
    threshold_profile: str,
) -> list[dict[str, object]]:
    ledger = pd.read_csv(ledger_path)
    snapshot = pd.read_csv(snapshot_path)
    checks = []

    key_cols = ["settlement_period", "freeze_id", "scope_type", "scope_name"]
    duplicate_count = int(ledger.duplicated(subset=key_cols).sum())
    duplicate_eval = _compare_metric(duplicate_count, 0.0, thresholds["duplicate_count"])
    checks.append(
        _check(
            "ledger.unique_scope",
            "structural change",
            _status_from_breach_level(duplicate_eval["breach_level"]),
            {
                "summary": "台账唯一键检查",
                "metrics": {"row_count": int(len(ledger)), "duplicate_count": duplicate_count},
                "threshold_breaches": [] if duplicate_count == 0 else ["settlement_period+freeze_id+scope 不唯一"],
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
                "threshold_rule": {"duplicate_count": thresholds["duplicate_count"]},
            },
        )
    )

    nonnull_required = [
        "settlement_period",
        "freeze_id",
        "scope_type",
        "scope_name",
        "metric_version",
        "schema_version",
        "run_id",
        "snapshot_start",
        "snapshot_end",
        "formula_name",
        "formula_inputs_json",
        "ledger_status",
    ]
    nulls = {column: int(ledger[column].isna().sum()) for column in nonnull_required if column in ledger.columns and ledger[column].isna().any()}
    null_count = int(sum(nulls.values()))
    null_eval = _compare_metric(null_count, 0.0, thresholds["null_field_count"])
    checks.append(
        _check(
            "ledger.required_fields",
            "contract violation",
            _status_from_breach_level(null_eval["breach_level"]),
            {
                "summary": "台账关键字段非空检查",
                "null_fields": nulls,
                "threshold_breaches": [f"{column} 存在空值={count}" for column, count in nulls.items()],
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
                "threshold_rule": {"null_field_count": thresholds["null_field_count"]},
            },
        )
    )

    join_cols = ["scope_type", "scope_name"]
    merged = ledger.merge(
        snapshot[join_cols + ["mtd_leads", "mtd_deals", "mtd_spend", "lead_target_month", "deal_target_month"]],
        on=join_cols,
        how="left",
        suffixes=("_ledger", "_snapshot"),
    )
    mismatch = []
    for _, row in merged.iterrows():
        for metric in ["mtd_leads", "mtd_deals", "mtd_spend", "lead_target_month", "deal_target_month"]:
            if not _close_enough(row[f"{metric}_ledger"], row[f"{metric}_snapshot"], tolerance=1e-6):
                mismatch.append(
                    {
                        "scope_type": row["scope_type"],
                        "scope_name": row["scope_name"],
                        "metric": metric,
                        "ledger": row[f"{metric}_ledger"],
                        "snapshot": row[f"{metric}_snapshot"],
                    }
                )
    mismatch_eval = _compare_metric(float(len(mismatch)), 0.0, thresholds["mismatch_count"])
    checks.append(
        _check(
            "ledger.snapshot_reconcile",
            "metric drift",
            _status_from_breach_level(mismatch_eval["breach_level"]),
            {
                "summary": "台账与日报快照关键指标对账",
                "mismatch_count": len(mismatch),
                "sample_mismatches": mismatch[:20],
                "threshold_breaches": [f"台账与快照存在 {len(mismatch)} 个指标不一致"] if mismatch else [],
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
                "threshold_rule": {"mismatch_count": thresholds["mismatch_count"]},
            },
        )
    )
    return checks


def check_export_contracts(
    manifest_paths: list[Path],
    *,
    expected_schema_version: str,
    expected_metric_version: str,
    expected_template_version: str,
    expected_run_id: str,
    expected_freeze_id: str,
) -> list[dict[str, object]]:
    manifest_specs = [{"path": path, "name": f"contract.{path.stem}"} for path in manifest_paths]
    return check_export_manifest_contracts(
        manifest_specs,
        expected_schema_version=expected_schema_version,
        expected_metric_version=expected_metric_version,
        expected_template_version=expected_template_version,
        expected_run_id=expected_run_id,
        expected_freeze_id=expected_freeze_id,
    )


def check_analysis_snapshot(
    analysis_snapshot_path: Path,
    *,
    thresholds: dict[str, dict[str, object]],
    threshold_source: str,
    threshold_profile: str,
) -> list[dict[str, object]]:
    snapshot = pd.read_csv(analysis_snapshot_path)
    expected_subject_areas = {"funnel", "sla", "quality", "host_anchor", "channel", "ops_review"}
    actual_subject_areas = set(snapshot["subject_area"].dropna().astype(str))
    missing = sorted(expected_subject_areas - actual_subject_areas)
    counts = snapshot.groupby("subject_area", as_index=False).size().rename(columns={"size": "row_count"})
    row_count = int(len(snapshot))
    missing_eval = _compare_metric(float(len(missing)), 0.0, thresholds["missing_subject_area_count"])
    row_eval = _compare_metric(float(row_count), 0.0, thresholds["min_total_row_count"])
    checks = [
        _check(
            "analysis.subject_areas",
            "contract violation",
            _status_from_breach_level(missing_eval["breach_level"]),
            {
                "summary": "分析主题完整性检查",
                "actual_subject_areas": sorted(actual_subject_areas),
                "missing_subject_areas": missing,
                "threshold_breaches": [f"缺少主题: {name}" for name in missing],
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
                "threshold_rule": {"missing_subject_area_count": thresholds["missing_subject_area_count"]},
            },
        ),
        _check(
            "analysis.row_count",
            "structural change",
            "pass" if row_eval["breach_level"] == "pass" and all(counts["row_count"] > 0) else "fail",
            {
                "summary": "分析快照行数与主题覆盖检查",
                "row_count": row_count,
                "subject_area_counts": counts.to_dict(orient="records"),
                "threshold_breaches": [] if row_count > 0 and all(counts["row_count"] > 0) else ["分析快照为空或存在空主题"],
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
                "threshold_rule": {"min_total_row_count": thresholds["min_total_row_count"]},
            },
        ),
        _check(
            "analysis.raw_evidence_topics",
            "pass",
            "pass",
            {
                "summary": "raw-evidence 主题迁移状态",
                "raw_evidence_topics": RAW_EVIDENCE_TOPICS,
                "raw_evidence_groups": summarize_raw_evidence_topics(),
                "threshold_breaches": [],
                "threshold_source": threshold_source,
                "threshold_profile": threshold_profile,
            },
        ),
    ]
    return checks


def _check(name: str, category: str, status: str, details: object) -> dict[str, object]:
    return {"name": name, "category": category, "status": status, "details": details}


def _evaluate_thresholds(
    current_metrics: dict[str, float],
    baseline_metrics: dict[str, float],
    thresholds: dict[str, dict[str, object]],
) -> tuple[dict[str, object], list[str], str]:
    drift_metrics: dict[str, object] = {}
    breaches: list[str] = []
    status = "pass"
    for metric, rule in thresholds.items():
        current = current_metrics.get(metric)
        baseline = baseline_metrics.get(metric)
        if current is None or baseline is None:
            continue
        comparison = _compare_metric(current, baseline, rule)
        drift_metrics[metric] = comparison
        if comparison["breach_level"] == "fail":
            status = "fail"
            breaches.append(f"{metric} 超过失败阈值: {comparison['value_for_threshold']}")
        elif comparison["breach_level"] == "warning" and status != "fail":
            status = "warning"
            breaches.append(f"{metric} 超过预警阈值: {comparison['value_for_threshold']}")
    return drift_metrics, breaches, status


def _compare_metric(current: float, baseline: float, rule: dict[str, object]) -> dict[str, object]:
    mode = str(rule.get("mode", "absolute"))
    if mode == "relative":
        value = _relative_delta(current, baseline)
    elif mode == "minimum":
        value = float(current)
    else:
        value = abs(float(current) - float(baseline))

    breach_level = "pass"
    if mode == "minimum":
        fail_threshold = float(rule.get("fail", 0))
        warning_threshold = float(rule.get("warning", 0))
        if value < fail_threshold:
            breach_level = "fail"
        elif value < warning_threshold:
            breach_level = "warning"
    else:
        if value > float(rule.get("fail", 0)):
            breach_level = "fail"
        elif value > float(rule.get("warning", 0)):
            breach_level = "warning"

    return {
        "baseline": float(baseline),
        "current": float(current),
        "delta": float(current) - float(baseline),
        "delta_ratio": _relative_delta(current, baseline),
        "threshold_mode": mode,
        "value_for_threshold": value,
        "warning_threshold": float(rule.get("warning", 0)),
        "fail_threshold": float(rule.get("fail", 0)),
        "breach_level": breach_level,
    }


def _status_from_breach_level(level: str) -> str:
    return {"fail": "fail", "warning": "warning"}.get(level, "pass")


def _relative_delta(current: object, baseline: object) -> float:
    try:
        current_value = float(current)
        baseline_value = float(baseline)
    except (TypeError, ValueError):
        return 0.0
    if baseline_value == 0:
        return 0.0 if current_value == 0 else 1.0
    return abs(current_value - baseline_value) / abs(baseline_value)


def _nonempty_ratio(series: pd.Series | None) -> float:
    if series is None or len(series) == 0:
        return 0.0
    values = series.fillna("").astype(str).str.strip()
    return float(values.ne("").mean())


def _blank_ratio(series: pd.Series | None) -> float:
    if series is None or len(series) == 0:
        return 1.0
    values = series.fillna("").astype(str).str.strip()
    return float(values.eq("").mean())


def _ratio(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _close_enough(current: object, baseline: object, *, tolerance: float) -> bool:
    try:
        current_value = float(current)
        baseline_value = float(baseline)
    except (TypeError, ValueError):
        return current == baseline
    return abs(current_value - baseline_value) <= tolerance
