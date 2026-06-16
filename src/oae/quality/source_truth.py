"""Raw source truth checks for daily Feishu report outputs."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import pandas as pd

from oae.exports.feishu_douyin_laike import build_douyin_laike_order_metrics
from oae.exports.feishu_report import load_report_topline_config
from oae.exports.feishu_seed_dashboard import (
    load_seed_monthly_targets,
    load_seed_sessions_from_workbooks,
)
from oae.exports.feishu_topline import _load_live_metrics, _parse_date_series, load_source_csv


SOURCE_TRUTH_COLUMNS = ["指标", "源表直算", "日报输出", "状态", "说明"]


def build_source_truth_report(
    *,
    report_date: str | pd.Timestamp,
    live_df: pd.DataFrame,
    leads_source: pd.DataFrame,
    deals_source: pd.DataFrame,
    seed_sessions: pd.DataFrame,
    seed_targets: pd.DataFrame,
    dashboard_source: pd.DataFrame,
    topline_config: dict[str, Any],
) -> dict[str, Any]:
    report_ts = pd.to_datetime(report_date, errors="raise").normalize()
    expected = _source_truth_metrics(
        report_date=report_ts,
        live_df=live_df,
        leads_source=leads_source,
        deals_source=deals_source,
        seed_sessions=seed_sessions,
        seed_targets=seed_targets,
        topline_config=topline_config,
    )
    actual = _dashboard_topline_metrics(dashboard_source)

    specs = [
        ("impressions", "曝光", 0.5, "直播进度表曝光 + EXEED 台账曝光"),
        ("mtd_deals", "实销", 0.0, "成交表：订单状态=已交车，成交日期在当月窗口内"),
        ("pending_cumulative", "待交车（累计）", 0.0, "成交表：订单状态=待交车，下订日期在待交车窗口内"),
        ("mtd_douyin_laike_orders", "来客唯一订单", 0.0, "直播进度表抖音-来客窗口 + 总部新媒体线索表手机号优先去重"),
    ]
    checks: list[dict[str, Any]] = []
    for metric_key, metric_name, tolerance, source_note in specs:
        expected_value = expected.get(metric_key)
        actual_value = actual.get(metric_key)
        if expected_value is None or actual_value is None:
            status = "failed"
            delta = None
        else:
            delta = float(actual_value) - float(expected_value)
            status = "passed" if abs(delta) <= float(tolerance) else "failed"
        checks.append(
            {
                "metric_key": metric_key,
                "metric_name": metric_name,
                "expected": _json_number(expected_value),
                "actual": _json_number(actual_value),
                "delta": _json_number(delta),
                "tolerance": tolerance,
                "status": status,
                "source_note": source_note,
            }
        )

    status = "passed" if all(item["status"] == "passed" for item in checks) else "failed"
    return {
        "report_type": "source_truth_report",
        "status": status,
        "report_date": report_ts.strftime("%Y-%m-%d"),
        "checks": checks,
        "source_windows": {
            "month_start": report_ts.to_period("M").to_timestamp().strftime("%Y-%m-%d"),
            "pending_start": (report_ts.to_period("M").to_timestamp() - pd.DateOffset(months=1)).strftime("%Y-%m-%d"),
            "report_date": report_ts.strftime("%Y-%m-%d"),
        },
    }


def source_truth_tsv(report: dict[str, Any]) -> str:
    lines = ["\t".join(SOURCE_TRUTH_COLUMNS)]
    for item in report.get("checks", []):
        lines.append(
            "\t".join(
                [
                    str(item.get("metric_name", "")),
                    _format_cell(item.get("expected")),
                    _format_cell(item.get("actual")),
                    "PASS" if item.get("status") == "passed" else "FAIL",
                    str(item.get("source_note", "")),
                ]
            )
        )
    return "\n".join(lines) + "\n"


def write_source_truth_report(report: dict[str, Any], *, json_path: str | Path, tsv_path: str | Path) -> tuple[Path, Path]:
    json_output = Path(json_path).expanduser().resolve()
    tsv_output = Path(tsv_path).expanduser().resolve()
    json_output.parent.mkdir(parents=True, exist_ok=True)
    tsv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    tsv_output.write_text(source_truth_tsv(report), encoding="utf-8")
    return json_output, tsv_output


def load_dashboard_source_tsv(path: str | Path) -> pd.DataFrame:
    source = pd.read_csv(Path(path).expanduser().resolve(), sep="\t", encoding="utf-8-sig")
    source.columns = [str(column).strip().lstrip("\ufeff") for column in source.columns]
    return source


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report_date = pd.to_datetime(args.report_date, errors="raise").normalize()
    live_df = pd.read_excel(Path(args.live_file).expanduser().resolve())
    leads_source = load_source_csv(Path(args.leads_file).expanduser().resolve())
    deals_source = load_source_csv(Path(args.deals_file).expanduser().resolve())
    seed_sessions = load_seed_sessions_from_workbooks([args.seed_workbook_file] if args.seed_workbook_file else [])
    seed_targets = load_seed_monthly_targets(args.seed_targets_file)
    dashboard_source = load_dashboard_source_tsv(args.dashboard_source_tsv)
    topline_config = load_report_topline_config(
        Path(args.topline_config).expanduser().resolve(),
        month=report_date.strftime("%Y-%m"),
    )

    report = build_source_truth_report(
        report_date=report_date,
        live_df=live_df,
        leads_source=leads_source,
        deals_source=deals_source,
        seed_sessions=seed_sessions,
        seed_targets=seed_targets,
        dashboard_source=dashboard_source,
        topline_config=topline_config,
    )
    json_path, tsv_path = write_source_truth_report(report, json_path=args.output_json, tsv_path=args.output_tsv)
    print(f"SOURCE_TRUTH_JSON={json_path}")
    print(f"SOURCE_TRUTH_TSV={tsv_path}")
    print(f"STATUS={report['status'].upper()}")
    for item in report["checks"]:
        line = (
            f"{item['metric_name']}: source={_format_cell(item['expected'])} "
            f"report={_format_cell(item['actual'])} status={item['status']}"
        )
        if item["status"] == "passed":
            print(f"OK: {line}")
        else:
            print(f"ERR: {line}")
    return 0 if report["status"] == "passed" else 1


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify daily report topline metrics against raw source tables")
    parser.add_argument("--report-date", required=True, help="日报日期 YYYY-MM-DD")
    parser.add_argument("--live-file", required=True, help="直播进度表 xlsx")
    parser.add_argument("--leads-file", required=True, help="总部新媒体线索 CSV")
    parser.add_argument("--deals-file", required=True, help="总部新媒体成交 CSV")
    parser.add_argument("--dashboard-source-tsv", required=True, help="feishu_dashboard_source_latest_*.tsv")
    parser.add_argument("--topline-config", required=True, help="顶部核心汇报配置")
    parser.add_argument("--seed-targets-file", default="", help="种草曝光月目标配置")
    parser.add_argument("--seed-workbook-file", default="", help="EXEED 台账 workbook")
    parser.add_argument("--output-json", required=True, help="输出 source truth JSON")
    parser.add_argument("--output-tsv", required=True, help="输出 source truth TSV")
    return parser.parse_args(argv)


def _source_truth_metrics(
    *,
    report_date: pd.Timestamp,
    live_df: pd.DataFrame,
    leads_source: pd.DataFrame,
    deals_source: pd.DataFrame,
    seed_sessions: pd.DataFrame,
    seed_targets: pd.DataFrame,
    topline_config: dict[str, Any],
) -> dict[str, float]:
    month_start = report_date.to_period("M").to_timestamp().normalize()
    live_metrics = _load_live_metrics(live_df, topline_config)
    live_impressions = float(live_metrics[live_metrics["date"].between(month_start, report_date)]["impressions"].sum())
    seed_impressions = _source_seed_impressions(seed_sessions, report_date)
    return {
        "impressions": live_impressions + seed_impressions,
        "mtd_deals": float(_source_deal_count(deals_source, report_date)),
        "pending_cumulative": float(_source_pending_count(deals_source, report_date, topline_config)),
        "mtd_douyin_laike_orders": float(
            build_douyin_laike_order_metrics(live_df=live_df, leads_df=leads_source, report_date=report_date)[0]
        ),
    }


def _source_seed_impressions(seed_sessions: pd.DataFrame, report_date: pd.Timestamp) -> float:
    if seed_sessions.empty or "date" not in seed_sessions.columns or "impressions" not in seed_sessions.columns:
        return 0.0
    month_start = report_date.to_period("M").to_timestamp().normalize()
    sessions = seed_sessions.copy()
    sessions["date"] = pd.to_datetime(sessions["date"], errors="coerce").dt.normalize()
    sessions["impressions"] = pd.to_numeric(sessions["impressions"], errors="coerce")
    scoped = sessions[sessions["date"].between(month_start, report_date) & sessions["impressions"].notna()].copy()
    return float(scoped["impressions"].sum()) if not scoped.empty else 0.0


def _source_deal_count(deals_source: pd.DataFrame, report_date: pd.Timestamp) -> int:
    source = _normalized_columns(deals_source)
    if "线索ID" not in source.columns or "订单状态" not in source.columns:
        return 0
    month_start = report_date.to_period("M").to_timestamp().normalize()
    deal_date = _parse_date_series(source, "成交日期")
    deal_date = deal_date.where(deal_date.notna(), _parse_date_series(source, "成交时间"))
    mask = source["订单状态"].astype(str).str.strip().eq("已交车") & deal_date.between(month_start, report_date)
    return int(source.loc[mask, "线索ID"].astype(str).str.strip().replace({"": pd.NA}).dropna().nunique())


def _source_pending_count(deals_source: pd.DataFrame, report_date: pd.Timestamp, topline_config: dict[str, Any]) -> int:
    source = _normalized_columns(deals_source)
    if "线索ID" not in source.columns or "订单状态" not in source.columns:
        return 0
    pending_rules = topline_config["pending_rules"]
    order_date = _parse_date_series(source, pending_rules["primary_date_field"])
    for field in pending_rules.get("fallback_date_fields", []):
        order_date = order_date.where(order_date.notna(), _parse_date_series(source, field))
    current_month_start = report_date.to_period("M").to_timestamp().normalize()
    pending_start = (current_month_start - pd.DateOffset(months=1)).normalize()
    mask = source["订单状态"].astype(str).str.strip().eq("待交车") & order_date.between(pending_start, report_date)
    return int(source.loc[mask, "线索ID"].astype(str).str.strip().replace({"": pd.NA}).dropna().nunique())


def _dashboard_topline_metrics(dashboard_source: pd.DataFrame) -> dict[str, float | None]:
    source = _normalized_columns(dashboard_source)
    out: dict[str, float | None] = {}
    required = {"source_table", "scope_type", "scope_name", "metric_key", "actual"}
    if not required.issubset(set(source.columns)):
        return out
    scoped = source[
        source["source_table"].astype(str).str.strip().eq("topline")
        & source["scope_type"].astype(str).str.strip().eq("department")
        & source["scope_name"].astype(str).str.strip().eq("全量")
    ].copy()
    for metric_key in ["impressions", "mtd_deals", "pending_cumulative", "mtd_douyin_laike_orders"]:
        hit = scoped[scoped["metric_key"].astype(str).str.strip().eq(metric_key)]
        if hit.empty:
            out[metric_key] = None
        else:
            value = pd.to_numeric(hit["actual"], errors="coerce")
            out[metric_key] = float(value.dropna().iloc[0]) if value.notna().any() else None
    return out


def _normalized_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(column).strip().lstrip("\ufeff") for column in out.columns]
    return out


def _json_number(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _format_cell(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.6f}".rstrip("0").rstrip(".")
