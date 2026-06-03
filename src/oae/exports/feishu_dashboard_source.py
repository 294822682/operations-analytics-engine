"""Stable long-form dashboard source export for daily BI payloads."""

from __future__ import annotations

import csv
import re
from io import StringIO
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from oae.exports.feishu_topline import SegmentTopline, ToplineSummary


DASHBOARD_SOURCE_COLUMNS = [
    "report_date",
    "source_table",
    "scope_type",
    "scope_name",
    "parent_scope",
    "metric_key",
    "metric_name",
    "actual",
    "target",
    "attain_rate",
    "unit",
    "source_column",
    "sort_order",
]


def build_dashboard_source_rows(
    *,
    report_date: str,
    topline_summary: ToplineSummary,
    account_table: pd.DataFrame,
    anchor_table: pd.DataFrame,
    seed_account_table: pd.DataFrame | None = None,
    seed_anchor_table: pd.DataFrame | None = None,
    lead_quality_line: str = "",
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    full = topline_summary.full_account
    account_summary_spend = _account_summary_spend(account_table)
    order_actual = getattr(topline_summary, "douyin_laike_orders", None)
    if order_actual is None:
        order_actual = _account_summary_metric(account_table, "抖音-来客订单数")
    order_target = _account_summary_metric(account_table, "订单KPI目标")
    full_spend = account_summary_spend
    if full_spend is None and full.cpl_actual is not None:
        full_spend = float(full.cpl_actual) * float(full.lead_actual)

    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "impressions",
        "曝光",
        full.impression_actual,
        full.impression_target,
        full.impression_attain,
        "次",
        "topline_summary.full_account.impression_actual",
        10,
    )
    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "mtd_unique_leads",
        "累计唯一线索",
        full.lead_actual,
        full.lead_target,
        full.lead_attain,
        "条",
        "topline_summary.full_account.lead_actual",
        20,
    )
    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "mtd_deals",
        "累计实销",
        full.deal_actual,
        full.deal_target,
        full.deal_attain,
        "台",
        "topline_summary.full_account.deal_actual",
        30,
    )
    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "mtd_spend",
        "累计线索费用",
        full_spend,
        None,
        None,
        "元",
        "账号层（母集）.线索组汇总.累计线索费用",
        40,
    )
    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "mtd_cpl",
        "总体 CPL",
        full.cpl_actual,
        full.cpl_target,
        None,
        "元/条",
        "topline_summary.full_account.cpl_actual",
        50,
    )
    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "mtd_cps",
        "总体 CPS",
        full.cps_actual,
        full.cps_target,
        None,
        "元/台",
        "topline_summary.full_account.cps_actual",
        60,
    )
    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "pending_day",
        "待交车（当日）",
        full.pending_day,
        None,
        None,
        "台",
        "topline_summary.full_account.pending_day",
        70,
    )
    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "pending_cumulative",
        "待交车（累计）",
        full.pending_cumulative,
        None,
        None,
        "台",
        "topline_summary.full_account.pending_cumulative",
        80,
    )
    _append(
        rows,
        report_date,
        "topline",
        "department",
        "全量",
        "",
        "mtd_douyin_laike_orders",
        "抖音-来客订单",
        order_actual,
        order_target,
        _safe_div(order_actual, order_target),
        "个",
        "账号层（母集）.线索组汇总.抖音-来客订单数",
        90,
    )

    _append_segment(rows, report_date, topline_summary.ex7, 100)
    _append_segment(rows, report_date, topline_summary.excluding_ex7, 200)
    _append_seed_account_rows(rows, report_date, seed_account_table if seed_account_table is not None else pd.DataFrame(), 700)
    _append_account_rows(rows, report_date, account_table, 1000)
    _append_seed_anchor_rows(rows, report_date, seed_anchor_table if seed_anchor_table is not None else pd.DataFrame(), 3000)
    _append_anchor_rows(rows, report_date, anchor_table, 5000)
    if lead_quality_line:
        _append_lead_quality_rows(rows, report_date, lead_quality_line, 9000)
    return rows


def dashboard_source_tsv(rows: list[dict[str, str]]) -> str:
    handle = StringIO()
    writer = csv.DictWriter(handle, fieldnames=DASHBOARD_SOURCE_COLUMNS, delimiter="\t", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in DASHBOARD_SOURCE_COLUMNS})
    return handle.getvalue()


def write_dashboard_source_tsv(rows: list[dict[str, str]], path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(dashboard_source_tsv(rows), encoding="utf-8-sig")
    return output_path


def _append_segment(rows: list[dict[str, str]], report_date: str, segment: SegmentTopline, base_order: int) -> None:
    spend = _segment_spend(segment)
    _append(rows, report_date, "topline_segment", "segment", segment.label, "", "mtd_unique_leads", "累计唯一线索", segment.leads, None, None, "条", f"topline_summary.{segment.label}.leads", base_order + 10)
    _append(rows, report_date, "topline_segment", "segment", segment.label, "", "mtd_deals", "累计实销", segment.deals, None, None, "台", f"topline_summary.{segment.label}.deals", base_order + 20)
    _append(rows, report_date, "topline_segment", "segment", segment.label, "", "mtd_spend", "累计线索费用", spend, None, None, "元", f"topline_summary.{segment.label}.derived_spend", base_order + 30)
    _append(rows, report_date, "topline_segment", "segment", segment.label, "", "mtd_cpl", "实际 CPL", segment.cpl_actual, None, None, "元/条", f"topline_summary.{segment.label}.cpl_actual", base_order + 40)
    _append(rows, report_date, "topline_segment", "segment", segment.label, "", "mtd_cps", "实际 CPS", segment.cps_actual, None, None, "元/台", f"topline_summary.{segment.label}.cps_actual", base_order + 50)


def _append_account_rows(rows: list[dict[str, str]], report_date: str, account_table: pd.DataFrame, base_order: int) -> None:
    if account_table.empty:
        return
    for row_index, (_, row) in enumerate(account_table.iterrows()):
        name = str(row.get("账号", "")).strip()
        if not name:
            continue
        order = base_order + row_index * 100
        specs = [
            ("daily_leads", "当日线索", "当日线索", "当日线索目标", "当日线索达成率", "条"),
            ("mtd_unique_leads", "累计唯一线索", "累计线索", "线索月目标", "累计线索达成率", "条"),
            ("daily_deals", "当日实销", "当日实销", "当日实销目标", "当日实销达成率", "台"),
            ("mtd_deals", "累计实销", "累计实销", "实销月目标", "累计实销达成率", "台"),
            ("mtd_douyin_laike_orders", "抖音-来客订单", "抖音-来客订单数", "订单KPI目标", "订单KPI完成率", "个"),
            ("mtd_spend", "累计线索费用", "累计线索费用", "线索费用月目标", "", "元"),
            ("mtd_cpl", "实际 CPL", "实际CPL", "CPL目标", "", "元/条"),
            ("mtd_cps", "实际 CPS", "实际CPS", "CPS目标", "", "元/台"),
        ]
        for offset, (metric_key, metric_name, actual_col, target_col, rate_col, unit) in enumerate(specs, start=1):
            _append(
                rows,
                report_date,
                "lead_account",
                "account",
                name,
                "",
                metric_key,
                metric_name,
                _num(row.get(actual_col)),
                _num(row.get(target_col)),
                _num(row.get(rate_col)) if rate_col else None,
                unit,
                f"账号层（母集）.{name}.{actual_col}",
                order + offset,
            )


def _append_seed_account_rows(rows: list[dict[str, str]], report_date: str, seed_account_table: pd.DataFrame, base_order: int) -> None:
    if seed_account_table.empty:
        return
    for row_index, (_, row) in enumerate(seed_account_table.iterrows()):
        name = str(row.get("账号", "")).strip()
        if not name:
            continue
        order = base_order + row_index * 100
        specs = [
            ("daily_impressions", "当日曝光", "当日曝光", "当日曝光目标", "当日曝光达成率", "人次"),
            ("mtd_impressions", "累计曝光", "累计曝光", "曝光目标", "累计曝光达成率", "人次"),
        ]
        for offset, (metric_key, metric_name, actual_col, target_col, rate_col, unit) in enumerate(specs, start=1):
            _append(
                rows,
                report_date,
                "seed_account",
                "account",
                name,
                "",
                metric_key,
                metric_name,
                _num(row.get(actual_col)),
                _num(row.get(target_col)),
                _num(row.get(rate_col)),
                unit,
                f"种草账号.{name}.{actual_col}",
                order + offset,
            )


def _append_anchor_rows(rows: list[dict[str, str]], report_date: str, anchor_table: pd.DataFrame, base_order: int) -> None:
    if anchor_table.empty:
        return
    for row_index, (_, row) in enumerate(anchor_table.iterrows()):
        name = str(row.get("主播", "")).strip()
        if not name:
            continue
        parent_scope = str(row.get("归属账号", "")).strip()
        order = base_order + row_index * 100
        specs = [
            ("daily_leads", "当日线索", "当日线索", "当日线索目标", "当日线索达成率", "条"),
            ("mtd_unique_leads", "累计唯一线索", "累计线索", "线索月目标", "累计线索达成率", "条"),
            ("daily_deals", "当日实销", "当日实销", "当日实销目标", "当日实销达成率", "台"),
            ("mtd_deals", "累计实销", "累计实销", "实销月目标", "累计实销达成率", "台"),
            ("mtd_douyin_laike_orders", "抖音-来客订单", "抖音-来客订单数", "订单KPI目标", "订单KPI完成率", "个"),
            ("mtd_spend", "累计线索费用", "累计线索费用", "单人线索费用目标", "", "元"),
            ("mtd_cpl", "实际 CPL", "实际CPL", "单人CPL目标", "", "元/条"),
            ("mtd_cps", "实际 CPS", "实际CPS", "单人CPS目标", "", "元/台"),
        ]
        for offset, (metric_key, metric_name, actual_col, target_col, rate_col, unit) in enumerate(specs, start=1):
            _append(
                rows,
                report_date,
                "lead_anchor",
                "anchor",
                name,
                parent_scope,
                metric_key,
                metric_name,
                _num(row.get(actual_col)),
                _num(row.get(target_col)),
                _num(row.get(rate_col)) if rate_col else None,
                unit,
                f"到人层（子集）.{name}.{actual_col}",
                order + offset,
            )


def _append_seed_anchor_rows(rows: list[dict[str, str]], report_date: str, seed_anchor_table: pd.DataFrame, base_order: int) -> None:
    if seed_anchor_table.empty:
        return
    for row_index, (_, row) in enumerate(seed_anchor_table.iterrows()):
        name = str(row.get("主播", "")).strip()
        if not name:
            continue
        parent_scope = str(row.get("归属账号", "")).strip()
        order = base_order + row_index * 100
        specs = [
            ("daily_impressions", "当日曝光", "当日曝光", "当日曝光目标", "当日曝光达成率", "人次"),
            ("mtd_impressions", "累计曝光", "累计曝光", "曝光目标", "累计曝光达成率", "人次"),
        ]
        for offset, (metric_key, metric_name, actual_col, target_col, rate_col, unit) in enumerate(specs, start=1):
            _append(
                rows,
                report_date,
                "seed_anchor",
                "anchor",
                name,
                parent_scope,
                metric_key,
                metric_name,
                _num(row.get(actual_col)),
                _num(row.get(target_col)),
                _num(row.get(rate_col)),
                unit,
                f"种草主播.{name}.{actual_col}",
                order + offset,
            )


def _append_lead_quality_rows(rows: list[dict[str, str]], report_date: str, lead_quality_line: str, base_order: int) -> None:
    specs = [
        ("raw_leads", "原始线索", _extract_first_int(lead_quality_line, r"原始线索[^）)]*[）)]?(\d+)"), "条"),
        ("lead_quality_unique_leads", "唯一线索", _extract_first_int(lead_quality_line, r"唯一线索[^）)]*[）)]?(\d+)"), "条"),
        ("unique_rate", "唯一率", _extract_first_rate(lead_quality_line, r"唯一率\s*([0-9.]+%)"), "比例"),
        ("unowned_leads", "无主线索", _extract_first_int(lead_quality_line, r"无主线索\s*(\d+)"), "条"),
        ("manual_overrides", "人工归属", _extract_first_int(lead_quality_line, r"人工确认归属\s*(\d+)\s*条"), "条"),
        ("manual_affected_rows", "人工归属影响行", _extract_first_int(lead_quality_line, r"影响样本\s*(\d+)\s*行"), "行"),
    ]
    for offset, (metric_key, metric_name, actual, unit) in enumerate(specs):
        _append(
            rows,
            report_date,
            "lead_quality",
            "department",
            "全量",
            "",
            metric_key,
            metric_name,
            actual,
            None,
            None,
            unit,
            "lead_quality_text",
            base_order + offset,
        )


def _append(
    rows: list[dict[str, str]],
    report_date: str,
    source_table: str,
    scope_type: str,
    scope_name: str,
    parent_scope: str,
    metric_key: str,
    metric_name: str,
    actual: object,
    target: object,
    attain_rate: object,
    unit: str,
    source_column: str,
    sort_order: int,
) -> None:
    rows.append(
        {
            "report_date": report_date,
            "source_table": source_table,
            "scope_type": scope_type,
            "scope_name": scope_name,
            "parent_scope": parent_scope,
            "metric_key": metric_key,
            "metric_name": metric_name,
            "actual": _cell(actual),
            "target": _cell(target),
            "attain_rate": _cell(attain_rate),
            "unit": unit,
            "source_column": source_column,
            "sort_order": str(sort_order),
        }
    )


def _account_summary_spend(account_table: pd.DataFrame) -> float | None:
    return _account_summary_metric(account_table, "累计线索费用")


def _account_summary_metric(account_table: pd.DataFrame, metric_column: str) -> float | None:
    if account_table.empty or "账号" not in account_table.columns:
        return None
    if metric_column not in account_table.columns:
        return None
    matched = account_table[account_table["账号"].astype(str).str.strip().eq("线索组汇总")]
    if matched.empty:
        return None
    return _num(matched.iloc[0].get(metric_column))


def _segment_spend(segment: SegmentTopline) -> float | None:
    if segment.cpl_actual is not None and segment.leads:
        return float(segment.cpl_actual) * float(segment.leads)
    if segment.cps_actual is not None and segment.deals:
        return float(segment.cps_actual) * float(segment.deals)
    return None


def _num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        number = float(value)
        return number if np.isfinite(number) else None
    text = str(value).strip()
    if not text or text in {"-", "N/A", "NA", "nan", "NaN", "None", "null"}:
        return None
    text = text.replace(",", "").replace("，", "").replace("￥", "").replace("¥", "")
    scale = 1.0
    if text.endswith("%"):
        scale = 0.01
        text = text[:-1]
    if text.endswith("万"):
        scale *= 10000
        text = text[:-1]
    for suffix in ("元/条", "元/台", "人次", "条", "台", "元"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
            break
    try:
        number = float(text)
    except ValueError:
        return None
    return number * scale if np.isfinite(number) else None


def _safe_div(numerator: object, denominator: object) -> float | None:
    actual = _num(numerator)
    target = _num(denominator)
    if actual is None or target is None or target <= 0:
        return None
    return actual / target


def _extract_first_int(text: str, pattern: str) -> int:
    matched = re.search(pattern, text)
    if not matched:
        return 0
    value = str(matched.group(1)).replace(",", "")
    return int(value) if value.isdigit() else 0


def _extract_first_rate(text: str, pattern: str) -> float:
    matched = re.search(pattern, text)
    if not matched:
        return 0.0
    return float(str(matched.group(1)).rstrip("%")) / 100


def _cell(value: object) -> str:
    number = _num(value)
    if number is None:
        return ""
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f"{number:.10f}".rstrip("0").rstrip(".")
