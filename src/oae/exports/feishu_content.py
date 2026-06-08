"""Content builders for Feishu markdown/TSV exports."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from oae.exports.feishu_topline import ToplineSummary, build_markdown_topline_lines, build_tsv_topline_lines
from oae.exports.feishu_formatters import tsv_table


@dataclass
class ReportContext:
    report_date_str: str
    topline_summary: ToplineSummary
    day_target_deal_accounts: str
    mtd_target_deal_accounts: str
    mtd_all_deal_accounts: str
    day_target_pending_accounts: str
    mtd_target_pending_accounts: str
    mtd_all_pending_accounts: str
    lead_quality_line: str
    acc_out: pd.DataFrame
    anc_out: pd.DataFrame
    acc_tsv_out: pd.DataFrame
    anc_tsv_out: pd.DataFrame


def build_markdown_content(ctx: ReportContext) -> str:
    lines = [
        f"日报日期：{ctx.report_date_str}",
        *build_markdown_topline_lines(ctx.topline_summary),
        *_build_markdown_order_breakdown_lines(ctx),
    ]
    return "\n".join(lines)


def build_tsv_content(ctx: ReportContext) -> str:
    lines = [
        *build_tsv_topline_lines(ctx.report_date_str, ctx.topline_summary),
        *_build_tsv_order_breakdown_lines(ctx),
        "",
        "成交账号\t结果",
        f"当日成交账号（线索组目标账号）\t{ctx.day_target_deal_accounts}",
        f"累计成交账号（线索组目标账号）\t{ctx.mtd_target_deal_accounts}",
        f"累计成交账号（全量账号）\t{ctx.mtd_all_deal_accounts}",
        f"当日待交车账号（线索组目标账号）\t{ctx.day_target_pending_accounts}",
        f"累计待交车账号（线索组目标账号）\t{ctx.mtd_target_pending_accounts}",
        f"累计待交车账号（全量账号）\t{ctx.mtd_all_pending_accounts}",
        f"线索质量口径\t{ctx.lead_quality_line}",
        "",
        "账号层（母集）",
        tsv_table(ctx.acc_tsv_out),
        "",
        "到人层（子集）",
        tsv_table(ctx.anc_tsv_out),
    ]
    return "\n".join(lines)


def _build_markdown_order_breakdown_lines(ctx: ReportContext) -> list[str]:
    return build_markdown_order_breakdown_lines(ctx.topline_summary, ctx.acc_tsv_out, ctx.anc_tsv_out)


def _build_tsv_order_breakdown_lines(ctx: ReportContext) -> list[str]:
    return build_tsv_order_breakdown_lines(ctx.topline_summary, ctx.acc_tsv_out, ctx.anc_tsv_out)


def build_markdown_order_breakdown_lines(
    topline_summary: ToplineSummary,
    acc_tsv_out: pd.DataFrame,
    anc_tsv_out: pd.DataFrame,
) -> list[str]:
    actual, target = _order_actual_and_target(topline_summary, acc_tsv_out)
    if actual is None:
        return []
    return [
        "来客线索数（手机号去重）",
        f"累计来客线索数（账号）：{_format_order_breakdown(acc_tsv_out, '账号')}",
        f"累计来客线索数（主播）：{_format_order_breakdown(anc_tsv_out, '主播')}",
    ]


def build_tsv_order_breakdown_lines(
    topline_summary: ToplineSummary,
    acc_tsv_out: pd.DataFrame,
    anc_tsv_out: pd.DataFrame,
) -> list[str]:
    actual, target = _order_actual_and_target(topline_summary, acc_tsv_out)
    if actual is None:
        return []
    return [
        "抖音-来客线索数（手机号去重）",
        f"累计来客线索数\t{_format_count(actual)}",
        f"来客线索目标\t{_format_count(target)}",
        f"来客线索达成率\t{_format_rate(actual, target)}",
        f"累计来客线索数（账号）\t{_format_order_breakdown(acc_tsv_out, '账号')}",
        f"累计来客线索数（主播）\t{_format_order_breakdown(anc_tsv_out, '主播')}",
    ]


def _order_actual_and_target(topline_summary: ToplineSummary, acc_tsv_out: pd.DataFrame) -> tuple[float | None, float | None]:
    actual = getattr(topline_summary, "douyin_laike_orders", None)
    if actual is None:
        actual = _sum_order_breakdown(acc_tsv_out, "账号")
    if actual is None:
        return None, None
    return actual, getattr(topline_summary, "douyin_laike_order_target", None)


def _sum_order_breakdown(df: pd.DataFrame, name_col: str) -> float | None:
    if "抖音-来客线索数（手机号去重）" not in df.columns or name_col not in df.columns:
        return None
    rows = [
        value
        for name, value in zip(df[name_col], df["抖音-来客线索数（手机号去重）"])
        if str(name).strip() not in {"线索组汇总", "合计", "汇总"}
    ]
    if not rows:
        return None
    return float(sum(_parse_count(value) for value in rows))


def _format_order_breakdown(df: pd.DataFrame, name_col: str) -> str:
    if "抖音-来客线索数（手机号去重）" not in df.columns or name_col not in df.columns:
        return "暂无"
    items: list[str] = []
    for name, raw_count in zip(df[name_col], df["抖音-来客线索数（手机号去重）"]):
        name_text = str(name).strip()
        if not name_text or name_text in {"线索组汇总", "合计", "汇总"}:
            continue
        count = _parse_count(raw_count)
        if count <= 0:
            continue
        items.append(f"{name_text}({_format_count(count)})")
    return "、".join(items) if items else "暂无"


def _parse_count(value) -> float:
    if pd.isna(value):
        return 0.0
    text = str(value).strip().replace(",", "")
    if not text or text.upper() in {"N/A", "NA", "-"}:
        return 0.0
    return float(text)


def _format_count(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return str(int(round(float(value))))


def _format_rate(actual: float | None, target: float | None) -> str:
    if actual is None or target is None or pd.isna(actual) or pd.isna(target) or float(target) == 0:
        return "-"
    return f"{float(actual) / float(target) * 100:.2f}%"
