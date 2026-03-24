"""Content builders for Feishu markdown/TSV exports."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from oae.exports.feishu_topline import ToplineSummary, build_markdown_topline_lines, build_tsv_topline_lines
from oae.exports.feishu_formatters import md_table, tsv_table


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
        f"**日报日期：{ctx.report_date_str}**",
        "",
        *build_markdown_topline_lines(ctx.topline_summary),
        "",
        "**成交账号**",
        f"- 当日成交账号（线索组目标账号）：{ctx.day_target_deal_accounts}",
        f"- 累计成交账号（线索组目标账号）：{ctx.mtd_target_deal_accounts}",
        f"- 累计成交账号（全量账号）：{ctx.mtd_all_deal_accounts}",
        f"- 当日待交车账号（线索组目标账号）：{ctx.day_target_pending_accounts}",
        f"- 累计待交车账号（线索组目标账号）：{ctx.mtd_target_pending_accounts}",
        f"- 累计待交车账号（全量账号）：{ctx.mtd_all_pending_accounts}",
        "",
        "**线索质量口径**",
        f"- {ctx.lead_quality_line}",
        "",
        "**账号层（母集）**",
        md_table(ctx.acc_out),
        "",
        "**到人层（子集）**",
        md_table(ctx.anc_out),
    ]
    return "\n".join(lines)


def build_tsv_content(ctx: ReportContext) -> str:
    lines = [
        *build_tsv_topline_lines(ctx.report_date_str, ctx.topline_summary),
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
