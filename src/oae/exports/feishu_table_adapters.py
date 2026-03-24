"""Account/anchor table adapters for Feishu export."""

from __future__ import annotations

import pandas as pd

from oae.exports.feishu_formatters import (
    add_num_columns,
    build_xy_column,
    format_parent_account_label,
    format_pct_columns,
    num_trim,
    sort_by_order,
)
from oae.exports.feishu_panel_utils import (
    ACCOUNT_LABEL_MAP,
    ANCHOR_ORDER,
    DISPLAY_ACCOUNT_ORDER,
    PCT_DISPLAY_COLS,
    PCT_RENAME_MAP,
)


def account_table(acc: pd.DataFrame, target_accounts: list[str]) -> pd.DataFrame:
    show_order = DISPLAY_ACCOUNT_ORDER + [item for item in target_accounts if item not in DISPLAY_ACCOUNT_ORDER] + ["线索组汇总"]
    x = sort_by_order(acc, show_order, scope_col="scope_name")
    x["账号"] = x["scope_name"].map(lambda s: ACCOUNT_LABEL_MAP.get(s, s))
    build_xy_column(x, "当日线索/目标", "daily_leads", "daily_lead_target", actual_digits=0, target_digits=2)
    build_xy_column(x, "累计线索/月目标", "mtd_leads", "lead_target_month", actual_digits=0, target_digits=0)
    build_xy_column(x, "当日实销/目标", "daily_deals", "daily_deal_target", actual_digits=0, target_digits=2)
    line_summary_mask = x["scope_name"] == "线索组汇总"
    build_xy_column(
        x,
        "累计实销/月目标",
        "mtd_deals",
        "deal_target_month",
        actual_digits=0,
        target_digits=2,
        target_override_digits=0,
        target_override_mask=line_summary_mask,
    )
    add_num_columns(
        x,
        {
            "线索费用月目标": ("lead_cost_target_month", 2),
            "CPL目标": ("cpl_target", 2),
            "CPS目标": ("cps_target", 2),
            "累计线索费用": ("mtd_spend", 2),
            "实际CPL": ("mtd_cpl", 2),
            "实际CPS": ("mtd_cps", 2),
        },
    )
    out = x[
        [
            "账号",
            "当日线索/目标",
            "daily_lead_attain_pct",
            "累计线索/月目标",
            "mtd_lead_attain_pct",
            "当日实销/目标",
            "daily_deal_attain_pct",
            "累计实销/月目标",
            "mtd_deal_attain_pct",
            "线索费用月目标",
            "CPL目标",
            "CPS目标",
            "累计线索费用",
            "实际CPL",
            "实际CPS",
        ]
    ].rename(columns=PCT_RENAME_MAP)
    format_pct_columns(out, PCT_DISPLAY_COLS)
    return out


def account_table_tsv(acc: pd.DataFrame, target_accounts: list[str]) -> pd.DataFrame:
    show_order = DISPLAY_ACCOUNT_ORDER + [item for item in target_accounts if item not in DISPLAY_ACCOUNT_ORDER] + ["线索组汇总"]
    x = sort_by_order(acc, show_order, scope_col="scope_name")
    x["账号"] = x["scope_name"].map(lambda s: ACCOUNT_LABEL_MAP.get(s, s))
    line_summary_mask = x["scope_name"] == "线索组汇总"
    out = pd.DataFrame(
        {
            "账号": x["账号"],
            "当日线索": x["daily_leads"].map(lambda v: f"{v:.0f}"),
            "当日线索目标": x["daily_lead_target"].map(lambda v: f"{v:.2f}"),
            "当日线索达成率": x["daily_lead_attain_pct"],
            "累计线索": x["mtd_leads"].map(lambda v: f"{v:.2f}"),
            "线索月目标": x["lead_target_month"].map(lambda v: f"{v:.0f}"),
            "累计线索达成率": x["mtd_lead_attain_pct"],
            "当日实销": x["daily_deals"].map(lambda v: f"{v:.0f}"),
            "当日实销目标": x["daily_deal_target"].map(lambda v: f"{v:.2f}"),
            "当日实销达成率": x["daily_deal_attain_pct"],
            "累计实销": x["mtd_deals"].map(lambda v: f"{v:.0f}"),
            "实销月目标": x["deal_target_month"].map(lambda v: f"{v:.2f}"),
            "累计实销达成率": x["mtd_deal_attain_pct"],
            "线索费用月目标": x["lead_cost_target_month"].map(lambda v: num_trim(v, 2)),
            "CPL目标": x["cpl_target"].map(lambda v: num_trim(v, 2)),
            "CPS目标": x["cps_target"].map(lambda v: num_trim(v, 2)),
            "累计线索费用": x["mtd_spend"].map(lambda v: num_trim(v, 2)),
            "实际CPL": x["mtd_cpl"].map(lambda v: num_trim(v, 2)),
            "实际CPS": x["mtd_cps"].map(lambda v: num_trim(v, 2)),
        }
    )
    out.loc[line_summary_mask, "实销月目标"] = x.loc[line_summary_mask, "deal_target_month"].map(lambda v: f"{v:.0f}")
    format_pct_columns(out, ["当日线索达成率", "累计线索达成率", "当日实销达成率", "累计实销达成率"])
    return out


def anchor_table(anc: pd.DataFrame) -> pd.DataFrame:
    x = sort_by_order(anc, ANCHOR_ORDER, scope_col="scope_name")
    x["归属账号"] = x["parent_account"].map(lambda v: format_parent_account_label(v, ACCOUNT_LABEL_MAP))
    build_xy_column(x, "当日线索/目标", "daily_leads", "daily_lead_target", actual_digits=2, target_digits=2)
    build_xy_column(x, "累计线索/月目标", "mtd_leads", "lead_target_month", actual_digits=2, target_digits=0)
    build_xy_column(x, "当日实销/目标", "daily_deals", "daily_deal_target", actual_digits=2, target_digits=2)
    build_xy_column(x, "累计实销/月目标", "mtd_deals", "deal_target_month", actual_digits=2, target_digits=0)
    add_num_columns(
        x,
        {
            "单人线索费用目标": ("lead_cost_target_month", 2),
            "单人CPL目标": ("cpl_target", 2),
            "单人CPS目标": ("cps_target", 0),
            "累计线索费用": ("mtd_spend", 2),
            "实际CPL": ("mtd_cpl", 2),
            "实际CPS": ("mtd_cps", 2),
        },
    )
    out = x[
        [
            "scope_name",
            "归属账号",
            "当日线索/目标",
            "daily_lead_attain_pct",
            "累计线索/月目标",
            "mtd_lead_attain_pct",
            "当日实销/目标",
            "daily_deal_attain_pct",
            "累计实销/月目标",
            "mtd_deal_attain_pct",
            "单人线索费用目标",
            "单人CPL目标",
            "单人CPS目标",
            "累计线索费用",
            "实际CPL",
            "实际CPS",
        ]
    ].rename(columns={"scope_name": "主播", **PCT_RENAME_MAP})
    format_pct_columns(out, PCT_DISPLAY_COLS)
    return out


def anchor_table_tsv(anc: pd.DataFrame) -> pd.DataFrame:
    x = sort_by_order(anc, ANCHOR_ORDER, scope_col="scope_name")
    x["归属账号"] = x["parent_account"].map(lambda v: format_parent_account_label(v, ACCOUNT_LABEL_MAP))
    out = pd.DataFrame(
        {
            "主播": x["scope_name"],
            "归属账号": x["归属账号"],
            "当日线索": x["daily_leads"].map(lambda v: f"{v:.2f}"),
            "当日线索目标": x["daily_lead_target"].map(lambda v: f"{v:.2f}"),
            "当日线索达成率": x["daily_lead_attain_pct"],
            "累计线索": x["mtd_leads"].map(lambda v: f"{v:.2f}"),
            "线索月目标": x["lead_target_month"].map(lambda v: f"{v:.0f}"),
            "累计线索达成率": x["mtd_lead_attain_pct"],
            "当日实销": x["daily_deals"].map(lambda v: f"{v:.2f}"),
            "当日实销目标": x["daily_deal_target"].map(lambda v: f"{v:.2f}"),
            "当日实销达成率": x["daily_deal_attain_pct"],
            "累计实销": x["mtd_deals"].map(lambda v: f"{v:.2f}"),
            "实销月目标": x["deal_target_month"].map(lambda v: f"{v:.0f}"),
            "累计实销达成率": x["mtd_deal_attain_pct"],
            "单人线索费用目标": x["lead_cost_target_month"].map(lambda v: num_trim(v, 2)),
            "单人CPL目标": x["cpl_target"].map(lambda v: num_trim(v, 2)),
            "单人CPS目标": x["cps_target"].map(lambda v: num_trim(v, 0)),
            "累计线索费用": x["mtd_spend"].map(lambda v: num_trim(v, 2)),
            "实际CPL": x["mtd_cpl"].map(lambda v: num_trim(v, 2)),
            "实际CPS": x["mtd_cps"].map(lambda v: num_trim(v, 2)),
        }
    )
    format_pct_columns(out, ["当日线索达成率", "累计线索达成率", "当日实销达成率", "累计实销达成率"])
    return out

