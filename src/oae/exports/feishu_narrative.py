"""Business explanation builders for Feishu export."""

from __future__ import annotations

import pandas as pd

from oae.exports.feishu_panel_utils import ACCOUNT_LABEL_MAP
from oae.exports.feishu_formatters import pct
from oae.exports.feishu_topline import build_pending_account_summary


def format_deal_accounts(df: pd.DataFrame) -> str:
    if df.empty:
        return "无"
    grouped = df.groupby("标准账号")["线索ID_norm"].nunique().sort_values(ascending=False)
    if grouped.empty:
        return "无"
    return "、".join([f"{ACCOUNT_LABEL_MAP.get(key, key)}({int(value)}台)" for key, value in grouped.items()])


def deal_accounts_text(
    fact: pd.DataFrame,
    report_date: pd.Timestamp,
    month_start: pd.Timestamp,
    target_accounts: list[str],
) -> tuple[str, str, str]:
    x = fact[["is_deal", "成交时间", "标准账号", "线索ID_norm"]].copy()
    x["is_deal"] = pd.to_numeric(x["is_deal"], errors="coerce").fillna(0)
    x["deal_date"] = pd.to_datetime(x["成交时间"], errors="coerce").dt.normalize()
    mtd = x[(x["is_deal"] == 1) & x["deal_date"].notna() & (x["deal_date"] >= month_start) & (x["deal_date"] <= report_date)]
    day = mtd[mtd["deal_date"] == report_date]
    return (
        format_deal_accounts(day[day["标准账号"].isin(target_accounts)]),
        format_deal_accounts(mtd[mtd["标准账号"].isin(target_accounts)]),
        format_deal_accounts(mtd),
    )


def pending_accounts_text(
    fact: pd.DataFrame,
    report_date: pd.Timestamp,
    month_start: pd.Timestamp,
    target_accounts: list[str],
    deals_source: pd.DataFrame | None = None,
    topline_config: dict | None = None,
) -> tuple[float, float, str, str, str]:
    if deals_source is not None and topline_config is not None:
        day_cnt, recent_cnt, day_target, recent_target, recent_all = build_pending_account_summary(
            fact=fact,
            deals_source=deals_source,
            report_date=report_date,
            target_accounts=target_accounts,
            config=topline_config,
        )
        return float(day_cnt), float(recent_cnt), day_target, recent_target, recent_all

    need_cols = ["成交时间", "标准账号", "线索ID_norm"]
    use_status_col = "订单状态" in fact.columns
    use_order_cols = ("is_order" in fact.columns) and ("is_deal" in fact.columns)
    if not use_status_col and not use_order_cols:
        return 0.0, 0.0, "无", "无", "无"

    cols = need_cols[:] + (["订单状态"] if use_status_col else []) + (["is_order", "is_deal"] if use_order_cols else [])
    x = fact[cols].copy()
    x["deal_date"] = pd.to_datetime(x["成交时间"], errors="coerce").dt.normalize()
    if use_status_col:
        pending_mask = x["订单状态"].astype(str).str.strip() == "待交车"
    else:
        x["is_order"] = pd.to_numeric(x["is_order"], errors="coerce").fillna(0)
        x["is_deal"] = pd.to_numeric(x["is_deal"], errors="coerce").fillna(0)
        pending_mask = (x["is_order"] == 1) & (x["is_deal"] == 0)

    pending = x[pending_mask].copy()
    if pending.empty:
        return 0.0, 0.0, "无", "无", "无"

    mtd = pending[
        (
            (pending["deal_date"].notna())
            & (pending["deal_date"] >= month_start)
            & (pending["deal_date"] <= report_date)
        )
        | pending["deal_date"].isna()
    ]
    day = pending[pending["deal_date"] == report_date]
    day_cnt = float(day["线索ID_norm"].nunique()) if not day.empty else 0.0
    mtd_cnt = float(mtd["线索ID_norm"].nunique()) if not mtd.empty else 0.0
    return (
        day_cnt,
        mtd_cnt,
        format_deal_accounts(day[day["标准账号"].isin(target_accounts)]),
        format_deal_accounts(mtd[mtd["标准账号"].isin(target_accounts)]),
        format_deal_accounts(mtd),
    )


def lead_quality_text(
    fact: pd.DataFrame,
    live_df: pd.DataFrame,
    report_date: pd.Timestamp,
    month_start: pd.Timestamp,
    live_file_label: str,
    manual_override_summary: dict[str, object] | None = None,
) -> str:
    raw_total = 0.0
    if not live_df.empty:
        try:
            live = live_df.copy()
            live.columns = [str(c).strip() for c in live.columns]
            if {"日期", "全场景线索人数"}.issubset(set(live.columns)):
                live["日期"] = pd.to_datetime(live["日期"], errors="coerce").dt.normalize()
                live["全场景线索人数"] = pd.to_numeric(live["全场景线索人数"], errors="coerce").fillna(0.0)
                m = live[(live["日期"] >= month_start) & (live["日期"] <= report_date)]
                raw_total = float(m["全场景线索人数"].sum())
        except (KeyError, ValueError, TypeError):
            raw_total = 0.0

    unique_total = 0
    unowned_total = 0
    try:
        x = fact[["date", "线索ID_norm", "归属状态"]].copy()
        x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
        x["线索ID_norm"] = x["线索ID_norm"].astype(str).str.strip()
        mtd = x[(x["date"] >= month_start) & (x["date"] <= report_date) & x["线索ID_norm"].ne("")]
        unique_total = int(mtd["线索ID_norm"].nunique())
        unowned = mtd[mtd["归属状态"].astype(str).str.strip() == "无主线索"]
        unowned_total = int(unowned["线索ID_norm"].nunique())
    except (KeyError, ValueError, TypeError):
        unique_total = 0
        unowned_total = 0

    unique_rate_text = pct((unique_total / raw_total) if raw_total > 0 else pd.NA)
    line = (
        f"原始线索（{live_file_label}全场景线索人数）{int(round(raw_total))}，"
        f"唯一线索（总部新媒体线索）{unique_total}，"
        f"唯一率{unique_rate_text}，无主线索{unowned_total}。"
    )
    override_summary = manual_override_summary or {}
    if int(override_summary.get("applied_override_count", 0) or 0) > 0:
        line += (
            f" 本次专项人工确认归属 {int(override_summary.get('applied_override_count', 0))} 条，"
            f"影响样本 {int(override_summary.get('applied_row_count', 0))} 行。"
        )
    return line
