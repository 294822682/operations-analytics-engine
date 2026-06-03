"""Douyin-Laike order helpers for Feishu exports and dashboard supplements."""

from __future__ import annotations

import logging

import pandas as pd

from oae.facts.attribution import find_matches_by_account
from oae.facts.leads import deduplicate_leads, standardize_lead_fields
from oae.facts.live_sessions import build_live_windows
from oae.performance.loader_utils import normalize_account, split_hosts
from oae.rules.columns import COLUMN_ALIASES, pick_col
from oae.rules.common import normalize_text


def _empty_metric_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["scope_name", "mtd_douyin_laike_orders"])


def _component_key(value: object) -> str:
    return (
        normalize_text(value)
        .replace(" ", "")
        .replace("\n", "")
        .replace("\t", "")
        .replace("－", "-")
        .replace("—", "-")
    )


def _douyin_laike_source_mask(df: pd.DataFrame) -> pd.Series:
    channel2_col = pick_col(df, ["渠道2"], required=False)
    if channel2_col is None:
        return pd.Series(False, index=df.index)
    compact = df[channel2_col].map(_component_key).str.replace("-", "", regex=False)
    return compact.eq("抖音来客直播")


def _build_douyin_laike_live_windows(live_df: pd.DataFrame, report_date: pd.Timestamp, logger: logging.Logger) -> pd.DataFrame:
    if live_df.empty:
        return pd.DataFrame()

    raw = live_df.copy()
    raw.columns = [str(col).strip() for col in raw.columns]

    date_col = pick_col(raw, COLUMN_ALIASES["live_date"], required=False)
    platform_col = pick_col(raw, ["平台&挂载组建", "平台& 挂载组建"], required=False)
    if not date_col or not platform_col:
        return pd.DataFrame()

    month_start = report_date.to_period("M").to_timestamp().normalize()
    live_date = pd.to_datetime(raw[date_col], errors="coerce").dt.normalize()
    component = raw[platform_col].map(_component_key)
    scoped = raw[
        live_date.notna()
        & live_date.between(month_start, report_date.normalize())
        & component.eq("抖音-来客")
    ].copy()
    if scoped.empty:
        return pd.DataFrame()

    return build_live_windows(
        scoped,
        logger,
        buffer_minutes=5,
        default_duration_minutes=240,
        max_duration_hours=24,
    )


def build_douyin_laike_order_metrics(
    live_df: pd.DataFrame,
    leads_df: pd.DataFrame,
    report_date: pd.Timestamp,
) -> tuple[float, pd.DataFrame, pd.DataFrame]:
    logger = logging.getLogger("oae.exports.feishu_douyin_laike")
    if live_df.empty or leads_df.empty:
        return 0.0, _empty_metric_frame(), _empty_metric_frame()

    report_date = pd.to_datetime(report_date).normalize()
    live_windows = _build_douyin_laike_live_windows(live_df, report_date, logger)
    if live_windows.empty:
        return 0.0, _empty_metric_frame(), _empty_metric_frame()

    leads_raw = leads_df.copy()
    leads_raw.columns = [str(col).strip() for col in leads_raw.columns]
    laike_leads = leads_raw[_douyin_laike_source_mask(leads_raw)].copy()
    if laike_leads.empty:
        return 0.0, _empty_metric_frame(), _empty_metric_frame()

    standardized, _, _ = standardize_lead_fields(
        laike_leads,
        logger,
        column_aliases=COLUMN_ALIASES,
        allowed_channel3={"直播", "其他", "主页", "星途星纪元直播营销中心"},
        fallback_channel2_value="抖音来客直播",
    )
    dedup = deduplicate_leads(standardized, logger)
    valid = dedup.dropna(subset=["线索创建时间"]).copy()
    valid = valid[valid["标准账号"] != ""].copy()
    if valid.empty:
        return 0.0, _empty_metric_frame(), _empty_metric_frame()

    match_maps = find_matches_by_account(valid, live_windows, match_mode="process_deal_data")
    if not match_maps.matched_idx:
        return 0.0, _empty_metric_frame(), _empty_metric_frame()

    matched = dedup[dedup["_idx"].isin(match_maps.matched_idx)].copy()
    total_orders = float(len(matched))

    matched["scope_name"] = matched["标准账号"].map(normalize_account)
    account_out = (
        matched[matched["scope_name"].astype(str).str.strip().ne("")]
        .groupby("scope_name", as_index=False)
        .size()
        .rename(columns={"size": "mtd_douyin_laike_orders"})
    )

    anchor_rows: list[dict[str, object]] = []
    for _, row in matched.iterrows():
        hosts = split_hosts(match_maps.hosts.get(int(row["_idx"]), ""))
        if not hosts:
            continue
        share = 1.0 / len(hosts)
        for host in hosts:
            anchor_rows.append({"scope_name": host, "mtd_douyin_laike_orders": share})

    if not anchor_rows:
        return total_orders, account_out, _empty_metric_frame()

    anchor_out = pd.DataFrame(anchor_rows).groupby("scope_name", as_index=False)["mtd_douyin_laike_orders"].sum()
    return total_orders, account_out, anchor_out
