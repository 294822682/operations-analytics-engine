"""Lead normalization and attribution entrypoints."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from oae.facts.attribution import apply_match_result, find_matches_by_account
from oae.rules.account_mapping import normalize_account, remap_douyin_laike_channel3
from oae.rules.columns import pick_col
from oae.rules.common import normalize_text
from oae.rules.identity import (
    build_matching_subject_key,
    normalize_lead_id,
    normalize_phone,
    vectorized_build_business_subject_key,
)


def apply_channel3_filter(
    df: pd.DataFrame,
    logger: logging.Logger,
    *,
    column_aliases: dict[str, list[str]],
    allowed_channel3: set[str],
    fallback_channel2_value: str,
) -> pd.DataFrame:
    channel3_col = pick_col(df, column_aliases["lead_channel3"], required=False)
    if channel3_col is None:
        logger.warning("线索源缺少渠道3列，跳过渠道过滤")
        return df

    out = df.copy()
    raw_count = len(out)
    channel2_col = pick_col(out, ["渠道2"], required=False)
    channel3_norm = out[channel3_col].map(normalize_text)
    allowed_mask = channel3_norm.isin(allowed_channel3)
    extra_keep_mask = pd.Series(False, index=out.index)
    if channel2_col is not None:
        channel2_norm = out[channel2_col].map(normalize_text)
        account_fallback_mask = channel2_norm.eq(fallback_channel2_value) & channel3_norm.ne("")
        extra_keep_mask = account_fallback_mask & ~allowed_mask
    keep_mask = allowed_mask | extra_keep_mask
    excluded = channel3_norm[~keep_mask].replace("", "空值").value_counts(dropna=False).to_dict()
    out = out.loc[keep_mask].copy()
    logger.info(
        "线索按渠道3过滤后保留: %s / %s（允许=%s，抖音来客直播旁路=%s）",
        len(out),
        raw_count,
        sorted(allowed_channel3),
        int(extra_keep_mask.sum()),
    )
    if excluded:
        logger.info("线索渠道3排除明细: %s", excluded)
    return out


def resolve_lead_account(
    df: pd.DataFrame,
    *,
    account_col: str,
    channel3_col: str | None,
    logger: logging.Logger,
    fallback_channel2_value: str,
) -> pd.Series:
    account_raw = df[account_col].map(normalize_text)
    if channel3_col is None:
        return account_raw.map(normalize_account)

    channel2_col = pick_col(df, ["渠道2"], required=False)
    if channel2_col is None:
        return account_raw.map(normalize_account)

    channel2_raw = df[channel2_col].map(normalize_text)
    channel3_raw = df[channel3_col].map(normalize_text)
    fallback_mask = channel2_raw.eq(fallback_channel2_value) & channel3_raw.ne("")
    if fallback_mask.any():
        fallback_channel3_remapped = channel3_raw.map(remap_douyin_laike_channel3)
        special_remap_mask = fallback_mask & fallback_channel3_remapped.ne(channel3_raw)
        logger.info(
            "线索账号映射规则命中: 渠道2=%s 时账号改取渠道3，共 %s 行",
            fallback_channel2_value,
            int(fallback_mask.sum()),
        )
        if special_remap_mask.any():
            logger.info(
                "抖音来客直播二次映射命中: 星途星纪元直播营销中心->官方 / 星途汽车官方直播间->直营，共 %s 行",
                int(special_remap_mask.sum()),
            )
    else:
        fallback_channel3_remapped = channel3_raw
    resolved = account_raw.where(~fallback_mask, fallback_channel3_remapped)
    return resolved.map(normalize_account)


def standardize_lead_fields(
    leads_raw: pd.DataFrame,
    logger: logging.Logger,
    *,
    column_aliases: dict[str, list[str]],
    allowed_channel3: set[str],
    fallback_channel2_value: str,
) -> tuple[pd.DataFrame, str, dict[str, str]]:
    df = leads_raw.copy()
    df.columns = df.columns.str.strip()
    df = apply_channel3_filter(
        df,
        logger,
        column_aliases=column_aliases,
        allowed_channel3=allowed_channel3,
        fallback_channel2_value=fallback_channel2_value,
    )

    lead_id_col = pick_col(df, column_aliases["lead_id"])
    lead_time_col = pick_col(df, column_aliases["lead_time"])
    account_col = pick_col(df, column_aliases["lead_account"])
    channel3_col = pick_col(df, column_aliases["lead_channel3"], required=False)
    phone_col = pick_col(df, column_aliases["lead_phone"], required=False)

    df["线索ID_norm"] = normalize_lead_id(df[lead_id_col])
    df["线索创建时间"] = pd.to_datetime(df[lead_time_col], errors="coerce")
    df["date"] = df["线索创建时间"].dt.date
    df["标准账号"] = resolve_lead_account(
        df,
        account_col=account_col,
        channel3_col=channel3_col,
        logger=logger,
        fallback_channel2_value=fallback_channel2_value,
    )
    df["手机号"] = df[phone_col].map(normalize_phone) if phone_col is not None else ""
    df["_lead_key"] = [
        build_matching_subject_key(lead_id, phone)
        for lead_id, phone in zip(df["线索ID_norm"].tolist(), df["手机号"].tolist())
    ]
    df["_orig_idx"] = np.arange(len(df), dtype=np.int64)
    df["business_subject_key"] = vectorized_build_business_subject_key(
        df["线索ID_norm"],
        df["手机号"],
        create_dt_series=df["线索创建时间"],
        fallback_row_series=df["_orig_idx"],
    )
    empty_lead_key = df["_lead_key"] == ""
    if empty_lead_key.any():
        df.loc[empty_lead_key, "_lead_key"] = "ROW:" + df.loc[empty_lead_key, "_orig_idx"].astype(str)

    df["_sort_time"] = df["线索创建时间"].fillna(pd.Timestamp.max)
    df["同手机号线索数"] = df.groupby("_lead_key")["_orig_idx"].transform("count").fillna(0).astype(int)
    df["是否手机号折叠"] = np.where(df["手机号"].ne("") & (df["同手机号线索数"] > 1), "是", "否")
    df["权重"] = 0.0
    df["_idx"] = np.arange(len(df), dtype=np.int64)

    lead_key_by_id = (
        df.loc[df["线索ID_norm"] != "", ["线索ID_norm", "_lead_key"]]
        .drop_duplicates(subset=["线索ID_norm"], keep="first")
        .set_index("线索ID_norm")["_lead_key"]
        .to_dict()
    )
    return df, lead_id_col, lead_key_by_id


def deduplicate_leads(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    dedup = (
        df.sort_values(["_lead_key", "_sort_time", "_orig_idx"], kind="stable")
        .drop_duplicates(subset=["_lead_key"], keep="first")
        .copy()
    )
    logger.info("原始线索行数: %s", len(df))
    logger.info("唯一线索行数(手机号优先去重，同手机号取最早创建时间): %s", len(dedup))
    logger.info("手机号发生折叠的唯一线索数: %s", int((dedup["是否手机号折叠"] == "是").sum()))
    return dedup


def match_leads(
    leads_raw: pd.DataFrame,
    live_windows: pd.DataFrame,
    logger: logging.Logger,
    *,
    match_mode: str,
    column_aliases: dict[str, list[str]],
    allowed_channel3: set[str],
    fallback_channel2_value: str,
    non_live_accounts: set[str],
) -> tuple[pd.DataFrame, dict[str, str]]:
    standardized, lead_id_col, lead_key_by_id = standardize_lead_fields(
        leads_raw,
        logger,
        column_aliases=column_aliases,
        allowed_channel3=allowed_channel3,
        fallback_channel2_value=fallback_channel2_value,
    )
    dedup = deduplicate_leads(standardized, logger)
    valid = dedup.dropna(subset=["线索创建时间"]).copy()
    valid = valid[valid["标准账号"] != ""].copy()
    match_maps = find_matches_by_account(valid, live_windows, match_mode=match_mode)
    dedup = apply_match_result(dedup, match_maps, live_windows, non_live_accounts=non_live_accounts)

    out_cols = [
        "business_subject_key",
        "_lead_key",
        "线索ID_norm",
        lead_id_col,
        "手机号",
        "线索创建时间",
        "date",
        "标准账号",
        "本场主播",
        "同手机号线索数",
        "是否手机号折叠",
        "命中场次数量",
        "权重",
        "归属状态",
        "无匹配原因",
        "report_bucket",
    ]
    out = dedup[out_cols].copy().rename(columns={lead_id_col: "线索ID"})
    logger.info("线索总行数: %s", len(out))
    logger.info(
        "匹配成功: %s, 无主线索: %s",
        int((out["归属状态"] == "匹配成功").sum()),
        int((out["归属状态"] == "无主线索").sum()),
    )
    return out, lead_key_by_id
