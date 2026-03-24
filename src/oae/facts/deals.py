"""Deal deduplication and linkage back to business subjects."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from oae.rules.columns import pick_col
from oae.rules.identity import build_matching_subject_key, normalize_lead_id, normalize_phone


def build_deal_dedup(
    deals_raw: pd.DataFrame,
    logger: logging.Logger,
    *,
    lead_key_by_id: dict[str, str] | None,
    column_aliases: dict[str, list[str]],
) -> pd.DataFrame:
    if deals_raw.empty:
        logger.warning("未提供成交明细，按空成交表处理")
        return pd.DataFrame(columns=["_lead_key", "订单状态", "is_order", "is_deal", "成交时间", "成交_账号", "渠道", "成交车型"])

    deals = deals_raw.copy()
    deals.columns = deals.columns.str.strip()

    lead_id_col = pick_col(deals, column_aliases["deal_id"])
    status_col = pick_col(deals, column_aliases["deal_status"], required=False)
    deal_time_col = pick_col(deals, column_aliases["deal_time"], required=False)
    account_col = pick_col(deals, column_aliases["deal_account"], required=False)
    channel_col = pick_col(deals, column_aliases["deal_channel"], required=False)
    model_col = pick_col(deals, column_aliases["deal_model"], required=False)
    phone_col = pick_col(deals, column_aliases["deal_phone"], required=False)

    deals["线索ID_norm"] = normalize_lead_id(deals[lead_id_col])
    deals["手机号"] = deals[phone_col].map(normalize_phone) if phone_col is not None else ""
    mapped_key = deals["线索ID_norm"].map((lead_key_by_id or {}))
    fallback_key = [
        build_matching_subject_key(lead_id, phone)
        for lead_id, phone in zip(deals["线索ID_norm"].tolist(), deals["手机号"].tolist())
    ]
    deals["_lead_key"] = mapped_key.where(mapped_key.notna() & mapped_key.ne(""), pd.Series(fallback_key, index=deals.index))
    deals = deals[deals["_lead_key"] != ""].copy()

    deals["订单状态"] = deals[status_col].fillna("").astype(str).str.strip() if status_col is not None else ""
    deals["_status_rank"] = deals["订单状态"].map({"已交车": 2, "待交车": 1}).fillna(0).astype(int)
    deals["_row_no"] = np.arange(len(deals), dtype=np.int64)
    deals = deals.sort_values(["_lead_key", "_status_rank", "_row_no"], ascending=[True, False, True], kind="stable")
    deals = deals.drop_duplicates(subset=["_lead_key"], keep="first").copy()

    deals["is_order"] = deals["订单状态"].isin(["待交车", "已交车"]).astype(int)
    deals["is_deal"] = (deals["订单状态"] == "已交车").astype(int)
    deals["成交时间"] = pd.to_datetime(deals[deal_time_col], errors="coerce") if deal_time_col is not None else pd.NaT

    keep = ["_lead_key", "订单状态", "is_order", "is_deal", "成交时间"]
    rename_map: dict[str, str] = {}
    if account_col is not None:
        keep.append(account_col)
        rename_map[account_col] = "成交_账号"
    if channel_col is not None and channel_col != account_col:
        keep.append(channel_col)
        rename_map[channel_col] = "渠道"
    if model_col is not None:
        keep.append(model_col)
        rename_map[model_col] = "成交车型"

    out = deals[keep].rename(columns=rename_map).copy()
    logger.info("成交去重后唯一线索数(手机号优先口径): %s", out["_lead_key"].nunique())
    return out
