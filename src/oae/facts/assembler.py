"""Final fact-table assembly."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from oae.rules.hosts import count_hosts_in_text


def build_fact(
    leads_attr: pd.DataFrame,
    deals_dedup: pd.DataFrame,
    logger: logging.Logger,
) -> pd.DataFrame:
    fact = leads_attr.merge(deals_dedup, on="_lead_key", how="left")
    fact["成交时间"] = pd.to_datetime(fact["成交时间"], errors="coerce")
    fact["订单状态"] = fact["订单状态"].fillna("")
    fact["is_order"] = pd.to_numeric(fact["is_order"], errors="coerce").fillna(0).astype(int)
    fact["is_deal"] = pd.to_numeric(fact["is_deal"], errors="coerce").fillna(0).astype(int)
    fact["权重"] = pd.to_numeric(fact["权重"], errors="coerce").fillna(0.0)
    fact["主播人数"] = fact["本场主播"].map(count_hosts_in_text)
    fact["成交分摊权重"] = np.where(
        (fact["is_deal"] == 1) & (fact["归属状态"] == "匹配成功") & (fact["主播人数"] > 0),
        1.0 / fact["主播人数"],
        0.0,
    )
    fact["orders_contrib"] = fact["is_order"] * fact["权重"]
    fact["deals_contrib"] = fact["is_deal"] * fact["成交分摊权重"]

    required_order = [
        "线索ID",
        "手机号",
        "线索创建时间",
        "date",
        "标准账号",
        "本场主播",
        "同手机号线索数",
        "是否手机号折叠",
        "权重",
        "归属状态",
        "无匹配原因",
        "report_bucket",
        "订单状态",
        "成交时间",
        "主播人数",
        "成交分摊权重",
        "is_order",
        "is_deal",
        "渠道",
        "成交车型",
        "orders_contrib",
        "deals_contrib",
        "business_subject_key",
        "_lead_key",
        "线索ID_norm",
        "命中场次数量",
    ]
    existing = [column for column in required_order if column in fact.columns]
    others = [column for column in fact.columns if column not in existing]
    out = fact[existing + others].copy()

    logger.info("事实表行数: %s", len(out))
    logger.info("事实表唯一线索数(手机号优先口径): %s", out["_lead_key"].nunique())
    logger.info("is_order=1 线索数: %s", int(out.loc[out["is_order"] == 1, "_lead_key"].nunique()))
    logger.info("is_deal=1 线索数: %s", int(out.loc[out["is_deal"] == 1, "_lead_key"].nunique()))
    return out
