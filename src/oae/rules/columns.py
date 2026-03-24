"""Column aliases and resilient matching rules."""

from __future__ import annotations

import re

import pandas as pd


COLUMN_ALIASES = {
    "live_date": ["创建时间", "直播日期", "日期"],
    "live_account": ["开播账号", "账号"],
    "live_start": ["开播时间", "开始时间", "直播开始时间"],
    "live_end": ["下播时间", "结束时间", "直播结束时间"],
    "live_host": ["本场主播", "主播"],
    "lead_channel3": ["渠道3", "渠道_3", "三级渠道"],
    "lead_id": ["线索ID", "线索id", "ID"],
    "lead_time": ["创建时间", "线索创建时间", "线索时间"],
    "lead_account": ["渠道2", "开播账号", "账号"],
    "lead_phone": ["手机号", "手机", "电话"],
    "deal_id": ["线索ID", "线索id", "ID"],
    "deal_status": ["订单状态"],
    "deal_time": ["成交时间"],
    "deal_account": ["账号", "渠道2", "开播账号"],
    "deal_channel": ["渠道", "渠道3", "渠道2"],
    "deal_model": ["成交车型", "车系"],
    "deal_phone": ["手机号", "手机", "电话"],
}


def fold_col_name(name: object) -> str:
    return re.sub(r"\s+", "", str(name).strip()).casefold()


def pick_col(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    cols = [str(col).strip() for col in df.columns]
    exact_map: dict[str, list[str]] = {}
    fold_map: dict[str, list[str]] = {}

    for col in cols:
        exact_map.setdefault(col, []).append(col)
        fold_map.setdefault(fold_col_name(col), []).append(col)

    for candidate in candidates:
        key = str(candidate).strip()
        exact_hit = exact_map.get(key, [])
        if len(exact_hit) == 1:
            return exact_hit[0]
        if len(exact_hit) > 1:
            raise ValueError(f"列名重复冲突: {key} -> {exact_hit}")

        fold_hit = sorted(set(fold_map.get(fold_col_name(key), [])))
        if len(fold_hit) == 1:
            return fold_hit[0]
        if len(fold_hit) > 1:
            raise ValueError(f"列名归一后冲突: 候选={key} 可匹配={fold_hit}")

    if required:
        raise ValueError(f"缺少列: 候选={candidates}；当前列={cols}")
    return None
