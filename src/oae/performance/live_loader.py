"""Live/session derived loaders for performance runtime."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from oae.performance.loader_utils import (
    join_unique_accounts,
    normalize_account,
    normalize_text,
    pick_live_column,
    split_hosts,
)


def load_anchor_accounts_from_live(path: Path, month_start: pd.Timestamp, month_end: pd.Timestamp) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["date", "scope_name", "parent_account"])

    try:
        workbook = pd.ExcelFile(path)
        raw = pd.read_excel(path, sheet_name=workbook.sheet_names[0])
    except Exception:
        return pd.DataFrame(columns=["date", "scope_name", "parent_account"])

    raw = raw.copy()
    raw.columns = [str(col).strip() for col in raw.columns]

    date_col = pick_live_column(raw, ["日期", "直播日期", "创建时间"], required=False)
    account_col = pick_live_column(raw, ["开播账号", "账号", "直播账号", "账号名称"], required=False)
    host_col = pick_live_column(raw, ["本场主播", "主播", "主播名称"], required=False)
    spend_col = pick_live_column(raw, ["消耗", "实际消耗", "当日消耗", "花费", "费用", "投放消耗", "总消耗"], required=False)
    if not date_col or not account_col or not host_col:
        return pd.DataFrame(columns=["date", "scope_name", "parent_account"])

    columns = [date_col, account_col, host_col]
    out_columns = ["date", "parent_account", "hosts_raw"]
    if spend_col:
        columns.append(spend_col)
        out_columns.append("daily_spend")
    data = raw[columns].copy()
    data.columns = out_columns
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.normalize()
    data["parent_account"] = data["parent_account"].apply(normalize_account)
    data["hosts"] = data["hosts_raw"].apply(split_hosts)
    data["host_count"] = data["hosts"].str.len()
    if "daily_spend" in data.columns:
        data["daily_spend"] = pd.to_numeric(data["daily_spend"], errors="coerce").fillna(0.0)
        data["daily_spend"] = data["daily_spend"] / data["host_count"].where(data["host_count"] > 0, 1)
    data = data.explode("hosts", ignore_index=True)
    data = data.rename(columns={"hosts": "scope_name"})
    data["scope_name"] = data["scope_name"].apply(normalize_text)
    data = data[
        data["date"].notna()
        & (data["date"] >= month_start)
        & (data["date"] <= month_end)
        & data["scope_name"].ne("")
        & data["parent_account"].ne("")
    ]
    if data.empty:
        return pd.DataFrame(columns=["date", "scope_name", "parent_account"])
    group_cols = ["date", "scope_name", "parent_account"]
    if "daily_spend" not in data.columns:
        return data[group_cols].drop_duplicates().sort_values(group_cols).reset_index(drop=True)
    return (
        data.groupby(group_cols, as_index=False)["daily_spend"]
        .sum()
        .sort_values(group_cols)
        .reset_index(drop=True)
    )
