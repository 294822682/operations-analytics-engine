"""Live/session derived loaders for performance runtime."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from oae.performance.loader_utils import join_unique_accounts, normalize_account, normalize_text, pick_live_column, split_hosts


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
    if not date_col or not account_col or not host_col:
        return pd.DataFrame(columns=["date", "scope_name", "parent_account"])

    data = raw[[date_col, account_col, host_col]].copy()
    data.columns = ["date", "parent_account", "hosts_raw"]
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.normalize()
    data["parent_account"] = data["parent_account"].apply(normalize_account)
    data["hosts"] = data["hosts_raw"].apply(split_hosts)
    data = data.explode("hosts", ignore_index=True)
    data = data.rename(columns={"hosts": "scope_name"})
    data["scope_name"] = data["scope_name"].apply(normalize_text)
    data = data[
        data["date"].notna()
        & (data["date"] >= month_start)
        & (data["date"] <= month_end)
        & data["scope_name"].ne("")
        & data["parent_account"].ne("")
    ][["date", "scope_name", "parent_account"]]
    if data.empty:
        return pd.DataFrame(columns=["date", "scope_name", "parent_account"])
    return data.drop_duplicates().sort_values(["date", "scope_name", "parent_account"]).reset_index(drop=True)

