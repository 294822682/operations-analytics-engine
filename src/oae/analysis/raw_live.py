"""Raw-evidence live/host analysis helpers."""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

from transform.lead_transform import ensure_str, parse_excel_mixed_datetime


HOST_SPLIT_PATTERN = re.compile(r"[，,、/；;]+")
DEFAULT_LIVE_DURATION_MINUTES = 240


def split_hosts(value) -> list:
    if pd.isna(value):
        return []
    text = str(value).strip()
    if text == "":
        return []
    return [part.strip() for part in HOST_SPLIT_PATTERN.split(text) if part.strip()]


def pick_first_existing_col(df: pd.DataFrame, candidates: list, required: bool = True):
    for column in candidates:
        if column in df.columns:
            return column
    if required:
        raise ValueError(f"缺少必要列，候选={candidates}")
    return None


def find_live_progress_file(workspace_dir: Path) -> Path | None:
    patterns = ["*直播进度表*月.xlsx", "*直播进度表*.xlsx", "*直播进度表*.xls"]
    for pattern in patterns:
        files = sorted(workspace_dir.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
        if files:
            return files[0]
    return None


def combine_date_and_time(date_series: pd.Series, dt_series: pd.Series, raw_time_series: pd.Series = None) -> pd.Series:
    out = dt_series.copy()
    date_floor = pd.to_datetime(date_series, errors="coerce").dt.floor("D")

    if raw_time_series is not None:
        raw = raw_time_series.fillna("").astype(str).str.strip()
        mask_hms = raw.str.match(r"^\d{1,2}:\d{2}(:\d{2})?$") & date_floor.notna()
        if mask_hms.any():
            out.loc[mask_hms] = pd.to_datetime(
                date_floor.loc[mask_hms].dt.strftime("%Y-%m-%d") + " " + raw.loc[mask_hms],
                errors="coerce",
            )

    mask_time_only = out.notna() & (out.dt.year <= 1901) & date_floor.notna()
    if mask_time_only.any():
        out.loc[mask_time_only] = pd.to_datetime(
            date_floor.loc[mask_time_only].dt.strftime("%Y-%m-%d") + " " + out.loc[mask_time_only].dt.strftime("%H:%M:%S"),
            errors="coerce",
        )

    mask_missing_time = out.isna() & date_floor.notna()
    if mask_missing_time.any():
        out.loc[mask_missing_time] = date_floor.loc[mask_missing_time]

    return out


def build_host_attribution_rows(base_df: pd.DataFrame, account_map: dict, workspace_dir: Path) -> pd.DataFrame:
    live_file = find_live_progress_file(workspace_dir)
    if live_file is None:
        return pd.DataFrame(columns=["创建时间", "创建日期", "标准账号", "本场主播", "权重", "到店量_加权", "试驾量_加权", "成交量_加权"])

    live_raw = pd.read_excel(live_file)
    date_col = pick_first_existing_col(live_raw, ["日期", "直播日期", "创建时间"])
    account_col = pick_first_existing_col(live_raw, ["开播账号", "账号", "直播账号", "账号名称"])
    start_col = pick_first_existing_col(live_raw, ["开播时间", "开始时间", "直播开始时间"])
    end_col = pick_first_existing_col(live_raw, ["下播时间", "结束时间", "直播结束时间"], required=False)
    host_col = pick_first_existing_col(live_raw, ["本场主播", "主播"])

    live = live_raw.copy()
    live["日期_dt"] = parse_excel_mixed_datetime(live[date_col])
    live["开播_dt"] = parse_excel_mixed_datetime(live[start_col])
    live["下播_dt"] = parse_excel_mixed_datetime(live[end_col]) if end_col is not None else pd.NaT
    live["开始时间"] = combine_date_and_time(live["日期_dt"], live["开播_dt"], live[start_col])
    live["结束时间"] = combine_date_and_time(live["日期_dt"], live["下播_dt"], live[end_col] if end_col is not None else None)
    live["结束时间"] = live["结束时间"].fillna(live["开始时间"] + pd.Timedelta(minutes=DEFAULT_LIVE_DURATION_MINUTES))
    cross_midnight = live["结束时间"] < live["开始时间"]
    if cross_midnight.any():
        live.loc[cross_midnight, "结束时间"] = live.loc[cross_midnight, "结束时间"] + pd.Timedelta(days=1)
    live["账号_原始"] = live[account_col].fillna("").astype(str).str.strip()
    live["标准账号"] = live["账号_原始"].map(account_map).fillna(live["账号_原始"])
    live["主播列表"] = live[host_col].apply(split_hosts)
    live = live[
        live["标准账号"].notna()
        & (live["标准账号"] != "")
        & live["开始时间"].notna()
        & live["结束时间"].notna()
    ].copy()

    sessions_by_account = {}
    for account, group in live.groupby("标准账号", dropna=False):
        sessions = []
        for _, row in group.iterrows():
            sessions.append(
                {
                    "start": row["开始时间"],
                    "end": row["结束时间"],
                    "mid": row["开始时间"] + (row["结束时间"] - row["开始时间"]) / 2,
                    "hosts": row["主播列表"] if len(row["主播列表"]) > 0 else ["【无主播】"],
                }
            )
        sessions_by_account[account] = sessions

    account_df = ensure_str(base_df.copy(), ["渠道2"])
    account_df["标准账号"] = account_df["渠道2"].map(account_map).fillna(account_df["渠道2"])
    account_df["创建日期"] = account_df["_创建DT"].dt.floor("D")

    host_rows = []
    for _, row in account_df.iterrows():
        account = row["标准账号"]
        created_at = row["_创建DT"]
        if pd.isna(created_at) or account not in sessions_by_account:
            continue
        matched = [session for session in sessions_by_account[account] if session["start"] <= created_at <= session["end"]]
        if not matched:
            continue
        best = min(matched, key=lambda session: abs((created_at - session["mid"]).total_seconds()))
        hosts = best["hosts"]
        weight = 1.0 / len(hosts) if hosts else np.nan
        for host in hosts:
            host_rows.append(
                {
                    "创建时间": created_at,
                    "创建日期": row["创建日期"],
                    "标准账号": account,
                    "本场主播": host,
                    "权重": weight,
                    "到店量_加权": row["是否到店"] * weight,
                    "试驾量_加权": row["是否试驾"] * weight,
                    "成交量_加权": row["是否成交"] * weight,
                }
            )

    if not host_rows:
        return pd.DataFrame(columns=["创建时间", "创建日期", "标准账号", "本场主播", "权重", "到店量_加权", "试驾量_加权", "成交量_加权"])
    return pd.DataFrame(host_rows)


def build_host_trace_table_latest(base_df: pd.DataFrame, account_map: dict, workspace_dir: Path):
    hdf = build_host_attribution_rows(base_df, account_map, workspace_dir)
    if hdf.empty:
        return pd.DataFrame(columns=["本场主播", "标准账号", "累计线索量_加权", "累计到店量_加权", "累计试驾量_加权", "累计成交量_加权", "累计到店率(创建->到店)%", "累计试驾率(创建->试驾)%", "累计成交率(创建->成交)%"])
    out = (
        hdf.groupby(["本场主播", "标准账号"], dropna=False)
        .agg(
            累计线索量_加权=("权重", "sum"),
            累计到店量_加权=("到店量_加权", "sum"),
            累计试驾量_加权=("试驾量_加权", "sum"),
            累计成交量_加权=("成交量_加权", "sum"),
        )
        .reset_index()
    )
    out["累计到店率(创建->到店)%"] = np.where(out["累计线索量_加权"] > 0, out["累计到店量_加权"] / out["累计线索量_加权"] * 100, np.nan)
    out["累计试驾率(创建->试驾)%"] = np.where(out["累计线索量_加权"] > 0, out["累计试驾量_加权"] / out["累计线索量_加权"] * 100, np.nan)
    out["累计成交率(创建->成交)%"] = np.where(out["累计线索量_加权"] > 0, out["累计成交量_加权"] / out["累计线索量_加权"] * 100, np.nan)
    return out.sort_values(["累计线索量_加权", "累计成交量_加权"], ascending=[False, False]).round(2)


def build_live_operation_table(base_df: pd.DataFrame, account_map: dict, workspace_dir: Path):
    live_file = find_live_progress_file(workspace_dir)
    if live_file is None:
        return pd.DataFrame(columns=["标准账号", "本场主播", "时段", "加权线索量", "到店率(创建->到店)%", "试驾率(创建->试驾)%", "成交率(试驾->成交)%", "总转化率(创建->成交)%"])

    host_rows = build_host_attribution_rows(base_df, account_map, workspace_dir)
    if host_rows.empty:
        return pd.DataFrame(columns=["标准账号", "本场主播", "时段", "加权线索量", "到店率(创建->到店)%", "试驾率(创建->试驾)%", "成交率(试驾->成交)%", "总转化率(创建->成交)%"])

    host_rows["时段"] = pd.to_datetime(host_rows["创建时间"], errors="coerce").dt.hour.map(
        lambda hour: "10-14" if pd.notna(hour) and 10 <= hour < 14 else ("14-18" if pd.notna(hour) and 14 <= hour < 18 else ("18-24" if pd.notna(hour) and 18 <= hour <= 23 else "其他"))
    )
    live_ops = (
        host_rows.groupby(["标准账号", "本场主播", "时段"], dropna=False)
        .agg(
            加权线索量=("权重", "sum"),
            加权到店量=("到店量_加权", "sum"),
            加权试驾量=("试驾量_加权", "sum"),
            加权成交量=("成交量_加权", "sum"),
        )
        .reset_index()
    )
    live_ops["到店率(创建->到店)%"] = np.where(live_ops["加权线索量"] > 0, live_ops["加权到店量"] / live_ops["加权线索量"] * 100, np.nan)
    live_ops["试驾率(创建->试驾)%"] = np.where(live_ops["加权线索量"] > 0, live_ops["加权试驾量"] / live_ops["加权线索量"] * 100, np.nan)
    live_ops["成交率(试驾->成交)%"] = np.where(live_ops["加权试驾量"] > 0, live_ops["加权成交量"] / live_ops["加权试驾量"] * 100, np.nan)
    live_ops["总转化率(创建->成交)%"] = np.where(live_ops["加权线索量"] > 0, live_ops["加权成交量"] / live_ops["加权线索量"] * 100, np.nan)
    for column in ["到店率(创建->到店)%", "试驾率(创建->试驾)%", "成交率(试驾->成交)%", "总转化率(创建->成交)%"]:
        live_ops[column] = live_ops[column].clip(lower=0, upper=100)
    live_ops = live_ops.sort_values(["加权线索量", "总转化率(创建->成交)%"], ascending=[False, False]).round(2)
    return live_ops[
        ["标准账号", "本场主播", "时段", "加权线索量", "到店率(创建->到店)%", "试驾率(创建->试驾)%", "成交率(试驾->成交)%", "总转化率(创建->成交)%"]
    ].copy()
