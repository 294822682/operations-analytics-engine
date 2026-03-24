"""Shared live-session window builder."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from oae.rules.account_mapping import normalize_account
from oae.rules.columns import COLUMN_ALIASES, pick_col
from oae.rules.datetime_utils import combine_date_time_series


def build_live_windows(
    live_raw: pd.DataFrame,
    logger: logging.Logger,
    buffer_minutes: int,
    default_duration_minutes: int,
    max_duration_hours: int,
) -> pd.DataFrame:
    df = live_raw.copy()
    df.columns = df.columns.str.strip()

    date_col = pick_col(df, COLUMN_ALIASES["live_date"])
    account_col = pick_col(df, COLUMN_ALIASES["live_account"])
    start_col = pick_col(df, COLUMN_ALIASES["live_start"], required=False)
    end_col = pick_col(df, COLUMN_ALIASES["live_end"], required=False)
    host_col = pick_col(df, COLUMN_ALIASES["live_host"])

    df["标准账号"] = df[account_col].map(normalize_account)
    date_only = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()

    if start_col is None:
        logger.warning("直播进度表缺少开播时间列，按日期全天窗口匹配")
        valid_start = date_only.copy()
    else:
        valid_start = combine_date_time_series(df[date_col], df[start_col]).fillna(date_only)

    if end_col is None:
        logger.warning("直播进度表缺少下播时间列，按日期全天窗口匹配")
        valid_end = date_only + pd.Timedelta(hours=23, minutes=59, seconds=59)
    else:
        valid_end = combine_date_time_series(df[date_col], df[end_col])

    missing_end = valid_end.isna() & valid_start.notna()
    if missing_end.any():
        valid_end.loc[missing_end] = valid_start.loc[missing_end] + pd.Timedelta(minutes=default_duration_minutes)

    crossed_midnight = valid_end.notna() & valid_start.notna() & (valid_end < valid_start)
    if crossed_midnight.any():
        valid_end.loc[crossed_midnight] = valid_end.loc[crossed_midnight] + pd.Timedelta(days=1)

    too_long = (
        valid_end.notna()
        & valid_start.notna()
        & ((valid_end - valid_start) > pd.Timedelta(hours=max_duration_hours))
    )
    if too_long.any():
        valid_end.loc[too_long] = valid_start.loc[too_long] + pd.Timedelta(minutes=default_duration_minutes)

    pad = pd.Timedelta(minutes=buffer_minutes)
    df["Valid_Start"] = valid_start
    df["Valid_End"] = valid_end
    df["Match_Start"] = valid_start - pad
    df["Match_End"] = valid_end + pad
    df["本场主播"] = df[host_col].fillna("").astype(str).str.strip()

    out = df.dropna(subset=["标准账号", "Valid_Start", "Valid_End"]).copy()
    out = out[out["标准账号"] != ""].copy()
    out["_live_order"] = np.arange(len(out), dtype=np.int64)

    logger.info("直播场次有效行数: %s / %s", len(out), len(df))
    return out[["标准账号", "Match_Start", "Match_End", "Valid_Start", "Valid_End", "本场主播", "_live_order"]]
