"""Shared normalization and loader helpers for performance runtime."""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

from oae.rules.account_mapping import normalize_account as shared_normalize_account
from oae.rules.file_discovery import (
    parse_year_month_from_live_filename as shared_parse_year_month_from_live_filename,
    pick_latest_live_file as shared_pick_latest_live_file,
)
from oae.rules.hosts import split_hosts_text


def normalize_text(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def to_number(v) -> float:
    if pd.isna(v):
        return np.nan
    try:
        return float(v)
    except (TypeError, ValueError):
        s = normalize_text(v).replace(",", "")
        if not s:
            return np.nan
        try:
            return float(s)
        except (TypeError, ValueError):
            return np.nan


def normalize_account(v) -> str:
    raw = normalize_text(v)
    if not raw:
        return ""
    return shared_normalize_account(raw)


def split_accounts(v) -> list[str]:
    s = normalize_text(v)
    if not s:
        return []
    if s.lower() in {"nan", "none", "null"}:
        return []

    parts = [x.strip() for x in re.split(r"[|/、,，;；]+", s)]
    parts = [x for x in parts if x and x.lower() not in {"nan", "none", "null"}]

    seen = set()
    out: list[str] = []
    for part in parts:
        account = normalize_account(part)
        if account and account not in seen:
            seen.add(account)
            out.append(account)
    return out


def split_hosts(v) -> list[str]:
    seen = set()
    out: list[str] = []
    for host in split_hosts_text(v):
        if host not in seen:
            seen.add(host)
            out.append(host)
    return out


def month_start_end(ym: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.to_datetime(f"{ym}-01", errors="raise")
    end = start + pd.offsets.MonthEnd(1)
    return start.normalize(), end.normalize()


def parse_year_month_from_live_filename(path: Path) -> tuple[int, int] | None:
    return shared_parse_year_month_from_live_filename(path)


def pick_latest_live_file(search_dirs: list[Path]) -> Path:
    return shared_pick_latest_live_file(search_dirs)


def pick_report_month(args_month: str, targets: pd.DataFrame) -> str:
    if args_month:
        return args_month
    non_empty = targets.loc[targets["month"].astype(str).str.strip() != "", "month"]
    if non_empty.empty:
        raise SystemExit("[ERROR] targets 里没有可用 month，请传 --month")
    return sorted(non_empty.astype(str).unique())[-1]


def pick_live_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    cols = {str(col).strip(): str(col).strip() for col in df.columns}
    lower = {str(col).strip().lower(): str(col).strip() for col in df.columns}
    for candidate in candidates:
        name = str(candidate).strip()
        if name in cols:
            return cols[name]
        if name.lower() in lower:
            return lower[name.lower()]
    if required:
        raise SystemExit(f"[ERROR] 缺少列: 候选={candidates}, 当前={list(df.columns)}")
    return None


def join_unique_accounts(series: pd.Series) -> str:
    seen = set()
    accounts: list[str] = []
    for value in series.tolist():
        account = normalize_text(value)
        if account and account not in seen:
            seen.add(account)
            accounts.append(account)
    return " / ".join(accounts)

