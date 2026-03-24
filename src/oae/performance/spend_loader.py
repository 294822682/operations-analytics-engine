"""Spend loaders for performance runtime."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from oae.performance.loader_utils import normalize_account, pick_live_column


def ensure_spend_template(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    pd.DataFrame(columns=["date", "account", "actual_spend"]).to_csv(path, index=False, encoding="utf-8-sig")


def load_spend(path: Path) -> pd.DataFrame:
    ensure_spend_template(path)
    spend = pd.read_csv(path, encoding="utf-8-sig")
    if spend.empty:
        return pd.DataFrame(columns=["date", "account", "actual_spend"])

    required = {"date", "account", "actual_spend"}
    missing = required - set(spend.columns)
    if missing:
        raise SystemExit(f"[ERROR] spend 缺少列: {sorted(missing)}")

    spend = spend.copy()
    spend["date"] = pd.to_datetime(spend["date"], errors="coerce").dt.normalize()
    spend["account"] = spend["account"].apply(normalize_account)
    spend["actual_spend"] = pd.to_numeric(spend["actual_spend"], errors="coerce")
    spend = spend.dropna(subset=["date", "account", "actual_spend"])
    return spend.groupby(["date", "account"], as_index=False)["actual_spend"].sum()


def load_spend_from_live(path: Path, strict: bool = False) -> pd.DataFrame:
    if not path.exists():
        if strict:
            raise SystemExit(f"[ERROR] live file not found: {path}")
        return pd.DataFrame(columns=["date", "account", "actual_spend"])

    try:
        workbook = pd.ExcelFile(path)
        raw = pd.read_excel(path, sheet_name=workbook.sheet_names[0])
    except Exception as exc:
        if strict:
            raise SystemExit(f"[ERROR] 读取直播进度表失败: {path}, err={exc}")
        return pd.DataFrame(columns=["date", "account", "actual_spend"])

    raw = raw.copy()
    raw.columns = [str(col).strip() for col in raw.columns]
    date_col = pick_live_column(raw, ["日期", "直播日期", "创建时间"])
    account_col = pick_live_column(raw, ["开播账号", "账号", "直播账号", "账号名称"])
    spend_col = pick_live_column(raw, ["消耗", "实际消耗", "当日消耗", "花费", "费用", "投放消耗", "总消耗"], required=False)
    cpl_col = pick_live_column(raw, ["CPL", "cpl", "线索成本"], required=False)

    if spend_col is None:
        if strict:
            raise SystemExit("[ERROR] 直播进度表未找到消耗字段，请新增列（如：消耗/实际消耗/当日消耗）")
        return pd.DataFrame(columns=["date", "account", "actual_spend"])

    spend = raw[[date_col, account_col, spend_col]].copy()
    spend.columns = ["date", "account", "actual_spend"]
    spend["date"] = pd.to_datetime(spend["date"], errors="coerce").dt.normalize()
    spend["account"] = spend["account"].apply(normalize_account)
    spend["actual_spend"] = pd.to_numeric(spend["actual_spend"], errors="coerce")
    spend = spend.dropna(subset=["date", "account", "actual_spend"])
    spend = spend.groupby(["date", "account"], as_index=False)["actual_spend"].sum()

    if cpl_col is not None:
        print(f"[INFO] 检测到直播进度表含CPL列({cpl_col})，已忽略该列并按统一口径自动计算CPL。")
    return spend


def resolve_spend_data(spend_source: str, spend_path: Path, live_path: Path) -> tuple[pd.DataFrame, str]:
    source = spend_source.lower().strip()
    if source == "csv":
        return load_spend(spend_path), "csv"
    if source == "live":
        return load_spend_from_live(live_path, strict=True), "live"

    live_spend = load_spend_from_live(live_path, strict=False)
    if not live_spend.empty:
        return live_spend, "live(auto)"

    csv_spend = load_spend(spend_path)
    if not csv_spend.empty:
        return csv_spend, "csv(auto)"
    return csv_spend, "empty"

