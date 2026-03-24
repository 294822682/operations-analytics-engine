"""Shared helpers for Feishu panel loading and validation."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from oae.rules.file_discovery import pick_latest_live_file as shared_pick_latest_live_file


DISPLAY_ACCOUNT_ORDER = ["抖音-星途汽车官方直播间", "抖音-星途汽车直播营销中心", "抖音-星途汽车直营中心"]
ACCOUNT_LABEL_MAP = {
    "抖音-星途汽车官方直播间": "星途汽车官方直播间",
    "抖音-星途汽车直播营销中心": "星途汽车直播营销中心",
    "抖音-星途汽车直营中心": "星途汽车直营中心",
    "线索组汇总": "线索组汇总",
}
ANCHOR_ORDER = ["丁俐佳", "孙慧敏", "何雯", "徐幻", "侯翩翩", "王馨", "曹嘉洋", "徐欣悦"]
PCT_RENAME_MAP = {
    "daily_lead_attain_pct": "当日线索达成率",
    "mtd_lead_attain_pct": "累计线索达成率",
    "daily_deal_attain_pct": "当日实销达成率",
    "mtd_deal_attain_pct": "累计实销达成率",
}
PCT_DISPLAY_COLS = list(PCT_RENAME_MAP.values())

ACCOUNT_REQUIRED_COLUMNS = [
    "scope_name",
    "daily_leads",
    "daily_lead_target",
    "daily_lead_attain_pct",
    "mtd_leads",
    "lead_target_month",
    "mtd_lead_attain_pct",
    "daily_deals",
    "daily_deal_target",
    "daily_deal_attain_pct",
    "mtd_deals",
    "deal_target_month",
    "mtd_deal_attain_pct",
    "lead_cost_target_month",
    "cpl_target",
    "cps_target",
    "mtd_spend",
    "mtd_cpl",
    "mtd_cps",
]
ANCHOR_REQUIRED_COLUMNS = [
    "scope_name",
    "parent_account",
    "daily_leads",
    "daily_lead_target",
    "daily_lead_attain_pct",
    "mtd_leads",
    "lead_target_month",
    "mtd_lead_attain_pct",
    "daily_deals",
    "daily_deal_target",
    "daily_deal_attain_pct",
    "mtd_deals",
    "deal_target_month",
    "mtd_deal_attain_pct",
    "lead_cost_target_month",
    "cpl_target",
    "cps_target",
    "mtd_spend",
    "mtd_cpl",
    "mtd_cps",
]
FACT_REQUIRED_COLUMNS = ["is_deal", "成交时间", "标准账号", "线索ID_norm", "date", "归属状态"]


def find_latest_file(reports_dir: Path, prefix: str) -> tuple[Path, str]:
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{4}}-\d{{2}}-\d{{2}})\.csv$")
    candidates = []
    for path in reports_dir.glob(f"{prefix}*.csv"):
        matched = pattern.match(path.name)
        if matched:
            candidates.append((matched.group(1), path))
    if not candidates:
        raise SystemExit(f"[ERROR] no files matched: {prefix}*.csv in {reports_dir}")
    date_str, path = sorted(candidates, key=lambda item: item[0])[-1]
    return path, date_str


def pick_latest_live_file(search_dirs: list[Path]) -> Path:
    return shared_pick_latest_live_file(search_dirs)


def load_panel_for_date(reports_dir: Path, report_date_str: str, scope: str) -> pd.DataFrame:
    latest_path = reports_dir / f"daily_goal_{scope}_latest_{report_date_str}.csv"
    if latest_path.exists():
        return pd.read_csv(latest_path)

    month_tag = report_date_str[:7]
    full_path = reports_dir / f"daily_goal_{scope}_{month_tag}.csv"
    if not full_path.exists():
        raise SystemExit(f"[ERROR] 缺少 {scope} 日报文件: {latest_path} 和 {full_path}")

    full_df = pd.read_csv(full_path)
    if "date" not in full_df.columns:
        raise SystemExit(f"[ERROR] {full_path} 缺少 date 列")

    out = full_df[pd.to_datetime(full_df["date"], errors="coerce").dt.normalize() == pd.to_datetime(report_date_str)].copy()
    if out.empty:
        raise SystemExit(f"[ERROR] {full_path} 中没有 {report_date_str} 的数据")
    return out


def load_panel_from_snapshot(snapshot_path: Path, report_date_str: str, scope: str) -> pd.DataFrame:
    snapshot = pd.read_csv(snapshot_path)
    if "date" not in snapshot.columns or "scope_type" not in snapshot.columns:
        raise SystemExit(f"[ERROR] snapshot 缺少 date/scope_type: {snapshot_path}")
    snapshot["date"] = pd.to_datetime(snapshot["date"], errors="coerce").dt.normalize()
    report_date = pd.to_datetime(report_date_str).normalize()
    scope_types = {"account": {"account", "account_total"}, "anchor": {"anchor"}}[scope]
    out = snapshot[(snapshot["date"] == report_date) & (snapshot["scope_type"].isin(scope_types))].copy()
    if out.empty:
        raise SystemExit(f"[ERROR] snapshot {snapshot_path} 中没有 {scope} / {report_date_str} 数据")
    return out


def resolve_report_date(args, snapshot_path: Path | None, reports_dir: Path) -> str:
    if args.report_date:
        return str(pd.to_datetime(args.report_date).date())
    if snapshot_path and snapshot_path.exists():
        snap = pd.read_csv(snapshot_path, usecols=lambda c: c in {"snapshot_date", "date"})
        if "snapshot_date" in snap.columns:
            val = snap["snapshot_date"].dropna().astype(str)
            if not val.empty:
                return val.iloc[0]
        if "date" in snap.columns:
            val = pd.to_datetime(snap["date"], errors="coerce").dropna()
            if not val.empty:
                return str(val.max().date())
    _, latest_date_str = find_latest_file(reports_dir, "daily_goal_account_latest_")
    return latest_date_str


def infer_run_id(frame: pd.DataFrame) -> str:
    if "run_id" not in frame.columns:
        return ""
    series = frame["run_id"].dropna().astype(str)
    return series.iloc[0] if not series.empty else ""


def validate_columns(df: pd.DataFrame, required: list[str], label: str) -> bool:
    missing = [column for column in required if column not in df.columns]
    if missing:
        print(f"[ERROR] {label} 缺少关键列: {missing}")
        print(f"[ERROR] {label} 当前列: {list(df.columns)}")
        return False
    return True


def get_target_accounts(acc: pd.DataFrame) -> list[str]:
    if acc.empty:
        return []
    out = acc[acc["scope_name"] != "线索组汇总"].copy()
    for col in ["lead_target_month", "deal_target_month"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0) if col in out.columns else 0.0
    out = out[(out["lead_target_month"] > 0) | (out["deal_target_month"] > 0)]
    if out.empty:
        return []
    have = set(out["scope_name"].dropna().astype(str).tolist())
    ordered = [account for account in DISPLAY_ACCOUNT_ORDER if account in have]
    remain = sorted([account for account in have if account not in DISPLAY_ACCOUNT_ORDER])
    return ordered + remain

