"""Target configuration loader."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from oae.performance.loader_utils import normalize_account, normalize_text, split_accounts


def ensure_targets_template(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return

    template = pd.DataFrame(
        [
            ["2026-03", "account", "抖音-星途汽车官方直播间", "", 14476, 65.14285714285714, 247864.68, np.nan, np.nan, "官方+直营目标池"],
            ["2026-03", "account", "抖音-星途汽车直播营销中心", "", 10857, 48.85714285714286, 185898.51, np.nan, np.nan, "直播营销目标池"],
            ["2026-03", "account", "抖音-星途汽车直营中心", "", 14476, 65.14285714285714, 247864.68, np.nan, np.nan, "官方+直营目标池"],
            ["2026-03", "anchor", "丁俐佳", "抖音-星途汽车官方直播间/抖音-星途汽车直营中心", 3619, 16.285714285714285, 61966.17, 17.12, 3804.94, ""],
            ["2026-03", "anchor", "孙慧敏", "抖音-星途汽车官方直播间/抖音-星途汽车直营中心", 3619, 16.285714285714285, 61966.17, 17.12, 3804.94, ""],
            ["2026-03", "anchor", "何雯", "抖音-星途汽车官方直播间/抖音-星途汽车直营中心", 3619, 16.285714285714285, 61966.17, 17.12, 3804.94, ""],
            ["2026-03", "anchor", "徐幻", "抖音-星途汽车官方直播间/抖音-星途汽车直营中心", 3619, 16.285714285714285, 61966.17, 17.12, 3804.94, ""],
            ["2026-03", "anchor", "侯翩翩", "抖音-星途汽车直播营销中心", 3619, 16.285714285714285, 61966.17, 17.12, 3804.94, ""],
            ["2026-03", "anchor", "王馨", "抖音-星途汽车直播营销中心", 3619, 16.285714285714285, 61966.17, 17.12, 3804.94, ""],
            ["2026-03", "anchor", "曹嘉洋", "抖音-星途汽车官方直播间/抖音-星途汽车直营中心", 0, 0, 0, np.nan, np.nan, ""],
            ["2026-03", "anchor", "徐欣悦", "抖音-星途汽车直播营销中心", 3619, 16.285714285714285, 61966.17, 17.12, 3804.94, ""],
        ],
        columns=[
            "month",
            "scope_type",
            "scope_name",
            "parent_account",
            "lead_target_month",
            "deal_target_month",
            "lead_cost_target_month",
            "cpl_target",
            "cps_target",
            "target_pool",
        ],
    )
    template.to_csv(path, index=False, encoding="utf-8-sig")


def backfill_cpl_cps(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["cpl_target"] = np.where(
        out["cpl_target"].notna(),
        out["cpl_target"],
        np.where(
            (out["lead_cost_target_month"].notna()) & (out["lead_target_month"] > 0),
            out["lead_cost_target_month"] / out["lead_target_month"],
            np.nan,
        ),
    )
    out["cps_target"] = np.where(
        out["cps_target"].notna(),
        out["cps_target"],
        np.where(
            (out["lead_cost_target_month"].notna()) & (out["deal_target_month"] > 0),
            out["lead_cost_target_month"] / out["deal_target_month"],
            np.nan,
        ),
    )
    return out


def load_targets(path: Path) -> pd.DataFrame:
    ensure_targets_template(path)
    targets = pd.read_csv(path, encoding="utf-8-sig")

    required = {"month", "scope_type", "scope_name", "parent_account", "lead_target_month", "deal_target_month"}
    missing = required - set(targets.columns)
    if missing:
        raise SystemExit(f"[ERROR] targets 缺少列: {sorted(missing)}")

    targets = targets.copy()
    targets["month"] = targets["month"].astype(str).str.strip()
    targets["scope_type"] = targets["scope_type"].astype(str).str.strip().str.lower()
    targets["scope_name"] = targets["scope_name"].astype(str).str.strip()
    targets["parent_account"] = targets["parent_account"].apply(normalize_text)
    targets.loc[targets["scope_type"] == "account", "scope_name"] = (
        targets.loc[targets["scope_type"] == "account", "scope_name"].apply(normalize_account)
    )
    targets.loc[targets["scope_type"] == "account", "parent_account"] = ""
    targets.loc[targets["scope_type"] == "anchor", "parent_account"] = (
        targets.loc[targets["scope_type"] == "anchor", "parent_account"].apply(lambda value: " / ".join(split_accounts(value)))
    )
    targets["lead_target_month"] = pd.to_numeric(targets["lead_target_month"], errors="coerce").fillna(0.0)
    targets["deal_target_month"] = pd.to_numeric(targets["deal_target_month"], errors="coerce").fillna(0.0)

    for column in ["lead_cost_target_month", "cpl_target", "cps_target"]:
        if column not in targets.columns:
            targets[column] = np.nan
        targets[column] = pd.to_numeric(targets[column], errors="coerce")

    if "target_pool" not in targets.columns:
        targets["target_pool"] = ""
    targets["target_pool"] = targets["target_pool"].apply(normalize_text)
    account_mask = targets["scope_type"] == "account"
    targets.loc[account_mask, "target_pool"] = (
        targets.loc[account_mask, "target_pool"].replace("", np.nan).fillna(targets.loc[account_mask, "scope_name"])
    )
    targets.loc[~account_mask, "target_pool"] = ""
    return backfill_cpl_cps(targets)
