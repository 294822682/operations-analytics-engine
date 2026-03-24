"""Performance panel calculators."""

from __future__ import annotations

import numpy as np
import pandas as pd

from oae.performance.loaders import backfill_cpl_cps, join_unique_accounts, normalize_text, split_hosts


def apply_progress(
    panel: pd.DataFrame,
    group_cols: list[str],
    lead_target_col: str,
    deal_target_col: str,
    month_end: pd.Timestamp,
) -> pd.DataFrame:
    out = panel.copy().sort_values(group_cols + ["date"])

    for col in ["daily_leads", "daily_deals", "daily_spend"]:
        if col not in out.columns:
            out[col] = 0.0

    out["mtd_leads"] = out.groupby(group_cols)["daily_leads"].cumsum()
    out["mtd_deals"] = out.groupby(group_cols)["daily_deals"].cumsum()
    out["mtd_spend"] = out.groupby(group_cols)["daily_spend"].cumsum()
    out["lead_target_month"] = pd.to_numeric(out[lead_target_col], errors="coerce").fillna(0.0)
    out["deal_target_month"] = pd.to_numeric(out[deal_target_col], errors="coerce").fillna(0.0)
    out["mtd_leads_before"] = out["mtd_leads"] - out["daily_leads"]
    out["mtd_deals_before"] = out["mtd_deals"] - out["daily_deals"]
    out["remain_days"] = (month_end - out["date"]).dt.days + 1

    out["daily_lead_target"] = np.where(
        out["remain_days"] > 0,
        np.maximum(out["lead_target_month"] - out["mtd_leads_before"], 0) / out["remain_days"],
        0,
    )
    out["daily_deal_target"] = np.where(
        out["remain_days"] > 0,
        np.maximum(out["deal_target_month"] - out["mtd_deals_before"], 0) / out["remain_days"],
        0,
    )
    out["daily_lead_attain"] = np.where(out["daily_lead_target"] > 0, out["daily_leads"] / out["daily_lead_target"], np.nan)
    out["daily_deal_attain"] = np.where(out["daily_deal_target"] > 0, out["daily_deals"] / out["daily_deal_target"], np.nan)
    out["mtd_lead_attain"] = np.where(out["lead_target_month"] > 0, out["mtd_leads"] / out["lead_target_month"], np.nan)
    out["mtd_deal_attain"] = np.where(out["deal_target_month"] > 0, out["mtd_deals"] / out["deal_target_month"], np.nan)
    out["daily_cpl"] = np.where(out["daily_leads"] > 0, out["daily_spend"] / out["daily_leads"], np.nan)
    out["mtd_cpl"] = np.where(out["mtd_leads"] > 0, out["mtd_spend"] / out["mtd_leads"], np.nan)
    out["daily_cps"] = np.where(out["daily_deals"] > 0, out["daily_spend"] / out["daily_deals"], np.nan)
    out["mtd_cps"] = np.where(out["mtd_deals"] > 0, out["mtd_spend"] / out["mtd_deals"], np.nan)
    return out.drop(columns=["mtd_leads_before", "mtd_deals_before", "remain_days"])


def _aggregate_unique_ids_by_account_date(
    data: pd.DataFrame,
    date_col: str,
    value_col: str,
    out_col: str,
) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=["date", "标准账号", out_col])
    out = data.groupby([date_col, "标准账号"], as_index=False)[value_col].nunique()
    return out.rename(columns={date_col: "date", value_col: out_col})


def build_account_panel(
    fact: pd.DataFrame,
    targets_month: pd.DataFrame,
    spend_month: pd.DataFrame,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
) -> pd.DataFrame:
    account_targets = targets_month[targets_month["scope_type"] == "account"].copy()
    mask_lead_month = (fact["date"] >= month_start) & (fact["date"] <= month_end)
    leads_month = fact.loc[mask_lead_month & fact["线索ID_norm"].notna() & (fact["线索ID_norm"] != "")].copy()
    leads_daily = _aggregate_unique_ids_by_account_date(leads_month, "date", "线索ID_norm", "daily_leads")

    deals_month = fact.loc[
        (fact["is_deal"] == 1)
        & fact["deal_date"].notna()
        & (fact["deal_date"] >= month_start)
        & (fact["deal_date"] <= month_end)
    ].copy()
    deals_daily = _aggregate_unique_ids_by_account_date(deals_month, "deal_date", "线索ID_norm", "daily_deals")

    accounts = set(account_targets["scope_name"].tolist())
    accounts.update(leads_daily["标准账号"].dropna().astype(str).tolist())
    accounts.update(deals_daily["标准账号"].dropna().astype(str).tolist())
    accounts.update(spend_month["account"].dropna().astype(str).tolist())
    accounts = sorted([item for item in accounts if item])

    dates = pd.date_range(month_start, month_end, freq="D")
    out = pd.MultiIndex.from_product([dates, accounts], names=["date", "scope_name"]).to_frame(index=False)
    out = out.merge(leads_daily.rename(columns={"标准账号": "scope_name"}), on=["date", "scope_name"], how="left")
    out = out.merge(deals_daily.rename(columns={"标准账号": "scope_name"}), on=["date", "scope_name"], how="left")
    out = out.merge(
        spend_month.rename(columns={"account": "scope_name", "actual_spend": "daily_spend"}),
        on=["date", "scope_name"],
        how="left",
    )

    out["daily_leads"] = pd.to_numeric(out["daily_leads"], errors="coerce").fillna(0.0)
    out["daily_deals"] = pd.to_numeric(out["daily_deals"], errors="coerce").fillna(0.0)
    out["daily_spend"] = pd.to_numeric(out["daily_spend"], errors="coerce").fillna(0.0)

    target_cols = [
        col
        for col in ["lead_target_month", "deal_target_month", "lead_cost_target_month", "cpl_target", "cps_target", "target_pool"]
        if col in account_targets.columns
    ]
    out = out.merge(account_targets.set_index("scope_name")[target_cols], left_on="scope_name", right_index=True, how="left")

    for col, default in [("lead_target_month", 0.0), ("deal_target_month", 0.0)]:
        if col not in out.columns:
            out[col] = default
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(default)

    for col in ["lead_cost_target_month", "cpl_target", "cps_target"]:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "target_pool" not in out.columns:
        out["target_pool"] = ""
    out["target_pool"] = out["target_pool"].fillna("").astype(str).str.strip()
    out.loc[out["target_pool"].isin(["", "nan", "none", "null"]), "target_pool"] = out["scope_name"]
    out = backfill_cpl_cps(out)
    out = apply_progress(out, ["scope_name"], "lead_target_month", "deal_target_month", month_end)
    out["scope_type"] = "account"
    out["parent_account"] = ""

    total_accounts = account_targets["scope_name"].dropna().astype(str).tolist()
    if total_accounts:
        totals = out[out["scope_name"].isin(total_accounts)].copy()
        total_daily = totals.groupby("date", as_index=False)[["daily_leads", "daily_deals", "daily_spend"]].sum().sort_values("date")
        total_daily["scope_name"] = "线索组汇总"
        total_daily["target_pool"] = ""

        pool_targets = account_targets.copy()
        if "target_pool" not in pool_targets.columns:
            pool_targets["target_pool"] = pool_targets["scope_name"]
        pool_targets["target_pool"] = pool_targets["target_pool"].fillna("").astype(str).str.strip()
        pool_targets.loc[pool_targets["target_pool"].isin(["", "nan", "none", "null"]), "target_pool"] = pool_targets["scope_name"]
        pool_targets = (
            pool_targets.sort_values("scope_name")
            .groupby("target_pool", as_index=False)
            .agg(
                lead_target_month=("lead_target_month", "max"),
                deal_target_month=("deal_target_month", "max"),
                lead_cost_target_month=("lead_cost_target_month", "max"),
            )
        )

        total_daily["lead_target_month"] = pool_targets["lead_target_month"].sum()
        total_daily["deal_target_month"] = pool_targets["deal_target_month"].sum()
        account_cost = pd.to_numeric(pool_targets["lead_cost_target_month"], errors="coerce")
        if account_cost.notna().any():
            total_cost = float(account_cost.sum())
        else:
            anchor_cost = pd.to_numeric(
                targets_month.loc[targets_month["scope_type"] == "anchor", "lead_cost_target_month"],
                errors="coerce",
            )
            total_cost = float(anchor_cost.sum()) if anchor_cost.notna().any() else np.nan

        total_daily["lead_cost_target_month"] = total_cost
        total_daily["cpl_target"] = np.where(
            (pd.notna(total_cost)) & (total_daily["lead_target_month"] > 0),
            total_cost / total_daily["lead_target_month"],
            np.nan,
        )
        total_daily["cps_target"] = np.where(
            (pd.notna(total_cost)) & (total_daily["deal_target_month"] > 0),
            total_cost / total_daily["deal_target_month"],
            np.nan,
        )
        total_daily = apply_progress(total_daily, ["scope_name"], "lead_target_month", "deal_target_month", month_end)
        total_daily["scope_type"] = "account_total"
        total_daily["parent_account"] = ""
        out = pd.concat([out, total_daily], ignore_index=True)
    return out


def _explode_by_hosts(data: pd.DataFrame, date_col: str) -> pd.DataFrame:
    base_cols = [date_col, "标准账号", "本场主播", "线索ID_norm"]
    if data.empty:
        return pd.DataFrame(columns=["date", "parent_account", "scope_name", "线索ID_norm", "weight"])

    exploded = data[base_cols].copy()
    exploded["hosts"] = exploded["本场主播"].apply(split_hosts)
    exploded["n_hosts"] = exploded["hosts"].str.len()
    exploded = exploded[exploded["n_hosts"] > 0].copy()
    if exploded.empty:
        return pd.DataFrame(columns=["date", "parent_account", "scope_name", "线索ID_norm", "weight"])

    exploded["weight"] = (100 // exploded["n_hosts"]) / 100.0
    exploded = exploded.explode("hosts", ignore_index=True)
    exploded["scope_name"] = exploded["hosts"].apply(normalize_text)
    exploded = exploded[exploded["scope_name"] != ""].copy()
    exploded = exploded.rename(columns={date_col: "date", "标准账号": "parent_account"})
    return exploded[["date", "parent_account", "scope_name", "线索ID_norm", "weight"]]


def _explode_leads_by_anchor(fact: pd.DataFrame, month_start: pd.Timestamp, month_end: pd.Timestamp) -> pd.DataFrame:
    base = fact.loc[
        (fact["date"] >= month_start)
        & (fact["date"] <= month_end)
        & fact["线索ID_norm"].notna()
        & (fact["线索ID_norm"] != "")
    ].copy()
    exploded = _explode_by_hosts(base, date_col="date")
    if exploded.empty:
        return pd.DataFrame(columns=["date", "parent_account", "scope_name", "daily_leads"])
    return exploded.groupby(["date", "parent_account", "scope_name"], as_index=False)["weight"].sum().rename(columns={"weight": "daily_leads"})


def _explode_deals_by_anchor(fact: pd.DataFrame, month_start: pd.Timestamp, month_end: pd.Timestamp) -> pd.DataFrame:
    base = fact.loc[
        (fact["is_deal"] == 1)
        & fact["deal_date"].notna()
        & (fact["deal_date"] >= month_start)
        & (fact["deal_date"] <= month_end)
    ].copy()
    exploded = _explode_by_hosts(base, date_col="deal_date")
    if exploded.empty:
        return pd.DataFrame(columns=["date", "parent_account", "scope_name", "daily_deals"])
    return exploded.groupby(["date", "parent_account", "scope_name"], as_index=False)["weight"].sum().rename(columns={"weight": "daily_deals"})


def _allocate_spend_to_anchors(leads_daily: pd.DataFrame, spend_month: pd.DataFrame) -> pd.DataFrame:
    if leads_daily.empty or spend_month.empty:
        return pd.DataFrame(columns=["date", "parent_account", "scope_name", "daily_spend"])

    share = leads_daily.copy()
    share["account_day_leads"] = share.groupby(["date", "parent_account"])["daily_leads"].transform("sum")
    share["lead_share"] = np.where(share["account_day_leads"] > 0, share["daily_leads"] / share["account_day_leads"], 0.0)
    spend_alloc = share.merge(
        spend_month.rename(columns={"account": "parent_account", "actual_spend": "account_daily_spend"}),
        on=["date", "parent_account"],
        how="left",
    )
    spend_alloc["account_daily_spend"] = pd.to_numeric(spend_alloc["account_daily_spend"], errors="coerce").fillna(0.0)
    spend_alloc["daily_spend"] = spend_alloc["lead_share"] * spend_alloc["account_daily_spend"]
    return spend_alloc[["date", "parent_account", "scope_name", "daily_spend"]].copy()


def _build_anchor_labels(
    leads_daily: pd.DataFrame,
    deals_daily: pd.DataFrame,
    spend_alloc: pd.DataFrame,
    live_anchor_accounts: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if live_anchor_accounts.empty:
        schedule_labels = pd.DataFrame(columns=["date", "scope_name", "scheduled_parent_account"])
    else:
        schedule_keys = live_anchor_accounts[["date", "scope_name", "parent_account"]].drop_duplicates()
        schedule_labels = (
            schedule_keys.groupby(["date", "scope_name"], as_index=False)["parent_account"].agg(join_unique_accounts)
        ).rename(columns={"parent_account": "scheduled_parent_account"})

    account_sources = []
    for accounts in [leads_daily, deals_daily, spend_alloc]:
        if not accounts.empty:
            account_sources.append(accounts[["date", "scope_name", "parent_account"]])

    if account_sources:
        actual_accounts = pd.concat(account_sources, ignore_index=True).drop_duplicates()
        actual_labels = (
            actual_accounts.groupby(["date", "scope_name"], as_index=False)["parent_account"].agg(join_unique_accounts)
        ).rename(columns={"parent_account": "actual_parent_account"})
    else:
        actual_labels = pd.DataFrame(columns=["date", "scope_name", "actual_parent_account"])
    return schedule_labels, actual_labels


def _split_sched_vs_all(
    daily: pd.DataFrame,
    value_col: str,
    schedule_keys: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_col = f"{value_col}_all"
    sched_col = f"{value_col}_sched"
    if daily.empty:
        return (
            pd.DataFrame(columns=["date", "scope_name", all_col]),
            pd.DataFrame(columns=["date", "scope_name", sched_col]),
        )

    all_agg = daily.groupby(["date", "scope_name"], as_index=False)[value_col].sum().rename(columns={value_col: all_col})
    if schedule_keys.empty:
        sched_agg = pd.DataFrame(columns=["date", "scope_name", sched_col])
    else:
        sched_agg = (
            daily.merge(schedule_keys, on=["date", "scope_name", "parent_account"], how="inner")
            .groupby(["date", "scope_name"], as_index=False)[value_col]
            .sum()
            .rename(columns={value_col: sched_col})
        )
    return all_agg, sched_agg


def build_anchor_panel(
    fact: pd.DataFrame,
    targets_month: pd.DataFrame,
    spend_month: pd.DataFrame,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
    live_anchor_accounts: pd.DataFrame | None = None,
) -> pd.DataFrame:
    anchor_targets = targets_month[targets_month["scope_type"] == "anchor"].copy()
    if anchor_targets.empty:
        return pd.DataFrame()

    leads_daily = _explode_leads_by_anchor(fact, month_start, month_end)
    deals_daily = _explode_deals_by_anchor(fact, month_start, month_end)
    spend_alloc = _allocate_spend_to_anchors(leads_daily, spend_month)

    live_anchor_accounts = live_anchor_accounts.copy() if live_anchor_accounts is not None else pd.DataFrame(columns=["date", "scope_name", "parent_account"])
    schedule_keys = (
        live_anchor_accounts[["date", "scope_name", "parent_account"]].drop_duplicates()
        if not live_anchor_accounts.empty
        else pd.DataFrame(columns=["date", "scope_name", "parent_account"])
    )
    schedule_labels, actual_labels = _build_anchor_labels(leads_daily, deals_daily, spend_alloc, live_anchor_accounts)
    leads_all, leads_sched = _split_sched_vs_all(leads_daily, "daily_leads", schedule_keys)
    deals_all, deals_sched = _split_sched_vs_all(deals_daily, "daily_deals", schedule_keys)
    spend_all, spend_sched = _split_sched_vs_all(spend_alloc, "daily_spend", schedule_keys)

    dates = pd.date_range(month_start, month_end, freq="D")
    anchor_meta = anchor_targets.copy()
    anchor_meta["scope_name"] = anchor_meta["scope_name"].apply(normalize_text)
    anchor_meta = anchor_meta[anchor_meta["scope_name"] != ""].reset_index(drop=True)
    if anchor_meta.empty:
        return pd.DataFrame()

    for col in ["lead_target_month", "deal_target_month", "lead_cost_target_month", "cpl_target", "cps_target"]:
        if col not in anchor_meta.columns:
            anchor_meta[col] = np.nan
        anchor_meta[col] = pd.to_numeric(anchor_meta[col], errors="coerce").astype(float)

    anchor_meta["anchor_idx"] = np.arange(len(anchor_meta), dtype=np.int64)
    out = pd.MultiIndex.from_product([dates, anchor_meta["anchor_idx"]], names=["date", "anchor_idx"]).to_frame(index=False)
    out = out.merge(
        anchor_meta[
            ["anchor_idx", "scope_name", "lead_target_month", "deal_target_month", "lead_cost_target_month", "cpl_target", "cps_target"]
        ],
        on="anchor_idx",
        how="left",
    ).drop(columns=["anchor_idx"])
    out = backfill_cpl_cps(out)
    out = out.merge(schedule_labels, on=["date", "scope_name"], how="left")
    out = out.merge(actual_labels, on=["date", "scope_name"], how="left")
    out = out.merge(leads_all, on=["date", "scope_name"], how="left")
    out = out.merge(leads_sched, on=["date", "scope_name"], how="left")
    out = out.merge(deals_all, on=["date", "scope_name"], how="left")
    out = out.merge(deals_sched, on=["date", "scope_name"], how="left")
    out = out.merge(spend_all, on=["date", "scope_name"], how="left")
    out = out.merge(spend_sched, on=["date", "scope_name"], how="left")

    has_schedule = out["scheduled_parent_account"].fillna("").ne("")
    out["parent_account"] = out["scheduled_parent_account"]
    out.loc[~has_schedule, "parent_account"] = out.loc[~has_schedule, "actual_parent_account"]
    out["parent_account"] = out["parent_account"].fillna("")
    out.loc[out["parent_account"].eq(""), "parent_account"] = "当日未开播"

    leads_sched_num = pd.to_numeric(out["daily_leads_sched"], errors="coerce").fillna(0.0)
    leads_all_num = pd.to_numeric(out["daily_leads_all"], errors="coerce").fillna(0.0)
    deals_sched_num = pd.to_numeric(out["daily_deals_sched"], errors="coerce").fillna(0.0)
    deals_all_num = pd.to_numeric(out["daily_deals_all"], errors="coerce").fillna(0.0)
    spend_sched_num = pd.to_numeric(out["daily_spend_sched"], errors="coerce").fillna(0.0)
    spend_all_num = pd.to_numeric(out["daily_spend_all"], errors="coerce").fillna(0.0)
    out["daily_leads"] = np.where(has_schedule, leads_sched_num, leads_all_num)
    out["daily_deals"] = np.where(has_schedule, deals_sched_num, deals_all_num)
    out["daily_spend"] = np.where(has_schedule, spend_sched_num, spend_all_num)

    out = out.drop(
        columns=[
            col
            for col in [
                "scheduled_parent_account",
                "actual_parent_account",
                "daily_leads_all",
                "daily_leads_sched",
                "daily_deals_all",
                "daily_deals_sched",
                "daily_spend_all",
                "daily_spend_sched",
            ]
            if col in out.columns
        ]
    )
    out = apply_progress(out, ["scope_name"], "lead_target_month", "deal_target_month", month_end)
    out["scope_type"] = "anchor"
    return out
