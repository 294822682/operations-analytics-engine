"""Performance output formatting helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _format_pct_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    out = pd.Series("N/A", index=numeric.index, dtype="object")
    mask = numeric.notna()
    out.loc[mask] = (numeric.loc[mask] * 100).map("{:.2f}%".format)
    return out


def _format_num_series(series: pd.Series, ndigits: int = 2) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    out = pd.Series("N/A", index=numeric.index, dtype="object")
    mask = numeric.notna()
    fmt = "{:." + str(ndigits) + "f}"
    out.loc[mask] = numeric.loc[mask].map(fmt.format)
    return out


def finalize_format(df: pd.DataFrame, report_month: str) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["report_month"] = report_month

    for col in [
        "daily_leads",
        "mtd_leads",
        "daily_deals",
        "mtd_deals",
        "lead_target_month",
        "deal_target_month",
        "daily_lead_target",
        "daily_deal_target",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).round(4)

    for col in ["lead_cost_target_month", "cpl_target", "cps_target"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(4)

    for col in ["daily_spend", "mtd_spend", "daily_cpl", "mtd_cpl", "daily_cps", "mtd_cps"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(4)

    for src_col, dst_col in [
        ("daily_lead_attain", "daily_lead_attain_pct"),
        ("mtd_lead_attain", "mtd_lead_attain_pct"),
        ("daily_deal_attain", "daily_deal_attain_pct"),
        ("mtd_deal_attain", "mtd_deal_attain_pct"),
    ]:
        src = out[src_col] if src_col in out.columns else pd.Series(np.nan, index=out.index)
        out[dst_col] = _format_pct_series(src)

    lead_cost_series = out["lead_cost_target_month"] if "lead_cost_target_month" in out.columns else pd.Series(np.nan, index=out.index)
    cpl_target_series = out["cpl_target"] if "cpl_target" in out.columns else pd.Series(np.nan, index=out.index)
    cps_target_series = out["cps_target"] if "cps_target" in out.columns else pd.Series(np.nan, index=out.index)
    out["lead_cost_target_month_disp"] = _format_num_series(lead_cost_series, 2)
    out["cpl_target_disp"] = _format_num_series(cpl_target_series, 2)
    out["cps_target_disp"] = _format_num_series(cps_target_series, 2)
    out["daily_cpl_disp"] = _format_num_series(out["daily_cpl"], 2)
    out["mtd_cpl_disp"] = _format_num_series(out["mtd_cpl"], 2)
    out["daily_cps_disp"] = _format_num_series(out["daily_cps"], 2)
    out["mtd_cps_disp"] = _format_num_series(out["mtd_cps"], 2)

    order = [
        "report_month",
        "date",
        "scope_type",
        "scope_name",
        "parent_account",
        "lead_target_month",
        "deal_target_month",
        "lead_cost_target_month",
        "lead_cost_target_month_disp",
        "cpl_target",
        "cpl_target_disp",
        "cps_target",
        "cps_target_disp",
        "daily_leads",
        "daily_lead_target",
        "daily_lead_attain",
        "daily_lead_attain_pct",
        "mtd_leads",
        "mtd_lead_attain",
        "mtd_lead_attain_pct",
        "daily_deals",
        "daily_deal_target",
        "daily_deal_attain",
        "daily_deal_attain_pct",
        "mtd_deals",
        "mtd_deal_attain",
        "mtd_deal_attain_pct",
        "daily_spend",
        "mtd_spend",
        "daily_cpl",
        "daily_cpl_disp",
        "mtd_cpl",
        "mtd_cpl_disp",
        "daily_cps",
        "daily_cps_disp",
        "mtd_cps",
        "mtd_cps_disp",
    ]
    existing = [col for col in order if col in out.columns]
    others = [col for col in out.columns if col not in existing]
    return out[existing + others].sort_values(["scope_type", "scope_name", "date"])
