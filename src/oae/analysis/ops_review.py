"""Unified-fact operations review theme."""

from __future__ import annotations

import pandas as pd


def build_theme(fact: pd.DataFrame, latest_date: pd.Timestamp, subject_key_col: str) -> tuple[dict[str, pd.DataFrame], list[dict[str, object]]]:
    fact = fact.copy()
    fact["date"] = pd.to_datetime(fact["date"], errors="coerce").dt.normalize()
    daily_account = (
        fact.groupby(["date", "标准账号"], dropna=False)
        .agg(
            unique_subjects=(subject_key_col, "nunique"),
            orders_contrib=("orders_contrib", "sum"),
            deals_contrib=("deals_contrib", "sum"),
            unowned_subjects=("归属状态", lambda s: (s == "无主线索").sum()),
        )
        .reset_index()
        .sort_values(["date", "标准账号"])
    )
    daily_account["deal_rate"] = daily_account["deals_contrib"] / daily_account["unique_subjects"].where(daily_account["unique_subjects"] > 0)
    latest_review = daily_account[daily_account["date"] == latest_date].copy()

    rows = []
    for _, row in latest_review.iterrows():
        for metric_name in ["unique_subjects", "orders_contrib", "deals_contrib", "unowned_subjects", "deal_rate"]:
            rows.append(
                {
                    "snapshot_date": latest_date.strftime("%Y-%m-%d"),
                    "analysis_mode": "unified-fact",
                    "subject_area": "ops_review",
                    "grain": "account",
                    "dimension_key": str(row["标准账号"]),
                    "metric_name": metric_name,
                    "metric_value": float(row[metric_name]) if pd.notna(row[metric_name]) else 0.0,
                }
            )
    return {"运营复盘_账号日趋势": daily_account, "运营复盘_最新日": latest_review}, rows
