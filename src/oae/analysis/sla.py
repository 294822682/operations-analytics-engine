"""Unified-fact SLA/cycle theme."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_theme(fact: pd.DataFrame, latest_date: pd.Timestamp, subject_key_col: str) -> tuple[dict[str, pd.DataFrame], list[dict[str, object]]]:
    deals = fact[(fact["is_deal"] == 1)].copy()
    deals["线索创建时间"] = pd.to_datetime(deals["线索创建时间"], errors="coerce")
    deals["成交时间"] = pd.to_datetime(deals["成交时间"], errors="coerce")
    deals["create_to_deal_days"] = (deals["成交时间"] - deals["线索创建时间"]).dt.total_seconds() / 86400
    deals.loc[deals["create_to_deal_days"] < 0, "create_to_deal_days"] = np.nan

    overall = pd.DataFrame(
        [
            {
                "metric": "deal_subjects",
                "value": float(deals[subject_key_col].nunique()),
            },
            {
                "metric": "avg_create_to_deal_days",
                "value": float(deals["create_to_deal_days"].mean()) if deals["create_to_deal_days"].notna().any() else 0.0,
            },
            {
                "metric": "p50_create_to_deal_days",
                "value": float(deals["create_to_deal_days"].median()) if deals["create_to_deal_days"].notna().any() else 0.0,
            },
            {
                "metric": "p90_create_to_deal_days",
                "value": float(deals["create_to_deal_days"].quantile(0.90)) if deals["create_to_deal_days"].notna().any() else 0.0,
            },
        ]
    )

    by_account = (
        deals.groupby("标准账号", dropna=False)
        .agg(
            deal_subjects=(subject_key_col, "nunique"),
            avg_create_to_deal_days=("create_to_deal_days", "mean"),
            p50_create_to_deal_days=("create_to_deal_days", "median"),
        )
        .reset_index()
        .sort_values(["deal_subjects", "标准账号"], ascending=[False, True])
    )

    rows = []
    for _, row in overall.iterrows():
        rows.append(
            {
                "snapshot_date": latest_date.strftime("%Y-%m-%d"),
                "analysis_mode": "unified-fact",
                "subject_area": "sla",
                "grain": "overall",
                "dimension_key": "overall",
                "metric_name": row["metric"],
                "metric_value": float(row["value"]),
            }
        )
    for _, row in by_account.iterrows():
        for metric_name in ["deal_subjects", "avg_create_to_deal_days", "p50_create_to_deal_days"]:
            rows.append(
                {
                    "snapshot_date": latest_date.strftime("%Y-%m-%d"),
                    "analysis_mode": "unified-fact",
                    "subject_area": "sla",
                    "grain": "account",
                    "dimension_key": str(row["标准账号"]),
                    "metric_name": metric_name,
                    "metric_value": float(row[metric_name]) if pd.notna(row[metric_name]) else 0.0,
                }
            )
    return {"成交周期总览": overall, "成交周期_账号": by_account}, rows
