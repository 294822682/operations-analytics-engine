"""Unified-fact quality theme."""

from __future__ import annotations

import pandas as pd


def build_theme(fact: pd.DataFrame, latest_date: pd.Timestamp, subject_key_col: str) -> tuple[dict[str, pd.DataFrame], list[dict[str, object]]]:
    quality = (
        fact.groupby("标准账号", dropna=False)
        .agg(
            unique_subjects=(subject_key_col, "nunique"),
            phone_missing_rate=("手机号", lambda s: s.fillna("").astype(str).eq("").mean()),
            unowned_ratio=("归属状态", lambda s: (s == "无主线索").mean()),
            deal_subjects=("is_deal", "sum"),
        )
        .reset_index()
        .sort_values(["unique_subjects", "标准账号"], ascending=[False, True])
    )
    quality["deal_rate"] = quality["deal_subjects"] / quality["unique_subjects"].where(quality["unique_subjects"] > 0)

    rows = []
    for _, row in quality.iterrows():
        for metric_name in ["unique_subjects", "phone_missing_rate", "unowned_ratio", "deal_rate"]:
            rows.append(
                {
                    "snapshot_date": latest_date.strftime("%Y-%m-%d"),
                    "analysis_mode": "unified-fact",
                    "subject_area": "quality",
                    "grain": "account",
                    "dimension_key": str(row["标准账号"]),
                    "metric_name": metric_name,
                    "metric_value": float(row[metric_name]) if pd.notna(row[metric_name]) else 0.0,
                }
            )
    return {"质量_账号": quality}, rows
