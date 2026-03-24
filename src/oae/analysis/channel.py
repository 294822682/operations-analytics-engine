"""Unified-fact channel theme."""

from __future__ import annotations

import pandas as pd


def build_theme(fact: pd.DataFrame, latest_date: pd.Timestamp, subject_key_col: str) -> tuple[dict[str, pd.DataFrame], list[dict[str, object]]]:
    report_bucket = (
        fact.groupby("report_bucket", dropna=False)
        .agg(
            unique_subjects=(subject_key_col, "nunique"),
            order_subjects=("is_order", "sum"),
            deal_subjects=("is_deal", "sum"),
        )
        .reset_index()
    )
    report_bucket["deal_rate"] = report_bucket["deal_subjects"] / report_bucket["unique_subjects"].where(report_bucket["unique_subjects"] > 0)

    channel_col = "渠道" if "渠道" in fact.columns else None
    channel_detail = (
        fact.groupby(channel_col, dropna=False)
        .agg(unique_subjects=(subject_key_col, "nunique"), deal_subjects=("is_deal", "sum"))
        .reset_index()
        .rename(columns={channel_col: "成交渠道"})
        if channel_col
        else pd.DataFrame(columns=["成交渠道", "unique_subjects", "deal_subjects"])
    )

    rows = []
    for _, row in report_bucket.iterrows():
        for metric_name in ["unique_subjects", "order_subjects", "deal_subjects", "deal_rate"]:
            rows.append(
                {
                    "snapshot_date": latest_date.strftime("%Y-%m-%d"),
                    "analysis_mode": "unified-fact",
                    "subject_area": "channel",
                    "grain": "report_bucket",
                    "dimension_key": str(row["report_bucket"]),
                    "metric_name": metric_name,
                    "metric_value": float(row[metric_name]) if pd.notna(row[metric_name]) else 0.0,
                }
            )
    return {"渠道_报告桶": report_bucket, "渠道_成交渠道": channel_detail}, rows
