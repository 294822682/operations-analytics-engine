"""Unified-fact funnel theme."""

from __future__ import annotations

import pandas as pd


def build_theme(fact: pd.DataFrame, latest_date: pd.Timestamp, subject_key_col: str) -> tuple[dict[str, pd.DataFrame], list[dict[str, object]]]:
    total_subjects = fact[subject_key_col].nunique()
    matched_subjects = fact.loc[fact["归属状态"] == "匹配成功", subject_key_col].nunique()
    unowned_subjects = fact.loc[fact["归属状态"] == "无主线索", subject_key_col].nunique()
    order_subjects = fact.loc[fact["is_order"] == 1, subject_key_col].nunique()
    deal_subjects = fact.loc[fact["is_deal"] == 1, subject_key_col].nunique()

    summary = pd.DataFrame(
        [
            {"stage": "subject_total", "count": total_subjects},
            {"stage": "matched_subjects", "count": matched_subjects},
            {"stage": "unowned_subjects", "count": unowned_subjects},
            {"stage": "ordered_subjects", "count": order_subjects},
            {"stage": "dealt_subjects", "count": deal_subjects},
        ]
    )
    summary["rate_vs_total"] = summary["count"] / total_subjects if total_subjects else 0.0
    summary["snapshot_date"] = latest_date.strftime("%Y-%m-%d")

    rows = []
    for _, row in summary.iterrows():
        rows.extend(
            [
                {
                    "snapshot_date": latest_date.strftime("%Y-%m-%d"),
                    "analysis_mode": "unified-fact",
                    "subject_area": "funnel",
                    "grain": "stage",
                    "dimension_key": row["stage"],
                    "metric_name": "count",
                    "metric_value": float(row["count"]),
                },
                {
                    "snapshot_date": latest_date.strftime("%Y-%m-%d"),
                    "analysis_mode": "unified-fact",
                    "subject_area": "funnel",
                    "grain": "stage",
                    "dimension_key": row["stage"],
                    "metric_name": "rate_vs_total",
                    "metric_value": float(row["rate_vs_total"]),
                },
            ]
        )

    return {"漏斗总览": summary}, rows
