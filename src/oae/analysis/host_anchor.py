"""Unified-fact host/anchor theme."""

from __future__ import annotations

import pandas as pd

from oae.rules.hosts import split_hosts_text


def build_theme(fact: pd.DataFrame, latest_date: pd.Timestamp, subject_key_col: str) -> tuple[dict[str, pd.DataFrame], list[dict[str, object]]]:
    rows = []
    for _, row in fact.iterrows():
        hosts = split_hosts_text(row.get("本场主播"))
        if not hosts:
            continue
        share = 1.0 / len(hosts)
        for host in hosts:
            rows.append(
                {
                    "date": row["date"],
                    "标准账号": row.get("标准账号", ""),
                    "主播": host,
                    "weighted_leads": share,
                    "weighted_deals": share * float(row.get("is_deal", 0) or 0),
                }
            )
    host_daily = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["date", "标准账号", "主播", "weighted_leads", "weighted_deals"])
    if not host_daily.empty:
        host_daily = (
            host_daily.groupby(["date", "标准账号", "主播"], as_index=False)[["weighted_leads", "weighted_deals"]]
            .sum()
            .sort_values(["date", "标准账号", "主播"])
        )
    host_summary = (
        host_daily.groupby(["标准账号", "主播"], as_index=False)[["weighted_leads", "weighted_deals"]].sum()
        if not host_daily.empty
        else pd.DataFrame(columns=["标准账号", "主播", "weighted_leads", "weighted_deals"])
    )
    rows_out = []
    for _, row in host_summary.iterrows():
        for metric_name in ["weighted_leads", "weighted_deals"]:
            rows_out.append(
                {
                    "snapshot_date": latest_date.strftime("%Y-%m-%d"),
                    "analysis_mode": "unified-fact",
                    "subject_area": "host_anchor",
                    "grain": "account_host",
                    "dimension_key": f"{row['标准账号']}|{row['主播']}",
                    "metric_name": metric_name,
                    "metric_value": float(row[metric_name]) if pd.notna(row[metric_name]) else 0.0,
                }
            )
    return {"主播贡献汇总": host_summary, "主播贡献日趋势": host_daily}, rows_out
