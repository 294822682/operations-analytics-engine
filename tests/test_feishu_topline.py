from __future__ import annotations

import pandas as pd

from oae.exports.feishu_topline import _load_live_metrics, build_pending_account_summary


def test_load_live_metrics_accepts_current_exposure_person_column() -> None:
    live_df = pd.DataFrame(
        [
            {
                "日期": "2026-05-28",
                "消耗": 123.45,
                "曝光人数": 1000,
                "车型": "EX7",
            }
        ]
    )

    metrics = _load_live_metrics(live_df, {"ex7_rules": {"live_model_field_candidates": ["车型"]}})

    assert metrics["impressions"].tolist() == [1000]


def test_load_live_metrics_accepts_current_exposure_count_column() -> None:
    live_df = pd.DataFrame(
        [
            {
                "日期": "2026-05-28",
                "消耗": 123.45,
                "曝光次数": 2000,
                "车型": "EX7",
            }
        ]
    )

    metrics = _load_live_metrics(live_df, {"ex7_rules": {"live_model_field_candidates": ["车型"]}})

    assert metrics["impressions"].tolist() == [2000]


def test_load_live_metrics_prefers_exposure_person_when_count_and_people_both_exist() -> None:
    live_df = pd.DataFrame(
        [
            {
                "日期": "2026-06-07",
                "消耗": 123.45,
                "曝光次数": 2000,
                "曝光人数": 1000,
                "车型": "EX7",
            }
        ]
    )

    metrics = _load_live_metrics(live_df, {"ex7_rules": {"live_model_field_candidates": ["车型"]}})

    assert metrics["impressions"].tolist() == [1000]


def test_pending_summary_counts_source_pending_orders_missing_from_fact() -> None:
    fact = pd.DataFrame(
        [
            {
                "线索ID_norm": "IN_FACT",
                "标准账号": "抖音-星途汽车官方直播间",
            }
        ]
    )
    deals_source = pd.DataFrame(
        [
            {
                "线索ID": "IN_FACT",
                "订单状态": "待交车",
                "下订日期": "2026-06-03",
                "渠道2": "抖音来客直播",
                "渠道3": "星途星纪元直播营销中心",
            },
            {
                "线索ID": "OLD_LEAD",
                "订单状态": "待交车",
                "下订日期": "2026-06-03",
                "渠道2": "抖音-星途汽车直营中心",
                "渠道3": "私信",
            },
            {
                "线索ID": "DELIVERED",
                "订单状态": "已交车",
                "下订日期": "2026-06-03",
                "渠道2": "抖音-星途汽车直营中心",
                "渠道3": "私信",
            },
        ]
    )
    config = {
        "pending_rules": {
            "primary_date_field": "下订日期",
            "fallback_date_fields": ["下订时间", "成交日期", "成交时间"],
        }
    }

    day_count, cumulative_count, day_target, cumulative_target, cumulative_all = build_pending_account_summary(
        fact=fact,
        deals_source=deals_source,
        report_date=pd.Timestamp("2026-06-15"),
        target_accounts=["抖音-星途汽车官方直播间", "抖音-星途汽车直营中心"],
        config=config,
    )

    assert day_count == 0
    assert cumulative_count == 2
    assert day_target == "无"
    assert cumulative_target == "星途汽车官方直播间(1台)、星途汽车直营中心(1台)"
    assert cumulative_all == "星途汽车官方直播间(1台)、星途汽车直营中心(1台)"
