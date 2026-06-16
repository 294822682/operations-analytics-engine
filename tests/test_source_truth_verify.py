from __future__ import annotations

import pandas as pd

from oae.quality.source_truth import build_source_truth_report


def test_source_truth_report_fails_when_pending_output_drops_source_order() -> None:
    report_date = pd.Timestamp("2026-06-15")
    live_df = pd.DataFrame(
        [
            {
                "日期": "2026-06-15",
                "开播账号": "抖音-星途汽车直播营销中心",
                "平台&挂载组件": "抖音-来客",
                "开播时间": "10:00",
                "下播时间": "11:00",
                "本场主播": "徐欣悦",
                "曝光人数": 100,
                "消耗": 0,
                "车型": "LX",
            }
        ]
    )
    leads_source = pd.DataFrame(
        [
            {
                "线索ID": "L1",
                "创建时间": "2026-06-15 10:20:00",
                "创建日期": "2026-06-15",
                "渠道2": "抖音来客直播",
                "渠道3": "星途汽车直播营销中心",
                "手机号": "13800000001",
            }
        ]
    )
    deals_source = pd.DataFrame(
        [
            {
                "线索ID": "D1",
                "订单状态": "已交车",
                "成交日期": "2026-06-14",
                "成交时间": "2026-06-14 12:00:00",
                "下订日期": "2026-06-13",
            },
            {
                "线索ID": "P1",
                "订单状态": "待交车",
                "下订日期": "2026-06-03",
                "渠道2": "抖音-星途汽车直营中心",
                "渠道3": "私信",
            },
            {
                "线索ID": "P2",
                "订单状态": "待交车",
                "下订日期": "2026-06-04",
                "渠道2": "抖音来客直播",
                "渠道3": "星途星纪元直播营销中心",
            },
        ]
    )
    seed_sessions = pd.DataFrame(
        [
            {
                "date": "2026-06-15",
                "account": "EXEED星途",
                "hosts_raw": "桂婕",
                "impressions": 300,
                "source_file": "seed.xlsx",
            }
        ]
    )
    seed_targets = pd.DataFrame(
        [
            {
                "month": "2026-06",
                "scope_type": "account",
                "scope_name": "EXEED星途",
                "parent_scope": "",
                "parent_account": "",
                "impression_target_month": 1000,
            }
        ]
    )
    dashboard_source = pd.DataFrame(
        [
            _topline_row("impressions", 400),
            _topline_row("mtd_deals", 1),
            _topline_row("pending_cumulative", 1),
            _topline_row("mtd_douyin_laike_orders", 1),
        ]
    )
    config = {
        "ex7_rules": {"keywords": ["EX7"], "live_model_field_candidates": ["车型"]},
        "pending_rules": {
            "primary_date_field": "下订日期",
            "fallback_date_fields": ["下订时间", "成交日期", "成交时间"],
        },
    }

    report = build_source_truth_report(
        report_date=report_date,
        live_df=live_df,
        leads_source=leads_source,
        deals_source=deals_source,
        seed_sessions=seed_sessions,
        seed_targets=seed_targets,
        dashboard_source=dashboard_source,
        topline_config=config,
    )

    assert report["status"] == "failed"
    pending = next(item for item in report["checks"] if item["metric_key"] == "pending_cumulative")
    assert pending["expected"] == 2.0
    assert pending["actual"] == 1.0
    assert pending["status"] == "failed"
    passed = {item["metric_key"] for item in report["checks"] if item["status"] == "passed"}
    assert {"impressions", "mtd_deals", "mtd_douyin_laike_orders"}.issubset(passed)


def _topline_row(metric_key: str, actual: float) -> dict[str, object]:
    return {
        "report_date": "2026-06-15",
        "source_table": "topline",
        "scope_type": "department",
        "scope_name": "全量",
        "parent_scope": "",
        "metric_key": metric_key,
        "metric_name": metric_key,
        "actual": actual,
        "target": "",
        "attain_rate": "",
        "unit": "",
        "source_column": "",
        "sort_order": "",
    }
