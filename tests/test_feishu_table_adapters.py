from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from oae.contracts.monthly_metric_contract import (
    load_monthly_metric_contract,
    project_monthly_targets,
    project_report_topline_config,
    project_seed_monthly_targets,
)
from oae.exports.feishu_table_adapters import account_table_tsv, anchor_table_tsv
from oae.performance.targets_loader import load_targets


def test_monthly_metric_contract_projects_legacy_config_shapes(tmp_path: Path) -> None:
    contract_path = tmp_path / "monthly_metric_contract.json"
    contract_path.write_text(
        json.dumps(
            {
                "version": 1,
                "months": {
                    "2026-06": {
                        "monthly_targets": [
                            {
                                "scope_type": "account",
                                "scope_name": "抖音-星途汽车直播营销中心",
                                "parent_account": "",
                                "lead_target_month": 0,
                                "deal_target_month": 100,
                                "lead_cost_target_month": 150000,
                                "cpl_target": None,
                                "cps_target": 1500,
                                "target_pool": "线索组目标池",
                                "order_target_month": 1000,
                            }
                        ],
                        "seed_monthly_targets": [
                            {
                                "scope_type": "account",
                                "scope_name": "EXEED星途",
                                "parent_scope": "",
                                "parent_account": "",
                                "impression_target_month": 25000000,
                                "spend_target_month": None,
                                "cpm_target": None,
                                "target_pool": "种草组目标池",
                            }
                        ],
                        "report_topline_config": {
                            "full_account_targets": {
                                "impressions": 25000000,
                                "leads": 0,
                                "deals": 100,
                                "cpl": 0,
                                "cps": 1500,
                            },
                            "ex7_rules": {
                                "keywords": ["EX7"],
                                "live_model_field_candidates": ["车型"],
                                "lead_model_field_candidates": ["首次意向车型"],
                                "deal_model_field_candidates": ["成交车型"],
                            },
                            "pending_rules": {
                                "primary_date_field": "下订日期",
                                "fallback_date_fields": ["成交日期"],
                            },
                        },
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    contract = load_monthly_metric_contract(contract_path)
    monthly_targets = project_monthly_targets(contract, "2026-06")
    seed_targets = project_seed_monthly_targets(contract, "2026-06")
    topline_config = project_report_topline_config(contract, "2026-06")

    assert monthly_targets.to_dict("records") == [
        {
            "month": "2026-06",
            "scope_type": "account",
            "scope_name": "抖音-星途汽车直播营销中心",
            "parent_account": "",
            "lead_target_month": 0,
            "deal_target_month": 100,
            "lead_cost_target_month": 150000,
            "cpl_target": None,
            "cps_target": 1500,
            "target_pool": "线索组目标池",
            "order_target_month": 1000,
        }
    ]
    assert seed_targets.to_dict("records") == [
        {
            "month": "2026-06",
            "scope_type": "account",
            "scope_name": "EXEED星途",
            "parent_scope": "",
            "parent_account": "",
            "impression_target_month": 25000000,
            "spend_target_month": None,
            "cpm_target": None,
            "target_pool": "种草组目标池",
        }
    ]
    assert topline_config["full_account_targets"]["impressions"] == 25000000
    assert topline_config["ex7_rules"]["lead_model_field_candidates"] == ["首次意向车型"]
    assert topline_config["pending_rules"]["fallback_date_fields"] == ["成交日期"]


def test_load_targets_preserves_order_target_month(tmp_path: Path) -> None:
    targets_path = tmp_path / "monthly_targets.csv"
    targets_path.write_text(
        "\n".join(
            [
                "month,scope_type,scope_name,parent_account,lead_target_month,deal_target_month,lead_cost_target_month,cpl_target,cps_target,target_pool,order_target_month",
                "2026-06,account,抖音-星途汽车直播营销中心,,0,100,150000,,1500,线索组目标池,1000",
                "2026-06,anchor,侯翩翩,抖音-星途汽车直播营销中心,0,17,25000,,1470.59,,167",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    targets = load_targets(targets_path)

    account_row = targets[(targets["scope_type"] == "account") & (targets["scope_name"] == "抖音-星途汽车直播营销中心")].iloc[0]
    anchor_row = targets[(targets["scope_type"] == "anchor") & (targets["scope_name"] == "侯翩翩")].iloc[0]
    assert account_row["order_target_month"] == 1000
    assert anchor_row["order_target_month"] == 167


def test_account_table_tsv_outputs_douyin_laike_order_columns() -> None:
    account_panel = pd.DataFrame(
        [
            {
                "scope_type": "account",
                "scope_name": "抖音-星途汽车直播营销中心",
                "daily_leads": 2,
                "daily_lead_target": 0,
                "daily_lead_attain_pct": "N/A",
                "mtd_leads": 8,
                "lead_target_month": 0,
                "mtd_lead_attain_pct": "N/A",
                "daily_deals": 2,
                "daily_deal_target": 3.54,
                "daily_deal_attain_pct": "56.57%",
                "mtd_deals": 3,
                "deal_target_month": 100,
                "mtd_deal_attain_pct": "3.00%",
                "mtd_douyin_laike_orders": 3,
                "order_target_month": 1000,
                "mtd_douyin_laike_order_attain_pct": "0.30%",
                "lead_cost_target_month": 150000,
                "cpl_target": float("nan"),
                "cps_target": 1500,
                "mtd_spend": 0,
                "mtd_cpl": 0,
                "mtd_cps": 0,
            }
        ]
    )

    out = account_table_tsv(account_panel, target_accounts=["抖音-星途汽车直播营销中心"])

    assert out["抖音-来客订单数"].tolist() == ["3"]
    assert out["订单KPI目标"].tolist() == ["1000"]
    assert out["订单KPI完成率"].tolist() == ["0.30%"]


def test_anchor_table_tsv_outputs_douyin_laike_order_columns() -> None:
    anchor_panel = pd.DataFrame(
        [
            {
                "scope_type": "anchor",
                "scope_name": "侯翩翩",
                "parent_account": "抖音-星途汽车直播营销中心",
                "daily_leads": 1,
                "daily_lead_target": 0,
                "daily_lead_attain_pct": "N/A",
                "mtd_leads": 3,
                "lead_target_month": 0,
                "mtd_lead_attain_pct": "N/A",
                "daily_deals": 1,
                "daily_deal_target": 0.61,
                "daily_deal_attain_pct": "164.71%",
                "mtd_deals": 1,
                "deal_target_month": 17,
                "mtd_deal_attain_pct": "5.88%",
                "mtd_douyin_laike_orders": 2,
                "order_target_month": 167,
                "mtd_douyin_laike_order_attain_pct": "1.20%",
                "lead_cost_target_month": 25000,
                "cpl_target": float("nan"),
                "cps_target": 1470.59,
                "mtd_spend": 0,
                "mtd_cpl": 0,
                "mtd_cps": 0,
            }
        ]
    )

    out = anchor_table_tsv(anchor_panel)

    assert out["抖音-来客订单数"].tolist() == ["2"]
    assert out["订单KPI目标"].tolist() == ["167"]
    assert out["订单KPI完成率"].tolist() == ["1.20%"]
