from __future__ import annotations

import pandas as pd

from oae.performance.live_loader import load_anchor_accounts_from_live
from oae.performance.panel_builders import build_anchor_panel


def test_anchor_spend_uses_live_row_host_not_lead_share(tmp_path) -> None:
    report_date = pd.Timestamp("2026-06-05")
    live_path = tmp_path / "2026年直播进度表6月.xlsx"
    pd.DataFrame(
        [
            {
                "日期": report_date,
                "开播账号": "抖音-星途汽车官方直播间",
                "本场主播": "何雯",
                "消耗": 0,
            },
            {
                "日期": report_date,
                "开播账号": "抖音-星途汽车官方直播间",
                "本场主播": "徐欣悦",
                "消耗": 1525.09,
            },
        ]
    ).to_excel(live_path, index=False)
    live_anchor_accounts = load_anchor_accounts_from_live(
        live_path,
        pd.Timestamp("2026-06-01"),
        pd.Timestamp("2026-06-30"),
    )
    fact = pd.DataFrame(
        [
            *[
                {
                    "date": report_date,
                    "deal_date": pd.NaT,
                    "is_deal": 0,
                    "标准账号": "抖音-星途汽车官方直播间",
                    "本场主播": "徐欣悦",
                    "线索ID_norm": f"XU-{idx}",
                }
                for idx in range(5)
            ],
            {
                "date": report_date,
                "deal_date": pd.NaT,
                "is_deal": 0,
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "何雯",
                "线索ID_norm": "HE-1",
            },
        ]
    )
    targets_month = pd.DataFrame(
        [
            {
                "scope_type": "anchor",
                "scope_name": "何雯",
                "lead_target_month": 0,
                "deal_target_month": 17,
                "lead_cost_target_month": 25000,
                "cpl_target": float("nan"),
                "cps_target": 1470.59,
            },
            {
                "scope_type": "anchor",
                "scope_name": "徐欣悦",
                "lead_target_month": 0,
                "deal_target_month": 17,
                "lead_cost_target_month": 25000,
                "cpl_target": float("nan"),
                "cps_target": 1470.59,
            },
        ]
    )
    spend_month = pd.DataFrame(
        [
            {
                "date": report_date,
                "account": "抖音-星途汽车官方直播间",
                "actual_spend": 1525.09,
            }
        ]
    )

    panel = build_anchor_panel(
        fact=fact,
        targets_month=targets_month,
        spend_month=spend_month,
        month_start=pd.Timestamp("2026-06-01"),
        month_end=pd.Timestamp("2026-06-30"),
        live_anchor_accounts=live_anchor_accounts,
    )
    latest = panel[panel["date"].eq(report_date)].set_index("scope_name")

    assert latest.at["何雯", "daily_spend"] == 0
    assert latest.at["何雯", "mtd_spend"] == 0
    assert latest.at["徐欣悦", "daily_spend"] == 1525.09
    assert latest.at["徐欣悦", "mtd_spend"] == 1525.09
