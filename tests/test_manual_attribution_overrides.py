from __future__ import annotations

import pandas as pd

from oae.contracts.models import ManualAttributionOverride
from oae.overrides import apply_manual_attribution_overrides


def test_manual_account_host_override_recomputes_single_host_contributions() -> None:
    fact = pd.DataFrame(
        [
            {
                "线索ID": "ID2067907746895507457",
                "手机号": "17511676690",
                "线索创建时间": "2026-06-19 17:50:01",
                "date": "2026-06-19",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "何雯,孙慧敏",
                "权重": 0.5,
                "归属状态": "匹配成功",
                "无匹配原因": "",
                "订单状态": "已交车",
                "成交时间": "2026-06-22 16:31:34",
                "主播人数": 2,
                "成交分摊权重": 0.5,
                "is_order": 1,
                "is_deal": 1,
                "orders_contrib": 0.5,
                "deals_contrib": 0.5,
                "business_subject_key": "PHONE:17511676690",
                "_lead_key": "PHONE:17511676690",
                "线索ID_norm": "ID2067907746895507457",
                "schema_version": "1.0.0",
                "metric_version": "metric-v1",
                "run_id": "test-run",
            }
        ]
    )
    override = ManualAttributionOverride(
        override_id="manual-override-20260619-17511676690",
        business_subject_key="PHONE:17511676690",
        phone="17511676690",
        lead_id="ID2067907746895507457",
        override_scope="account_host",
        target_account="抖音-星途汽车官方直播间",
        target_host="何雯",
        reason="挖潜",
        evidence_note="人工专项归属确认",
        confirmed_by="业务人工确认",
        confirmed_at="2026-06-23 13:10:00",
        effective_from="2026-06-19",
        effective_to="",
        status="active",
        metric_version="metric-v1",
        run_id="",
    )

    result, summary = apply_manual_attribution_overrides(
        fact=fact,
        overrides=[override],
        source_path="test://manual_attribution_overrides.csv",
    )

    row = result.iloc[0]
    assert summary["applied_override_count"] == 1
    assert row["最终本场主播"] == "何雯"
    assert row["本场主播"] == "何雯"
    assert row["主播人数"] == 1
    assert row["权重"] == 1.0
    assert row["成交分摊权重"] == 1.0
    assert row["orders_contrib"] == 1.0
    assert row["deals_contrib"] == 1.0
