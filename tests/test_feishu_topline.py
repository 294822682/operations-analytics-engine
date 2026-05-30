from __future__ import annotations

import pandas as pd

from oae.exports.feishu_topline import _load_live_metrics


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
