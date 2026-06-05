from __future__ import annotations

import logging
from datetime import time

import pandas as pd

from oae.facts.live_sessions import build_live_windows
from oae.rules.datetime_utils import combine_date_time_series


def test_combine_date_time_series_parses_colon_time_strings() -> None:
    out = combine_date_time_series(
        pd.Series(["2026-06-02", "2026-06-03"]),
        pd.Series(["13:59:00", "18:19"]),
    )

    assert out.tolist() == [
        pd.Timestamp("2026-06-02 13:59:00"),
        pd.Timestamp("2026-06-03 18:19:00"),
    ]


def test_combine_date_time_series_parses_python_time_values() -> None:
    out = combine_date_time_series(
        pd.Series(["2026-06-03"]),
        pd.Series([time(18, 19)]),
    )

    assert out.iloc[0] == pd.Timestamp("2026-06-03 18:19:00")


def test_combine_date_time_series_parses_mixed_datetime_and_time_values() -> None:
    out = combine_date_time_series(
        pd.Series(["2026-06-02", "2026-06-02", "2026-06-03"]),
        pd.Series(
            [
                pd.Timestamp("2026-06-02 16:30:10"),
                time(13, 59),
                "18:19:00",
            ]
        ),
    )

    assert out.tolist() == [
        pd.Timestamp("2026-06-02 16:30:10"),
        pd.Timestamp("2026-06-02 13:59:00"),
        pd.Timestamp("2026-06-03 18:19:00"),
    ]


def test_build_live_windows_keeps_mixed_time_windows() -> None:
    live_df = pd.DataFrame(
        [
            {
                "日期": "2026-06-03",
                "开播账号": "抖音-星途汽车直播营销中心",
                "开播时间": pd.Timestamp("2026-06-03 09:00:00"),
                "下播时间": pd.Timestamp("2026-06-03 10:30:00"),
                "本场主播": "徐欣悦",
            },
            {
                "日期": "2026-06-03",
                "开播账号": "抖音-星途汽车直播营销中心",
                "开播时间": time(18, 19),
                "下播时间": time(19, 50),
                "本场主播": "侯翩翩",
            }
        ]
    )

    windows = build_live_windows(
        live_df,
        logging.getLogger("test"),
        buffer_minutes=5,
        default_duration_minutes=240,
        max_duration_hours=24,
    )

    row = windows[windows["本场主播"].eq("侯翩翩")].iloc[0]
    assert row["Valid_Start"] == pd.Timestamp("2026-06-03 18:19:00")
    assert row["Valid_End"] == pd.Timestamp("2026-06-03 19:50:00")
    assert row["Match_Start"] == pd.Timestamp("2026-06-03 18:14:00")
    assert row["Match_End"] == pd.Timestamp("2026-06-03 19:55:00")
