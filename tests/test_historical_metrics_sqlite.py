from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from oae.cli.build_historical_metrics_db import main as build_historical_metrics_db_main
from oae.storage.historical_metrics import (
    HistoricalSourceRecord,
    initialize_historical_metrics_db,
    scan_historical_sources,
)


def _write_month_sources(workspace: Path, month_dir_name: str = "2026年5月") -> None:
    month_dir = workspace / "历史文件" / month_dir_name
    month_dir.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "日期": "2026-05-01",
                "开播账号": "抖音-星途汽车直播营销中心",
                "平台&挂载组建": "抖音-来客",
                "本场主播": "张三",
                "开播时间": "10:00",
                "下播时间": "11:00",
                "曝光人数": 1000,
                "全场景线索人数": 3,
            }
        ]
    ).to_excel(month_dir / "2026年5月直播进度表.xlsx", index=False)

    pd.DataFrame(
        [
            {
                "创建时间": "2026-05-01",
                "开播账号": "抖音-EXEED星途",
                "开播时间": "08:00",
                "下播时间": "09:00",
                "本场主播": "桂婕",
                "曝光人数": 2000,
                "直播全场景商机量": 1,
            }
        ]
    ).to_excel(month_dir / "EXEED星途台账（五月）.xlsx", index=False)

    pd.DataFrame(
        [
            {
                "线索ID": "ID1",
                "手机号": "13000000000",
                "创建时间": "2026-05-01 10:10:00",
                "渠道2": "抖音来客直播",
                "渠道3": "星途汽车直播营销中心",
            }
        ]
    ).to_csv(month_dir / "总部新媒体线索2026-06-01.csv", index=False, encoding="utf-8-sig")

    pd.DataFrame(
        [
            {
                "线索ID": "ID1",
                "订单编号": "ORDER1",
                "订单状态": "已交车",
                "下订时间": "2026-05-01 10:20:00",
                "成交时间": "2026-05-02 09:00:00",
                "成交车型": "EX7",
            }
        ]
    ).to_csv(month_dir / "总部新媒体成交2026-06-01.csv", index=False, encoding="utf-8-sig")


def test_scan_historical_sources_registers_month_file_kinds_and_missing_a3(tmp_path: Path) -> None:
    _write_month_sources(tmp_path)

    records = scan_historical_sources(tmp_path, start_month="2026-05", end_month="2026-05")

    assert [record.source_kind for record in records] == [
        "live_progress",
        "seed_ledger",
        "lead_csv",
        "deal_csv",
    ]
    assert {record.month for record in records} == {"2026-05"}
    assert all(record.included_in_rollup for record in records)
    assert all(record.row_count == 1 for record in records)

    seed_record = next(record for record in records if record.source_kind == "seed_ledger")
    assert seed_record.required_columns_status == "missing"
    assert "A3人群增长" in seed_record.missing_columns
    assert seed_record.note == "A3 source field missing; keep NULL instead of filling 0"


def test_scan_historical_sources_accepts_seed_ledger_date_and_account_aliases(tmp_path: Path) -> None:
    month_dir = tmp_path / "历史文件" / "2026年1月"
    month_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "直播日期": "2026-01-01",
                "直播账号": "EXEED星途（官号）",
                "开播时间": "08:00",
                "下播时间": "09:00",
                "本场主播": "桂婕",
                "曝光人数": 2000,
                "直播全场景商机量": 1,
            }
        ]
    ).to_excel(month_dir / "EXEED星途台账（一月）.xlsx", index=False)

    records = scan_historical_sources(tmp_path, start_month="2026-01", end_month="2026-01")

    assert len(records) == 1
    assert records[0].source_kind == "seed_ledger"
    assert records[0].missing_columns == ["A3人群增长"]


def test_initialize_historical_metrics_db_writes_source_registry(tmp_path: Path) -> None:
    records = [
        HistoricalSourceRecord(
            source_id="2026-05:live_progress:2026年5月直播进度表.xlsx",
            month="2026-05",
            source_kind="live_progress",
            source_path="历史文件/2026年5月/2026年5月直播进度表.xlsx",
            sheet_name="Sheet1",
            row_count=1,
            required_columns_status="passed",
            missing_columns=[],
            included_in_rollup=True,
            note="",
        )
    ]
    db_path = tmp_path / "output" / "historical_metrics.db"

    initialize_historical_metrics_db(db_path, records)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT source_id, month, source_kind, source_path, sheet_name, row_count,
                   required_columns_status, missing_columns, included_in_rollup, note
            FROM hist_source_registry
            """
        ).fetchall()

    assert rows == [
        (
            "2026-05:live_progress:2026年5月直播进度表.xlsx",
            "2026-05",
            "live_progress",
            "历史文件/2026年5月/2026年5月直播进度表.xlsx",
            "Sheet1",
            1,
            "passed",
            "",
            1,
            "",
        )
    ]


def test_build_historical_metrics_db_cli_initializes_registry(tmp_path: Path) -> None:
    _write_month_sources(tmp_path)
    db_path = tmp_path / "output" / "historical_metrics.db"

    exit_code = build_historical_metrics_db_main(
        [
            "--workspace",
            str(tmp_path),
            "--start-month",
            "2026-05",
            "--end-month",
            "2026-05",
            "--db",
            str(db_path),
        ]
    )

    assert exit_code == 0
    with sqlite3.connect(db_path) as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM hist_source_registry").fetchone()[0]

    assert row_count == 4


def test_build_historical_metrics_db_cli_builds_staging_and_monthly_summary(tmp_path: Path) -> None:
    _write_month_sources(tmp_path)
    db_path = tmp_path / "output" / "historical_metrics.db"

    exit_code = build_historical_metrics_db_main(
        [
            "--workspace",
            str(tmp_path),
            "--start-month",
            "2026-05",
            "--end-month",
            "2026-05",
            "--db",
            str(db_path),
        ]
    )

    assert exit_code == 0
    with sqlite3.connect(db_path) as conn:
        summary = conn.execute(
            """
            SELECT month, ROUND(live_hours, 4), impressions, lead_rows,
                   douyin_laike_orders, delivered_deals, a3_growth, a3_source_status
            FROM v_monthly_summary
            """
        ).fetchone()
        table_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            ).fetchall()
        }

    assert {
        "stg_live_sessions",
        "stg_seed_sessions",
        "stg_leads",
        "stg_deals",
        "stg_douyin_laike_orders",
        "v_monthly_summary",
    }.issubset(table_names)
    assert summary == (
        "2026-05",
        2.0,
        3000.0,
        1,
        1.0,
        1,
        None,
        "missing_source_field",
    )
