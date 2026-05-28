from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from oae.services.dashboard_daily_service import DashboardDailyService
from tests.test_feishu_dashboard_interactive_html import _row, _sample_rows, _write_tsv
from tests.api.helpers import build_temp_repo, create_test_app


def test_get_daily_dashboard_returns_read_only_bi_payload(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    source_path = repo_root / "output" / "sql_reports" / "feishu_dashboard_source_latest_2026-05-14.tsv"
    _write_tsv(source_path, _sample_rows())
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/2026-05-14")

    assert response.status_code == 200
    payload = response.json()
    assert payload["report_date"] == "2026-05-14"
    assert payload["source"]["type"] == "feishu_dashboard_source_tsv"
    assert payload["source"]["path"] == "output/sql_reports/feishu_dashboard_source_latest_2026-05-14.tsv"
    assert payload["available_report_dates"] == ["2026-05-14"]
    assert payload["overview"]["impressions"]["actual"] == 21886318
    assert payload["overview"]["mtd_unique_leads"]["actual"] == 13323
    assert payload["overview"]["mtd_spend"]["target"] is None
    assert payload["overview"]["mtd_spend"]["attain_rate"] is None
    assert payload["overview"]["pending_day"]["actual"] == 0
    assert payload["overview"]["pending_day"]["target"] is None
    assert payload["funnel"][0]["label"] == "曝光"
    assert payload["funnel"][1]["key"] == "raw_leads"
    assert payload["segments"]["ex7"]["label"] == "EX7 专项"
    assert payload["segments"]["non_ex7"]["label"] == "不含 EX7"
    assert payload["segments"]["deltas"]["cps_delta"] == 46542.242
    assert payload["lead_anchors"][0]["name"] == "徐幻"
    assert payload["lead_anchors"][0]["metrics"]["mtd_unique_leads"]["actual"] == 2634
    assert payload["seed_account"]["actual"] == 7221796
    assert payload["seed_anchors"][0]["name"] == "桂婕"
    assert payload["seed_anchors"][0]["metrics"]["daily_impressions"]["actual"] == 0
    assert payload["seed_anchors"][0]["metrics"]["daily_impressions"]["attain_rate"] == 0
    assert payload["interactions"]["module_anchors"] == [
        "overview",
        "funnel",
        "segment-compare",
        "lead-anchors",
        "seed-exposure",
    ]
    assert "mtd_douyin_laike_orders" in payload["interactions"]["lead_anchor_sort_keys"]
    assert "mtd_impressions" in payload["interactions"]["seed_anchor_sort_keys"]


def test_get_daily_dashboard_missing_source_returns_404(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/2026-05-14")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["code"] == "DASHBOARD_SOURCE_NOT_FOUND"
    assert payload["error"]["details"]["report_date"] == "2026-05-14"


def test_get_latest_daily_dashboard_returns_newest_source_tsv(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-13.tsv", _sample_rows())
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-14.tsv", _sample_rows())
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/latest")

    assert response.status_code == 200
    payload = response.json()
    assert payload["report_date"] == "2026-05-14"
    assert payload["source"]["path"] == "output/sql_reports/feishu_dashboard_source_latest_2026-05-14.tsv"
    assert payload["available_report_dates"] == ["2026-05-13", "2026-05-14"]
    assert payload["overview"]["mtd_unique_leads"]["actual"] == 13323


def test_get_daily_dashboard_trends_aggregates_dashboard_sources_by_date(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-14.tsv", _trend_rows("2026-05-14", leads="13323"))
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-13.tsv", _trend_rows("2026-05-13", leads="12000"))
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends")

    assert response.status_code == 200
    payload = response.json()
    assert payload["date_range"]["start"] == "2026-03-01"
    assert payload["date_range"]["start_date"] == "2026-03-01"
    assert payload["date_range"]["end"] == "2026-05-14"
    assert payload["date_range"]["end_date"] == "2026-05-14"
    assert payload["date_range"]["days"] == 75
    assert payload["selected_range_days"] == 75
    assert payload["date_range"]["date_count"] == 2
    assert payload["available_report_dates"] == ["2026-05-13", "2026-05-14"]
    assert payload["source"]["type"] == "feishu_dashboard_source_tsv_history"
    assert payload["source"]["date_range_label"] == "2026-05-13 至 2026-05-14"
    assert payload["source"]["paths"] == [
        "output/sql_reports/feishu_dashboard_source_latest_2026-05-13.tsv",
        "output/sql_reports/feishu_dashboard_source_latest_2026-05-14.tsv",
    ]
    assert [point["report_date"] for point in payload["core_kpis"]["mtd_unique_leads"]["points"]] == [
        "2026-05-13",
        "2026-05-14",
    ]
    assert [point["actual"] for point in payload["core_kpis"]["mtd_unique_leads"]["points"]] == [12000, 13323]
    assert payload["core_kpis"]["mtd_spend"]["points"][0]["target"] is None
    assert payload["segments"]["ex7"]["metrics"]["mtd_unique_leads"]["points"][0]["actual"] == 9219
    assert payload["segments"]["non_ex7"]["metrics"]["mtd_deals"]["points"][1]["actual"] == 35
    assert payload["accounts"][0]["name"] == "星途汽车官方直播间"
    assert payload["accounts"][0]["metrics"]["mtd_unique_leads"]["points"][1]["actual"] == 10045
    seed_daily = payload["seed_anchors"][0]["metrics"]["daily_impressions"]["points"][0]
    assert seed_daily["actual"] == 0
    assert seed_daily["target"] == 55790.22
    assert seed_daily["attain_rate"] == 0
    assert payload["quality_note"] == "质量状态仅用于人工提示；N8 v0 不改写 dashboard source 数据。"


def test_get_daily_dashboard_trends_defaults_end_date_to_quarter_window(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-03-01.tsv", _trend_rows("2026-03-01", leads="9000"))
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-22.tsv", _trend_rows("2026-05-22", leads="13323"))
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-24.tsv", _trend_rows("2026-05-24", leads="14000"))
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?end_date=2026-05-22")

    assert response.status_code == 200
    payload = response.json()
    assert payload["date_range"]["start"] == "2026-03-01"
    assert payload["date_range"]["start_date"] == "2026-03-01"
    assert payload["date_range"]["end"] == "2026-05-22"
    assert payload["date_range"]["end_date"] == "2026-05-22"
    assert payload["date_range"]["days"] == 83
    assert payload["selected_range_days"] == 83
    assert payload["available_report_dates"] == ["2026-03-01", "2026-05-22"]


def test_get_daily_dashboard_trends_returns_v1_contract_metadata_and_quality_annotations(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-13.tsv", _trend_rows("2026-05-13", leads="12000"))
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-15.tsv", _trend_rows("2026-05-15", leads="13323"))
    _write_run_evidence(
        repo_root,
        run_id="run-20260513T010000Z",
        report_date="2026-05-13",
        quality_status="pass",
        quality_decision="allow",
        release_readiness="review",
    )
    _write_run_evidence(
        repo_root,
        run_id="run-20260513T020000Z",
        report_date="2026-05-13",
        quality_status="warning",
        quality_decision="investigate",
        release_readiness="review",
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-13&end_date=2026-05-15")

    assert response.status_code == 200
    payload = response.json()
    assert payload["contract_version"] == "n8-v1-trend-1"
    assert payload["date_range"] == {
        "start": "2026-05-13",
        "end": "2026-05-15",
        "start_date": "2026-05-13",
        "end_date": "2026-05-15",
        "days": 3,
        "selected_range_days": 3,
        "date_count": 2,
        "available_dates": ["2026-05-13", "2026-05-15"],
        "missing_dates": ["2026-05-14"],
    }
    assert payload["available_dates"] == ["2026-05-13", "2026-05-15"]
    assert payload["missing_dates"] == ["2026-05-14"]
    assert payload["source_type"] == "feishu_dashboard_source_tsv_history"
    assert payload["source"]["quality_annotation_source"] == "run_manifest_and_quality_report"

    annotation = payload["quality_annotations"]["2026-05-13"]
    assert annotation["run_id"] == "run-20260513T020000Z"
    assert annotation["quality_status"] == "warning"
    assert annotation["quality_decision"] == "investigate"
    assert annotation["release_readiness"] == "review"
    assert annotation["evidence_paths"] == [
        "artifacts/runs/run_manifest_run-20260513T020000Z.json",
        "artifacts/runs/quality_report_run-20260513T020000Z.json",
    ]
    assert payload["quality_annotations"]["2026-05-15"] == {
        "report_date": "2026-05-15",
        "run_id": "",
        "quality_status": "unknown",
        "quality_decision": "unknown",
        "release_readiness": "unknown",
        "evidence_paths": [],
    }

    spend_point = payload["core_kpis"]["mtd_spend"]["points"][0]
    assert spend_point["target"] is None
    assert spend_point["attain_rate"] is None
    assert spend_point["is_missing"] is False
    zero_point = payload["seed_anchors"][0]["metrics"]["daily_impressions"]["points"][0]
    assert zero_point["actual"] == 0
    assert zero_point["is_missing"] is False
    assert payload["core_kpis"]["mtd_unique_leads"]["points"][0]["quality_status"] == "warning"


def test_get_daily_dashboard_trends_supports_explicit_end_date_filter(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-13.tsv", _trend_rows("2026-05-13", leads="12000"))
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-22.tsv", _trend_rows("2026-05-22", leads="13323"))
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-24.tsv", _trend_rows("2026-05-24", leads="14000"))
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?end_date=2026-05-22")

    assert response.status_code == 200
    payload = response.json()
    assert payload["date_range"]["start"] == "2026-03-01"
    assert payload["date_range"]["start_date"] == "2026-03-01"
    assert payload["date_range"]["end"] == "2026-05-22"
    assert payload["date_range"]["end_date"] == "2026-05-22"
    assert payload["date_range"]["days"] == 83
    assert payload["selected_range_days"] == 83
    assert payload["date_range"]["date_count"] == 2
    assert payload["available_report_dates"] == ["2026-05-13", "2026-05-22"]
    assert payload["source"]["paths"] == [
        "output/sql_reports/feishu_dashboard_source_latest_2026-05-13.tsv",
        "output/sql_reports/feishu_dashboard_source_latest_2026-05-22.tsv",
    ]


def test_get_daily_dashboard_trends_allows_92_days_and_rejects_longer_ranges(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-03-01.tsv", _trend_rows("2026-03-01", leads="1"))
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-31.tsv", _trend_rows("2026-05-31", leads="2"))
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-06-01.tsv", _trend_rows("2026-06-01", leads="3"))
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        allowed = client.get("/dashboard/daily/trends?start_date=2026-03-01&end_date=2026-05-31")
        response = client.get("/dashboard/daily/trends?start_date=2026-03-01&end_date=2026-06-01")

    assert allowed.status_code == 200
    assert allowed.json()["date_range"]["days"] == 92
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"]["code"] == "DASHBOARD_RANGE_TOO_LONG"
    assert payload["error"]["message"] == "单次查看范围建议不超过一个季度，请缩小日期范围。"


def test_get_daily_dashboard_trends_returns_fact_based_detail_metrics(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            {
                "线索ID": "L1",
                "手机号": "13800000001",
                "线索创建时间": "2026-05-01 10:00:00",
                "date": "2026-05-01",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "已交车",
                "成交时间": "2026-05-02 12:00:00",
                "is_order": "1",
                "is_deal": "1",
                "成交车型": "EX7",
                "orders_contrib": "1",
                "deals_contrib": "1",
                "线索ID_norm": "L1",
                "is_perf_lead_scope": "1",
            },
            {
                "线索ID": "L2",
                "手机号": "13900000002",
                "线索创建时间": "2026-05-02 10:00:00",
                "date": "2026-05-02",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "成交车型": "",
                "orders_contrib": "0",
                "deals_contrib": "0",
                "线索ID_norm": "L2",
                "is_perf_lead_scope": "1",
            },
        ],
    )
    _write_raw_leads_csv(
        repo_root / "源文件" / "总部新媒体线索2026-05-26.csv",
        [
            {"线索ID": "L1", "创建日期": "2026-05-01", "到店日期": "2026-05-02", "首次意向车型": "EX7", "渠道2": "抖音-星途汽车官方直播间", "渠道3": "直播"},
            {"线索ID": "L2", "创建日期": "2026-05-02", "到店日期": "", "首次意向车型": "LX", "渠道2": "抖音-星途汽车官方直播间", "渠道3": "直播"},
        ],
    )
    _write_raw_deals_csv(
        repo_root / "源文件" / "总部新媒体成交2026-05-26.csv",
        [{"线索ID": "L1", "订单状态": "已交车", "成交日期": "2026-05-02", "成交车型": "EX7", "渠道2": "抖音-星途汽车官方直播间", "渠道3": "直播"}],
    )
    _write_live_workbook(
        repo_root / "源文件" / "2026年5月直播进度表.xlsx",
        [
            {
                "日期": "2026-05-01",
                "开播账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "车型": "EX7",
                "消耗": 100,
                "曝光人数": 1000,
            },
            {
                "日期": "2026-05-02",
                "开播账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "车型": "LX",
                "消耗": 0,
                "曝光人数": 0,
            },
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-02")

    assert response.status_code == 200
    payload = response.json()
    assert payload["daily_trends"][1]["key"] == "leads"
    assert payload["daily_trends"][1]["points"] == [
        {"date": "2026-05-01", "value": 1.0},
        {"date": "2026-05-02", "value": 1.0},
    ]
    account = payload["account_summary"][0]
    assert account["name"] == "抖音-星途汽车官方直播间"
    assert account["metrics"]["leads"]["actual"] == 2.0
    assert account["metrics"]["visits"]["actual"] == 1.0
    assert account["metrics"]["deals"]["actual"] == 1.0
    assert account["metrics"]["visit_rate"]["actual"] == 0.5
    assert account["metrics"]["lead_deal_rate"]["actual"] == 0.5
    assert account["metrics"]["cpl"]["actual"] == 50.0
    assert account["metrics"]["cps"]["actual"] == 100.0
    assert account["metrics"]["ex7_leads"]["actual"] == 1.0
    assert account["metrics"]["ex7_deals"]["actual"] == 1.0
    assert account["metrics"]["ex7_deal_rate"]["actual"] == 1.0
    anchor = payload["anchor_summary"][0]
    assert anchor["name"] == "丁俐佳"
    assert anchor["parent_scope"] == "抖音-星途汽车官方直播间"
    assert anchor["metric_groups"]["到店"]["visit_rate"]["actual"] == 0.5
    assert anchor["metric_groups"]["成交"]["lead_deal_rate"]["actual"] == 0.5
    assert anchor["metric_groups"]["成本"]["cpl"]["actual"] == 50.0
    assert anchor["metric_groups"]["EX7"]["ex7_deal_rate"]["actual"] == 1.0


def test_get_daily_dashboard_trends_filters_cancelled_accounts_from_account_summary(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    cancelled_accounts = [
        "视频号-星途星纪元",
        "星途星纪元",
        "星途星纪元直播营销中心+",
        "抖音",
        "快手-星途星纪元",
        "抖店",
    ]
    rows = [
        {
            "线索ID": "KEEP-1",
            "手机号": "13700001000",
            "线索创建时间": "2026-05-01 10:00:00",
            "date": "2026-05-01",
            "标准账号": "快手-EXEED星途",
            "本场主播": "丁俐佳",
            "订单状态": "",
            "成交时间": "",
            "is_order": "0",
            "is_deal": "0",
            "成交车型": "EX7",
            "orders_contrib": "0",
            "deals_contrib": "0",
            "线索ID_norm": "KEEP-1",
            "is_perf_lead_scope": "1",
        }
    ]
    for index, account_name in enumerate(cancelled_accounts, start=1):
        rows.append(
            {
                "线索ID": f"HIDE-{index}",
                "手机号": f"13700001{index:03d}",
                "线索创建时间": "2026-05-01 11:00:00",
                "date": "2026-05-01",
                "标准账号": account_name,
                "本场主播": "丁俐佳",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "成交车型": "LX",
                "orders_contrib": "0",
                "deals_contrib": "0",
                "线索ID_norm": f"HIDE-{index}",
                "is_perf_lead_scope": "1",
            }
        )
    _write_fact_csv(repo_root / "output" / "fact_attribution.csv", rows)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-01")

    assert response.status_code == 200
    account_names = {item["name"] for item in response.json()["account_summary"]}
    assert "快手-EXEED星途" in account_names
    assert account_names.isdisjoint(cancelled_accounts)


def test_get_daily_dashboard_trends_displays_conversion_rates_above_100_percent(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    rows: list[dict[str, str]] = []
    rows.extend(
        _fact_row(f"L-{index}", date="2026-05-01", account="抖音-星途极速拍档", model="EX7")
        for index in range(1, 4)
    )
    rows.extend(
        _fact_row(f"V-{index}", date="2026-04-01", account="抖音-星途极速拍档", model="LX")
        for index in range(1, 100)
    )
    rows.extend(
        _fact_row(
            f"D-{index}",
            date="2026-04-01",
            account="抖音-星途极速拍档",
            model="EX7",
            deal_time="2026-05-01 12:00:00",
        )
        for index in range(1, 11)
    )
    rows.extend(
        _fact_row(f"FAST-V-{index}", date="2026-04-01", account="快手-EXEED星途", model="LX")
        for index in range(1, 3)
    )
    rows.extend(
        _fact_row(
            f"FAST-D-{index}",
            date="2026-04-01",
            account="快手-EXEED星途",
            model="LX",
            deal_time="2026-05-01 15:00:00",
        )
        for index in range(1, 4)
    )
    _write_fact_csv(repo_root / "output" / "fact_attribution.csv", rows)
    _write_raw_leads_csv(
        repo_root / "源文件" / "总部新媒体线索2026-05-26.csv",
        [
            *[
                {"线索ID": f"V-{index}", "创建日期": "2026-04-01", "到店日期": "2026-05-01", "首次意向车型": "LX", "渠道2": "抖音-星途极速拍档"}
                for index in range(1, 100)
            ],
            *[
                {"线索ID": f"FAST-V-{index}", "创建日期": "2026-04-01", "到店日期": "2026-05-01", "首次意向车型": "LX", "渠道2": "快手-EXEED星途"}
                for index in range(1, 3)
            ],
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-01")

    assert response.status_code == 200
    account = next(item for item in response.json()["account_summary"] if item["name"] == "抖音-星途极速拍档")
    assert account["metrics"]["leads"]["actual"] == 3.0
    assert account["metrics"]["visits"]["actual"] == 99.0
    assert account["metrics"]["deals"]["actual"] == 10.0

    assert account["metrics"]["visit_rate"]["actual"] == 33.0
    assert account["metrics"]["lead_deal_rate"]["actual"] == pytest.approx(10 / 3)
    assert account["metrics"]["ex7_deal_rate"]["actual"] == pytest.approx(10 / 3)
    for metric_key in ["visit_rate", "lead_deal_rate", "ex7_deal_rate"]:
        metric = account["metrics"][metric_key]
        assert metric["source_status"] == "available"
        assert "display_value" not in metric
        assert metric.get("display_value") != "口径待确认"

    featured = next(item for item in response.json()["account_summary"] if item["name"] == "快手-EXEED星途")
    assert featured["metrics"]["visits"]["actual"] == 2.0
    assert featured["metrics"]["deals"]["actual"] == 3.0
    assert featured["metrics"]["visit_deal_rate"]["actual"] == 1.5
    assert featured["metrics"]["visit_deal_rate"]["source_status"] == "available"
    assert "display_value" not in featured["metrics"]["visit_deal_rate"]


def test_get_daily_dashboard_trends_keeps_zero_denominator_rates_missing() -> None:
    metric = DashboardDailyService._metric_summary(
        "visit_rate",
        "到店率",
        DashboardDailyService._safe_div_value(10, 0),
        None,
        "比例",
    )

    assert metric["actual"] is None
    assert metric["source_status"] == "available"
    assert "display_value" not in metric


def test_get_daily_dashboard_trends_hides_departed_anchor_and_keeps_fixed_seed_anchor(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            _fact_row("WANG-LEAD", date="2026-05-01", account="抖音-星途汽车官方直播间", model="LX", host="王君如"),
            _fact_row("GUI-LEAD", date="2026-05-01", account="抖音-EXEED星途", model="LX", host="桂婕"),
        ],
    )
    (repo_root / "config" / "seed_monthly_targets.csv").write_text(
        "\n".join(
            [
                "month,scope_type,scope_name,parent_scope,parent_account,impression_target_month,spend_target_month,cpm_target,target_pool",
                "2026-05,host,桂婕,种草组,抖音-EXEED星途,4000000,,,种草组曝光目标池",
            ]
        )
        + "\n",
        encoding="utf-8-sig",
    )
    _write_live_workbook(
        repo_root / "源文件" / "EXEED星途台账（五月）.xlsx",
        [
            {"日期": "2026-05-01", "开播账号": "抖音-EXEED星途", "本场主播": "王君如", "车型": "LX", "曝光人数": 500},
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-01")

    assert response.status_code == 200
    payload = response.json()
    anchor_names = {item["name"] for item in payload["anchor_summary"]}
    seed_anchor_names = {item["name"] for item in payload["seed_exposure_summary"]["anchors"]}

    assert "王君如" not in anchor_names
    assert "王君如" not in seed_anchor_names
    assert "桂婕" not in anchor_names
    assert "桂婕" in seed_anchor_names
    gui_seed = next(item for item in payload["seed_exposure_summary"]["anchors"] if item["name"] == "桂婕")
    assert gui_seed["display_type"] == "主播曝光"
    assert gui_seed["parent_scope"] == "抖音-EXEED星途"
    assert gui_seed["metrics"]["impressions"]["target"] == pytest.approx(4000000 / 31)


def test_get_daily_dashboard_trends_returns_previous_period_and_monthly_comparison(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            {
                "线索ID": "P1",
                "手机号": "13700000001",
                "线索创建时间": "2025-12-08 10:00:00",
                "date": "2025-12-08",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "线索ID_norm": "P1",
                "is_perf_lead_scope": "1",
            },
            {
                "线索ID": "P2",
                "手机号": "13700000002",
                "线索创建时间": "2026-02-28 10:00:00",
                "date": "2026-02-28",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "线索ID_norm": "P2",
                "is_perf_lead_scope": "1",
            },
            {
                "线索ID": "C1",
                "手机号": "13800000001",
                "线索创建时间": "2026-03-01 10:00:00",
                "date": "2026-03-01",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "线索ID_norm": "C1",
                "is_perf_lead_scope": "1",
            },
            {
                "线索ID": "C2",
                "手机号": "13800000002",
                "线索创建时间": "2026-05-22 10:00:00",
                "date": "2026-05-22",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "已交车",
                "成交时间": "2026-05-22 12:00:00",
                "is_order": "1",
                "is_deal": "1",
                "成交车型": "EX7",
                "orders_contrib": "1",
                "deals_contrib": "1",
                "线索ID_norm": "C2",
                "is_perf_lead_scope": "1",
            },
        ],
    )
    _write_live_workbook(
        repo_root / "源文件" / "2026年5月直播进度表.xlsx",
        [
            {"日期": "2025-12-08", "开播账号": "抖音-星途汽车官方直播间", "本场主播": "丁俐佳", "车型": "LX", "消耗": 60, "曝光人数": 600},
            {"日期": "2026-02-28", "开播账号": "抖音-星途汽车官方直播间", "本场主播": "丁俐佳", "车型": "LX", "消耗": 40, "曝光人数": 400},
            {"日期": "2026-03-01", "开播账号": "抖音-星途汽车官方直播间", "本场主播": "丁俐佳", "车型": "LX", "消耗": 100, "曝光人数": 1000},
            {"日期": "2026-04-01", "开播账号": "抖音-星途汽车官方直播间", "本场主播": "丁俐佳", "车型": "LX", "消耗": 80, "曝光人数": 0},
            {"日期": "2026-05-22", "开播账号": "抖音-星途汽车官方直播间", "本场主播": "丁俐佳", "车型": "EX7", "消耗": 150, "曝光人数": 1500},
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?end_date=2026-05-22")

    assert response.status_code == 200
    payload = response.json()
    assert payload["previous_period"] == {
        "start_date": "2025-12-08",
        "end_date": "2026-02-28",
        "days": 83,
        "has_data": True,
        "message": "",
    }
    previous_leads = next(item for item in payload["previous_period_trends"] if item["key"] == "leads")
    previous_spend = next(item for item in payload["previous_period_trends"] if item["key"] == "spend")
    assert previous_leads["points"][0] == {"date": "2025-12-08", "value": 1.0}
    assert previous_spend["points"][-1] == {"date": "2026-02-28", "value": 40.0}

    assert [month["label"] for month in payload["monthly_comparison"]] == ["2026年3月", "2026年4月", "2026年5月"]
    march = payload["monthly_comparison"][0]["metrics"]
    april = payload["monthly_comparison"][1]["metrics"]
    may = payload["monthly_comparison"][2]["metrics"]
    assert march["leads"]["value"] == 1.0
    assert march["spend"]["value"] == 100.0
    assert march["cpl"]["value"] == 100.0
    assert april["impressions"]["value"] == 0.0
    assert april["leads"]["value"] is None
    assert april["cpl"]["value"] is None
    assert may["cps"]["value"] == 150.0


def test_get_daily_dashboard_trends_uses_raw_historical_deals_for_quarter_deals(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            {
                "线索ID": "D-MAR",
                "手机号": "13800001001",
                "线索创建时间": "2026-03-01 10:00:00",
                "date": "2026-03-01",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "成交车型": "",
                "orders_contrib": "0",
                "deals_contrib": "0",
                "线索ID_norm": "D-MAR",
                "is_perf_lead_scope": "1",
            },
            {
                "线索ID": "D-APR",
                "手机号": "13800001002",
                "线索创建时间": "2026-04-01 10:00:00",
                "date": "2026-04-01",
                "标准账号": "抖音-星途汽车直播营销中心",
                "本场主播": "徐幻",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "成交车型": "",
                "orders_contrib": "0",
                "deals_contrib": "0",
                "线索ID_norm": "D-APR",
                "is_perf_lead_scope": "1",
            },
        ],
    )
    _write_raw_deals_csv(
        repo_root / "历史文件" / "2026年3月" / "总部新媒体成交2026-04-01.csv",
        [{"线索ID": "D-MAR", "订单状态": "已交车", "成交日期": "2026-03-05", "成交车型": "EX7", "渠道2": "抖音-星途汽车官方直播间", "渠道3": "直播"}],
    )
    _write_raw_deals_csv(
        repo_root / "历史文件" / "2026年4月" / "总部新媒体成交2026-05-01.csv",
        [{"线索ID": "D-APR", "订单状态": "已交车", "成交日期": "2026-04-10", "成交车型": "LX", "渠道2": "抖音-星途汽车直播营销中心", "渠道3": "直播"}],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?end_date=2026-05-22")

    assert response.status_code == 200
    payload = response.json()
    deals = next(item for item in payload["daily_trends"] if item["key"] == "deals")
    assert {"date": "2026-03-05", "value": 1.0} in deals["points"]
    assert {"date": "2026-04-10", "value": 1.0} in deals["points"]
    assert payload["monthly_comparison"][0]["metrics"]["deals"]["value"] == 1.0
    assert payload["monthly_comparison"][1]["metrics"]["deals"]["value"] == 1.0
    ex7_segment = next(item for item in payload["model_segment_summary"] if item["name"] == "EX7")
    assert ex7_segment["daily_trends"]["deals"][4]["value"] == 1.0
    official_account = next(item for item in payload["account_summary"] if item["name"] == "抖音-星途汽车官方直播间")
    assert official_account["metrics"]["deals"]["actual"] == 1.0


def test_get_daily_dashboard_trends_marks_missing_previous_period_without_fake_values(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            {
                "线索ID": "C1",
                "手机号": "13800000001",
                "线索创建时间": "2026-05-01 10:00:00",
                "date": "2026-05-01",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "线索ID_norm": "C1",
                "is_perf_lead_scope": "1",
            }
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-02")

    assert response.status_code == 200
    payload = response.json()
    assert payload["previous_period"]["has_data"] is False
    assert payload["previous_period"]["message"] == "上一周期数据不足"
    assert payload["previous_period_trends"] == []


def test_get_daily_dashboard_trends_keeps_missing_points_distinct_from_zero(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            {
                "线索ID": "L1",
                "手机号": "13800000001",
                "线索创建时间": "2026-05-01 10:00:00",
                "date": "2026-05-01",
                "标准账号": "抖音-星途汽车官方直播间",
                "本场主播": "丁俐佳",
                "订单状态": "",
                "成交时间": "",
                "is_order": "0",
                "is_deal": "0",
                "成交车型": "",
                "orders_contrib": "0",
                "deals_contrib": "0",
                "线索ID_norm": "L1",
                "is_perf_lead_scope": "1",
            }
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-03")

    assert response.status_code == 200
    payload = response.json()
    leads = next(item for item in payload["daily_trends"] if item["key"] == "leads")
    spend = next(item for item in payload["daily_trends"] if item["key"] == "spend")
    assert leads["points"] == [
        {"date": "2026-05-01", "value": 1.0},
        {"date": "2026-05-02", "value": None},
        {"date": "2026-05-03", "value": None},
    ]
    assert spend["points"] == [
        {"date": "2026-05-01", "value": None},
        {"date": "2026-05-02", "value": None},
        {"date": "2026-05-03", "value": None},
    ]


def test_get_daily_dashboard_trends_requires_existing_sources(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["code"] == "DASHBOARD_SOURCE_NOT_FOUND"
    assert payload["error"]["details"]["report_date"] == "trend"


def test_get_daily_dashboard_trends_prototype_returns_read_only_html(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends/prototype")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    html = response.text
    assert "经营趋势看板" in html
    assert "核心经营表现" in html
    assert "车型结构对比" in html
    assert "账号表现" in html
    assert "主播表现" in html
    assert "种草曝光表现" in html
    assert "查看近期核心经营指标变化、车型结构、账号表现、主播表现与种草曝光情况，辅助日常经营复盘。" in html
    assert "READ-ONLY FILE TREND" not in html
    assert "文件级趋势视图" not in html
    assert '<span>API</span>' not in html
    for term in [
        "质量标注",
        "数据状态",
        "发布边界",
        "数据来源文件",
        "可用日期",
        "缺失日期",
        "运行证据",
        "需人工复核",
        "阻断",
        "暂停使用",
        "run-",
        "dashboard source",
        "dashboard source TSV",
        "文件级趋势",
        "contract_version",
        "release_readiness",
        "publish-ready",
        "API 路径",
        "TSV",
        "pipeline",
    ]:
        assert term not in html
    assert 'const DATA_URL = "/dashboard/daily/trends"' in html
    assert 'method: "GET"' in html
    assert 'method: "POST"' not in html
    assert 'function isTrendDataPath(path)' in html
    assert 'url.pathname === "/dashboard/daily/trends"' in html
    assert "kpi-card" in html
    assert "model-compare-card" in html
    assert "account-card" in html
    assert "anchor-card" in html
    assert "seed-card" in html
    assert 'id="anchor-toolbar"' in html
    assert 'id="anchor-search-input"' in html
    assert 'placeholder="搜索主播姓名或所属账号"' in html
    assert 'id="anchor-sort-select"' in html
    assert 'class="anchor-filter-chip is-active"' in html
    assert 'id="anchor-clear-filters"' in html
    assert 'id="anchor-filter-summary"' in html
    assert "当前条件：全部主播" in html
    assert "无匹配主播" in html
    assert "function applyAnchorListState" in html
    assert "function anchorMatchesFilter" in html
    assert "function bindAnchorDetailToggles" in html
    assert "progress-bar" in html
    assert "progress-fill" in html
    assert "当前范围" in html
    assert "查看天数" in html
    assert "应用范围" in html
    assert "近三个月" in html
    assert "本季度" in html
    assert "上季度" in html
    assert 'id="trend-start-date" name="start_date" type="date" required' in html
    assert 'id="trend-end-date" name="end_date" type="date" required' in html
    assert "单次查看范围建议不超过一个季度，请缩小日期范围。" in html
    assert "x-axis" in html
    assert "y-axis" in html
    assert "chart-tooltip" in html
    assert "chart-legend" in html
    assert "chart-grid" in html
    assert "data-date" in html
    assert "data-value" in html
    assert "本期" in html
    assert "上一周期" in html
    assert "本期；" in html
    assert "上一周期：未提供" in html
    assert "月度对比" in html
    assert "指标说明" in html
    assert "真实 0 保持 0" in html
    assert "缺失值显示未提供" in html
    assert "字段未接入显示未接入" in html
    assert "缺失趋势点不补 0" in html
    assert "--color-bg" in html
    assert "--color-surface" in html
    assert "--shadow-tooltip" in html
    assert "@media (prefers-reduced-motion: reduce)" in html
    assert "成本比值" in html
    assert "达成率" not in html
    assert "账号趋势" in html
    assert 'data-account-trend-key="leads"' in html
    assert 'data-account-trend-key="deals"' in html
    assert "HIDDEN_ACCOUNT_NAMES" in html
    assert "视频号-星途星纪元" in html
    assert "星途星纪元直播营销中心+" in html
    assert "快手-星途星纪元" in html
    assert "抖店" in html
    assert "featuredAccountCard" in html
    assert "featured-account-card" in html
    assert "快手-EXEED星途趋势" in html
    assert "线索趋势" in html
    assert "成交趋势" in html
    assert "实销趋势" in html
    assert "曝光趋势" in html
    assert "function sparkline(" not in html
    assert "${sparkline(" not in html
    assert "<table" not in html
    assert "<td></td>" not in html
    assert "<th>趋势</th>" not in html
    assert "daily_pipeline" not in html
    assert "/execution" not in html
    assert "method: \"POST\"" not in html


def test_get_daily_dashboard_trends_prototype_preserves_query_date_inputs(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends/prototype?start_date=2026-03-01&end_date=2026-05-22")

    assert response.status_code == 200
    html = response.text
    assert 'const DATA_URL = "/dashboard/daily/trends?start_date=2026-03-01&end_date=2026-05-22"' in html
    assert 'id="trend-start-date" name="start_date" type="date" required value="2026-03-01"' in html
    assert 'id="trend-end-date" name="end_date" type="date" required value="2026-05-22"' in html
    assert "2026-05-26" not in html


def test_get_daily_dashboard_prototype_returns_api_connected_html(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/2026-05-14/prototype")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    html = response.text
    assert 'const API_PATH = "/dashboard/daily/2026-05-14"' in html
    assert "fetchDashboard(path)" in html
    assert 'method: "GET"' in html
    assert 'data-api-path="/dashboard/daily/2026-05-14"' in html
    assert 'id="report-date-select"' in html
    assert "日报可交互 BI 原型 · 2026-05-14" in html


def test_get_latest_daily_dashboard_prototype_fetches_latest_api(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/latest/prototype")

    assert response.status_code == 200
    html = response.text
    assert 'const API_PATH = "/dashboard/daily/latest"' in html
    assert "fetchDashboard(path)" in html
    assert 'method: "GET"' in html
    assert 'data-api-path="/dashboard/daily/latest"' in html
    assert 'id="report-date-select"' in html
    assert "日报可交互 BI 原型 · latest" in html


def test_get_latest_daily_dashboard_feishu_link_returns_read_only_trial_html(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/latest/feishu-link")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    html = response.text
    assert "运营日报看板" in html
    assert "经营链路" in html
    assert "维度工作台" in html
    assert "数据新鲜度" in html
    assert "本地预览边界" in html
    assert "曝光" in html
    assert "线索" in html
    assert "唯一线索" in html
    assert "订单" in html
    assert "实销" in html
    assert "CPL" in html
    assert "CPS" in html
    assert "总览" in html
    assert "车型 / EX7" in html
    assert "主播" in html
    assert "账号 / 渠道" in html
    assert "成本效率" in html
    assert "每日经营结果" in html
    assert 'id="decision"' in html
    assert 'id="workbench"' in html
    assert "今日判断" in html
    assert "核心 KPI" in html
    assert "达成状态" in html
    assert "重点关注" in html
    assert "EX7 专项" in html
    assert "主播线索" in html
    assert "种草曝光" in html
    assert "renderDecision(payload)" in html
    assert 'const API_PATH = "/dashboard/daily/latest"' in html
    assert 'data-api-path="/dashboard/daily/latest"' in html
    assert 'data-dashboard-mode="business"' in html
    assert 'method: "GET"' in html
    assert 'method: "POST"' not in html
    assert 'function isDashboardReadOnlyPath(path)' in html
    assert 'path === "/dashboard/daily/latest"' in html
    assert '/^\\/dashboard\\/daily\\/\\d{4}-\\d{2}-\\d{2}$/' in html
    assert "/dashboard/daily/latest/prototype" not in html
    assert "/dashboard/daily/latest/feishu-link" not in html
    assert "N9-B READ-ONLY LINK TRIAL" not in html
    assert "飞书链接试用" not in html
    assert "Source path" not in html
    assert "Source rows" not in html
    assert "GET only" not in html
    assert "API payload" not in html
    assert "/execution" not in html
    assert "/reports" not in html
    assert "daily_pipeline" not in html
    assert "tenant_access_token" not in html
    assert "access_token" not in html
    assert "Authorization" not in html
    assert "cookie" not in html.lower()


def test_get_dated_daily_dashboard_feishu_link_returns_read_only_trial_html(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/2026-05-14/feishu-link")

    assert response.status_code == 200
    html = response.text
    assert "运营日报看板" in html
    assert "经营链路" in html
    assert "维度工作台" in html
    assert "车型 / EX7" in html
    assert "主播贡献" in html
    assert "账号 / 渠道" in html
    assert "成本效率" in html
    assert 'id="decision"' in html
    assert 'id="workbench"' in html
    assert "今日判断" in html
    assert "核心 KPI" in html
    assert "重点关注" in html
    assert 'const API_PATH = "/dashboard/daily/2026-05-14"' in html
    assert 'data-api-path="/dashboard/daily/2026-05-14"' in html
    assert 'data-dashboard-mode="business"' in html
    assert 'method: "GET"' in html
    assert "N9-B READ-ONLY LINK TRIAL" not in html
    assert "Source path" not in html
    assert "GET only" not in html
    assert "API payload" not in html
    assert "/dashboard/daily/2026-05-14/prototype" not in html


def test_get_daily_dashboard_prototype_is_not_polluted_by_n9b_workbench(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/2026-05-14/prototype")

    assert response.status_code == 200
    html = response.text
    assert "日报可交互 BI 原型 · 2026-05-14" in html
    assert "N7 V0 READ-ONLY BI" in html
    assert "Source path" in html
    assert "Source rows" in html
    assert "GET only" in html
    assert 'data-dashboard-mode="technical"' in html
    assert "维度工作台" not in html
    assert 'id="workbench"' not in html


def _trend_rows(report_date: str, *, leads: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in _sample_rows():
        copied = dict(row)
        copied["report_date"] = report_date
        if copied["metric_key"] == "mtd_unique_leads" and copied["scope_type"] == "department":
            copied["actual"] = leads
        rows.append(copied)
    rows.extend(
        [
            _dated_row(
                report_date,
                "lead_account",
                "account",
                "星途汽车官方直播间",
                "",
                "mtd_unique_leads",
                "累计唯一线索",
                "10045",
                "14850",
                "0.6764",
                "条",
            ),
            _dated_row(
                report_date,
                "lead_account",
                "account",
                "星途汽车官方直播间",
                "",
                "mtd_deals",
                "累计实销",
                "40",
                "50",
                "0.8",
                "台",
            ),
            _dated_row(
                report_date,
                "lead_account",
                "account",
                "星途汽车官方直播间",
                "",
                "mtd_spend",
                "累计线索费用",
                "331070.5",
                "",
                "",
                "元",
            ),
            _dated_row(
                report_date,
                "lead_account",
                "account",
                "星途汽车官方直播间",
                "",
                "mtd_cpl",
                "实际CPL",
                "32.96",
                "55",
                "",
                "元/条",
            ),
            _dated_row(
                report_date,
                "lead_account",
                "account",
                "星途汽车官方直播间",
                "",
                "mtd_cps",
                "实际CPS",
                "8276.76",
                "9060.02",
                "",
                "元/台",
            ),
        ]
    )
    return rows


def _dated_row(
    report_date: str,
    source_table: str,
    scope_type: str,
    scope_name: str,
    parent_scope: str,
    metric_key: str,
    metric_name: str,
    actual: str,
    target: str,
    attain_rate: str,
    unit: str,
) -> dict[str, str]:
    row = _row(source_table, scope_type, scope_name, parent_scope, metric_key, metric_name, actual, target, attain_rate, unit)
    row["report_date"] = report_date
    return row


def _fact_row(
    lead_id: str,
    *,
    date: str,
    account: str,
    model: str,
    deal_time: str = "",
    host: str = "丁俐佳",
) -> dict[str, str]:
    is_deal = "1" if deal_time else "0"
    return {
        "线索ID": lead_id,
        "手机号": f"137{abs(hash(lead_id)) % 100000000:08d}",
        "线索创建时间": f"{date} 10:00:00",
        "date": date,
        "标准账号": account,
        "本场主播": host,
        "订单状态": "已交车" if deal_time else "",
        "成交时间": deal_time,
        "is_order": is_deal,
        "is_deal": is_deal,
        "成交车型": model,
        "orders_contrib": is_deal,
        "deals_contrib": is_deal,
        "线索ID_norm": lead_id,
        "is_perf_lead_scope": "1",
    }


def _write_run_evidence(
    repo_root: Path,
    *,
    run_id: str,
    report_date: str,
    quality_status: str,
    quality_decision: str,
    release_readiness: str,
) -> None:
    runs_dir = repo_root / "artifacts" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_id,
        "report_date": report_date,
        "canonical_report_date": report_date,
        "generated_at": run_id.removeprefix("run-").removesuffix("Z"),
        "quality_status": quality_status,
        "quality_decision": quality_decision,
        "release_readiness": release_readiness,
    }
    quality_report = {
        "run_id": run_id,
        "report_date": report_date,
        "canonical_report_date": report_date,
        "overall_status": quality_status,
        "summary": {"overall_status": quality_status, "operational_decision": quality_decision},
        "release_readiness": release_readiness,
    }
    (runs_dir / f"run_manifest_{run_id}.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (runs_dir / f"quality_report_{run_id}.json").write_text(
        json.dumps(quality_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _write_fact_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "线索ID",
        "手机号",
        "线索创建时间",
        "date",
        "标准账号",
        "本场主播",
        "同手机号线索数",
        "是否手机号折叠",
        "权重",
        "归属状态",
        "无匹配原因",
        "report_bucket",
        "订单状态",
        "成交时间",
        "主播人数",
        "成交分摊权重",
        "is_order",
        "is_deal",
        "渠道",
        "成交车型",
        "orders_contrib",
        "deals_contrib",
        "business_subject_key",
        "_lead_key",
        "线索ID_norm",
        "命中场次数量",
        "is_perf_lead_scope",
        "成交_账号",
        "schema_version",
        "metric_version",
        "run_id",
    ]
    path.write_text(
        "\n".join(
            [
                ",".join(columns),
                *[",".join(str(row.get(column, "")) for column in columns) for row in rows],
            ]
        )
        + "\n",
        encoding="utf-8-sig",
    )


def _write_raw_leads_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["线索ID", "创建日期", "创建时间", "到店日期", "到店时间", "首次意向车型", "意向车型", "试驾车型", "下订车型", "成交车型", "渠道2", "渠道3"]
    path.write_text(
        "\n".join([",".join(columns), *[",".join(str(row.get(column, "")) for column in columns) for row in rows]]) + "\n",
        encoding="utf-8-sig",
    )


def _write_raw_deals_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["线索ID", "订单状态", "成交日期", "成交时间", "下订日期", "下订时间", "下订车型", "成交车型", "渠道2", "渠道3"]
    path.write_text(
        "\n".join([",".join(columns), *[",".join(str(row.get(column, "")) for column in columns) for row in rows]]) + "\n",
        encoding="utf-8-sig",
    )


def _write_live_workbook(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_excel(path, index=False)
