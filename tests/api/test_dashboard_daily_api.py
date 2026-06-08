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
    assert "segments" not in payload
    assert payload["lead_anchors"][0]["name"] == "徐幻"
    assert payload["lead_anchors"][0]["metrics"]["mtd_unique_leads"]["actual"] == 2634
    assert payload["seed_account"]["actual"] == 7221796
    assert payload["seed_anchors"][0]["name"] == "桂婕"
    assert payload["seed_anchors"][0]["metrics"]["daily_impressions"]["actual"] == 0
    assert payload["seed_anchors"][0]["metrics"]["daily_impressions"]["attain_rate"] == 0
    assert payload["interactions"]["module_anchors"] == [
        "overview",
        "funnel",
        "lead-anchors",
        "seed-exposure",
        "daily-bi-trends",
    ]
    assert "mtd_douyin_laike_orders" in payload["interactions"]["lead_anchor_sort_keys"]
    assert "mtd_impressions" in payload["interactions"]["seed_anchor_sort_keys"]


def test_dashboard_daily_service_normalizes_legacy_douyin_laike_order_label() -> None:
    metric = DashboardDailyService._metric_from_row(
        "mtd_douyin_laike_orders",
        {
            "metric_name": "抖音-来客订单",
            "actual": "38",
            "target": "1000",
            "attain_rate": "0.038",
            "unit": "个",
            "source_column": "账号层（母集）.线索组汇总.抖音-来客订单数",
        },
    )

    assert metric.label == "抖音-来客线索（手机号去重）"
    assert metric.unit == "条"
    assert metric.note == "账号层（母集）.线索组汇总.抖音-来客线索数（手机号去重）"


def test_get_daily_dashboard_missing_source_returns_404(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/2026-05-14")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["code"] == "DASHBOARD_SOURCE_NOT_FOUND"
    assert payload["error"]["details"]["report_date"] == "2026-05-14"


def test_dashboard_daily_service_prefers_monthly_metric_contract_targets(tmp_path: Path) -> None:
    repo_root, _ = build_temp_repo(tmp_path)
    config_dir = repo_root / "config"
    (config_dir / "monthly_targets.csv").write_text(
        "month,scope_type,scope_name,parent_account,lead_target_month,deal_target_month,lead_cost_target_month,cpl_target,cps_target,target_pool,order_target_month\n"
        "2026-05,account,抖音-星途汽车官方直播间,,555,5,5000,9,99,旧目标池,55\n"
        "2026-06,account,抖音-星途汽车官方直播间,,1,1,1,1,1,旧目标池,1\n",
        encoding="utf-8-sig",
    )
    (config_dir / "seed_monthly_targets.csv").write_text(
        "month,scope_type,scope_name,parent_scope,parent_account,impression_target_month,spend_target_month,cpm_target,target_pool\n"
        "2026-05,account,EXEED星途,,,555555,,,旧种草池\n"
        "2026-06,account,EXEED星途,,,1,,,旧种草池\n",
        encoding="utf-8-sig",
    )
    (config_dir / "report_topline_config.json").write_text(
        '{"full_account_targets":{"impressions":5,"leads":4,"deals":3,"cpl":2,"cps":1},"ex7_rules":{"keywords":["OLD"],"lead_model_field_candidates":["旧字段"],"deal_model_field_candidates":["旧字段"],"live_model_field_candidates":["旧字段"]},"pending_rules":{"primary_date_field":"旧日期","fallback_date_fields":[]}}',
        encoding="utf-8",
    )
    (config_dir / "monthly_metric_contract.json").write_text(
        json.dumps(
            {
                "version": 1,
                "months": {
                    "2026-06": {
                        "monthly_targets": [
                            {
                                "scope_type": "account",
                                "scope_name": "抖音-星途汽车官方直播间",
                                "parent_account": "",
                                "lead_target_month": 321,
                                "deal_target_month": 12,
                                "lead_cost_target_month": 6543,
                                "cpl_target": 20.38,
                                "cps_target": 545.25,
                                "target_pool": "线索组目标池",
                                "order_target_month": 99,
                            }
                        ],
                        "seed_monthly_targets": [
                            {
                                "scope_type": "account",
                                "scope_name": "EXEED星途",
                                "parent_scope": "",
                                "parent_account": "",
                                "impression_target_month": 7654321,
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

    service = DashboardDailyService(repo_root=repo_root)
    targets = service._load_targets()
    seed_targets = service._load_seed_targets()
    june_topline_targets = service._load_topline_targets(month="2026-06")
    may_topline_targets = service._load_topline_targets(month="2026-05")

    target_row = targets[targets["month"].eq("2026-06") & targets["scope_name"].eq("抖音-星途汽车官方直播间")].iloc[0]
    seed_row = seed_targets[seed_targets["month"].eq("2026-06") & seed_targets["scope_name"].eq("EXEED星途")].iloc[0]
    legacy_target_row = targets[targets["month"].eq("2026-05") & targets["scope_name"].eq("抖音-星途汽车官方直播间")].iloc[0]
    legacy_seed_row = seed_targets[seed_targets["month"].eq("2026-05") & seed_targets["scope_name"].eq("EXEED星途")].iloc[0]
    assert target_row["lead_target_month"] == 321
    assert target_row["order_target_month"] == 99
    assert seed_row["impression_target_month"] == 7654321
    assert legacy_target_row["lead_target_month"] == 555
    assert legacy_target_row["order_target_month"] == 55
    assert legacy_seed_row["impression_target_month"] == 555555
    assert june_topline_targets == {"impressions": 25000000.0, "leads": 0.0, "deals": 100.0, "cpl": 0.0, "cps": 1500.0}
    assert may_topline_targets == {"impressions": 5.0, "leads": 4.0, "deals": 3.0, "cpl": 2.0, "cps": 1.0}


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
    assert "segments" not in payload
    assert "model_segment_summary" not in payload
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


def test_get_daily_dashboard_trends_requires_dashboard_source_even_when_fact_exists(tmp_path: Path) -> None:
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
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-01")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["code"] == "DASHBOARD_SOURCE_NOT_FOUND"
    assert payload["error"]["details"]["report_date"] == "trend"


def test_get_daily_dashboard_trends_reads_release_metrics_from_dashboard_source(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-28.tsv", _release_dashboard_source_rows("2026-05-28"))
    _write_run_evidence(
        repo_root,
        run_id="run-20260530T083218Z",
        report_date="2026-05-28",
        quality_status="pass",
        quality_decision="safe",
        release_readiness="ready",
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-28&end_date=2026-05-28")

    assert response.status_code == 200
    payload = response.json()
    summary = {item["key"]: item for item in payload["core_kpi_summary"]}
    assert summary["impressions"]["actual"] == 24286300
    assert summary["spend"]["actual"] == 784252.48
    assert summary["cpl"]["actual"] == 34.58
    assert summary["cpl"]["attain_rate"] == pytest.approx(53 / 34.58)
    assert summary["cps"]["actual"] == 8618.16
    assert summary["cps"]["attain_rate"] == pytest.approx(6267 / 8618.16)

    trends = {item["key"]: item for item in payload["daily_trends"]}
    assert trends["impressions"]["points"] == [{"date": "2026-05-28", "value": 24286300}]
    assert trends["spend"]["points"] == [{"date": "2026-05-28", "value": 784252.48}]
    assert trends["cpl"]["points"] == [{"date": "2026-05-28", "value": 34.58}]
    assert trends["cps"]["points"] == [{"date": "2026-05-28", "value": 8618.16}]

    account = next(item for item in payload["account_summary"] if item["name"] == "星途汽车直播营销中心")
    assert account["metrics"]["spend"]["actual"] == 338841.04
    assert account["metrics"]["cpl"]["actual"] == 41.22
    assert account["metrics"]["cpl"]["attain_rate"] == pytest.approx(55 / 41.22)
    assert account["metrics"]["cps"]["actual"] == 14118.38

    anchor = next(item for item in payload["anchor_summary"] if item["name"] == "徐欣悦")
    assert anchor["parent_scope"] == "星途汽车直播营销中心"
    assert anchor["metrics"]["spend"]["actual"] == 120921.25
    assert anchor["metrics"]["cpl"]["actual"] == 41.12
    assert anchor["metrics"]["cps"]["actual"] == 10076.77

    annotation = payload["quality_annotations"]["2026-05-28"]
    assert annotation["run_id"] == "run-20260530T083218Z"
    assert annotation["quality_status"] == "pass"
    assert annotation["quality_decision"] == "safe"
    assert annotation["release_readiness"] == "ready"


def test_get_daily_dashboard_trends_monthly_comparison_uses_latest_source_mtd_for_month(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-13.tsv",
        _minimal_dashboard_source_rows("2026-05-13", impressions=1000, leads=10, orders=3, deals=1, spend=100),
    )
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-22.tsv",
        _minimal_dashboard_source_rows("2026-05-22", impressions=2000, leads=20, orders=7, deals=2, spend=200),
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-03-01&end_date=2026-05-22")

    assert response.status_code == 200
    payload = response.json()
    assert payload["date_range"]["available_dates"] == ["2026-05-13", "2026-05-22"]
    assert [month["label"] for month in payload["monthly_comparison"]] == ["2026年5月"]
    may = payload["monthly_comparison"][0]["metrics"]
    assert may["impressions"]["value"] == 2000.0
    assert may["leads"]["value"] == 20.0
    assert may["douyin_laike_orders"]["value"] == 7.0
    assert may["deals"]["value"] == 2.0
    assert may["spend"]["value"] == 200.0
    assert may["cpl"]["value"] == 10.0


def test_get_daily_dashboard_trends_supplements_visit_metrics_without_ex7_from_detail_sources(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-29.tsv",
        _minimal_dashboard_source_rows(
            "2026-05-29",
            accounts=[
                {"name": "线索组汇总", "leads": 2, "deals": 1, "spend": 100, "cpl": 50, "cps": 100},
                {"name": "星途汽车官方直播间", "leads": 2, "deals": 1, "spend": 100, "cpl": 50, "cps": 100},
            ],
            anchors=[
                {"name": "丁俐佳", "parent_scope": "星途汽车官方直播间", "leads": 2, "deals": 1, "spend": 100, "cpl": 50, "cps": 100},
            ],
        ),
    )
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            _fact_row("L1", date="2026-05-29", account="抖音-星途汽车官方直播间", model="EX7", host="丁俐佳", deal_time="2026-05-29 12:00:00"),
            _fact_row("L2", date="2026-05-29", account="抖音-星途汽车官方直播间", model="TXL", host="丁俐佳"),
        ],
    )
    _write_raw_leads_csv(
        repo_root / "源文件" / "总部新媒体线索2026-05-29.csv",
        [
            {"线索ID": "L1", "创建日期": "2026-05-29", "到店日期": "2026-05-29", "成交车型": "EX7"},
            {"线索ID": "L2", "创建日期": "2026-05-29", "到店日期": "", "成交车型": "TXL"},
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-29&end_date=2026-05-29")

    assert response.status_code == 200
    payload = response.json()
    account = next(item for item in payload["account_summary"] if item["name"] == "星途汽车官方直播间")
    assert account["metrics"]["visits"]["actual"] == 1.0
    assert account["metrics"]["visit_rate"]["actual"] == pytest.approx(0.5)
    assert account["metrics"]["visit_deal_rate"]["actual"] == pytest.approx(1.0)
    assert account["metrics"]["visits"]["source"] == "fact_attribution + 总部新媒体线索到店日期"
    assert "到店" in account["metric_groups"]
    assert {"ex7_leads", "ex7_deals", "ex7_deal_rate"}.isdisjoint(account["metrics"])
    assert "EX7" not in account["metric_groups"]

    line_summary = next(item for item in payload["account_summary"] if item["name"] == "线索组汇总")
    assert line_summary["metrics"]["visits"]["actual"] == 1.0
    assert line_summary["metrics"]["visit_rate"]["actual"] == pytest.approx(0.5)
    assert line_summary["metrics"]["visit_deal_rate"]["actual"] == pytest.approx(1.0)
    assert "到店" in line_summary["metric_groups"]
    assert {"ex7_leads", "ex7_deals", "ex7_deal_rate"}.isdisjoint(line_summary["metrics"])
    assert "EX7" not in line_summary["metric_groups"]

    anchor = next(item for item in payload["anchor_summary"] if item["name"] == "丁俐佳")
    assert anchor["metrics"]["visits"]["actual"] == 1.0
    assert anchor["metrics"]["visit_rate"]["actual"] == pytest.approx(0.5)
    assert anchor["metrics"]["visit_deal_rate"]["actual"] == pytest.approx(1.0)
    assert "到店" in anchor["metric_groups"]
    assert {"ex7_leads", "ex7_deals", "ex7_deal_rate"}.isdisjoint(anchor["metrics"])
    assert "EX7" not in anchor["metric_groups"]
    assert payload["metric_source_status"] == [
        {
            "metric": "曝光/唯一线索/来客线索/实销/费用/CPL/CPS/账号/主播/种草曝光/月度对比",
            "status": "available",
            "source": "output/sql_reports/feishu_dashboard_source_latest_*.tsv",
        },
        {
            "metric": "到店数/到店率/到店成交率",
            "status": "available",
            "source": "output/fact_attribution.csv + 源文件/总部新媒体线索*.csv",
        },
    ]


def test_get_daily_dashboard_trends_keeps_zero_visit_metrics_when_visit_source_has_no_rows(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-30.tsv",
        _minimal_dashboard_source_rows(
            "2026-05-30",
            accounts=[
                {"name": "线索组汇总", "leads": 2, "deals": 1, "spend": 100, "cpl": 50, "cps": 100},
                {"name": "星途汽车官方直播间", "leads": 2, "deals": 1, "spend": 100, "cpl": 50, "cps": 100},
            ],
            anchors=[
                {"name": "丁俐佳", "parent_scope": "星途汽车官方直播间", "leads": 2, "deals": 1, "spend": 100, "cpl": 50, "cps": 100},
            ],
        ),
    )
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            _fact_row("L1", date="2026-05-30", account="抖音-星途汽车官方直播间", model="TXL", host="丁俐佳", deal_time="2026-05-30 12:00:00"),
            _fact_row("L2", date="2026-05-30", account="抖音-星途汽车官方直播间", model="TXL", host="丁俐佳"),
        ],
    )
    _write_raw_leads_csv(
        repo_root / "源文件" / "总部新媒体线索2026-05-30.csv",
        [
            {"线索ID": "L1", "创建日期": "2026-05-30", "到店日期": "", "成交车型": "TXL"},
            {"线索ID": "L2", "创建日期": "2026-05-30", "到店日期": "", "成交车型": "TXL"},
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-30&end_date=2026-05-30")

    assert response.status_code == 200
    payload = response.json()
    for collection_name, entity_name in [
        ("account_summary", "星途汽车官方直播间"),
        ("account_summary", "线索组汇总"),
        ("anchor_summary", "丁俐佳"),
    ]:
        entity = next(item for item in payload[collection_name] if item["name"] == entity_name)
        assert entity["metrics"]["visits"]["actual"] == 0.0
        assert entity["metrics"]["visit_rate"]["actual"] == 0.0
        assert entity["metrics"]["visit_deal_rate"]["actual"] is None
        assert entity["metrics"]["visits"]["source_status"] == "available"
        assert entity["metrics"]["visits"]["source"] == "fact_attribution + 总部新媒体线索到店日期"
        assert "到店" in entity["metric_groups"]
        assert "EX7" not in entity["metric_groups"]


def test_get_daily_dashboard_trends_monthly_comparison_ignores_history_when_source_months_are_missing(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-29.tsv",
        _minimal_dashboard_source_rows("2026-05-29", impressions=500, leads=5, orders=5, deals=1, spend=50),
    )
    _write_live_workbook(
        repo_root / "历史文件" / "2026年3月" / "2026年3月直播进度表.xlsx",
        [
            {
                "日期": "2026-03-05",
                "开播账号": "抖音-星途汽车官方直播间",
                "平台&挂载组建": "抖音-来客",
                "开播时间": "10:00",
                "下播时间": "11:00",
                "本场主播": "丁俐佳",
                "订单数": 800,
                "消耗": 80,
                "曝光人数": 800,
            },
            {
                "日期": "2026-03-05",
                "开播账号": "抖音-星途汽车官方直播间",
                "平台&挂载组建": "直播",
                "开播时间": "12:00",
                "下播时间": "13:00",
                "本场主播": "丁俐佳",
                "订单数": 99,
                "消耗": 20,
                "曝光人数": 200,
            },
        ],
    )
    _write_live_workbook(
        repo_root / "历史文件" / "2026年4月" / "2026年4月直播进度表.xlsx",
        [
            {
                "日期": "2026-04-05",
                "开播账号": "抖音-星途汽车直播营销中心",
                "平台&挂载组建": "抖音-来客",
                "开播时间": "10:00",
                "下播时间": "11:00",
                "本场主播": "徐欣悦",
                "订单数": 900,
                "消耗": 90,
                "曝光人数": 900,
            },
        ],
    )
    _write_live_workbook(
        repo_root / "源文件" / "2026年5月直播进度表.xlsx",
        [
            {
                "日期": "2026-05-29",
                "开播账号": "抖音-星途汽车官方直播间",
                "平台&挂载组建": "抖音-来客",
                "开播时间": "10:00",
                "下播时间": "11:00",
                "本场主播": "丁俐佳",
                "订单数": 990,
                "消耗": 990,
                "曝光人数": 9900,
            },
        ],
    )
    _write_raw_leads_csv(
        repo_root / "历史文件" / "2026年3月" / "总部新媒体线索2026-04-01.csv",
        [
            {"线索ID": "M-L1", "创建时间": "2026-03-05 10:10:00", "渠道2": "抖音来客直播", "渠道3": "星途星纪元直播营销中心", "手机号": "13800000001"},
            {"线索ID": "M-L2", "创建时间": "2026-03-05 10:20:00", "渠道2": "抖音来客直播", "渠道3": "星途星纪元直播营销中心", "手机号": "13800000001"},
            {"线索ID": "M-L3", "创建时间": "2026-03-05 10:30:00", "渠道2": "抖音来客直播", "渠道3": "星途星纪元直播营销中心", "手机号": ""},
            {"线索ID": "M-L4", "创建时间": "2026-03-05 12:30:00", "渠道2": "抖音来客直播", "渠道3": "星途星纪元直播营销中心", "手机号": "13800000004"},
        ],
    )
    _write_raw_leads_csv(
        repo_root / "历史文件" / "2026年4月" / "总部新媒体线索2026-05-01.csv",
        [
            {"线索ID": "A-L1", "创建时间": "2026-04-05 10:10:00", "渠道2": "抖音来客直播", "渠道3": "星途汽车直播营销中心", "手机号": "13900000001"},
            {"线索ID": "A-L2", "创建时间": "2026-04-05 10:20:00", "渠道2": "抖音来客直播", "渠道3": "星途汽车直播营销中心", "手机号": "13800000001"},
        ],
    )
    _write_raw_leads_csv(
        repo_root / "源文件" / "总部新媒体线索2026-06-01.csv",
        [
            {"线索ID": "Y-L1", "创建时间": "2026-05-29 10:10:00", "渠道2": "抖音来客直播", "渠道3": "星途汽车官方直播间", "手机号": "13700000001"},
            {"线索ID": "Y-L2", "创建时间": "2026-05-29 10:20:00", "渠道2": "抖音来客直播", "渠道3": "星途汽车官方直播间", "手机号": "13700000002"},
        ],
    )
    _write_fact_csv(
        repo_root / "output" / "fact_attribution.csv",
        [
            _fact_row("M1", date="2026-03-05", account="抖音-星途汽车官方直播间", model="TXL", deal_time="2026-03-06 12:00:00"),
            _fact_row("A1", date="2026-04-05", account="抖音-星途汽车官方直播间", model="TXL"),
            _fact_row("Y1", date="2026-05-29", account="抖音-星途汽车官方直播间", model="TXL"),
        ],
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-03-01&end_date=2026-05-29")

    assert response.status_code == 200
    payload = response.json()
    assert [month["label"] for month in payload["monthly_comparison"]] == ["2026年5月"]
    may = payload["monthly_comparison"][0]["metrics"]
    assert may["impressions"]["value"] == 500.0
    assert may["leads"]["value"] == 5.0
    assert may["douyin_laike_orders"]["value"] == 5.0
    assert may["deals"]["value"] == 1.0
    assert may["spend"]["value"] == 50.0


def test_get_daily_dashboard_trends_filters_cancelled_accounts_from_account_summary(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    cancelled_accounts = [
        "视频号-星途星纪元",
        "星途星纪元",
        "星途星纪元直播营销中心+",
        "抖音",
        "快手-星途星纪元",
        "抖店",
    ]
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-01.tsv",
        _minimal_dashboard_source_rows(
            "2026-05-01",
            accounts=[
                {"name": "快手-EXEED星途", "leads": 1, "deals": 0, "spend": 100, "cpl": 100, "cps": 0},
                *[{"name": name, "leads": 1, "deals": 0, "spend": 0, "cpl": 0, "cps": 0} for name in cancelled_accounts],
            ],
        ),
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-01")

    assert response.status_code == 200
    account_names = {item["name"] for item in response.json()["account_summary"]}
    assert "快手-EXEED星途" in account_names
    assert account_names.isdisjoint(cancelled_accounts)


def test_get_daily_dashboard_trends_derives_source_conversion_rates_above_100_percent(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-01.tsv",
        _minimal_dashboard_source_rows(
            "2026-05-01",
            accounts=[
                {"name": "抖音-星途极速拍档", "leads": 3, "deals": 10, "spend": 100, "cpl": 33.33, "cps": 10},
                {"name": "快手-EXEED星途", "leads": 2, "deals": 3, "spend": 90, "cpl": 45, "cps": 30},
            ],
        ),
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-01")

    assert response.status_code == 200
    account = next(item for item in response.json()["account_summary"] if item["name"] == "抖音-星途极速拍档")
    assert account["metrics"]["leads"]["actual"] == 3.0
    assert account["metrics"]["deals"]["actual"] == 10.0
    assert account["metrics"]["lead_deal_rate"]["actual"] == pytest.approx(10 / 3)
    assert account["metrics"]["lead_deal_rate"]["source_status"] == "available"
    assert {"visits", "visit_rate", "visit_deal_rate", "ex7_leads", "ex7_deals", "ex7_deal_rate"}.isdisjoint(account["metrics"])

    featured = next(item for item in response.json()["account_summary"] if item["name"] == "快手-EXEED星途")
    assert featured["metrics"]["deals"]["actual"] == 3.0
    assert featured["metrics"]["lead_deal_rate"]["actual"] == pytest.approx(1.5)
    assert {"visits", "visit_rate", "visit_deal_rate", "ex7_leads", "ex7_deals", "ex7_deal_rate"}.isdisjoint(featured["metrics"])


def test_get_daily_dashboard_trends_keeps_zero_denominator_rates_missing() -> None:
    metric = DashboardDailyService._metric_summary(
        "lead_deal_rate",
        "线索成交率",
        DashboardDailyService._safe_div_value(10, 0),
        None,
        "比例",
    )

    assert metric["actual"] is None
    assert metric["source_status"] == "available"
    assert "display_value" not in metric


def test_get_daily_dashboard_trends_hides_departed_anchor_and_keeps_fixed_seed_anchor(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-01.tsv",
        _minimal_dashboard_source_rows(
            "2026-05-01",
            anchors=[
                {"name": "王君如", "parent_scope": "抖音-星途汽车官方直播间", "leads": 1},
                {"name": "桂婕", "parent_scope": "抖音-EXEED星途", "leads": 1},
            ],
            seed_anchors=[
                {"name": "王君如", "parent_scope": "抖音-EXEED星途", "daily_impressions": 500, "mtd_impressions": 500},
                {"name": "桂婕", "parent_scope": "抖音-EXEED星途", "daily_impressions": 0, "mtd_impressions": 0},
            ],
        ),
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
    assert gui_seed["metrics"]["impressions"]["actual"] == 0


def test_get_daily_dashboard_trends_returns_previous_period_and_monthly_comparison(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    fixtures = {
        "2025-12-08": {"impressions": 600, "leads": 1, "deals": 0, "spend": 60},
        "2026-02-28": {"impressions": 400, "leads": 1, "deals": 0, "spend": 40},
        "2026-03-01": {"impressions": 1000, "leads": 1, "deals": 0, "spend": 100},
        "2026-04-01": {"impressions": 0, "leads": 0, "deals": 0, "spend": 80},
        "2026-05-22": {"impressions": 1500, "leads": 1, "deals": 1, "spend": 150},
    }
    for report_date, values in fixtures.items():
        _write_tsv(
            reports_dir / f"feishu_dashboard_source_latest_{report_date}.tsv",
            _minimal_dashboard_source_rows(report_date, **values),
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
    assert april["leads"]["value"] == 0.0
    assert april["cpl"]["value"] is None
    assert may["cps"]["value"] == 150.0


def test_get_daily_dashboard_trends_uses_dashboard_source_history_for_quarter_deals(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-03-05.tsv",
        _minimal_dashboard_source_rows(
            "2026-03-05",
            leads=1,
            deals=1,
            spend=100,
            accounts=[{"name": "抖音-星途汽车官方直播间", "leads": 1, "deals": 1, "spend": 100, "cpl": 100, "cps": 100}],
        ),
    )
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-04-10.tsv",
        _minimal_dashboard_source_rows(
            "2026-04-10",
            leads=1,
            deals=1,
            spend=200,
            accounts=[{"name": "星途汽车直播营销中心", "leads": 1, "deals": 1, "spend": 200, "cpl": 200, "cps": 200}],
        ),
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
    official_account = next(item for item in payload["account_summary"] if item["name"] == "抖音-星途汽车官方直播间")
    assert official_account["metrics"]["deals"]["actual"] == 1.0


def test_get_daily_dashboard_trends_marks_missing_previous_period_without_fake_values(tmp_path: Path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-01.tsv",
        _minimal_dashboard_source_rows("2026-05-01", leads=1, spend=100),
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
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-01.tsv",
        _minimal_dashboard_source_rows("2026-05-01", leads=1, include_spend=False),
    )
    _write_tsv(
        reports_dir / "feishu_dashboard_source_latest_2026-05-03.tsv",
        _minimal_dashboard_source_rows("2026-05-03", leads=0, spend=0),
    )
    app = create_test_app(repo_root, runs_root)

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/trends?start_date=2026-05-01&end_date=2026-05-03")

    assert response.status_code == 200
    payload = response.json()
    leads = next(item for item in payload["daily_trends"] if item["key"] == "leads")
    spend = next(item for item in payload["daily_trends"] if item["key"] == "spend")
    assert payload["missing_dates"] == ["2026-05-02"]
    assert leads["points"] == [
        {"date": "2026-05-01", "value": 1.0},
        {"date": "2026-05-03", "value": 0.0},
    ]
    assert spend["points"] == [
        {"date": "2026-05-01", "value": None},
        {"date": "2026-05-03", "value": 0.0},
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
    assert "账号表现" in html
    assert "主播表现" in html
    assert "种草曝光表现" in html
    assert "查看近期核心经营指标变化、账号表现、主播表现与种草曝光情况，辅助日常经营复盘。" in html
    assert "车型结构对比" not in html
    assert "车型结构" not in html
    assert "EX7" not in html
    assert "到店" in html
    assert "字段未接入" not in html
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
    assert "运营日报 BI" in html
    assert "经营链路" in html
    assert "维度工作台" in html
    assert "数据新鲜度" in html
    assert "BI 数据口径" in html
    assert "曝光" in html
    assert "线索" in html
    assert "唯一线索" in html
    assert "来客线索" in html
    assert "实销" in html
    assert "CPL" in html
    assert "CPS" in html
    assert "总览" in html
    assert "主播" in html
    assert "账号 / 渠道" in html
    assert "成本效率" in html
    assert "历史趋势" in html
    assert "月度对比" in html
    assert 'id="daily-bi-trends"' in html
    assert 'id="daily-bi-monthly-comparison"' in html
    assert "daily-bi-history-grid history-chart-grid" in html
    assert "function dailyBiLineChart" in html
    assert "dailyBiHistoryPanel(series, previousByKey)" in html
    assert "function bindDailyBiChartInteractions" in html
    assert "monthly-card daily-bi-month-card" in html
    assert "日报详细版" in html
    assert "历史趋势 · dashboard source TSV" not in html
    assert "dashboard source TSV</div>" not in html
    assert "每日经营结果" in html
    assert 'id="decision"' in html
    assert 'id="workbench"' in html
    assert "今日判断" in html
    assert "核心 KPI" in html
    assert "达成状态" in html
    assert "重点关注" in html
    assert "主播线索" in html
    assert "种草曝光" in html
    assert "renderDecision(payload)" in html
    assert "loadDailyBiTrends(payload)" in html
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
    assert "车型 / EX7" not in html
    assert "EX7 专项" not in html
    assert "EX7 组" not in html
    assert "EX7 线索数" not in html
    assert "EX7 成交数" not in html
    assert "EX7 成交率" not in html
    assert "到店数" in html
    assert "到店率" in html
    assert "到店成交率" in html
    assert "字段未接入" not in html
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
    assert "运营日报 BI" in html
    assert "经营链路" in html
    assert "维度工作台" in html
    assert "主播贡献" in html
    assert "账号 / 渠道" in html
    assert "成本效率" in html
    assert "历史趋势" in html
    assert "月度对比" in html
    assert 'id="decision"' in html
    assert 'id="workbench"' in html
    assert 'id="daily-bi-trends"' in html
    assert "今日判断" in html
    assert "核心 KPI" in html
    assert "重点关注" in html
    assert 'const API_PATH = "/dashboard/daily/2026-05-14"' in html
    assert 'data-api-path="/dashboard/daily/2026-05-14"' in html
    assert "车型 / EX7" not in html
    assert "EX7 专项" not in html
    assert "到店数" in html
    assert "字段未接入" not in html
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


def _release_dashboard_source_rows(report_date: str) -> list[dict[str, str]]:
    return [
        _dated_row(report_date, "topline", "department", "全量", "", "impressions", "曝光", "24286300", "20000000", "1.214315", "次"),
        _dated_row(report_date, "topline", "department", "全量", "", "mtd_unique_leads", "累计唯一线索", "22682", "25333", "0.8954", "条"),
        _dated_row(report_date, "topline", "department", "全量", "", "mtd_deals", "累计实销", "91", "114", "0.7982", "台"),
        _dated_row(report_date, "topline", "department", "全量", "", "mtd_spend", "累计线索费用", "784252.48", "", "", "元"),
        _dated_row(report_date, "topline", "department", "全量", "", "mtd_cpl", "总体 CPL", "34.58", "53", "", "元/条"),
        _dated_row(report_date, "topline", "department", "全量", "", "mtd_cps", "总体 CPS", "8618.16", "6267", "", "元/台"),
        _dated_row(report_date, "topline_segment", "segment", "EX7 专项", "", "mtd_unique_leads", "累计唯一线索", "10975", "", "", "条"),
        _dated_row(report_date, "topline_segment", "segment", "EX7 专项", "", "mtd_deals", "累计实销", "34", "", "", "台"),
        _dated_row(report_date, "topline_segment", "segment", "EX7 专项", "", "mtd_spend", "累计线索费用", "331884", "", "", "元"),
        _dated_row(report_date, "topline_segment", "segment", "EX7 专项", "", "mtd_cpl", "实际 CPL", "30.24", "", "", "元/条"),
        _dated_row(report_date, "topline_segment", "segment", "EX7 专项", "", "mtd_cps", "实际 CPS", "9761.53", "", "", "元/台"),
        _dated_row(report_date, "topline_segment", "segment", "不含 EX7", "", "mtd_unique_leads", "累计唯一线索", "11707", "", "", "条"),
        _dated_row(report_date, "topline_segment", "segment", "不含 EX7", "", "mtd_deals", "累计实销", "57", "", "", "台"),
        _dated_row(report_date, "topline_segment", "segment", "不含 EX7", "", "mtd_spend", "累计线索费用", "452368.48", "", "", "元"),
        _dated_row(report_date, "topline_segment", "segment", "不含 EX7", "", "mtd_cpl", "实际 CPL", "38.64", "", "", "元/条"),
        _dated_row(report_date, "topline_segment", "segment", "不含 EX7", "", "mtd_cps", "实际 CPS", "7936.15", "", "", "元/台"),
        _dated_row(report_date, "lead_account", "account", "星途汽车直播营销中心", "", "mtd_unique_leads", "累计唯一线索", "8221", "14850", "0.5536", "条"),
        _dated_row(report_date, "lead_account", "account", "星途汽车直播营销中心", "", "mtd_deals", "累计实销", "24", "50", "0.48", "台"),
        _dated_row(report_date, "lead_account", "account", "星途汽车直播营销中心", "", "mtd_spend", "累计线索费用", "338841.04", "453001.13", "", "元"),
        _dated_row(report_date, "lead_account", "account", "星途汽车直播营销中心", "", "mtd_cpl", "实际 CPL", "41.22", "55", "", "元/条"),
        _dated_row(report_date, "lead_account", "account", "星途汽车直播营销中心", "", "mtd_cps", "实际 CPS", "14118.38", "9060.02", "", "元/台"),
        _dated_row(report_date, "lead_anchor", "anchor", "徐欣悦", "星途汽车直播营销中心", "mtd_unique_leads", "累计唯一线索", "2941", "3713", "0.7921", "条"),
        _dated_row(report_date, "lead_anchor", "anchor", "徐欣悦", "星途汽车直播营销中心", "mtd_deals", "累计实销", "12", "13", "0.9231", "台"),
        _dated_row(report_date, "lead_anchor", "anchor", "徐欣悦", "星途汽车直播营销中心", "mtd_spend", "累计线索费用", "120921.25", "113250.28", "", "元"),
        _dated_row(report_date, "lead_anchor", "anchor", "徐欣悦", "星途汽车直播营销中心", "mtd_cpl", "实际 CPL", "41.12", "55", "", "元/条"),
        _dated_row(report_date, "lead_anchor", "anchor", "徐欣悦", "星途汽车直播营销中心", "mtd_cps", "实际 CPS", "10076.77", "9060", "", "元/台"),
    ]


def _minimal_dashboard_source_rows(
    report_date: str,
    *,
    impressions: float = 0,
    leads: float = 0,
    orders: float | None = None,
    deals: float = 0,
    spend: float | None = 0,
    cpl: float | None = None,
    cps: float | None = None,
    include_spend: bool = True,
    accounts: list[dict[str, object]] | None = None,
    anchors: list[dict[str, object]] | None = None,
    seed_anchors: list[dict[str, object]] | None = None,
) -> list[dict[str, str]]:
    rows = [
        _dated_row(report_date, "topline", "department", "全量", "", "impressions", "曝光", str(impressions), "", "", "次"),
        _dated_row(report_date, "topline", "department", "全量", "", "mtd_unique_leads", "累计唯一线索", str(leads), "", "", "条"),
        _dated_row(report_date, "topline", "department", "全量", "", "mtd_deals", "累计实销", str(deals), "", "", "台"),
    ]
    if orders is not None:
        rows.append(_dated_row(report_date, "topline", "department", "全量", "", "mtd_douyin_laike_orders", "抖音-来客线索（手机号去重）", str(orders), "", "", "条"))
    if include_spend:
        rows.append(_dated_row(report_date, "topline", "department", "全量", "", "mtd_spend", "累计线索费用", "" if spend is None else str(spend), "", "", "元"))
    if cpl is not None:
        rows.append(_dated_row(report_date, "topline", "department", "全量", "", "mtd_cpl", "总体 CPL", str(cpl), "", "", "元/条"))
    if cps is not None:
        rows.append(_dated_row(report_date, "topline", "department", "全量", "", "mtd_cps", "总体 CPS", str(cps), "", "", "元/台"))
    for account in accounts or []:
        name = str(account["name"])
        rows.extend(
            [
                _dated_row(report_date, "lead_account", "account", name, "", "mtd_unique_leads", "累计唯一线索", str(account.get("leads", 0)), "", "", "条"),
                _dated_row(report_date, "lead_account", "account", name, "", "mtd_deals", "累计实销", str(account.get("deals", 0)), "", "", "台"),
                _dated_row(report_date, "lead_account", "account", name, "", "mtd_spend", "累计线索费用", str(account.get("spend", 0)), "", "", "元"),
                _dated_row(report_date, "lead_account", "account", name, "", "mtd_cpl", "实际 CPL", str(account.get("cpl", 0)), "", "", "元/条"),
                _dated_row(report_date, "lead_account", "account", name, "", "mtd_cps", "实际 CPS", str(account.get("cps", 0)), "", "", "元/台"),
            ]
        )
    for anchor in anchors or []:
        name = str(anchor["name"])
        parent_scope = str(anchor.get("parent_scope", ""))
        rows.extend(
            [
                _dated_row(report_date, "lead_anchor", "anchor", name, parent_scope, "mtd_unique_leads", "累计唯一线索", str(anchor.get("leads", 0)), "", "", "条"),
                _dated_row(report_date, "lead_anchor", "anchor", name, parent_scope, "mtd_deals", "累计实销", str(anchor.get("deals", 0)), "", "", "台"),
                _dated_row(report_date, "lead_anchor", "anchor", name, parent_scope, "mtd_spend", "累计线索费用", str(anchor.get("spend", 0)), "", "", "元"),
                _dated_row(report_date, "lead_anchor", "anchor", name, parent_scope, "mtd_cpl", "实际 CPL", str(anchor.get("cpl", 0)), "", "", "元/条"),
                _dated_row(report_date, "lead_anchor", "anchor", name, parent_scope, "mtd_cps", "实际 CPS", str(anchor.get("cps", 0)), "", "", "元/台"),
            ]
        )
    for anchor in seed_anchors or []:
        name = str(anchor["name"])
        parent_scope = str(anchor.get("parent_scope", ""))
        rows.extend(
            [
                _dated_row(report_date, "seed_anchor", "anchor", name, parent_scope, "daily_impressions", "当日曝光", str(anchor.get("daily_impressions", 0)), "", "", "次"),
                _dated_row(report_date, "seed_anchor", "anchor", name, parent_scope, "mtd_impressions", "累计曝光", str(anchor.get("mtd_impressions", 0)), "", "", "次"),
            ]
        )
    return rows


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
    columns = ["线索ID", "创建日期", "创建时间", "到店日期", "到店时间", "首次意向车型", "意向车型", "试驾车型", "下订车型", "成交车型", "渠道2", "渠道3", "手机号"]
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
