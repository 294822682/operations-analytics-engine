from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path

import pandas as pd

from oae.contracts.models import RunMetadata
from oae.exports.feishu_dashboard_source import (
    DASHBOARD_SOURCE_COLUMNS,
    build_dashboard_source_rows,
    dashboard_source_tsv,
)
from oae.exports.feishu_manifest import write_feishu_manifests
from oae.exports.feishu_topline import FullAccountTopline, SegmentTopline, ToplineSummary


def test_build_dashboard_source_rows_preserves_release_metrics_for_trends_contract() -> None:
    rows = build_dashboard_source_rows(
        report_date="2026-05-28",
        topline_summary=_release_topline_summary(),
        account_table=_release_account_table(),
        anchor_table=_release_anchor_table(),
    )

    assert rows
    assert set(rows[0]) == set(DASHBOARD_SOURCE_COLUMNS)
    assert _find(rows, "topline", "department", "全量", "impressions")["actual"] == "24286300"
    assert _find(rows, "topline", "department", "全量", "mtd_spend")["actual"] == "784252.48"
    assert _find(rows, "topline", "department", "全量", "mtd_cpl")["actual"] == "34.58"
    assert _find(rows, "topline", "department", "全量", "mtd_cps")["actual"] == "8618.16"

    account_cpl = _find(rows, "lead_account", "account", "星途汽车直播营销中心", "mtd_cpl")
    assert account_cpl["actual"] == "41.22"
    assert account_cpl["target"] == "55"

    anchor_spend = _find(rows, "lead_anchor", "anchor", "徐欣悦", "mtd_spend")
    assert anchor_spend["actual"] == "120921.25"
    assert anchor_spend["parent_scope"] == "星途汽车直播营销中心"


def test_dashboard_source_tsv_uses_stable_header_and_utf8_tsv_roundtrip() -> None:
    rows = build_dashboard_source_rows(
        report_date="2026-05-28",
        topline_summary=_release_topline_summary(),
        account_table=_release_account_table(),
        anchor_table=_release_anchor_table(),
    )

    text = dashboard_source_tsv(rows)
    parsed = list(csv.DictReader(StringIO(text), delimiter="\t"))

    assert text.splitlines()[0].split("\t") == DASHBOARD_SOURCE_COLUMNS
    assert len(parsed) == len(rows)
    assert parsed[0]["report_date"] == "2026-05-28"
    assert _find(parsed, "lead_anchor", "anchor", "徐欣悦", "mtd_cps")["actual"] == "10076.77"


def test_build_dashboard_source_rows_includes_seed_quality_and_order_contract_rows() -> None:
    rows = build_dashboard_source_rows(
        report_date="2026-05-28",
        topline_summary=_release_topline_summary(),
        account_table=_release_account_table_with_orders(),
        anchor_table=_release_anchor_table_with_orders(),
        seed_account_table=_release_seed_account_table(),
        seed_anchor_table=_release_seed_anchor_table(),
        lead_quality_line="线索质量：原始线索（去重前）24759，唯一线索（去重后）22682，唯一率 91.61%；无主线索 35；人工确认归属 12 条，影响样本 19 行。",
    )

    topline_laike = _find(rows, "topline", "department", "全量", "mtd_douyin_laike_orders")
    assert topline_laike["actual"] == "90"
    assert topline_laike["metric_name"] == "抖音-来客线索（手机号去重）"
    assert topline_laike["unit"] == "条"
    assert _find(rows, "lead_account", "account", "星途汽车直播营销中心", "mtd_douyin_laike_orders")["actual"] == "38"
    assert _find(rows, "lead_anchor", "anchor", "徐欣悦", "mtd_douyin_laike_orders")["actual"] == "20"
    assert _find(rows, "seed_account", "account", "EXEED星途", "mtd_impressions")["actual"] == "7221796"
    assert _find(rows, "seed_anchor", "anchor", "桂婕", "daily_impressions")["actual"] == "0"
    assert _find(rows, "lead_quality", "department", "全量", "raw_leads")["actual"] == "24759"
    assert _find(rows, "lead_quality", "department", "全量", "unique_rate")["actual"] == "0.9161"
    assert _find(rows, "lead_quality", "department", "全量", "manual_affected_rows")["actual"] == "19"


def test_write_feishu_manifests_includes_dashboard_source_contract(tmp_path: Path) -> None:
    export_dir = tmp_path / "artifacts" / "exports"
    md_path = tmp_path / "output" / "sql_reports" / "feishu_report_latest_2026-05-28.md"
    tsv_path = tmp_path / "output" / "sql_reports" / "feishu_table_latest_2026-05-28.tsv"
    dashboard_source_path = tmp_path / "output" / "sql_reports" / "feishu_dashboard_source_latest_2026-05-28.tsv"
    for path in [md_path, tsv_path, dashboard_source_path]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture", encoding="utf-8")

    write_feishu_manifests(
        export_dir=export_dir,
        report_date="2026-05-28",
        metadata=RunMetadata(
            run_id="run-20260530T083218Z",
            schema_version="schema",
            metric_version="metric",
            template_version="template",
            freeze_id="freeze",
        ),
        row_count=50,
        snapshot_path=None,
        ledger_path=None,
        analysis_snapshot_path=None,
        fact_path=tmp_path / "output" / "fact_attribution.csv",
        md_path=md_path,
        tsv_path=tsv_path,
        dashboard_source_path=dashboard_source_path,
        dashboard_source_row_count=42,
    )

    manifest = json.loads((export_dir / "feishu_dashboard_source_latest_2026-05-28.manifest.json").read_text(encoding="utf-8"))
    assert manifest["export_name"] == "feishu_dashboard_source_latest"
    assert manifest["consumer"] == "dashboard_daily_trends_api"
    assert manifest["row_count"] == 42
    assert manifest["output_path"] == str(dashboard_source_path)


def _release_topline_summary() -> ToplineSummary:
    return ToplineSummary(
        full_account=FullAccountTopline(
            impression_target=20_000_000,
            impression_actual=24_286_300,
            impression_attain=1.214315,
            lead_target=25_333,
            lead_actual=22_682,
            lead_attain=0.8954,
            deal_target=114,
            deal_actual=91,
            deal_attain=0.7982,
            cpl_target=53,
            cpl_actual=34.58,
            cps_target=6267,
            cps_actual=8618.16,
            pending_day=0,
            pending_cumulative=23,
        ),
        excluding_ex7=SegmentTopline(label="不含 EX7", leads=11707, deals=57, cpl_actual=38.64, cps_actual=7936.15),
        ex7=SegmentTopline(label="EX7 专项", leads=10975, deals=34, cpl_actual=30.24, cps_actual=9761.53),
    )


def _release_account_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "账号": "星途汽车直播营销中心",
                "当日线索": "250",
                "当日线索目标": "1719.75",
                "当日线索达成率": "14.54%",
                "累计线索": "8221.00",
                "线索月目标": "14850",
                "累计线索达成率": "55.36%",
                "当日实销": "1",
                "当日实销目标": "6.75",
                "当日实销达成率": "14.81%",
                "累计实销": "24",
                "实销月目标": "50.00",
                "累计实销达成率": "48.00%",
                "线索费用月目标": "453001.13",
                "CPL目标": "55",
                "CPS目标": "9060.02",
                "累计线索费用": "338841.04",
                "实际CPL": "41.22",
                "实际CPS": "14118.38",
            },
            {
                "账号": "线索组汇总",
                "当日线索": "250",
                "当日线索目标": "1897.25",
                "当日线索达成率": "13.18%",
                "累计线索": "22361.00",
                "线索月目标": "29700",
                "累计线索达成率": "75.29%",
                "当日实销": "1",
                "当日实销目标": "8.00",
                "当日实销达成率": "12.50%",
                "累计实销": "69",
                "实销月目标": "100",
                "累计实销达成率": "69.00%",
                "线索费用月目标": "906002.26",
                "CPL目标": "30.51",
                "CPS目标": "9060.02",
                "累计线索费用": "784252.48",
                "实际CPL": "35.07",
                "实际CPS": "11365.98",
            },
        ]
    )


def _release_anchor_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "主播": "徐欣悦",
                "归属账号": "星途汽车直播营销中心",
                "当日线索": "48.00",
                "当日线索目标": "205.00",
                "当日线索达成率": "23.41%",
                "累计线索": "2941.00",
                "线索月目标": "3713",
                "累计线索达成率": "79.21%",
                "当日实销": "1.00",
                "当日实销目标": "0.50",
                "当日实销达成率": "200.00%",
                "累计实销": "12.00",
                "实销月目标": "13",
                "累计实销达成率": "92.31%",
                "单人线索费用目标": "113250.28",
                "单人CPL目标": "55",
                "单人CPS目标": "9060",
                "累计线索费用": "120921.25",
                "实际CPL": "41.12",
                "实际CPS": "10076.77",
            }
        ]
    )


def _release_account_table_with_orders() -> pd.DataFrame:
    table = _release_account_table().copy()
    table["抖音-来客线索数（手机号去重）"] = ["38", "90"]
    table["来客线索KPI目标"] = ["60", "300"]
    table["来客线索KPI完成率"] = ["63.33%", "30.00%"]
    return table


def _release_anchor_table_with_orders() -> pd.DataFrame:
    table = _release_anchor_table().copy()
    table["抖音-来客线索数（手机号去重）"] = ["20"]
    table["来客线索KPI目标"] = ["37.5"]
    table["来客线索KPI完成率"] = ["53.33%"]
    return table


def _release_seed_account_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "账号": "EXEED星途",
                "当日曝光": "0",
                "当日曝光目标": "55790.22",
                "当日曝光达成率": "0.00%",
                "累计曝光": "7221796",
                "曝光目标": "20000000",
                "累计曝光达成率": "36.11%",
            }
        ]
    )


def _release_seed_anchor_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "主播": "桂婕",
                "归属账号": "EXEED星途",
                "当日曝光": "0",
                "当日曝光目标": "55790.22",
                "当日曝光达成率": "0.00%",
                "累计曝光": "2995776",
                "曝光目标": "4000000",
                "累计曝光达成率": "74.89%",
            }
        ]
    )


def _find(
    rows: list[dict[str, str]],
    source_table: str,
    scope_type: str,
    scope_name: str,
    metric_key: str,
) -> dict[str, str]:
    for row in rows:
        if (
            row["source_table"] == source_table
            and row["scope_type"] == scope_type
            and row["scope_name"] == scope_name
            and row["metric_key"] == metric_key
        ):
            return row
    raise AssertionError(f"missing row: {source_table}/{scope_type}/{scope_name}/{metric_key}")
