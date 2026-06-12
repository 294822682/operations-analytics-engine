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
from oae.exports.feishu_report import _build_visit_dashboard_source_tables
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
    assert _find(rows, "topline", "department", "全量", "mtd_unique_leads")["actual"] == "22361"
    assert _find(rows, "topline", "department", "全量", "mtd_unique_leads")["metric_name"] == "风车线索（去重）"
    assert _find(rows, "topline", "department", "全量", "mtd_deals")["actual"] == "69"
    assert _find(rows, "topline", "department", "全量", "mtd_spend")["actual"] == "784252.48"
    assert _find(rows, "topline", "department", "全量", "mtd_cpl")["actual"] == "35.07"
    assert _find(rows, "topline", "department", "全量", "mtd_cps")["actual"] == "11365.98"
    assert _find(rows, "topline", "department", "全量", "mtd_cpl")["source_column"] == "账号层（母集）.线索组汇总.实际CPL"

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


def test_build_dashboard_source_rows_keeps_blank_summary_unit_costs_from_topline_fallback() -> None:
    account_table = _release_account_table()
    summary_mask = account_table["账号"].astype(str).eq("线索组汇总")
    account_table.loc[summary_mask, "累计线索"] = "0"
    account_table.loc[summary_mask, "实际CPL"] = ""

    rows = build_dashboard_source_rows(
        report_date="2026-05-01",
        topline_summary=_release_topline_summary(),
        account_table=account_table,
        anchor_table=_release_anchor_table(),
    )

    cpl = _find(rows, "topline", "department", "全量", "mtd_cpl")
    assert cpl["actual"] == ""
    assert cpl["target"] == "30.51"
    assert cpl["source_column"] == "账号层（母集）.线索组汇总.实际CPL"


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
    assert topline_laike["metric_name"] == "抖音来客订单（去重）"
    assert topline_laike["unit"] == "条"
    assert _find(rows, "lead_account", "account", "星途汽车直播营销中心", "mtd_douyin_laike_orders")["actual"] == "38"
    assert _find(rows, "lead_anchor", "anchor", "徐欣悦", "mtd_douyin_laike_orders")["actual"] == "20"
    assert _find(rows, "seed_account", "account", "EXEED星途", "mtd_impressions")["actual"] == "7221796"
    assert _find(rows, "seed_anchor", "anchor", "桂婕", "daily_impressions")["actual"] == "0"
    assert _find(rows, "lead_quality", "department", "全量", "raw_leads")["actual"] == "24759"
    assert _find(rows, "lead_quality", "department", "全量", "unique_rate")["actual"] == "0.9161"
    assert _find(rows, "lead_quality", "department", "全量", "manual_affected_rows")["actual"] == "19"


def test_build_dashboard_source_rows_includes_visit_contract_rows_when_supplied() -> None:
    rows = build_dashboard_source_rows(
        report_date="2026-05-28",
        topline_summary=_release_topline_summary(),
        account_table=_release_account_table_with_orders(),
        anchor_table=_release_anchor_table_with_orders(),
        visit_account_table=_release_visit_account_table(),
        visit_anchor_table=_release_visit_anchor_table(),
    )

    account_visits = _find(rows, "lead_account", "account", "星途汽车直播营销中心", "visits")
    assert account_visits["actual"] == "12"
    assert account_visits["metric_name"] == "到店数"
    assert account_visits["unit"] == "条"
    assert account_visits["source_column"] == "到店补充.星途汽车直播营销中心.到店数"
    assert _find(rows, "lead_account", "account", "星途汽车直播营销中心", "visit_deals")["actual"] == "6"
    assert _find(rows, "lead_account", "account", "星途汽车直播营销中心", "visit_rate")["actual"] == "0.24"
    assert _find(rows, "lead_account", "account", "星途汽车直播营销中心", "visit_deal_rate")["actual"] == "0.5"

    anchor_visits = _find(rows, "lead_anchor", "anchor", "徐欣悦", "visits")
    assert anchor_visits["actual"] == "3"
    assert anchor_visits["parent_scope"] == "星途汽车直播营销中心"
    assert _find(rows, "lead_anchor", "anchor", "徐欣悦", "visit_deals")["actual"] == "2"
    assert _find(rows, "lead_anchor", "anchor", "徐欣悦", "visit_rate")["actual"] == "0.15"
    assert _find(rows, "lead_anchor", "anchor", "徐欣悦", "visit_deal_rate")["actual"] == "0.6667"


def test_visit_account_rows_use_dashboard_account_names_and_include_summary() -> None:
    account_table = _release_account_table_with_orders()
    account_table.loc[0, "累计线索"] = "50"
    account_table.loc[0, "累计实销"] = "6"
    account_table.loc[1, "累计线索"] = "50"
    account_table.loc[1, "累计实销"] = "6"
    visit_account_table = pd.DataFrame(
        [
            {
                "账号": "抖音-星途汽车直播营销中心",
                "到店数": "12",
                "到店成交数": "6",
                "到店率": "24.00%",
                "到店成交率": "50.00%",
            },
            {
                "账号": "快手-EXEED星途",
                "到店数": "7",
                "到店成交数": "1",
                "到店率": "10.00%",
                "到店成交率": "20.00%",
            },
        ]
    )

    rows = build_dashboard_source_rows(
        report_date="2026-05-28",
        topline_summary=_release_topline_summary(),
        account_table=account_table,
        anchor_table=_release_anchor_table_with_orders(),
        visit_account_table=visit_account_table,
    )

    account_visits = _find(rows, "lead_account", "account", "星途汽车直播营销中心", "visits")
    assert account_visits["actual"] == "12"
    assert account_visits["source_column"] == "到店补充.星途汽车直播营销中心.到店数"
    assert _find(rows, "lead_account", "account", "星途汽车直播营销中心", "visit_deals")["actual"] == "6"
    assert _find(rows, "lead_account", "account", "星途汽车直播营销中心", "visit_rate")["actual"] == "0.24"
    assert _find(rows, "lead_account", "account", "星途汽车直播营销中心", "visit_deal_rate")["actual"] == "0.5"
    assert _maybe_find(rows, "lead_account", "account", "抖音-星途汽车直播营销中心", "visits") is None
    assert _maybe_find(rows, "lead_account", "account", "快手-EXEED星途", "visits") is None
    assert _find(rows, "lead_account", "account", "线索组汇总", "visits")["actual"] == "12"
    assert _find(rows, "lead_account", "account", "线索组汇总", "visit_deals")["actual"] == "6"
    assert _find(rows, "lead_account", "account", "线索组汇总", "visit_rate")["actual"] == "0.24"
    assert _find(rows, "lead_account", "account", "线索组汇总", "visit_deal_rate")["actual"] == "0.5"


def test_visit_dashboard_source_tables_use_same_lead_visit_and_raw_deal_intersection() -> None:
    fact = pd.DataFrame(
        [
            _visit_fact_row("L1", "2026-05-29", account="抖音-星途汽车官方直播间"),
            _visit_fact_row("L2", "2026-05-29", account="抖音-星途汽车官方直播间"),
            _visit_fact_row("L3", "2026-05-29", account="抖音-星途汽车官方直播间"),
        ]
    )
    leads_source = pd.DataFrame(
        [
            {"线索ID": "L1", "创建日期": "2026-05-29", "到店日期": "2026-05-29", "成交车型": "TXL"},
            {"线索ID": "L2", "创建日期": "2026-05-29", "到店日期": "", "成交车型": "TXL"},
            {"线索ID": "L3", "创建日期": "2026-05-29", "到店日期": "", "成交车型": "TXL"},
        ]
    )
    deals_source = pd.DataFrame(
        [
            {"线索ID": "L1", "下订日期": "2026-05-29", "成交车型": "TXL"},
            {"线索ID": "L3", "下订日期": "2026-05-29", "成交车型": "TXL"},
        ]
    )

    account_table, anchor_table = _build_visit_dashboard_source_tables(
        fact=fact,
        leads_source=leads_source,
        deals_source=deals_source,
        report_date=pd.Timestamp("2026-05-29"),
    )

    account = account_table[account_table["账号"].astype(str).eq("抖音-星途汽车官方直播间")].iloc[0]
    anchor = anchor_table[anchor_table["主播"].astype(str).eq("丁俐佳")].iloc[0]
    assert account["到店数"] == 1.0
    assert account["到店成交数"] == 1.0
    assert account["到店率"] == 1 / 3
    assert account["到店成交率"] == 1.0
    assert anchor["到店数"] == 1.0
    assert anchor["到店成交数"] == 1.0
    assert anchor["到店成交率"] == 1.0


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


def _release_visit_account_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "账号": "星途汽车直播营销中心",
                "到店数": "12",
                "到店成交数": "6",
                "到店率": "24.00%",
                "到店成交率": "50.00%",
            }
        ]
    )


def _release_visit_anchor_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "主播": "徐欣悦",
                "归属账号": "星途汽车直播营销中心",
                "到店数": "3",
                "到店成交数": "2",
                "到店率": "15.00%",
                "到店成交率": "66.67%",
            }
        ]
    )


def _visit_fact_row(lead_id: str, date: str, *, account: str, host: str = "丁俐佳") -> dict[str, str]:
    return {
        "线索ID": lead_id,
        "线索ID_norm": lead_id,
        "手机号": f"137{abs(hash(lead_id)) % 100000000:08d}",
        "线索创建时间": f"{date} 10:00:00",
        "date": date,
        "标准账号": account,
        "本场主播": host,
        "订单状态": "",
        "成交时间": "",
        "is_order": "0",
        "is_deal": "0",
        "成交车型": "TXL",
        "is_perf_lead_scope": "1",
    }


def _find(
    rows: list[dict[str, str]],
    source_table: str,
    scope_type: str,
    scope_name: str,
    metric_key: str,
) -> dict[str, str]:
    row = _maybe_find(rows, source_table, scope_type, scope_name, metric_key)
    if row is not None:
        return row
    raise AssertionError(f"missing row: {source_table}/{scope_type}/{scope_name}/{metric_key}")


def _maybe_find(
    rows: list[dict[str, str]],
    source_table: str,
    scope_type: str,
    scope_name: str,
    metric_key: str,
) -> dict[str, str] | None:
    for row in rows:
        if (
            row["source_table"] == source_table
            and row["scope_type"] == scope_type
            and row["scope_name"] == scope_name
            and row["metric_key"] == metric_key
        ):
            return row
    return None
