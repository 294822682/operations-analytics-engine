from __future__ import annotations

from pathlib import Path

import pandas as pd

from oae.exports.feishu_dashboard_visual import (
    DASHBOARD_PAGE_SPECS,
    render_dashboard_long_compact_svg,
    render_dashboard_page_svgs,
    write_dashboard_visual_long_compact_files,
)


def test_long_compact_dashboard_uses_fixed_pages_without_ex7_comparison() -> None:
    table = _dashboard_source_frame()

    pages = render_dashboard_page_svgs(table, run_id="run-test")
    svg = render_dashboard_long_compact_svg(table, run_id="run-test")

    assert [spec.page_id for spec in DASHBOARD_PAGE_SPECS] == [
        "p1_overview",
        "p2_lead_detail",
        "p3_seed_detail",
        "p5_account_channel",
    ]
    assert list(pages) == [spec.page_id for spec in DASHBOARD_PAGE_SPECS]
    assert "height='4540'" in svg
    assert "P1 总览驾驶舱" in svg
    assert "P2 线索组明细" in svg
    assert "P3 种草组明细" in svg
    assert "P4 账号拆解" in svg
    assert "来客订单拆解" in svg
    assert "种草账号" in svg
    assert "种草主播" in svg
    for removed in ["EX7 / 不含", "车型对比", "P4 车型", "车型/EX7"]:
        assert removed not in svg


def test_write_long_compact_dashboard_visual_outputs_formal_latest_svg(tmp_path: Path) -> None:
    svg_path = tmp_path / "feishu_dashboard_visual_p1_p5_long_compact_latest_2026-06-05.svg"

    written = write_dashboard_visual_long_compact_files(
        _dashboard_source_frame(),
        svg_path=svg_path,
        png_path=None,
        run_id="run-test",
    )

    assert written == {"svg": svg_path}
    svg = svg_path.read_text(encoding="utf-8")
    assert svg.startswith("<svg")
    assert "feishu_dashboard_source_latest_2026-06-05.tsv" in svg
    assert "P4 账号拆解" in svg


def _dashboard_source_frame() -> pd.DataFrame:
    rows = [
        _row("topline", "department", "全量", "", "impressions", "曝光", 1_250_000, 25_000_000, 0.05),
        _row("topline", "department", "全量", "", "mtd_unique_leads", "唯一线索", 138, 0, 0),
        _row("topline", "department", "全量", "", "mtd_douyin_laike_orders", "抖音-来客订单", 19, 1000, 0.019),
        _row("topline", "department", "全量", "", "mtd_deals", "实销", 16, 100, 0.16),
        _row("topline", "department", "全量", "", "mtd_spend", "累计消耗", 0, 0, 0),
        _row("topline", "department", "全量", "", "mtd_cpl", "总体 CPL", 0, 0, 0),
        _row("topline", "department", "全量", "", "mtd_cps", "总体 CPS", 0, 1500, 0),
        _row("topline", "department", "全量", "", "pending_day", "待交车（当日）", 1, "", ""),
        _row("topline", "department", "全量", "", "pending_cumulative", "待交车（累计）", 3, "", ""),
        _row("lead_quality", "department", "全量", "", "raw_leads", "原始线索", 11, "", ""),
        _row("lead_quality", "department", "全量", "", "unique_rate", "唯一率", 12.5455, "", ""),
        _row("lead_quality", "department", "全量", "", "unowned_leads", "无主线索", 112, "", ""),
        _row("lead_quality", "department", "全量", "", "manual_overrides", "人工归属", 5, "", ""),
        _row("lead_account", "account", "星途汽车官方直播间", "", "daily_leads", "当日线索", 7, "", ""),
        _row("lead_account", "account", "星途汽车官方直播间", "", "mtd_unique_leads", "累计唯一线索", 42, 0, 0),
        _row("lead_account", "account", "星途汽车官方直播间", "", "mtd_douyin_laike_orders", "抖音-来客订单", 15, 1000, 0.015),
        _row("lead_account", "account", "星途汽车官方直播间", "", "mtd_deals", "累计实销", 9, 100, 0.09),
        _row("lead_account", "account", "星途汽车直播营销中心", "", "daily_leads", "当日线索", 1, "", ""),
        _row("lead_account", "account", "星途汽车直播营销中心", "", "mtd_unique_leads", "累计唯一线索", 20, 0, 0),
        _row("lead_account", "account", "星途汽车直播营销中心", "", "mtd_douyin_laike_orders", "抖音-来客订单", 4, 1000, 0.004),
        _row("lead_account", "account", "星途汽车直播营销中心", "", "mtd_deals", "累计实销", 4, 100, 0.04),
        _row("lead_account", "account", "线索组汇总", "", "daily_leads", "当日线索", 8, "", ""),
        _row("lead_account", "account", "线索组汇总", "", "mtd_unique_leads", "累计唯一线索", 64, 0, 0),
        _row("lead_account", "account", "线索组汇总", "", "mtd_douyin_laike_orders", "抖音-来客订单", 19, 1000, 0.019),
        _row("lead_account", "account", "线索组汇总", "", "mtd_deals", "累计实销", 13, 100, 0.13),
        _row("lead_anchor", "anchor", "丁俐佳", "当日未开播", "mtd_unique_leads", "累计唯一线索", 4, 0, 0),
        _row("lead_anchor", "anchor", "丁俐佳", "当日未开播", "mtd_douyin_laike_orders", "抖音-来客订单", 4, 167, 0.024),
        _row("lead_anchor", "anchor", "何雯", "当日未开播", "mtd_unique_leads", "累计唯一线索", 5, 0, 0),
        _row("lead_anchor", "anchor", "何雯", "当日未开播", "mtd_douyin_laike_orders", "抖音-来客订单", 5, 167, 0.0299),
        _row("seed_account", "account", "EXEED星途", "", "daily_impressions", "当日曝光", 0, 0, 0),
        _row("seed_account", "account", "EXEED星途", "", "mtd_impressions", "累计曝光", 967_200, 25_000_000, 0.0387),
        _row("seed_anchor", "anchor", "刘花旗", "EXEED星途", "daily_impressions", "当日曝光", 0, 35714, 0),
        _row("seed_anchor", "anchor", "刘花旗", "EXEED星途", "mtd_impressions", "累计曝光", 263_000, 357_140, 0.0736),
        _row("seed_anchor", "anchor", "桂婕", "EXEED星途", "daily_impressions", "当日曝光", 0, 35714, 0),
        _row("seed_anchor", "anchor", "桂婕", "EXEED星途", "mtd_impressions", "累计曝光", 121_600, 357_140, 0.0341),
    ]
    return pd.DataFrame(rows)


def _row(
    source_table: str,
    scope_type: str,
    scope_name: str,
    parent_scope: str,
    metric_key: str,
    metric_name: str,
    actual: object,
    target: object,
    attain_rate: object,
) -> dict[str, object]:
    return {
        "report_date": "2026-06-05",
        "source_table": source_table,
        "scope_type": scope_type,
        "scope_name": scope_name,
        "parent_scope": parent_scope,
        "metric_key": metric_key,
        "metric_name": metric_name,
        "actual": actual,
        "target": target,
        "attain_rate": attain_rate,
        "unit": "",
        "source_column": "",
        "sort_order": 0,
    }
