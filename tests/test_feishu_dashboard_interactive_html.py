from __future__ import annotations

import html as html_lib
from pathlib import Path
import re

import oae.exports.feishu_dashboard_interactive_html as dashboard_html
from oae.exports.feishu_dashboard_interactive_html import (
    DashboardSource,
    render_api_connected_dashboard_html,
    render_feishu_link_trial_dashboard_html,
    render_interactive_dashboard_html,
    write_api_connected_dashboard_html,
    write_interactive_dashboard_html,
)


def test_render_interactive_dashboard_html_contains_required_modules_and_controls() -> None:
    source = DashboardSource(_sample_rows())

    html = render_interactive_dashboard_html(source, source_label="fixture.tsv")

    assert "日报可交互 BI 原型 · 2026-05-14" in html
    assert 'id="overview"' in html
    assert 'id="funnel"' in html
    assert 'id="lead-anchors"' in html
    assert 'id="seed-exposure"' in html
    assert "总览 KPI" in html
    assert "全链路转化" in html
    assert "线索组主播唯一线索与来客线索 KPI" in html
    assert "种草曝光累计达成" in html
    assert "2,188.63万" in html
    assert "13,323" in html
    assert "未提供" in html
    assert "无目标" not in html
    assert "0 / 21" in html
    assert "0.00万" in html
    assert "来客线索" in html
    assert "按唯一线索排序" in html
    assert "按来客线索排序" in html
    assert "按累计曝光排序" in html
    assert 'data-sort-table="lead-anchor-table"' in html
    assert 'data-sort-table="seed-anchor-table"' in html
    assert "function sortTable(" in html
    assert "data-tooltip=" in html
    assert "prototype_only" in html
    assert "EX7" not in html


def test_write_interactive_dashboard_html_uses_non_formal_output_path(tmp_path: Path) -> None:
    source_path = tmp_path / "dashboard.tsv"
    output_path = tmp_path / "preview.html"
    _write_tsv(source_path, _sample_rows())

    written = write_interactive_dashboard_html(source_tsv=source_path, output_html=output_path)

    assert written == output_path
    html = output_path.read_text(encoding="utf-8")
    assert "dashboard.tsv" in html
    assert 'id="lead-anchor-table"' in html
    assert 'id="seed-anchor-table"' in html


def test_render_api_connected_dashboard_html_fetches_daily_dashboard_api() -> None:
    html = render_api_connected_dashboard_html("2026-05-14")

    assert "日报可交互 BI 原型 · 2026-05-14" in html
    assert 'data-api-path="/dashboard/daily/2026-05-14"' in html
    assert 'const API_PATH = "/dashboard/daily/2026-05-14"' in html
    assert "fetchDashboard(path)" in html
    assert 'method: "GET"' in html
    assert 'id="report-date-select"' in html
    assert "populateDateSelect(payload)" in html
    assert "dashboardPathForSelection(selected)" in html
    assert 'id="overview"' in html
    assert 'id="funnel"' in html
    assert 'id="lead-anchors"' in html
    assert 'id="seed-exposure"' in html
    assert "renderDashboard(payload)" in html
    assert "function sortTable(" in html
    assert "按来客线索排序" in html
    assert "按累计曝光排序" in html


def test_render_api_connected_dashboard_html_exposes_n7_read_only_bi_controls() -> None:
    html = render_api_connected_dashboard_html("latest", api_path="/dashboard/daily/latest")

    assert "N7 V0 READ-ONLY BI" in html
    assert "只读状态" in html
    assert 'id="report-date-value"' in html
    assert 'id="source-path-value"' in html
    assert 'id="source-rows-value"' in html
    assert 'id="lead-anchor-search"' in html
    assert 'data-filter-table="lead-anchor-table"' in html
    assert 'id="seed-anchor-search"' in html
    assert 'data-filter-table="seed-anchor-table"' in html
    assert "function renderSortControls(payload)" in html
    assert "payload.interactions?.lead_anchor_sort_keys" in html
    assert "payload.interactions?.seed_anchor_sort_keys" in html
    assert "function bindSearchControls()" in html
    assert 'method: "GET"' in html
    assert 'method: "POST"' not in html
    assert "/execution" not in html
    assert "/reports" not in html
    assert "daily_pipeline" not in html


def test_render_feishu_link_dashboard_html_exposes_business_workbench_without_engineering_metadata() -> None:
    html = render_feishu_link_trial_dashboard_html("latest", api_path="/dashboard/daily/latest")

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
    assert "主播贡献" in html
    assert "账号 / 渠道" in html
    assert "种草" in html
    assert "成本效率" in html
    assert "历史趋势" in html
    assert "月度对比" in html
    assert 'id="daily-bi-trends"' in html
    assert 'id="daily-bi-monthly-comparison"' in html
    assert 'const TREND_API_PATH = "/dashboard/daily/trends"' in html
    assert "loadDailyBiTrends(payload)" in html
    assert "daily-bi-history-grid history-chart-grid" in html
    assert "function dailyBiLineChart" in html
    assert "dailyBiHistoryPanel(series, previousByKey)" in html
    assert "function bindDailyBiChartInteractions" in html
    assert "monthly-card daily-bi-month-card" in html
    assert "daily-bi-month-metrics" in html
    assert "日报详细版" in html
    assert "范围查询" in html
    assert 'id="business-range-query" action="/dashboard/daily/trends/prototype" method="get"' in html
    assert 'id="business-start-date" name="start_date" type="date" required' in html
    assert 'id="business-end-date" name="end_date" type="date" required' in html
    assert "三个月内任意时间段，单次查看上限 92 天。" in html
    assert "function bindBusinessRangeQuery(payload)" in html
    assert "businessTrendPrototypePath(startInput.value, endInput.value)" in html
    assert "单次查看范围建议不超过一个季度，请缩小日期范围。" in html
    assert "历史趋势 · dashboard source TSV" not in html
    assert "dashboard source TSV</div>" not in html
    assert 'IS_BUSINESS_MODE ? "" : `<span>${escapeHtml(step.key)}</span>`' in html
    assert 'const funnelConversion = IS_BUSINESS_MODE ? "" : `<div class="funnel-conversion">${escapeHtml(conversion)}</div>`;' in html
    assert 'const qualityStrip = IS_BUSINESS_MODE ? "" :' in html
    assert "未提供" in html
    assert 'fmtWan(metricValue(daily))' in html
    assert 'method: "GET"' in html
    assert 'method: "POST"' not in html
    assert 'function isDashboardReadOnlyPath(path)' in html
    assert 'path === "/dashboard/daily/latest"' in html
    assert '/^\\/dashboard\\/daily\\/\\d{4}-\\d{2}-\\d{2}$/' in html
    assert 'data-dashboard-mode="business"' in html
    assert 'class="metric-table lead-metric-table"' in html
    assert 'class="metric-table seed-metric-table"' in html
    assert 'class="bar-metric-cell"' in html
    assert "bar-track-cell" in html
    assert 'class="metric-value-cell"' in html
    assert 'class="metric-value-header"' in html
    assert "所属账号 / 直播间" in html
    assert "const spend = anchorMetric(anchor, \"mtd_spend\");" in html
    assert 'class="anchor-parent-cell"' in html
    assert "<th>到店数</th>" in html
    assert "<th>到店率</th>" in html
    assert '<th>费用</th>' in html
    assert "function metricProgressRate(metric)" in html
    assert "const progressRate = metricProgressRate(metric);" in html
    assert "线索进度" in html
    assert "曝光进度" in html
    assert 'class="bar-metric"' in html
    assert 'class="bar-value"' in html
    assert 'body[data-dashboard-mode="business"] .bar-metric' in html
    assert "N7 V0 READ-ONLY BI" not in html
    assert "车型 / EX7" not in html
    assert "EX7 / 不含 EX7 对比" not in html
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


def test_render_trend_dashboard_html_is_business_presentation_without_governance_blocks() -> None:
    render_trend_dashboard_html = getattr(dashboard_html, "render_trend_dashboard_html", None)
    assert render_trend_dashboard_html is not None
    html = render_trend_dashboard_html(api_path="/dashboard/daily/trends")

    assert "经营趋势看板" in html
    assert "查看近期核心经营指标变化、账号表现、主播表现与种草曝光情况，辅助日常经营复盘。" in html
    assert "N8 V1 · READ-ONLY FILE TREND" not in html
    assert "READ-ONLY FILE TREND" not in html
    assert "文件级趋势视图" not in html
    assert '<span>API</span>' not in html
    assert '<div class="source-pill"' not in html
    forbidden_terms = [
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
    ]
    for term in forbidden_terms:
        assert term not in html
    subjective_terms = [
        "待提升",
        "稳步推进",
        "表现较好",
        "表现优秀",
        "需关注",
        "风险",
        "落后",
        "异常",
        "较差",
        "达标",
        "未达标",
        "优秀",
        "警告",
        "预警",
    ]
    for term in subjective_terms:
        assert term not in html
    assert "当前范围" in html
    assert "查看天数" in html
    assert "自定义范围" in html
    assert "应用范围" in html
    assert "<span>日期范围</span>" not in html
    assert 'id="trend-start-date"' in html
    assert 'id="trend-end-date"' in html
    assert "本月" in html
    assert "上月" in html
    assert "近7天" in html
    assert "近15天" in html
    assert "近30天" in html
    assert "近三个月" in html
    assert "本季度" in html
    assert "上季度" in html
    assert 'id="trend-start-date" name="start_date" type="date" required' in html
    assert 'id="trend-end-date" name="end_date" type="date" required' in html
    assert "单次查看范围建议不超过一个季度，请缩小日期范围。" in html
    assert "核心经营表现" in html
    assert "历史趋势" in html
    assert "月度对比" in html
    assert "账号表现" in html
    assert "主播表现" in html
    assert "种草曝光表现" in html
    assert "指标说明" in html
    assert "本页仅供内部经营复盘参考，最终以原始日报与人工确认为准。" in html
    assert "真实 0 保持 0" in html
    assert "缺失值显示未提供" in html
    assert "缺失趋势点不补 0" in html
    assert "未提供" in html
    assert "车型结构对比" not in html
    assert "车型结构" not in html
    assert "EX7" not in html
    assert "到店" in html
    assert "字段未接入" not in html
    assert 'const DATA_URL = "/dashboard/daily/trends"' in html
    assert 'method: "GET"' in html
    assert 'method: "POST"' not in html
    assert 'function isTrendDataPath(path)' in html
    assert "renderTrendDashboard(payload)" in html
    assert "renderQualityAnnotations(payload)" not in html
    assert "daily_pipeline" not in html
    assert "/execution" not in html
    assert "/reports" not in html
    assert "tenant_access_token" not in html
    assert "access_token" not in html
    assert "Authorization" not in html
    assert "cookie" not in html.lower()
    assert "kpi-card" in html
    assert "account-card" in html
    assert "anchor-card" in html
    assert "seed-card" in html
    assert "progress-bar" in html
    assert "progress-fill" in html
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
    assert "<td></td>" not in html
    assert "<th>趋势</th>" not in html
    assert "point.value === null" in html
    assert "Number(point.value)" in html


def test_render_trend_dashboard_html_exposes_n12a_design_tokens_and_motion_rules() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for token in [
        "--color-bg",
        "--color-surface",
        "--color-border",
        "--color-text",
        "--color-series-current",
        "--color-series-previous",
        "--radius-card",
        "--shadow-tooltip",
    ]:
        assert token in html
    assert "--font-size-title" in html
    assert "--space-1" in html
    assert "--space-6" in html
    assert "@media (prefers-reduced-motion: reduce)" in html


def test_render_trend_dashboard_html_exposes_n12a_filter_toolbar_state() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert 'class="date-filter-panel trend-filter-toolbar"' in html
    assert 'id="trend-start-date" name="start_date" type="date" required value="2026-03-01"' in html
    assert 'id="trend-end-date" name="end_date" type="date" required value="2026-05-22"' in html
    assert "<span>常用范围</span>" in html
    assert "<span>自然周期</span>" in html
    assert "<span>扩展范围</span>" in html
    for label in ["近7天", "近15天", "近30天", "本月", "上月", "本季度", "上季度", "近三个月"]:
        assert label in html
    assert 'id="trend-active-range"' in html
    assert 'id="trend-range-days"' in html
    assert "当前范围：2026-03-01 至 2026-05-22" in html
    assert "查看天数：83 / 92" in html
    assert 'data-range-state="custom"' in html
    assert 'class="range-state-pill"' in html
    for term in ["可用日期", "缺失日期", "质量标注", "数据状态", "发布边界", "数据来源文件", "运行证据"]:
        assert term not in html


def test_render_trend_dashboard_html_exposes_n12a_kpi_card_copy_rules() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert 'const CORE_KPI_KEYS = ["impressions", "leads", "douyin_laike_orders", "deals", "spend", "cpl", "cps"];' in html
    assert "coreSummary.slice(0, 7)" in html
    assert 'data-kpi-key="${escapeHtml(metric?.key || "")}"' in html
    assert "费用" in html
    assert "目标参考：" in html
    assert 'return "未提供";' in html
    assert "当前 / 目标：未提供" not in html
    assert "成本比值" in html
    assert "达成率" not in html
    assert "metricProgressRate(metric)" in html
    assert "rateText(metric)" in html


def test_render_trend_dashboard_html_derives_cost_progress_when_source_rate_is_missing() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert "function metricProgressRate(metric)" in html
    assert "if (isCostMetric(metric)) return Number(metric.target) / Number(metric.actual);" in html
    assert "const progressRate = metricProgressRate(metric);" in html
    assert "Math.min(progressRate * 100, 100)" in html


def test_render_trend_dashboard_html_exposes_n12a_tooltip_focus_and_text_rules() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert "kpi-card-help" in html
    assert "chart-tooltip" in html
    assert 'tabindex="0"' in html
    assert ":focus-visible" in html
    assert "positionTooltip" in html
    assert "本期" in html
    assert "上一周期" in html
    assert "差值" in html
    assert "变化率" in html
    for forbidden in ["异常", "风险", "预警", "待提升", "表现较好", "稳步推进", "达标", "未达标", "质量标注", "数据状态", "发布边界", "dashboard source", "contract_version", "release_readiness", "publish-ready", "pipeline"]:
        assert forbidden not in html


def test_render_trend_dashboard_html_hides_kpi_tooltip_until_hover_or_focus() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert ".kpi-card-help" in html
    assert "visibility: hidden;" in html
    assert ".kpi-card:hover .kpi-card-help" in html
    assert ".kpi-card:focus-visible .kpi-card-help" in html
    assert "visibility: visible;" in html


def test_render_trend_dashboard_html_prefills_end_date_only_quarter_window() -> None:
    render_trend_dashboard_html = getattr(dashboard_html, "render_trend_dashboard_html", None)
    assert render_trend_dashboard_html is not None

    html = render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert 'const DATA_URL = "/dashboard/daily/trends?end_date=2026-05-22"' in html
    assert 'id="trend-start-date" name="start_date" type="date" required value="2026-03-01"' in html
    assert 'id="trend-end-date" name="end_date" type="date" required value="2026-05-22"' in html
    assert 'value="2026-05-26"' not in html


def test_render_trend_dashboard_html_avoids_visible_raw_seed_scope_and_target_duplication() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    visible_text = _visible_text(html)
    for raw_field in ["account", "anchor", "metric_type", "source_type"]:
        assert raw_field not in visible_text
    assert "entity?.parent_scope || entity?.type" not in html
    assert "seedScopeText(entity)" in html
    assert "目标参考 目标未提供" not in html
    assert "目标 目标未提供" not in html
    assert "目标参考：" in html


def test_render_trend_dashboard_html_chart_text_has_visible_separators() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert "范围：" in html
    assert "dateAxisLabel" in html
    assert "本期；上一周期：未提供" in html
    assert "本期；</span><span>上一周期：未提供" not in html
    for glued in [
        "05-0105-07",
        "本期上一周期数据不足",
        "成交趋势范围：",
        "线索趋势范围：",
        "曝光趋势范围：",
        "实销趋势范围：",
        "成交趋势2026",
        "线索趋势2026",
        "曝光趋势2026",
        "03-0103-22",
        "012 03-01",
        "023 03-01",
        "035 03-01",
        "本期上一周期",
    ]:
        assert glued not in html


def test_render_trend_dashboard_html_hides_chart_tooltips_until_hover_or_focus() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert ".chart-tooltip" in html
    assert ".chart-tooltip.is-visible" in html
    assert "visibility: hidden;" in html
    assert "visibility: visible;" in html
    assert 'role="status"></div>' in html


def test_render_trend_dashboard_html_lazy_renders_account_inactive_trend_pane() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert "data-account-trend-panel" in html
    assert "data-trend-leads" in html
    assert "data-trend-deals" in html
    assert "function renderAccountTrendPane" in html
    assert "panel.innerHTML = compactTrendChart" in html
    assert "data-account-trend-pane=" not in html
    assert 'data-account-trend-pane="leads"' not in html
    assert 'data-account-trend-pane="deals"' not in html


def test_render_trend_dashboard_html_separates_chart_text_for_runtime_text_content() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert '.join("\\n")' in html
    assert "<span>${escapeHtml(label)}</span>\n        <small>范围：" in html
    assert "本期</span>\n        <span><i class=\"legend-previous\"></i>上一周期" in html
    assert "<span>${escapeHtml(label)}</span><small>范围：" not in html
    assert "本期</span><span><i class=\"legend-previous\"></i>上一周期" not in html


def test_render_trend_dashboard_html_metric_group_keeps_raw_ratio_display_policy() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert "function metricDisplayText" in html
    assert "口径待确认" not in html
    assert 'item?.source_status === "scope_pending"' not in html
    assert 'item?.source_status === "not_connected"' in html
    assert 'return fmtMetricValue(item?.actual, item?.unit || "");' in html
    assert "部分比例可能超过 100%" in html


def test_render_trend_dashboard_html_exposes_n12b_history_trend_panel_structure() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        "trend-panel",
        "trend-panel-header",
        "trend-panel-title",
        "trend-panel-value",
        "trend-panel-meta",
        "trend-chart",
        "chart-grid",
        "chart-axis",
        "chart-legend",
        "chart-tooltip",
    ]:
        assert marker in html
    for label in ["曝光", "线索", "实销", "费用", "CPL", "CPS"]:
        assert label in html
    assert "本期" in html
    assert "上一周期：未提供" in html
    assert "hasPreviousValues" in html
    assert "范围：" in html
    assert "point.value === null" in html
    assert "日期：" in html
    assert "指标：" in html
    assert "本期值：" in html
    assert "上一周期值：" in html
    assert "差值：" in html
    assert "变化率：" in html
    assert "说明：" in html


def test_render_trend_dashboard_html_exposes_n12b_monthly_matrix_and_coverage_rules() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        "monthly-comparison",
        "monthly-matrix",
        "monthly-row",
        "monthly-cell",
        "monthly-bar",
        "monthly-month-coverage",
        "monthCoverageLabel(row, payload)",
    ]:
        assert marker in html
    assert "月度对比矩阵" in html
    for label in ["曝光", "线索", "实销", "费用", "CPL", "CPS"]:
        assert label in html
    assert '"douyin_laike_orders"' in html
    assert "2026年3月" in html
    assert "2026年4月" in html
    assert "2026年5月" in html
    assert "2026-03-01 至 2026-03-31" in html
    assert "2026-04-01 至 2026-04-30" in html
    assert "2026-05-01 至 2026-05-22" in html
    assert "monthCoverageRange(row, payload)" in html
    assert "monthlyCellTooltip" in html
    assert "new Date(year, month, 0).getDate()" in html
    assert "return padDate(new Date(year, month, 0));" not in html


def test_render_trend_dashboard_html_keeps_n12b_visible_text_clean() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")
    visible_text = _visible_text(html)

    for forbidden in [
        "成交趋势范围：",
        "线索趋势范围：",
        "曝光趋势范围：",
        "实销趋势范围：",
        "线索趋势2026",
        "成交趋势2026",
        "曝光趋势2026",
        "03-0103-22",
        "012 03-01",
        "023 03-01",
        "035 03-01",
        "本期上一周期",
        "目标参考 目标未提供",
        "目标 目标未提供",
        "异常",
        "风险",
        "预警",
        "待提升",
        "表现较好",
        "稳步推进",
        "达标",
        "未达标",
        "质量标注",
        "数据状态",
        "发布边界",
        "dashboard source",
        "contract_version",
        "release_readiness",
        "publish-ready",
        "pipeline",
        "口径待确认",
    ]:
        assert forbidden not in visible_text


def test_render_trend_dashboard_html_exposes_n121c1_account_toolbar_controls() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert 'id="account-toolbar"' in html
    assert 'id="account-search-input"' in html
    assert 'placeholder="搜索账号名称"' in html
    assert 'id="account-sort-select"' in html
    for label in ["默认排序", "线索数", "成交数", "费用", "CPL", "CPS", "当前 / 目标"]:
        assert label in html
    for label in ["全部账号", "有目标参考", "目标未提供", "有成交", "有费用", "比率超过 100%"]:
        assert label in html
    for label in ["到店数", "到店率", "到店成交率"]:
        assert label in html
    for forbidden in ["EX7 线索数", "EX7 成交数", "EX7 有成交", "字段未接入"]:
        assert forbidden not in html
    assert "清除条件" in html
    assert "当前条件：全部账号" in html
    assert "无匹配账号" in html
    assert "可尝试清除搜索词或筛选条件。" in html
    assert "function renderAccountToolbar" in html
    assert "function applyAccountListState" in html
    assert "function accountMatchesFilter" in html
    for forbidden in ["异常账号", "风险账号", "待确认账号", "口径待确认"]:
        assert forbidden not in html


def test_render_trend_dashboard_html_exposes_n121d_runtime_acceptance_dom_contract() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        '<link rel="icon" href="data:,">',
        'id="account-toolbar"',
        'id="account-search-input"',
        'class="account-search-input"',
        'id="account-sort-select"',
        'class="account-sort-select"',
        'class="account-filter-chip is-active"',
        'id="account-clear-filters"',
        'class="account-clear-filters"',
        'id="account-filter-summary"',
        'class="account-filter-summary"',
        'document.getElementById("account-search-input")',
        'document.getElementById("account-sort-select")',
        'document.getElementById("account-clear-filters")',
        'document.getElementById("account-filter-summary")',
    ]:
        assert marker in html

    for stale_marker in [
        'id="account-search"',
        'id="account-sort"',
        'id="account-clear-conditions"',
        'id="account-condition-summary"',
        'document.getElementById("account-search")',
        'document.getElementById("account-sort")',
        'document.getElementById("account-clear-conditions")',
        'document.getElementById("account-condition-summary")',
    ]:
        assert stale_marker not in html


def test_render_trend_dashboard_html_keeps_mobile_nav_labels_on_one_line() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert ".nav {" in html
    assert ".nav a {" in html
    nav_link_block = re.search(r"\.nav a \{(?P<body>.*?)\}", html, flags=re.DOTALL)
    assert nav_link_block is not None
    assert "white-space: nowrap;" in nav_link_block.group("body")
    assert "flex: 0 0 auto;" in nav_link_block.group("body")


def test_render_trend_dashboard_html_exposes_n121c1_account_summary_and_expand_details() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        "account-summary-grid",
        "account-detail-panel",
        "account-detail-toggle",
        'data-account-detail-toggle',
        'aria-expanded="${expanded ? "true" : "false"}"',
        "展开详情",
        "收起详情",
    ]:
        assert marker in html
    for label in ["线索数", "成交数", "费用", "CPL", "CPS", "当前 / 目标"]:
        assert label in html
    for group in ["线索组", "到店组", "成交组", "成本组", "趋势"]:
        assert group in html
    for label in [
        "唯一线索数",
        "线索成交率",
        "到店数",
        "到店率",
        "到店成交率",
    ]:
        assert label in html
    for forbidden in ["EX7 组", "EX7 线索数", "EX7 成交数", "EX7 成交率"]:
        assert forbidden not in html
    assert "function bindAccountDetailToggles" in html
    assert "account-detail-panel.is-expanded" in html
    assert "max-height: 2200px" not in html
    assert "overflow: visible;" in html
    assert "比率超过 100%" in html
    assert "Math.min(progressRate * 100, 100)" in html
    assert "口径待确认" not in html


def test_render_trend_dashboard_html_exposes_n121c1_account_visibility_note_without_expanding_hide_list() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert "部分已取消或非当前经营复盘范围账号未在主列表展示；当前账号列表不因高比例数据进行隐藏。" in html
    hidden_names = [
        "视频号-星途星纪元",
        "星途星纪元",
        "星途星纪元直播营销中心+",
        "抖音",
        "快手-星途星纪元",
        "抖店",
    ]
    for name in hidden_names:
        assert name in html
    assert "HIDDEN_ACCOUNT_NAMES = new Set([" in html
    assert "const HIDDEN_ACCOUNT_NAMES = new Set([" in html
    assert "快手-EXEED星途趋势" in html
    assert "featuredAccountCard" in html
    assert "businessAccounts.sort" in html
    assert "featuredMatchesAccountState" in html
    for forbidden in ["异常", "风险", "预警", "待确认账号"]:
        assert forbidden not in html


def test_render_trend_dashboard_html_exposes_n121c1_focused_acceptance_contracts() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        'id="account-toolbar"',
        'id="account-search-input"',
        'id="account-sort-select"',
        'data-account-filter="over_100"',
        'id="account-clear-filters"',
        'id="account-filter-summary"',
    ]:
        assert marker in html

    assert 'class="${panelClass}"${expanded ? "" : " hidden"} aria-hidden="${expanded ? "false" : "true"}"' in html
    assert 'aria-expanded="${expanded ? "true" : "false"}"' in html
    assert "panel.hidden = false;" in html
    assert "panel.hidden = true;" in html
    assert 'button.textContent = expanded ? "收起详情" : "展开详情";' in html
    assert "function accountOver100Summary(entity)" in html
    assert "accountOver100Summary(entity)" in html
    assert 'item?.unit?.includes("比例")' in html

    assert "function accountMatchesSearch(entity)" in html
    assert "accountDisplayName(entity).toLowerCase().includes(query)" in html
    assert "accountListState.search = search.value;" in html
    assert 'accountListState.search = "";' in html

    assert "function accountMatchesFilter(entity, filterKey = accountListState.filter)" in html
    assert 'if (filterKey === "over_100") return accountHasOver100Ratio(entity);' in html
    assert 'if (filterKey === "not_connected") return accountHasNotConnectedField(entity);' not in html
    assert "accountHasOver100Ratio" in html
    assert "口径待确认" not in html

    assert "function compareAccountsByState" in html
    assert "businessAccounts.sort((a, b) => compareAccountsByState(a, b, accountOrder));" in html
    assert "featuredBlock" in html
    assert "featuredMatchesAccountState(featured)" in html

    assert '<div class="account-empty-state"><strong>无匹配账号</strong><span>可尝试清除搜索词或筛选条件。</span></div>' in html
    assert "未提供</strong><span>可尝试清除搜索词或筛选条件。" not in html

    assert "data-account-trend-pane=" not in html
    assert 'data-account-trend-pane="leads"' not in html
    assert 'data-account-trend-pane="deals"' not in html
    assert "panel.innerHTML = compactTrendChart" in html
    for forbidden in ["成交趋势范围：", "线索趋势范围：", "03-0103-22", "012 03-01", "本期上一周期"]:
        assert forbidden not in html


def test_render_trend_dashboard_html_exposes_n121c2_anchor_toolbar_controls() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        'id="anchor-toolbar"',
        'class="anchor-toolbar"',
        'id="anchor-search-input"',
        'class="anchor-search-input"',
        'placeholder="搜索主播姓名或所属账号"',
        'id="anchor-sort-select"',
        'class="anchor-sort-select"',
        'class="anchor-filter-chip is-active"',
        'id="anchor-clear-filters"',
        'class="anchor-clear-filters"',
        'id="anchor-filter-summary"',
        'class="anchor-filter-summary"',
    ]:
        assert marker in html

    for label in ["默认排序", "线索数", "成交数", "费用", "CPL", "CPS", "当前 / 目标"]:
        assert label in html
    for label in ["全部主播", "有目标参考", "目标未提供", "有成交", "有费用", "比率超过 100%"]:
        assert label in html
    for label in ["到店数", "到店率", "到店成交率"]:
        assert label in html
    for forbidden in ["EX7 线索数", "EX7 成交数", "EX7 有成交", "字段未接入"]:
        assert forbidden not in html
    assert "清除条件" in html
    assert "当前条件：全部主播" in html
    assert '<div class="anchor-empty-state"><strong>无匹配主播</strong><span>可尝试清除搜索词或筛选条件。</span></div>' in html
    assert "未提供</strong><span>可尝试清除搜索词或筛选条件。" not in html


def test_render_trend_dashboard_html_exposes_n121c2_anchor_search_filter_sort_logic() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert "const anchorListState = { search: \"\", sort: \"default\", filter: \"all\" };" in html
    assert "function renderAnchorToolbar" in html
    assert "function applyAnchorListState" in html
    assert "function anchorMatchesSearch(entity)" in html
    assert "anchorDisplayName(entity).toLowerCase().includes(query)" in html
    assert "anchorParentScope(entity).toLowerCase().includes(query)" in html
    assert "anchorListState.search = search.value;" in html
    assert 'anchorListState.search = "";' in html
    assert "function anchorMatchesFilter(entity, filterKey = anchorListState.filter)" in html
    assert 'if (filterKey === "target_missing") return !anchorHasTarget(entity);' in html
    assert 'if (filterKey === "has_deals") return (anchorMetricNumber(metric(entity, "deals")) || 0) > 0;' in html
    assert 'if (filterKey === "has_spend") return (anchorMetricNumber(metric(entity, "spend")) || 0) !== 0;' in html
    assert 'if (filterKey === "ex7_has_deals") return (anchorMetricNumber(metric(entity, "ex7_deals")) || 0) > 0;' not in html
    assert 'if (filterKey === "not_connected") return anchorHasNotConnectedField(entity);' not in html
    assert 'if (filterKey === "over_100") return anchorHasOver100Ratio(entity);' in html
    assert "function compareAnchorsByState" in html
    assert "visibleAnchors.sort((a, b) => compareAnchorsByState(a, b, anchorOrder));" in html
    assert 'anchors.map((entity) => detailCard(entity, "anchor-card"))' not in html
    assert "visibleAnchors.map((entity) => anchorDetailCard(entity, \"anchor-card\"))" in html
    assert "if (av === null) return 1;" in html
    assert "if (bv === null) return -1;" in html
    assert "return (bv - av)" in html
    assert "anchorListState.filter = \"all\";" in html
    assert "anchorConditionSummaryText" in html
    for forbidden in ["异常主播", "风险主播", "待确认主播", "口径待确认"]:
        assert forbidden not in html


def test_render_trend_dashboard_html_exposes_n121c2_anchor_summary_and_expand_details() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        "anchor-summary-grid",
        "anchor-detail-panel",
        "anchor-detail-toggle",
        "data-anchor-detail-toggle",
        'aria-expanded="${expanded ? "true" : "false"}"',
        "展开详情",
        "收起详情",
        "anchor-parent-scope",
        "anchor-full-account",
        "data-anchor-trend",
        "data-anchor-trend-panel",
        "function bindAnchorTrendSwitchers",
        "function bindAnchorDetailToggles",
    ]:
        assert marker in html

    for label in ["线索数", "成交数", "费用", "CPL", "CPS", "当前 / 目标"]:
        assert label in html
    for group in ["线索组", "到店组", "成交组", "成本组", "趋势"]:
        assert group in html
    for label in ["唯一线索数", "线索成交率", "到店数", "到店率", "到店成交率"]:
        assert label in html
    for forbidden in ["EX7 组", "EX7 线索数", "EX7 成交数", "EX7 成交率"]:
        assert forbidden not in html

    assert 'class="${panelClass}"${expanded ? "" : " hidden"} aria-hidden="${expanded ? "false" : "true"}"' in html
    assert "panel.hidden = false;" in html
    assert "panel.hidden = true;" in html
    assert 'button.textContent = expanded ? "收起详情" : "展开详情";' in html
    assert "panel.innerHTML = compactTrendChart" in html
    assert "data-anchor-trend-pane=" not in html
    assert 'data-anchor-trend-pane="leads"' not in html
    assert 'data-anchor-trend-pane="deals"' not in html
    assert "口径待确认" not in html


def test_render_trend_dashboard_html_exposes_n121c3_seed_toolbar_controls() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        'id="seed-toolbar"',
        'class="seed-toolbar"',
        'id="seed-search-input"',
        'class="seed-search-input"',
        'placeholder="搜索种草账号或主播"',
        'id="seed-sort-select"',
        'class="seed-sort-select"',
        'class="seed-filter-chip is-active"',
        'id="seed-clear-filters"',
        'class="seed-clear-filters"',
        'id="seed-filter-summary"',
        'class="seed-filter-summary"',
    ]:
        assert marker in html

    for label in ["默认排序", "曝光", "目标参考", "当前 / 目标", "最新曝光", "名称"]:
        assert label in html
    for label in ["全部种草", "账号总曝光", "主播曝光", "有目标参考", "目标未提供", "曝光大于 0", "当前 / 目标超过 100%"]:
        assert label in html
    assert "字段未接入" not in html
    assert "清除条件" in html
    assert "当前条件：全部种草" in html
    assert '<div class="seed-empty-state"><strong>无匹配种草项</strong><span>可尝试清除搜索词或筛选条件。</span></div>' in html
    assert "未提供</strong><span>可尝试清除搜索词或筛选条件。" not in html


def test_render_trend_dashboard_html_exposes_n121c3_seed_search_filter_sort_logic() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    assert 'const seedListState = { search: "", sort: "default", filter: "all" };' in html
    assert "function renderSeedToolbar" in html
    assert "function applySeedListState" in html
    assert "function seedMatchesSearch(entity)" in html
    assert "seedDisplayName(entity).toLowerCase().includes(query)" in html
    assert "seedParentScope(entity).toLowerCase().includes(query)" in html
    assert "seedListState.search = search.value;" in html
    assert 'seedListState.search = "";' in html
    assert "function seedMatchesFilter(entity, filterKey = seedListState.filter)" in html
    assert 'if (filterKey === "account_total") return seedIsAccountTotal(entity);' in html
    assert 'if (filterKey === "anchor_exposure") return !seedIsAccountTotal(entity);' in html
    assert 'if (filterKey === "has_target") return seedHasTarget(entity);' in html
    assert 'if (filterKey === "target_missing") return !seedHasTarget(entity);' in html
    assert 'if (filterKey === "positive_exposure") return (seedMetricNumber(seedImpressionsMetric(entity)) || 0) > 0;' in html
    assert 'if (filterKey === "over_100") return seedHasOver100Ratio(entity);' in html
    assert 'if (filterKey === "not_connected") return seedHasNotConnectedField(entity);' not in html
    assert "function compareSeedsByState" in html
    assert "visibleAccounts.sort((a, b) => compareSeedsByState(a, b, seedOrder));" in html
    assert "visibleAnchors.sort((a, b) => compareSeedsByState(a, b, seedOrder));" in html
    assert "seedLatestExposureValue" in html
    assert "return seedMetricNumber(seedLatestMetric(entity)) ?? seedMetricNumber(seedImpressionsMetric(entity));" in html
    assert "seedListState.filter = \"all\";" in html
    assert "seedConditionSummaryText" in html
    assert "当前 / 目标超过 100%" in html
    for forbidden in ["异常种草", "风险曝光", "待确认曝光", "口径待确认"]:
        assert forbidden not in html


def test_render_trend_dashboard_html_exposes_n121c3_seed_summary_and_expand_details() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")

    for marker in [
        "seed-summary-grid",
        "seed-detail-panel",
        "seed-detail-toggle",
        "data-seed-detail-toggle",
        'aria-expanded="${expanded ? "true" : "false"}"',
        "展开详情",
        "收起详情",
        "seed-parent-scope",
        "seed-type-chip",
        "seed-account-slot",
        "data-seed-trend",
        "data-seed-trend-panel",
        "function bindSeedDetailToggles",
    ]:
        assert marker in html

    for label in ["类型：", "账号总曝光", "主播曝光", "曝光", "目标参考", "当前 / 目标", "最新曝光", "曝光趋势"]:
        assert label in html
    for group in ["曝光组", "目标组", "来源组", "趋势组"]:
        assert group in html
    assert "function seedTargetSummary" in html
    assert "if (!hasValue(metricItem?.target)) return seedMetricCell(\"目标参考\", seedMissingTargetMetric(metricItem));" in html
    assert "当前 / 目标：未提供" not in html
    assert 'class="${panelClass}"${expanded ? "" : " hidden"} aria-hidden="${expanded ? "false" : "true"}"' in html
    assert "panel.hidden = false;" in html
    assert "panel.hidden = true;" in html
    assert 'button.textContent = expanded ? "收起详情" : "展开详情";' in html
    assert "口径待确认" not in html


def test_render_trend_dashboard_html_keeps_n121c3_seed_visible_text_clean() -> None:
    html = dashboard_html.render_trend_dashboard_html(api_path="/dashboard/daily/trends?end_date=2026-05-22")
    visible_text = _visible_text(html)

    for forbidden in [
        "成交趋势范围：",
        "线索趋势范围：",
        "曝光趋势范围：",
        "实销趋势范围：",
        "线索趋势2026",
        "成交趋势2026",
        "曝光趋势2026",
        "03-0103-22",
        "012 03-01",
        "023 03-01",
        "035 03-01",
        "本期上一周期",
        "account",
        "anchor",
        "metric_type",
        "source_type",
        "异常",
        "风险",
        "预警",
        "待提升",
        "表现较好",
        "稳步推进",
        "达标",
        "未达标",
        "质量标注",
        "数据状态",
        "发布边界",
        "dashboard source",
        "contract_version",
        "release_readiness",
        "publish-ready",
        "pipeline",
        "口径待确认",
    ]:
        assert forbidden not in visible_text


def test_write_api_connected_dashboard_html_uses_non_formal_output_path(tmp_path: Path) -> None:
    output_path = tmp_path / "api-preview.html"

    written = write_api_connected_dashboard_html(report_date="2026-05-14", output_html=output_path)

    assert written == output_path
    html = output_path.read_text(encoding="utf-8")
    assert 'const API_PATH = "/dashboard/daily/2026-05-14"' in html
    assert "fetchDashboard(path)" in html
    assert "启动 FastAPI 后打开" in html


def _visible_text(markup: str) -> str:
    without_scripts = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", "", markup, flags=re.IGNORECASE | re.DOTALL)
    without_tags = re.sub(r"<[^>]+>", " ", without_scripts)
    return " ".join(html_lib.unescape(without_tags).split())


def _write_tsv(path: Path, rows: list[dict[str, str]]) -> None:
    columns = [
        "report_date",
        "source_table",
        "scope_type",
        "scope_name",
        "parent_scope",
        "metric_key",
        "metric_name",
        "actual",
        "target",
        "attain_rate",
        "unit",
        "source_column",
        "sort_order",
    ]
    path.write_text(
        "\n".join(
            [
                "\t".join(columns),
                *["\t".join(str(row.get(column, "")) for column in columns) for row in rows],
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _sample_rows() -> list[dict[str, str]]:
    return [
        _row("topline", "department", "全量", "", "impressions", "曝光", "21886318", "20000000", "1.0943159", "人次"),
        _row("topline", "department", "全量", "", "mtd_unique_leads", "唯一线索", "13323", "29700", "0.4485858585858586", "条"),
        _row("topline", "department", "全量", "", "mtd_deals", "实销", "41", "100", "0.41", "台"),
        _row("topline", "department", "全量", "", "mtd_spend", "消耗费用", "515957.85", "", "", "元"),
        _row("topline", "department", "全量", "", "mtd_cpl", "总体CPL", "38.726852060346765", "55", "", "元/条"),
        _row("topline", "department", "全量", "", "mtd_cps", "总体CPS", "12584.337804878049", "9060.02", "", "元/台"),
        _row("topline", "department", "全量", "", "pending_day", "待交车（当日）", "0", "", "", "台"),
        _row("topline", "department", "全量", "", "pending_cumulative", "待交车（累计）", "21", "", "", "台"),
        _row("topline", "department", "全量", "", "mtd_douyin_laike_orders", "抖音-来客线索（手机号去重）", "106", "300", "0.35333333333333333", "条"),
        _row("lead_quality", "department", "全量", "", "raw_leads", "原始线索", "14775", "", "", "条"),
        _row("lead_quality", "department", "全量", "", "unique_rate", "唯一率", "0.9017", "", "", "比例"),
        _row("lead_quality", "department", "全量", "", "unowned_leads", "无主线索", "150", "", "", "条"),
        _row("lead_quality", "department", "全量", "", "manual_overrides", "人工归属", "27", "", "", "条"),
        _row("topline_segment", "segment", "EX7 专项", "", "mtd_unique_leads", "唯一线索", "9219", "", "", "条"),
        _row("topline_segment", "segment", "EX7 专项", "", "mtd_deals", "实销", "6", "", "", "台"),
        _row("topline_segment", "segment", "EX7 专项", "", "mtd_spend", "消耗费用", "313893.12", "", "", "元"),
        _row("topline_segment", "segment", "EX7 专项", "", "mtd_cpl", "实际CPL", "34.048499837292546", "", "", "元/条"),
        _row("topline_segment", "segment", "EX7 专项", "", "mtd_cps", "实际CPS", "52315.52", "", "", "元/台"),
        _row("topline_segment", "segment", "不含 EX7", "", "mtd_unique_leads", "唯一线索", "4104", "", "", "条"),
        _row("topline_segment", "segment", "不含 EX7", "", "mtd_deals", "实销", "35", "", "", "台"),
        _row("topline_segment", "segment", "不含 EX7", "", "mtd_spend", "消耗费用", "202064.73", "", "", "元"),
        _row("topline_segment", "segment", "不含 EX7", "", "mtd_cpl", "实际CPL", "49.23604532163742", "", "", "元/条"),
        _row("topline_segment", "segment", "不含 EX7", "", "mtd_cps", "实际CPS", "5773.277999999999", "", "", "元/台"),
        _row("lead_anchor", "anchor", "徐幻", "星途汽车官方直播间", "mtd_unique_leads", "累计唯一线索", "2634", "3713", "0.7094", "条"),
        _row("lead_anchor", "anchor", "徐幻", "星途汽车官方直播间", "mtd_douyin_laike_orders", "抖音-来客线索（手机号去重）", "35", "37.5", "0.9333", "条"),
        _row("lead_anchor", "anchor", "徐幻", "星途汽车官方直播间", "mtd_deals", "累计实销", "4.08", "13", "0.3141", "台"),
        _row("lead_anchor", "anchor", "徐幻", "星途汽车官方直播间", "mtd_cpl", "实际CPL", "28.47", "55", "", "元/条"),
        _row("lead_anchor", "anchor", "徐幻", "星途汽车官方直播间", "mtd_cps", "实际CPS", "18364.39", "9060", "", "元/台"),
        _row("lead_anchor", "anchor", "丁俐佳", "星途汽车官方直播间", "mtd_unique_leads", "累计唯一线索", "1821", "3713", "0.4904", "条"),
        _row("lead_anchor", "anchor", "丁俐佳", "星途汽车官方直播间", "mtd_douyin_laike_orders", "抖音-来客线索（手机号去重）", "20", "37.5", "0.5333", "条"),
        _row("lead_anchor", "anchor", "丁俐佳", "星途汽车官方直播间", "mtd_deals", "累计实销", "8.92", "13", "0.6859", "台"),
        _row("lead_anchor", "anchor", "丁俐佳", "星途汽车官方直播间", "mtd_cpl", "实际CPL", "35.26", "55", "", "元/条"),
        _row("lead_anchor", "anchor", "丁俐佳", "星途汽车官方直播间", "mtd_cps", "实际CPS", "7201.75", "9060", "", "元/台"),
        _row("seed_account", "account", "EXEED星途", "", "mtd_impressions", "累计曝光", "7221796", "20000000", "0.3611", "人次"),
        _row("seed_anchor", "anchor", "桂婕", "EXEED星途", "mtd_impressions", "累计曝光", "2995776", "4000000", "0.7489", "人次"),
        _row("seed_anchor", "anchor", "桂婕", "EXEED星途", "daily_impressions", "当日曝光", "0", "55790.22", "0", "人次"),
        _row("seed_anchor", "anchor", "刘花旗", "EXEED星途", "mtd_impressions", "累计曝光", "2040472.5", "4000000", "0.5101", "人次"),
        _row("seed_anchor", "anchor", "刘花旗", "EXEED星途", "daily_impressions", "当日曝光", "0", "108862.64", "0", "人次"),
    ]


def _row(
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
    return {
        "report_date": "2026-05-14",
        "source_table": source_table,
        "scope_type": scope_type,
        "scope_name": scope_name,
        "parent_scope": parent_scope,
        "metric_key": metric_key,
        "metric_name": metric_name,
        "actual": actual,
        "target": target,
        "attain_rate": attain_rate,
        "unit": unit,
        "source_column": "fixture",
        "sort_order": "0",
    }
