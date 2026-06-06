"""Render the daily Feishu dashboard as a portable SVG/PNG visual."""

from __future__ import annotations

import html
import shutil
import subprocess
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


WIDTH = 1920
HEIGHT = 1440
LONG_COMPACT_MIN_HEIGHTS = {
    "p1_overview": HEIGHT,
    "p2_lead_detail": 1200,
    "p3_seed_detail": 1080,
    "p5_account_channel": 820,
}


@dataclass(frozen=True)
class DashboardPageSpec:
    page_id: str
    title: str
    nav_label: str


@dataclass(frozen=True)
class DashboardFieldSpec:
    metric_key: str
    header: str
    value_kind: str = "actual"
    formatter: str = "int"
    placeholder: str = "未提供"


DASHBOARD_PAGE_SPECS = (
    DashboardPageSpec("p1_overview", "P1 总览驾驶舱", "部门总览"),
    DashboardPageSpec("p2_lead_detail", "P2 线索组明细", "线索组"),
    DashboardPageSpec("p3_seed_detail", "P3 种草组明细", "种草组"),
    DashboardPageSpec("p5_account_channel", "P4 账号拆解", "账号拆解"),
)


DASHBOARD_PAGE_FIELD_SPECS: dict[str, dict[str, tuple[DashboardFieldSpec, ...]]] = {
    "p2_lead_detail": {
        "scope_metrics": (
            DashboardFieldSpec("mtd_unique_leads", "唯一线索", formatter="int"),
            DashboardFieldSpec("mtd_douyin_laike_orders", "来客订单", formatter="int"),
            DashboardFieldSpec("mtd_douyin_laike_orders", "订单达成", value_kind="rate", formatter="pct"),
            DashboardFieldSpec("mtd_deals", "实销", formatter="decimal"),
            DashboardFieldSpec("mtd_cpl", "CPL", formatter="money"),
            DashboardFieldSpec("mtd_cps", "CPS", formatter="money"),
        ),
        "anchor_metrics": (
            DashboardFieldSpec("mtd_unique_leads", "唯一线索", formatter="int"),
            DashboardFieldSpec("mtd_douyin_laike_orders", "来客订单", formatter="int"),
            DashboardFieldSpec("mtd_douyin_laike_orders", "订单达成", value_kind="rate", formatter="pct"),
            DashboardFieldSpec("mtd_deals", "实销", formatter="decimal"),
            DashboardFieldSpec("mtd_spend", "个人消耗", formatter="money"),
            DashboardFieldSpec("mtd_cpl", "CPL", formatter="money"),
            DashboardFieldSpec("mtd_cps", "CPS", formatter="money"),
        ),
    },
    "p3_seed_detail": {
        "scope_metrics": (
            DashboardFieldSpec("daily_impressions", "当日曝光", formatter="wan"),
            DashboardFieldSpec("daily_impressions", "当日达成", value_kind="rate", formatter="pct_or_na"),
            DashboardFieldSpec("mtd_impressions", "累计曝光", formatter="wan"),
            DashboardFieldSpec("mtd_impressions", "累计目标", value_kind="target", formatter="wan"),
            DashboardFieldSpec("mtd_impressions", "累计达成", value_kind="rate", formatter="pct_or_na"),
        ),
    },
    "p5_account_channel": {
        "lead_account_metrics": (
            DashboardFieldSpec("daily_leads", "当日", formatter="int"),
            DashboardFieldSpec("mtd_unique_leads", "累计", formatter="int"),
            DashboardFieldSpec("mtd_douyin_laike_orders", "订单", formatter="int"),
            DashboardFieldSpec("mtd_deals", "实销", formatter="int"),
            DashboardFieldSpec("mtd_cpl", "CPL", formatter="money"),
            DashboardFieldSpec("mtd_cps", "CPS", formatter="money"),
        ),
        "seed_account_metrics": (
            DashboardFieldSpec("daily_impressions", "当日", formatter="wan"),
            DashboardFieldSpec("mtd_impressions", "累计", formatter="wan"),
        ),
    },
}


def render_dashboard_svg(table: pd.DataFrame, *, run_id: str = "", width: int = WIDTH, height: int = HEIGHT) -> str:
    """Render a one-page objective daily dashboard from the long-form source table."""

    source = _Source(table)
    report_date = source.report_date
    impressions = source.value("department", "全量", "impressions")
    impressions_target = source.target("department", "全量", "impressions")
    impressions_rate = source.rate("department", "全量", "impressions")
    unique = source.value("department", "全量", "mtd_unique_leads")
    unique_target = source.target("department", "全量", "mtd_unique_leads")
    unique_rate = source.rate("department", "全量", "mtd_unique_leads")
    deals = source.value("department", "全量", "mtd_deals")
    deals_target = source.target("department", "全量", "mtd_deals")
    deals_rate = source.rate("department", "全量", "mtd_deals")
    orders = source.value("department", "全量", "mtd_douyin_laike_orders")
    orders_target = source.target("department", "全量", "mtd_douyin_laike_orders")
    orders_rate = source.rate("department", "全量", "mtd_douyin_laike_orders")
    spend = source.value("department", "全量", "mtd_spend")
    cpl = source.value("department", "全量", "mtd_cpl")
    cps = source.value("department", "全量", "mtd_cps")
    pending_day = source.value("department", "全量", "pending_day")
    pending_cumulative = source.value("department", "全量", "pending_cumulative")
    raw_leads = source.value("department", "全量", "raw_leads")
    quality_unique_rate = source.value("department", "全量", "unique_rate")
    unowned_leads = source.value("department", "全量", "unowned_leads")
    manual_overrides = source.value("department", "全量", "manual_overrides")

    lead_accounts = source.scopes(
        "lead_account",
        "account",
        ["mtd_unique_leads", "mtd_douyin_laike_orders", "mtd_deals"],
    )
    lead_anchors = source.anchors("lead_anchor", ["mtd_unique_leads", "mtd_douyin_laike_orders"])
    seed_anchors = source.anchors("seed_anchor", ["mtd_impressions"])

    colors = {
        "ink": "#E8F1FF",
        "muted": "#8CA2C2",
        "muted_dark": "#6F86A8",
        "line": "#31527D",
        "blue": "#4F8CFF",
        "cyan": "#1EF3FF",
        "green": "#16C784",
        "amber": "#FFAD26",
        "red": "#FF5A64",
        "purple": "#B991FF",
    }

    def rect(
        x: float,
        y: float,
        w: float,
        h: float,
        fill: str,
        stroke: str = "none",
        sw: float = 1,
        rx: float = 18,
        opacity: float | None = None,
        extra: str = "",
    ) -> str:
        opacity_attr = f" opacity='{opacity}'" if opacity is not None else ""
        extra_attr = f" {extra}" if extra else ""
        return (
            f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='{rx}' fill='{fill}' "
            f"stroke='{stroke}' stroke-width='{sw}'{opacity_attr}{extra_attr}/>"
        )

    def text(
        x: float,
        y: float,
        value: object,
        size: int = 28,
        fill: str = colors["ink"],
        weight: int = 500,
        anchor: str = "start",
        opacity: float | None = None,
        family: str | None = None,
    ) -> str:
        opacity_attr = f" opacity='{opacity}'" if opacity is not None else ""
        family_attr = (
            family
            or "Avenir Next, PingFang SC, Hiragino Sans GB, Microsoft YaHei, sans-serif"
        )
        return (
            f"<text x='{x}' y='{y}' font-family='{family_attr}' font-size='{size}' "
            f"font-weight='{weight}' fill='{fill}' text-anchor='{anchor}'{opacity_attr}>{_esc(value)}</text>"
        )

    def progress(x: float, y: float, w: float, h: float, rate: float, color: str) -> str:
        clamped = max(0.0, min(1.0, rate))
        return rect(x, y, w, h, "#1F2E46", rx=h / 2) + rect(x, y, max(4.0, w * clamped), h, color, rx=h / 2)

    def card(x: float, y: float, title: str, big: str, sub: str, rate_value: float, color: str) -> str:
        w = 420
        return "".join(
            [
                rect(x, y, w, 155, "url(#cardGrad)", stroke="rgba(159,197,255,.20)", rx=26),
                rect(x, y, w, 5, color, rx=2.5),
                text(x + 28, y + 44, title, 21, "#9AB0D1", 720),
                text(
                    x + 28,
                    y + 98,
                    big,
                    42,
                    "#F6FBFF",
                    850,
                    family="DIN Alternate, Avenir Next Condensed, PingFang SC, sans-serif",
                ),
                text(x + 28, y + 134, sub, 18, "#8396B6", 600),
                progress(x + w - 182, y + 112, 128, 12, rate_value, color),
                text(x + w - 28, y + 98, _fmt_pct(rate_value), 22, color, 850, "end"),
            ]
        )

    def section_title(x: float, y: float, title: str, subtitle: str = "") -> str:
        out = [text(x, y, title, 30, "#F4F8FF", 850)]
        if subtitle:
            out.append(text(x, y + 34, subtitle, 17, colors["muted"], 600))
        return "".join(out)

    def nav_chip(x: float, y: float, index: str, label: str, active: bool = False) -> str:
        fill = "rgba(31,243,255,.16)" if active else "rgba(255,255,255,.045)"
        stroke = "#49D7FF" if active else "rgba(124,164,220,.28)"
        label_color = "#DFF8FF" if active else "#9FB4D2"
        return "".join(
            [
                rect(x, y, 132, 40, fill, stroke=stroke, sw=1.1, rx=20),
                text(x + 20, y + 26, index, 14, "#64D7FF" if active else "#61799E", 850),
                text(x + 52, y + 26, label, 17, label_color, 780),
            ]
        )

    svg: list[str] = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>",
        "<defs>"
        "<linearGradient id='bg' x1='0' y1='0' x2='1' y2='1'>"
        "<stop offset='0' stop-color='#06101F'/><stop offset='.45' stop-color='#081B34'/>"
        "<stop offset='1' stop-color='#041923'/></linearGradient>"
        "<radialGradient id='glowA' cx='18%' cy='0%' r='68%'>"
        "<stop offset='0' stop-color='#1B5CFF' stop-opacity='.42'/><stop offset='1' stop-color='#1B5CFF' stop-opacity='0'/>"
        "</radialGradient>"
        "<radialGradient id='glowB' cx='96%' cy='12%' r='72%'>"
        "<stop offset='0' stop-color='#00D5FF' stop-opacity='.34'/><stop offset='1' stop-color='#00D5FF' stop-opacity='0'/>"
        "</radialGradient>"
        "<linearGradient id='cardGrad' x1='0' y1='0' x2='1' y2='1'>"
        "<stop offset='0' stop-color='#14243C' stop-opacity='.92'/><stop offset='1' stop-color='#0B1528' stop-opacity='.90'/>"
        "</linearGradient>"
        "<linearGradient id='panelGrad' x1='0' y1='0' x2='1' y2='1'>"
        "<stop offset='0' stop-color='#101D35' stop-opacity='.88'/><stop offset='1' stop-color='#071629' stop-opacity='.94'/>"
        "</linearGradient>"
        "<linearGradient id='gold' x1='0' y1='0' x2='1' y2='0'>"
        "<stop offset='0' stop-color='#F6C85F'/><stop offset='1' stop-color='#FF8A00'/></linearGradient>"
        "<linearGradient id='cyan' x1='0' y1='0' x2='1' y2='0'>"
        "<stop offset='0' stop-color='#1EF3FF'/><stop offset='1' stop-color='#208BFF'/></linearGradient>"
        "<linearGradient id='green' x1='0' y1='0' x2='1' y2='0'>"
        "<stop offset='0' stop-color='#35F6A5'/><stop offset='1' stop-color='#11A85A'/></linearGradient>"
        "<linearGradient id='red' x1='0' y1='0' x2='1' y2='0'>"
        "<stop offset='0' stop-color='#FF6B6B'/><stop offset='1' stop-color='#E42626'/></linearGradient>"
        "<filter id='shadow' x='-20%' y='-20%' width='140%' height='140%'>"
        "<feDropShadow dx='0' dy='18' stdDeviation='24' flood-color='#000814' flood-opacity='.42'/></filter>"
        "<pattern id='grid' width='48' height='48' patternUnits='userSpaceOnUse'>"
        "<path d='M 48 0 L 0 0 0 48' fill='none' stroke='#6EA8FF' stroke-width='.6' opacity='.11'/>"
        "</pattern>"
        "</defs>",
        rect(0, 0, width, height, "url(#bg)", rx=0),
        rect(0, 0, width, height, "url(#glowA)", rx=0),
        rect(0, 0, width, height, "url(#glowB)", rx=0),
        rect(0, 0, width, height, "url(#grid)", rx=0, opacity=0.55),
        rect(44, 34, 1832, 132, "rgba(8,19,38,.74)", stroke="#3F68A8", sw=1.2, rx=34, extra="filter='url(#shadow)'"),
        text(84, 106, "数据驾驶舱", 56, "#FFFFFF", 850),
        text(88, 140, "P1 总览驾驶舱", 18, "#8EA6CA", 720),
        nav_chip(1098, 78, "01", "部门总览", True),
        nav_chip(1246, 78, "02", "线索组"),
        nav_chip(1394, 78, "03", "种草组"),
        nav_chip(1542, 78, "04", "账号拆解"),
    ]

    x0, y0 = 54, 205
    svg.extend(
        [
            card(x0, y0, "累计曝光", _fmt_wan(impressions), f"目标 {_fmt_wan(impressions_target)}", impressions_rate, "url(#cyan)"),
            card(x0 + 450, y0, "累计唯一线索", _fmt_count_compact(unique), f"目标 {_fmt_count_compact(unique_target)}", unique_rate, colors["green"]),
            card(x0 + 900, y0, "抖音-来客订单", _fmt_int(orders), f"目标 {_fmt_int(orders_target)}", orders_rate, "url(#gold)"),
            card(x0 + 1350, y0, "累计实销", _fmt_int(deals), f"目标 {_fmt_int(deals_target)}", deals_rate, "url(#red)"),
        ]
    )

    svg.append(rect(54, 390, 1812, 104, "rgba(7,18,35,.72)", stroke="#23446F", rx=26))
    for idx, (x, title, big, color) in enumerate(
        [
            (96, "累计消耗", _fmt_money_compact(spend), colors["purple"]),
            (430, "总体 CPL", _fmt_money_compact(cpl), "#4BB3FF"),
            (748, "总体 CPS", _fmt_money_compact(cps), colors["red"]),
            (1075, "待交车", f"{_fmt_int(pending_day)} / {_fmt_int(pending_cumulative)}", "#35F6A5"),
            (1390, "数据口径", "客观数据 / 无主观判断", "#F6C85F"),
        ]
    ):
        svg.extend(
            [
                text(x, 430, title, 19, "#8396B6", 700),
                text(
                    x,
                    468,
                    big,
                    34,
                    color,
                    850,
                    family="DIN Alternate, Avenir Next Condensed, PingFang SC, sans-serif",
                ),
            ]
        )
    for x in [380, 698, 1028, 1338]:
        svg.append(f"<line x1='{x}' y1='410' x2='{x}' y2='474' stroke='#5D789F' stroke-width='1' opacity='.32'/>")

    main_y, main_h = 525, 455
    svg.extend(
        [
            rect(54, main_y, 870, main_h, "url(#panelGrad)", stroke=colors["line"], rx=32, extra="filter='url(#shadow)'"),
            section_title(88, main_y + 50, "全链路转化", "曝光 / 原始线索 / 唯一线索 / 来客订单 / 实销"),
        ]
    )
    funnel = [
        ("曝光", impressions, _fmt_wan(impressions), "url(#cyan)"),
        ("原始线索", raw_leads, _fmt_count_compact(raw_leads), "#18C6D9"),
        ("唯一线索", unique, _fmt_count_compact(unique), "url(#green)"),
        ("来客订单", orders, _fmt_int(orders), "url(#gold)"),
        ("实销", deals, _fmt_int(deals), "url(#red)"),
    ]
    funnel = [item for item in funnel if item[1] > 0]
    max_funnel = max([item[1] for item in funnel] + [1])
    for idx, (label, value, display, color) in enumerate(funnel[:5]):
        y = main_y + 135 + idx * 52
        svg.extend(
            [
                text(90, y + 21, label, 20, "#DBE8FF", 760),
                rect(206, y, 530, 24, "#1D2B43", rx=12),
                rect(206, y, max(4.0, 530 * _safe_log_ratio(value, max_funnel)), 24, color, rx=12),
                text(
                    850,
                    y + 21,
                    display,
                    24,
                    "#FFFFFF",
                    850,
                    "end",
                    family="DIN Alternate, Avenir Next Condensed, PingFang SC, sans-serif",
                ),
            ]
        )
    chip_values = [
        ("唯一率", quality_unique_rate or _safe_div(unique, raw_leads)),
        ("无主线索", unowned_leads),
        ("人工归属", manual_overrides),
    ]
    for idx, (label, value) in enumerate(chip_values):
        x = 92 + idx * 265
        svg.extend(
            [
                rect(x, main_y + 390, 224, 38, "rgba(180,210,255,.10)", stroke="#5C7FAE", rx=19),
                text(x + 20, main_y + 416, label, 17, colors["muted"], 700),
                text(x + 202, main_y + 416, _fmt_pct(value) if label == "唯一率" else _fmt_int(value), 22, "#FFFFFF", 850, "end"),
            ]
        )

    svg.extend(
        [
            rect(954, main_y, 912, main_h, "url(#panelGrad)", stroke=colors["line"], rx=32, extra="filter='url(#shadow)'"),
            section_title(988, main_y + 50, "来客订单拆解", "账号 / 主播累计订单、线索与实销"),
        ]
    )
    max_account_orders = max([item.get("mtd_douyin_laike_orders_actual", 0.0) for item in lead_accounts] + [1])
    for idx, item in enumerate(lead_accounts[:3]):
        x = 990
        y = main_y + 118 + idx * 74
        orders_actual = item.get("mtd_douyin_laike_orders_actual", 0.0)
        deals_actual = item.get("mtd_deals_actual", 0.0)
        leads_actual = item.get("mtd_unique_leads_actual", 0.0)
        svg.extend(
            [
                text(x, y + 17, str(item["name"]), 18, "#DDEAFF", 780),
                rect(x + 270, y, 318, 18, "#20314A", rx=9),
                rect(x + 270, y, 318 * _safe_div(orders_actual, max_account_orders), 18, colors["amber"], rx=9),
                text(x + 620, y + 18, f"订单 {_fmt_int(orders_actual)}", 19, colors["amber"], 850),
                text(x + 760, y + 18, f"线索 {_fmt_int(leads_actual)}", 18, "#8CA2C2", 760),
                text(x + 760, y + 46, f"实销 {_fmt_decimal(deals_actual)}", 18, "#8CA2C2", 760),
            ]
        )
    svg.append(text(990, main_y + 343, "主播订单 KPI", 18, "#90A8CA", 800))
    for idx, item in enumerate(lead_anchors[:3]):
        y = main_y + 368 + idx * 33
        order_rate = item.get("mtd_douyin_laike_orders_rate", 0.0)
        orders_actual = item.get("mtd_douyin_laike_orders_actual", 0.0)
        svg.extend(
            [
                text(990, y + 16, str(item["name"]), 16, "#D9E7FF", 800),
                rect(1090, y, 420, 18, "#3A2D17", rx=9),
                rect(1090, y, 420 * max(0.0, min(1.0, order_rate)), 18, colors["amber"], rx=9),
                text(1810, y + 18, f"{_fmt_int(orders_actual)} / {_fmt_pct(order_rate)}", 19, colors["amber"], 850, "end", family="DIN Alternate, Avenir Next Condensed, PingFang SC, sans-serif"),
            ]
        )

    bottom_y, bottom_h = 1015, 330
    svg.extend(
        [
            rect(54, bottom_y, 870, bottom_h, "url(#panelGrad)", stroke=colors["line"], rx=32, extra="filter='url(#shadow)'"),
            section_title(88, bottom_y + 50, "线索组主播累计：唯一线索 / 来客订单 KPI", "蓝条：累计唯一线索；金条：来客订单KPI"),
        ]
    )
    max_anchor_leads = max([item.get("mtd_unique_leads_actual", 0.0) for item in lead_anchors] + [1])
    for idx, item in enumerate(lead_anchors[:8]):
        y = bottom_y + 108 + idx * 25
        leads = item.get("mtd_unique_leads_actual", 0.0)
        order_rate = item.get("mtd_douyin_laike_orders_rate", 0.0)
        orders_actual = item.get("mtd_douyin_laike_orders_actual", 0.0)
        svg.extend(
            [
                text(88, y + 16, str(item["name"]), 16, "#E3EDFF", 760),
                rect(168, y, 286, 12, "#1F2E46", rx=6),
                rect(168, y, 286 * _safe_div(leads, max_anchor_leads), 12, colors["blue"], rx=6),
                text(478, y + 15, _fmt_int(leads), 16, "#DBE8FF", 760, "end"),
                rect(525, y, 140, 12, "#3A2D17", rx=6),
                rect(525, y, 140 * max(0.0, min(1.0, order_rate)), 12, colors["amber"], rx=6),
                text(688, y + 15, f"{_fmt_int(orders_actual)} / {_fmt_pct(order_rate)}", 16, "#FFCF75", 760),
            ]
        )

    svg.extend(
        [
            rect(954, bottom_y, 912, bottom_h, "url(#panelGrad)", stroke=colors["line"], rx=32, extra="filter='url(#shadow)'"),
            section_title(
                988,
                bottom_y + 50,
                "种草曝光累计目标达成",
                f"EXEED星途累计曝光 {_fmt_wan(seed_total := source.value('account', 'EXEED星途', 'mtd_impressions'))} / "
                f"目标 {_fmt_wan(seed_total_target := source.target('account', 'EXEED星途', 'mtd_impressions'))} / "
                f"达成 {_fmt_pct(source.rate('account', 'EXEED星途', 'mtd_impressions'))}",
            ),
        ]
    )
    max_seed_target = max([item.get("mtd_impressions_target", 0.0) for item in seed_anchors] + [1])
    for idx, item in enumerate(seed_anchors[:5]):
        y = bottom_y + 125 + idx * 39
        mtd = item.get("mtd_impressions_actual", 0.0)
        rate_value = item.get("mtd_impressions_rate", 0.0)
        svg.extend(
            [
                text(990, y + 17, str(item["name"]), 18, "#E3EDFF", 760),
                rect(1088, y, 500, 16, "#1F2E46", rx=8),
                rect(1088, y, 500 * _safe_div(mtd, max_seed_target), 16, colors["green"], rx=8),
                text(1710, y + 18, _fmt_wan(mtd), 18, "#DBE8FF", 760, "end", family="DIN Alternate, Avenir Next Condensed, PingFang SC, sans-serif"),
                text(1810, y + 18, _fmt_pct(rate_value), 18, "#35F6A5", 820, "end"),
            ]
        )
    svg.extend(
        [
            text(54, 1402, "数据来源：feishu_dashboard_source_latest_{date}.tsv。".format(date=report_date), 17, colors["muted_dark"], 600),
            "</svg>",
        ]
    )
    return "\n".join(svg)


def render_dashboard_page_svgs(
    table: pd.DataFrame,
    *,
    run_id: str = "",
    width: int = WIDTH,
    height: int = HEIGHT,
) -> dict[str, str]:
    """Render the fixed multi-page daily dashboard template set.

    P1 intentionally reuses the existing single-page dashboard to preserve the
    current daily report visual contract. Detail pages are backed by the
    same dashboard source long table.
    """

    source = _Source(table)
    return {
        "p1_overview": render_dashboard_svg(table, run_id=run_id, width=width, height=height),
        "p2_lead_detail": _render_lead_detail_page(source, run_id=run_id, width=width, height=height),
        "p3_seed_detail": _render_seed_detail_page(source, run_id=run_id, width=width, height=height),
        "p5_account_channel": _render_account_channel_page(source, run_id=run_id, width=width, height=height),
    }


def render_dashboard_long_compact_svg(
    table: pd.DataFrame,
    *,
    run_id: str = "",
    width: int = WIDTH,
    height: int = HEIGHT,
) -> str:
    """Render the fixed pages as one vertically cropped, compact long SVG."""

    pages = render_dashboard_page_svgs(table, run_id=run_id, width=width, height=height)
    crop_heights = _long_compact_page_heights(table, height=height)
    total_height = sum(crop_heights[spec.page_id] for spec in DASHBOARD_PAGE_SPECS)
    out = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{total_height}' viewBox='0 0 {width} {total_height}'>"
    ]
    y = 0
    for spec in DASHBOARD_PAGE_SPECS:
        crop_height = crop_heights[spec.page_id]
        out.append(
            f"<svg x='0' y='{y}' width='{width}' height='{crop_height}' "
            f"viewBox='0 0 {width} {crop_height}' overflow='hidden'>"
        )
        out.append(_svg_inner(pages[spec.page_id]))
        out.append("</svg>")
        y += crop_height
    out.append("</svg>")
    return "\n".join(out)


def write_dashboard_visual_page_files(
    table: pd.DataFrame,
    *,
    output_dir: Path,
    report_date: str,
    run_id: str = "",
    write_png: bool = True,
    width: int = WIDTH,
) -> dict[str, dict[str, Path]]:
    """Write fixed dashboard page SVGs and optional PNG derivatives."""

    output_dir.mkdir(parents=True, exist_ok=True)
    converter = shutil.which("rsvg-convert")
    pages = render_dashboard_page_svgs(table, run_id=run_id, width=WIDTH, height=HEIGHT)
    written: dict[str, dict[str, Path]] = {}
    for spec in DASHBOARD_PAGE_SPECS:
        svg_path = output_dir / f"feishu_dashboard_visual_{spec.page_id}_latest_{report_date}.svg"
        svg_path.write_text(pages[spec.page_id], encoding="utf-8")
        written[spec.page_id] = {"svg": svg_path}
        if write_png and converter:
            png_path = output_dir / f"feishu_dashboard_visual_{spec.page_id}_latest_{report_date}.png"
            subprocess.run([converter, "--width", str(width), str(svg_path), "-o", str(png_path)], check=True)
            written[spec.page_id]["png"] = png_path
    return written


def write_dashboard_visual_long_compact_files(
    table: pd.DataFrame,
    *,
    svg_path: Path,
    png_path: Path | None = None,
    run_id: str = "",
    width: int = WIDTH,
) -> dict[str, Path]:
    """Write the fixed compact long image SVG and optional PNG derivative."""

    svg_path.parent.mkdir(parents=True, exist_ok=True)
    svg_path.write_text(render_dashboard_long_compact_svg(table, run_id=run_id, width=WIDTH, height=HEIGHT), encoding="utf-8")
    written = {"svg": svg_path}
    converter = shutil.which("rsvg-convert")
    if png_path and converter:
        png_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([converter, "--width", str(width), str(svg_path), "-o", str(png_path)], check=True)
        written["png"] = png_path
    return written


def write_dashboard_visual_files(
    table: pd.DataFrame,
    *,
    svg_path: Path,
    png_path: Path | None = None,
    run_id: str = "",
    width: int = WIDTH,
) -> dict[str, Path]:
    """Write SVG and, when rsvg-convert is installed, a PNG derivative."""

    svg_path.parent.mkdir(parents=True, exist_ok=True)
    svg_path.write_text(render_dashboard_svg(table, run_id=run_id, width=WIDTH, height=HEIGHT), encoding="utf-8")
    written = {"svg": svg_path}
    converter = shutil.which("rsvg-convert")
    if png_path and converter:
        png_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([converter, "--width", str(width), str(svg_path), "-o", str(png_path)], check=True)
        written["png"] = png_path
    return written


def _long_compact_page_heights(table: pd.DataFrame, *, height: int = HEIGHT) -> dict[str, int]:
    source = _Source(table)
    fields = DASHBOARD_PAGE_FIELD_SPECS
    lead_scope_fields = fields["p2_lead_detail"]["scope_metrics"]
    lead_anchor_fields = fields["p2_lead_detail"]["anchor_metrics"]
    seed_fields = fields["p3_seed_detail"]["scope_metrics"]
    account_fields = fields["p5_account_channel"]["lead_account_metrics"]
    seed_account_fields = fields["p5_account_channel"]["seed_account_metrics"]

    row_counts = {
        "p2_lead_detail": [
            len(source.scopes("lead_account", "account", _metric_keys(lead_scope_fields))[:6]),
            len(source.scopes("lead_anchor", "anchor", _metric_keys(lead_anchor_fields))[:14]),
        ],
        "p3_seed_detail": [
            len(source.scopes("seed_account", "account", _metric_keys(seed_fields))[:8]),
            len(source.scopes("seed_anchor", "anchor", _metric_keys(seed_fields))[:14]),
        ],
        "p5_account_channel": [
            len(source.scopes("lead_account", "account", _metric_keys(account_fields))[:8])
            + len(source.scopes("seed_account", "account", _metric_keys(seed_account_fields))[:6])
        ],
    }
    out = {"p1_overview": height}
    for page_id, counts in row_counts.items():
        content_height = _compact_table_content_height(counts)
        out[page_id] = min(height, max(LONG_COMPACT_MIN_HEIGHTS[page_id], content_height))
    return out


def _compact_table_content_height(row_counts: list[int]) -> int:
    y = 245
    for row_count in row_counts:
        visible_row_count = min(max(row_count, 1), 12)
        section_height = max(260, 142 + visible_row_count * 47)
        y += section_height + 34
    return y + 36


def _svg_inner(svg: str) -> str:
    return svg.split(">", 1)[1].rsplit("</svg>", 1)[0]


def _render_lead_detail_page(
    source: "_Source",
    *,
    run_id: str = "",
    width: int = WIDTH,
    height: int = HEIGHT,
) -> str:
    fields = DASHBOARD_PAGE_FIELD_SPECS["p2_lead_detail"]["scope_metrics"]
    anchor_fields = DASHBOARD_PAGE_FIELD_SPECS["p2_lead_detail"]["anchor_metrics"]
    accounts = source.scopes(
        "lead_account",
        "account",
        _metric_keys(fields),
    )
    anchors = source.scopes(
        "lead_anchor",
        "anchor",
        _metric_keys(anchor_fields),
    )
    rows = _scope_rows(accounts[:6], fields)
    anchor_rows = _scope_rows(anchors[:14], anchor_fields)
    return _render_table_page(
        source,
        page_id="p2_lead_detail",
        title="P2 线索组明细",
        subtitle="账号与主播的唯一线索、来客订单、实销、个人消耗、费用效率",
        sections=[
            ("线索组账号", ["账号", "归属", *[field.header for field in fields]], rows),
            ("线索组主播", ["主播", "归属账号", *[field.header for field in anchor_fields]], anchor_rows),
        ],
        run_id=run_id,
        width=width,
        height=height,
    )


def _render_seed_detail_page(
    source: "_Source",
    *,
    run_id: str = "",
    width: int = WIDTH,
    height: int = HEIGHT,
) -> str:
    fields = DASHBOARD_PAGE_FIELD_SPECS["p3_seed_detail"]["scope_metrics"]
    accounts = source.scopes("seed_account", "account", _metric_keys(fields))
    anchors = source.scopes("seed_anchor", "anchor", _metric_keys(fields))
    account_rows = _scope_rows(accounts[:8], fields)
    anchor_rows = _scope_rows(anchors[:14], fields)
    return _render_table_page(
        source,
        page_id="p3_seed_detail",
        title="P3 种草组明细",
        subtitle="账号与主播的当日曝光、累计曝光、目标达成",
        sections=[
            ("种草账号", ["账号", "归属", *[field.header for field in fields]], account_rows),
            ("种草主播", ["主播", "归属账号", *[field.header for field in fields]], anchor_rows),
        ],
        run_id=run_id,
        width=width,
        height=height,
    )


def _render_account_channel_page(
    source: "_Source",
    *,
    run_id: str = "",
    width: int = WIDTH,
    height: int = HEIGHT,
) -> str:
    lead_fields = DASHBOARD_PAGE_FIELD_SPECS["p5_account_channel"]["lead_account_metrics"]
    seed_fields = DASHBOARD_PAGE_FIELD_SPECS["p5_account_channel"]["seed_account_metrics"]
    lead_accounts = source.scopes(
        "lead_account",
        "account",
        _metric_keys(lead_fields),
    )
    seed_accounts = source.scopes("seed_account", "account", _metric_keys(seed_fields))
    account_rows = [
        [
            str(item["name"]),
            "线索账号",
            *[_metric_cell(item, field) for field in lead_fields],
        ]
        for item in lead_accounts[:8]
    ]
    account_rows.extend(
        [
            [
                str(item["name"]),
                "种草账号",
                *[_metric_cell(item, field) for field in seed_fields],
                "-",
                "-",
                "-",
                "-",
            ]
            for item in seed_accounts[:6]
        ]
    )
    return _render_table_page(
        source,
        page_id="p5_account_channel",
        title="P4 账号拆解",
        subtitle="账号层当日、累计、订单、实销与费用效率",
        sections=[
            ("账号拆解", ["账号", "类型", *[field.header for field in lead_fields]], account_rows),
        ],
        run_id=run_id,
        width=width,
        height=height,
    )


def _metric_keys(fields: tuple[DashboardFieldSpec, ...]) -> list[str]:
    return list(dict.fromkeys(field.metric_key for field in fields))


def _scope_rows(items: list[dict[str, object]], fields: tuple[DashboardFieldSpec, ...]) -> list[list[str]]:
    rows: list[list[str]] = []
    for item in items:
        rows.append(
            [
                str(item["name"]),
                str(item.get("parent_scope", "")) or "-",
                *[_metric_cell(item, field) for field in fields],
            ]
        )
    return rows


def _metric_cell(item: dict[str, object], field: DashboardFieldSpec) -> str:
    present_key = f"{field.metric_key}_{field.value_kind}_present"
    if not item.get(present_key, False):
        if field.formatter == "pct_or_na" and _metric_target_is_not_applicable(item, field.metric_key):
            return "N/A"
        return field.placeholder
    value = float(item.get(f"{field.metric_key}_{field.value_kind}", 0.0) or 0.0)
    if field.formatter == "pct_or_na":
        target = float(item.get(f"{field.metric_key}_target", 0.0) or 0.0)
        return _fmt_pct_or_na(value, target)
    return _format_metric_value(value, field.formatter)


def _metric_target_is_not_applicable(item: dict[str, object], metric_key: str) -> bool:
    if not item.get(f"{metric_key}_target_present", False):
        return False
    return float(item.get(f"{metric_key}_target", 0.0) or 0.0) <= 0


def _format_metric_value(value: float, formatter: str) -> str:
    if formatter == "int":
        return _fmt_int(value)
    if formatter == "decimal":
        return _fmt_decimal(value)
    if formatter == "wan":
        return _fmt_wan(value)
    if formatter == "money":
        return _fmt_money_compact(value)
    if formatter == "pct":
        return _fmt_pct(value)
    return str(value)


def _render_table_page(
    source: "_Source",
    *,
    page_id: str,
    title: str,
    subtitle: str,
    sections: list[tuple[str, list[str], list[list[str]]]],
    run_id: str = "",
    width: int = WIDTH,
    height: int = HEIGHT,
) -> str:
    svg = _svg_base(width, height)
    svg.extend(_svg_header(title, page_id))
    svg.append(_svg_text(84, 186, subtitle, 24, "#9EB5D6", 720))
    y = 245
    for section_title, headers, rows in sections:
        visible_rows = rows[:12]
        row_count = min(max(len(visible_rows), 1), 12)
        section_height = max(260, 142 + row_count * 47)
        svg.append(_svg_rect(54, y, 1812, section_height, "url(#panelGrad)", stroke="#31527D", rx=30, extra="filter='url(#shadow)'"))
        svg.append(_svg_text(88, y + 52, section_title, 32, "#F4F8FF", 850))
        svg.extend(_svg_table(88, y + 84, 1744, headers, visible_rows))
        if not rows:
            svg.append(_svg_text(110, y + 156, "当前数据源未提供该模块明细行", 22, "#8CA2C2", 650))
        if len(rows) > len(visible_rows):
            svg.append(_svg_text(110, y + section_height - 22, f"仅展示前 {len(visible_rows)} 行", 16, "#7F96BA", 650))
        y += section_height + 34
    footer = f"数据来源：日报驾驶舱数据源 {source.report_date}"
    if run_id:
        footer = f"{footer} / run_id={run_id}"
    svg.append(_svg_text(54, 1402, footer, 17, "#6F86A8", 600))
    svg.append("</svg>")
    return "\n".join(svg)


def _svg_base(width: int, height: int) -> list[str]:
    return [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>",
        "<defs>"
        "<linearGradient id='bg' x1='0' y1='0' x2='1' y2='1'>"
        "<stop offset='0' stop-color='#06101F'/><stop offset='.48' stop-color='#09213E'/>"
        "<stop offset='1' stop-color='#031923'/></linearGradient>"
        "<radialGradient id='glowA' cx='12%' cy='0%' r='70%'>"
        "<stop offset='0' stop-color='#1B5CFF' stop-opacity='.40'/><stop offset='1' stop-color='#1B5CFF' stop-opacity='0'/>"
        "</radialGradient>"
        "<radialGradient id='glowB' cx='96%' cy='8%' r='72%'>"
        "<stop offset='0' stop-color='#00D5FF' stop-opacity='.30'/><stop offset='1' stop-color='#00D5FF' stop-opacity='0'/>"
        "</radialGradient>"
        "<linearGradient id='panelGrad' x1='0' y1='0' x2='1' y2='1'>"
        "<stop offset='0' stop-color='#101D35' stop-opacity='.90'/><stop offset='1' stop-color='#071629' stop-opacity='.96'/>"
        "</linearGradient>"
        "<filter id='shadow' x='-20%' y='-20%' width='140%' height='140%'>"
        "<feDropShadow dx='0' dy='18' stdDeviation='24' flood-color='#000814' flood-opacity='.42'/></filter>"
        "<pattern id='grid' width='48' height='48' patternUnits='userSpaceOnUse'>"
        "<path d='M 48 0 L 0 0 0 48' fill='none' stroke='#6EA8FF' stroke-width='.6' opacity='.11'/>"
        "</pattern>"
        "</defs>",
        _svg_rect(0, 0, width, height, "url(#bg)", rx=0),
        _svg_rect(0, 0, width, height, "url(#glowA)", rx=0),
        _svg_rect(0, 0, width, height, "url(#glowB)", rx=0),
        _svg_rect(0, 0, width, height, "url(#grid)", rx=0, opacity=0.55),
    ]


def _svg_header(title: str, active_page_id: str) -> list[str]:
    out = [
        _svg_rect(44, 34, 1832, 132, "rgba(8,19,38,.76)", stroke="#3F68A8", sw=1.2, rx=34, extra="filter='url(#shadow)'"),
        _svg_text(84, 104, "数据驾驶舱", 54, "#FFFFFF", 850),
        _svg_text(88, 140, title, 18, "#8EA6CA", 720),
    ]
    x = 952
    for idx, spec in enumerate(DASHBOARD_PAGE_SPECS, start=1):
        active = spec.page_id == active_page_id
        out.append(_svg_nav_chip(x, 78, f"{idx:02d}", spec.nav_label, active))
        x += 176
    return out


def _svg_table(x: float, y: float, w: float, headers: list[str], rows: list[list[str]]) -> list[str]:
    col_widths = _svg_table_column_widths(headers, w)
    col_starts = [x]
    for col_width in col_widths[:-1]:
        col_starts.append(col_starts[-1] + col_width)
    out = [_svg_rect(x, y, w, 44, "rgba(79,140,255,.13)", stroke="#2B4B75", rx=14)]
    for idx, header in enumerate(headers):
        col_width = col_widths[idx]
        anchor = _svg_table_column_anchor(header)
        text_x = _svg_table_cell_x(col_starts[idx], col_width, anchor)
        out.append(_svg_text(text_x, y + 29, _fit_svg_cell_text(header, col_width), 18, "#94ACCF", 760, anchor=anchor))
    for row_idx, row in enumerate(rows):
        row_y = y + 54 + row_idx * 47
        fill = "rgba(255,255,255,.045)" if row_idx % 2 == 0 else "rgba(255,255,255,.025)"
        out.append(_svg_rect(x, row_y, w, 39, fill, stroke="rgba(83,118,166,.22)", rx=12))
        for col_idx, value in enumerate(row[: len(headers)]):
            col_width = col_widths[col_idx]
            anchor = _svg_table_column_anchor(headers[col_idx])
            text_x = _svg_table_cell_x(col_starts[col_idx], col_width, anchor)
            color = "#E3EDFF" if anchor == "start" else "#FFFFFF"
            out.append(_svg_text(text_x, row_y + 26, _fit_svg_cell_text(value, col_width), 18, color, 740, anchor=anchor))
    return out


def _svg_table_column_widths(headers: list[str], total_width: float) -> list[float]:
    count = max(len(headers), 1)
    if count >= 8 and len(headers) >= 2 and headers[0] in {"账号", "主播"}:
        weights = [1.25, 1.75, *([0.9] * (count - 2))]
        weights[-1] = 1.05
    elif count >= 7 and len(headers) >= 2 and headers[0] in {"账号", "主播"}:
        weights = [1.35, 1.55, *([1.0] * (count - 2))]
    elif headers and headers[0] == "分组":
        weights = [1.15, *([1.0] * (count - 1))]
    elif headers and headers[0] == "指标":
        weights = [1.25, *([1.0] * (count - 1))]
    else:
        weights = [1.0] * count
    scale = total_width / sum(weights)
    return [weight * scale for weight in weights]


def _svg_table_column_anchor(header: str) -> str:
    return "start" if header in {"账号", "归属", "主播", "归属账号", "类型", "分组", "指标"} else "middle"


def _svg_table_cell_x(col_start: float, col_width: float, anchor: str) -> float:
    if anchor == "middle":
        return col_start + col_width / 2
    return col_start + 18


def _fit_svg_cell_text(value: object, col_width: float, *, font_size: int = 18) -> str:
    text_value = str(value)
    max_units = max(8, int((col_width - 32) / (font_size * 0.47)))
    if _display_units(text_value) <= max_units:
        return text_value
    target_units = max_units - 1
    current_units = 0
    chars: list[str] = []
    for char in text_value:
        char_units = _display_units(char)
        if current_units + char_units > target_units:
            break
        chars.append(char)
        current_units += char_units
    return "".join(chars).rstrip() + "…"


def _display_units(value: str) -> int:
    return sum(2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1 for char in value)


def _svg_nav_chip(x: float, y: float, index: str, label: str, active: bool = False) -> str:
    fill = "rgba(31,243,255,.16)" if active else "rgba(255,255,255,.045)"
    stroke = "#49D7FF" if active else "rgba(124,164,220,.28)"
    label_color = "#DFF8FF" if active else "#9FB4D2"
    return "".join(
        [
            _svg_rect(x, y, 152, 40, fill, stroke=stroke, sw=1.1, rx=20),
            _svg_text(x + 20, y + 26, index, 14, "#64D7FF" if active else "#61799E", 850),
            _svg_text(x + 52, y + 26, label, 17, label_color, 780),
        ]
    )


def _svg_rect(
    x: float,
    y: float,
    w: float,
    h: float,
    fill: str,
    stroke: str = "none",
    sw: float = 1,
    rx: float = 18,
    opacity: float | None = None,
    extra: str = "",
) -> str:
    opacity_attr = f" opacity='{opacity}'" if opacity is not None else ""
    extra_attr = f" {extra}" if extra else ""
    return (
        f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='{rx}' fill='{fill}' "
        f"stroke='{stroke}' stroke-width='{sw}'{opacity_attr}{extra_attr}/>"
    )


def _svg_text(
    x: float,
    y: float,
    value: object,
    size: int = 28,
    fill: str = "#E8F1FF",
    weight: int = 500,
    anchor: str = "start",
    opacity: float | None = None,
    family: str | None = None,
) -> str:
    opacity_attr = f" opacity='{opacity}'" if opacity is not None else ""
    family_attr = family or "Avenir Next, PingFang SC, Hiragino Sans GB, Microsoft YaHei, sans-serif"
    return (
        f"<text x='{x}' y='{y}' font-family='{family_attr}' font-size='{size}' "
        f"font-weight='{weight}' fill='{fill}' text-anchor='{anchor}'{opacity_attr}>{_esc(value)}</text>"
    )


class _Source:
    def __init__(self, table: pd.DataFrame) -> None:
        self.table = table.copy()
        self.report_date = ""
        if not self.table.empty and "report_date" in self.table.columns:
            self.report_date = str(self.table["report_date"].iloc[0])

    def value(self, scope_type: str, scope_name: str, metric_key: str) -> float:
        return self._metric(scope_type, scope_name, metric_key, "actual")

    def target(self, scope_type: str, scope_name: str, metric_key: str) -> float:
        return self._metric(scope_type, scope_name, metric_key, "target")

    def rate(self, scope_type: str, scope_name: str, metric_key: str) -> float:
        return self._metric(scope_type, scope_name, metric_key, "attain_rate")

    def has_metric(self, scope_type: str, scope_name: str, metric_key: str, column: str = "actual") -> bool:
        if self.table.empty:
            return False
        required = {"scope_type", "scope_name", "metric_key", column}
        if not required.issubset(self.table.columns):
            return False
        matched = self.table[
            self.table["scope_type"].astype(str).eq(scope_type)
            & self.table["scope_name"].astype(str).eq(scope_name)
            & self.table["metric_key"].astype(str).eq(metric_key)
        ]
        if matched.empty:
            return False
        value = matched.iloc[0][column]
        try:
            if value is None or pd.isna(value):
                return False
        except TypeError:
            pass
        return str(value).strip() not in {"", "nan", "NaN", "None"}

    def anchors(self, source_table: str, metric_keys: list[str]) -> list[dict[str, object]]:
        return self.scopes(source_table, "anchor", metric_keys)

    def scopes(self, source_table: str, scope_type: str, metric_keys: list[str]) -> list[dict[str, object]]:
        if self.table.empty or "source_table" not in self.table.columns:
            return []
        subset = self.table[
            self.table["source_table"].astype(str).eq(source_table)
            & self.table["scope_type"].astype(str).eq(scope_type)
        ].copy()
        if subset.empty:
            return []
        names = list(dict.fromkeys(subset["scope_name"].astype(str).tolist()))
        out: list[dict[str, object]] = []
        for name in names:
            matched = subset[subset["scope_name"].astype(str).eq(name)]
            parent_scope = ""
            if "parent_scope" in matched.columns and not matched.empty:
                parent_scope = _text_or_blank(matched.iloc[0].get("parent_scope", ""))
            item: dict[str, object] = {"name": name, "parent_scope": parent_scope}
            for metric_key in metric_keys:
                item[f"{metric_key}_actual"] = self.value(scope_type, name, metric_key)
                item[f"{metric_key}_target"] = self.target(scope_type, name, metric_key)
                item[f"{metric_key}_rate"] = self.rate(scope_type, name, metric_key)
                item[f"{metric_key}_actual_present"] = self.has_metric(scope_type, name, metric_key, "actual")
                item[f"{metric_key}_target_present"] = self.has_metric(scope_type, name, metric_key, "target")
                item[f"{metric_key}_rate_present"] = self.has_metric(scope_type, name, metric_key, "attain_rate")
            out.append(item)
        return out

    def _metric(self, scope_type: str, scope_name: str, metric_key: str, column: str) -> float:
        if self.table.empty:
            return 0.0
        required = {"scope_type", "scope_name", "metric_key", column}
        if not required.issubset(self.table.columns):
            return 0.0
        matched = self.table[
            self.table["scope_type"].astype(str).eq(scope_type)
            & self.table["scope_name"].astype(str).eq(scope_name)
            & self.table["metric_key"].astype(str).eq(metric_key)
        ]
        if matched.empty:
            return 0.0
        return _num(matched.iloc[0][column])


def _esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def _text_or_blank(value: object) -> str:
    try:
        if value is None or pd.isna(value):
            return ""
    except TypeError:
        pass
    text_value = str(value).strip()
    return "" if text_value in {"nan", "NaN", "None"} else text_value


def _num(value: object) -> float:
    try:
        if value is None or pd.isna(value):
            return 0.0
    except TypeError:
        pass
    text_value = str(value).strip().replace(",", "")
    if not text_value or text_value in {"-", "N/A", "nan", "NaN"}:
        return 0.0
    if text_value.endswith("%"):
        text_value = text_value[:-1]
        scale = 0.01
    else:
        scale = 1.0
    try:
        return float(text_value) * scale
    except ValueError:
        return 0.0


def _fmt_int(value: float) -> str:
    return f"{value:,.0f}"


def _fmt_decimal(value: float) -> str:
    return f"{value:,.2f}"


def _fmt_wan(value: float) -> str:
    return f"{value / 10000:.2f}万"


def _fmt_money(value: float) -> str:
    return f"¥{value:,.2f}"


def _fmt_count_compact(value: float) -> str:
    if abs(value) >= 10000:
        return _fmt_wan(value)
    return _fmt_int(value)


def _fmt_money_compact(value: float) -> str:
    if abs(value) >= 10000:
        return f"¥{value / 10000:.2f}万"
    return _fmt_money(value)


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _fmt_pct_or_na(value: float, target: float) -> str:
    if target <= 0:
        return "N/A"
    return _fmt_pct(value)


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _safe_log_ratio(value: float, max_value: float) -> float:
    if value <= 0 or max_value <= 0:
        return 0.0
    import math

    return math.log10(value + 1) / math.log10(max_value + 1)
