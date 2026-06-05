import pandas as pd

from oae.exports.feishu_content import ReportContext, build_markdown_content, build_tsv_content
from oae.exports.feishu_topline import FullAccountTopline, SegmentTopline, ToplineSummary


def test_markdown_content_includes_standalone_douyin_laike_order_breakdown() -> None:
    ctx = _report_context()

    content = build_markdown_content(ctx)

    assert (
        "**抖音-来客订单数**\n"
        "- 累计订单数：19\n"
        "- 订单目标：1000，达成率：1.90%\n"
        "- 累计订单数（账号）：星途汽车官方直播间(15)、星途汽车直播营销中心(4)\n"
        "- 累计订单数（主播）：丁俐佳(4)、侯翩翩(2)"
    ) in content
    assert content.index("**EX7 专项**") < content.index("**抖音-来客订单数**") < content.index("**成交账号**")


def test_tsv_content_includes_standalone_douyin_laike_order_breakdown() -> None:
    ctx = _report_context()

    content = build_tsv_content(ctx)

    assert (
        "抖音-来客订单数\n"
        "累计订单数\t19\n"
        "订单目标\t1000\n"
        "订单达成率\t1.90%\n"
        "累计订单数（账号）\t星途汽车官方直播间(15)、星途汽车直播营销中心(4)\n"
        "累计订单数（主播）\t丁俐佳(4)、侯翩翩(2)"
    ) in content
    assert content.index("EX7 专项") < content.index("抖音-来客订单数") < content.index("成交账号\t结果")


def _report_context() -> ReportContext:
    summary = ToplineSummary(
        full_account=FullAccountTopline(
            impression_target=25_000_000,
            impression_actual=282_800,
            impression_attain=0.0113,
            lead_target=0,
            lead_actual=113,
            lead_attain=None,
            deal_target=100,
            deal_actual=14,
            deal_attain=0.14,
            cpl_target=0,
            cpl_actual=0,
            cps_target=1500,
            cps_actual=0,
            pending_day=1,
            pending_cumulative=3,
        ),
        excluding_ex7=SegmentTopline(label="不含 EX7", leads=85, deals=8, cpl_actual=0, cps_actual=0),
        ex7=SegmentTopline(label="EX7 专项", leads=28, deals=6, cpl_actual=0, cps_actual=0),
    )
    setattr(summary, "douyin_laike_orders", 19.0)
    setattr(summary, "douyin_laike_order_target", 1000.0)
    account_table = pd.DataFrame(
        [
            {"账号": "星途汽车官方直播间", "抖音-来客订单数": "15"},
            {"账号": "星途汽车直播营销中心", "抖音-来客订单数": "4"},
            {"账号": "星途汽车直营中心", "抖音-来客订单数": "0"},
            {"账号": "线索组汇总", "抖音-来客订单数": "19"},
        ]
    )
    anchor_table = pd.DataFrame(
        [
            {"主播": "丁俐佳", "归属账号": "星途汽车官方直播间", "抖音-来客订单数": "4"},
            {"主播": "侯翩翩", "归属账号": "星途汽车直播营销中心", "抖音-来客订单数": "2"},
            {"主播": "徐欣悦", "归属账号": "星途汽车直播营销中心", "抖音-来客订单数": "0"},
        ]
    )
    return ReportContext(
        report_date_str="2026-06-04",
        topline_summary=summary,
        day_target_deal_accounts="-",
        mtd_target_deal_accounts="-",
        mtd_all_deal_accounts="-",
        day_target_pending_accounts="-",
        mtd_target_pending_accounts="-",
        mtd_all_pending_accounts="-",
        lead_quality_line="-",
        acc_out=account_table,
        anc_out=anchor_table,
        acc_tsv_out=account_table,
        anc_tsv_out=anchor_table,
    )
