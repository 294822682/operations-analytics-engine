from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from oae.exports import feishu_report
from oae.exports.feishu_seed_dashboard import (
    build_seed_dashboard_tables,
    load_seed_monthly_targets,
    load_seed_sessions_from_workbooks,
)
from oae.exports.feishu_topline import FullAccountTopline, SegmentTopline, ToplineSummary
from oae.quality import tsv_verify


def test_build_seed_dashboard_tables_uses_seed_target_pool_contract(tmp_path: Path) -> None:
    targets_path = tmp_path / "seed_monthly_targets.csv"
    targets_path.write_text(
        "\n".join(
            [
                "month,scope_type,scope_name,parent_scope,parent_account,impression_target_month,spend_target_month,cpm_target,target_pool",
                "2026-06,account,EXEED星途,,,28000,,,种草组目标池",
                "2026-06,host,刘花旗,,EXEED星途,9000,,,种草组目标池",
                "2026-06,host,桂婕,,EXEED星途,6000,,,种草组目标池",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    seed_workbook = tmp_path / "EXEED星途台账（六月）.xlsx"
    pd.DataFrame(
        [
            {"创建时间": "2026-06-01", "开播账号": "抖音-EXEED星途", "本场主播": "刘花旗", "曝光人数": 1000},
            {"创建时间": "2026-06-02", "开播账号": "星途驭见星豪华", "本场主播": "陶雯义", "曝光人数": 3000},
            {"创建时间": "2026-06-03", "开播账号": "抖音-EXEED星途", "本场主播": "刘花旗", "曝光人数": 5000},
            {"创建时间": "2026-06-03", "开播账号": "抖音-EXEED星途", "本场主播": "陶雯义", "曝光人数": 7000},
        ]
    ).to_excel(seed_workbook, index=False)

    targets = load_seed_monthly_targets(targets_path)
    sessions = load_seed_sessions_from_workbooks([seed_workbook])
    seed_account, seed_anchor = build_seed_dashboard_tables(
        report_date="2026-06-03",
        seed_sessions=sessions,
        seed_targets=targets,
    )

    assert seed_account.to_dict("records") == [
        {
            "账号": "EXEED星途",
            "当日曝光": 12000.0,
            "当日曝光目标": 857.1428571428571,
            "当日曝光达成率": 14.0,
            "累计曝光": 16000.0,
            "曝光目标": 28000.0,
            "累计曝光达成率": 0.5714285714285714,
        }
    ]

    liu = seed_anchor[seed_anchor["主播"].eq("刘花旗")].iloc[0].to_dict()
    assert liu == {
        "主播": "刘花旗",
        "归属账号": "EXEED星途",
        "当日曝光": 5000.0,
        "当日曝光目标": 285.7142857142857,
        "当日曝光达成率": 17.5,
        "累计曝光": 6000.0,
        "曝光目标": 9000.0,
        "累计曝光达成率": 0.6666666666666666,
    }

    gui = seed_anchor[seed_anchor["主播"].eq("桂婕")].iloc[0].to_dict()
    assert gui["归属账号"] == "EXEED星途"
    assert gui["当日曝光"] == 0.0
    assert gui["累计曝光"] == 0.0
    assert gui["曝光目标"] == 6000.0


def test_feishu_report_writes_seed_rows_to_dashboard_source(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "output" / "sql_reports"
    reports_dir.mkdir(parents=True)
    export_dir = tmp_path / "artifacts" / "exports"
    fact_path = tmp_path / "output" / "fact_attribution.csv"
    fact_path.write_text("fixture\n", encoding="utf-8")
    live_path = tmp_path / "dynamic" / "2026年直播进度表.xlsx"
    live_path.parent.mkdir(parents=True)
    pd.DataFrame([{"日期": "2026-06-03", "消耗": 0, "曝光人数": 0, "车型": "EX7"}]).to_excel(live_path, index=False)
    leads_path = tmp_path / "dynamic" / "总部新媒体线索.csv"
    leads_path.write_text("线索ID,创建日期\nL1,2026-06-03\n", encoding="utf-8-sig")
    deals_path = tmp_path / "dynamic" / "总部新媒体成交.csv"
    deals_path.write_text("线索ID,订单状态\nL1,已交车\n", encoding="utf-8-sig")
    topline_config_path = tmp_path / "config" / "report_topline_config.json"
    topline_config_path.parent.mkdir(parents=True)
    topline_config_path.write_text(
        '{"full_account_targets":{"impressions":1,"leads":1,"deals":1,"cpl":1,"cps":1},"ex7_rules":{"keywords":["EX7"],"lead_model_field_candidates":["车型"],"deal_model_field_candidates":["车型"],"live_model_field_candidates":["车型"]},"pending_rules":{"primary_date_field":"下订日期","fallback_date_fields":[]}}',
        encoding="utf-8",
    )
    seed_targets_path = tmp_path / "config" / "seed_monthly_targets.csv"
    seed_targets_path.write_text(
        "\n".join(
            [
                "month,scope_type,scope_name,parent_scope,parent_account,impression_target_month,spend_target_month,cpm_target,target_pool",
                "2026-06,account,EXEED星途,,,28000,,,种草组目标池",
                "2026-06,host,刘花旗,,EXEED星途,9000,,,种草组目标池",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    seed_workbook = tmp_path / "dynamic" / "EXEED星途台账（六月）.xlsx"
    pd.DataFrame(
        [
            {"创建时间": "2026-06-02", "开播账号": "抖音-EXEED星途", "本场主播": "刘花旗", "曝光人数": 3000},
            {"创建时间": "2026-06-03", "开播账号": "抖音-EXEED星途", "本场主播": "刘花旗", "曝光人数": 5000},
        ]
    ).to_excel(seed_workbook, index=False)
    snapshot_path = tmp_path / "artifacts" / "snapshots" / "daily_performance_snapshot_latest_2026-06-03.csv"
    snapshot_path.parent.mkdir(parents=True)
    pd.DataFrame([_snapshot_row("account_total", "线索组汇总"), _snapshot_row("anchor", "丁俐佳", parent_account="抖音-星途汽车官方直播间")]).to_csv(
        snapshot_path,
        index=False,
        encoding="utf-8-sig",
    )

    fact = pd.DataFrame(
        [
            {
                "is_deal": 0,
                "成交时间": "",
                "标准账号": "抖音-星途汽车官方直播间",
                "线索ID_norm": "L1",
                "date": "2026-06-03",
                "归属状态": "直接归属",
            }
        ]
    )
    fact.attrs["manual_override_summary"] = {}
    visual_calls = []

    def fake_write_dashboard_visual_long_compact_files(
        table: pd.DataFrame,
        *,
        svg_path: Path,
        png_path: Path | None = None,
        run_id: str = "",
    ) -> dict[str, Path]:
        visual_calls.append(
            {
                "source_tables": set(table["source_table"].astype(str)),
                "svg_path": svg_path,
                "png_path": png_path,
                "run_id": run_id,
            }
        )
        svg_path.parent.mkdir(parents=True, exist_ok=True)
        svg_path.write_text("<svg>fixture</svg>", encoding="utf-8")
        written = {"svg": svg_path}
        if png_path is not None:
            png_path.write_text("png", encoding="utf-8")
            written["png"] = png_path
        return written

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(feishu_report, "parse_args", lambda: SimpleNamespace(
        reports_dir=str(reports_dir),
        fact_csv=str(fact_path),
        manual_override_file="",
        live_file=str(live_path),
        leads_file=str(leads_path),
        deals_file=str(deals_path),
        topline_config=str(topline_config_path),
        report_date="2026-06-03",
        snapshot_csv=str(snapshot_path),
        ledger_csv="",
        analysis_snapshot_csv="",
        export_dir=str(export_dir),
        run_id="run-test",
        schema_version="schema",
        metric_version="metric",
        template_version="template",
        freeze_id="freeze",
        output_md=str(reports_dir / "feishu_report_latest_2026-06-03.md"),
        output_tsv=str(reports_dir / "feishu_table_latest_2026-06-03.tsv"),
        output_dashboard_source_tsv=str(reports_dir / "feishu_dashboard_source_latest_2026-06-03.tsv"),
        output_dashboard_visual_svg=str(reports_dir / "feishu_dashboard_visual_p1_p5_long_compact_latest_2026-06-03.svg"),
        output_dashboard_visual_png=str(reports_dir / "feishu_dashboard_visual_p1_p5_long_compact_latest_2026-06-03.png"),
        skip_dashboard_visual_png=False,
        seed_targets_file=str(seed_targets_path),
        seed_workbook_file="",
    ))
    monkeypatch.setattr(feishu_report, "load_fact_with_manual_overrides", lambda *args, **kwargs: fact)
    monkeypatch.setattr(feishu_report, "_attach_douyin_laike_order_metrics", lambda **kwargs: (kwargs["account_panel"], kwargs["anchor_panel"], 0.0))
    monkeypatch.setattr(feishu_report, "build_topline_summary", lambda **kwargs: _topline_summary())
    monkeypatch.setattr(feishu_report, "deal_accounts_text", lambda **kwargs: ("无", "无", "无"))
    monkeypatch.setattr(feishu_report, "pending_accounts_text", lambda **kwargs: (0, 0, "无", "无", "无"))
    monkeypatch.setattr(feishu_report, "lead_quality_text", lambda **kwargs: "")
    monkeypatch.setattr(feishu_report, "write_dashboard_visual_long_compact_files", fake_write_dashboard_visual_long_compact_files)

    feishu_report.main()

    dashboard_source = pd.read_csv(reports_dir / "feishu_dashboard_source_latest_2026-06-03.tsv", sep="\t")
    seed_rows = dashboard_source[dashboard_source["source_table"].isin(["seed_account", "seed_anchor"])]

    assert set(seed_rows["source_table"]) == {"seed_account", "seed_anchor"}
    account_mtd = seed_rows[
        seed_rows["source_table"].eq("seed_account")
        & seed_rows["scope_name"].eq("EXEED星途")
        & seed_rows["metric_key"].eq("mtd_impressions")
    ].iloc[0]
    assert account_mtd["actual"] == 8000
    assert account_mtd["target"] == 28000

    topline_impressions = dashboard_source[
        dashboard_source["source_table"].eq("topline")
        & dashboard_source["scope_name"].eq("全量")
        & dashboard_source["metric_key"].eq("impressions")
    ].iloc[0]
    assert topline_impressions["actual"] == 8000

    markdown = (reports_dir / "feishu_report_latest_2026-06-03.md").read_text(encoding="utf-8")
    assert "曝光：目标 1万，实际 0.8万，达成率 80.00%" in markdown
    assert visual_calls == [
        {
            "source_tables": {"topline", "topline_segment", "lead_account", "lead_anchor", "seed_account", "seed_anchor"},
            "svg_path": reports_dir / "feishu_dashboard_visual_p1_p5_long_compact_latest_2026-06-03.svg",
            "png_path": reports_dir / "feishu_dashboard_visual_p1_p5_long_compact_latest_2026-06-03.png",
            "run_id": "run-test",
        }
    ]
    assert (reports_dir / "feishu_dashboard_visual_p1_p5_long_compact_latest_2026-06-03.svg").exists()
    assert (reports_dir / "feishu_dashboard_visual_p1_p5_long_compact_latest_2026-06-03.png").exists()


def test_tsv_verify_uses_snapshot_and_seed_inputs_from_report_contract(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "output" / "sql_reports"
    reports_dir.mkdir(parents=True)
    export_dir = tmp_path / "artifacts" / "exports"
    fact_path = tmp_path / "output" / "fact_attribution.csv"
    fact_path.write_text("fixture\n", encoding="utf-8")
    live_path = tmp_path / "dynamic" / "2026年直播进度表.xlsx"
    live_path.parent.mkdir(parents=True)
    pd.DataFrame([{"日期": "2026-06-03", "消耗": 0, "曝光人数": 0, "车型": "EX7"}]).to_excel(live_path, index=False)
    leads_path = tmp_path / "dynamic" / "总部新媒体线索.csv"
    leads_path.write_text("线索ID,创建日期\nL1,2026-06-03\n", encoding="utf-8-sig")
    deals_path = tmp_path / "dynamic" / "总部新媒体成交.csv"
    deals_path.write_text("线索ID,订单状态\nL1,已交车\n", encoding="utf-8-sig")
    topline_config_path = tmp_path / "config" / "report_topline_config.json"
    topline_config_path.parent.mkdir(parents=True)
    topline_config_path.write_text(
        '{"full_account_targets":{"impressions":10000,"leads":1,"deals":1,"cpl":1,"cps":1},"ex7_rules":{"keywords":["EX7"],"lead_model_field_candidates":["车型"],"deal_model_field_candidates":["车型"],"live_model_field_candidates":["车型"]},"pending_rules":{"primary_date_field":"下订日期","fallback_date_fields":[]}}',
        encoding="utf-8",
    )
    seed_targets_path = tmp_path / "config" / "seed_monthly_targets.csv"
    seed_targets_path.write_text(
        "\n".join(
            [
                "month,scope_type,scope_name,parent_scope,parent_account,impression_target_month,spend_target_month,cpm_target,target_pool",
                "2026-06,account,EXEED星途,,,28000,,,种草组目标池",
                "2026-06,host,刘花旗,,EXEED星途,9000,,,种草组目标池",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    seed_workbook = tmp_path / "dynamic" / "EXEED星途台账（六月）.xlsx"
    pd.DataFrame(
        [
            {"创建时间": "2026-06-02", "开播账号": "抖音-EXEED星途", "本场主播": "刘花旗", "曝光人数": 3000},
            {"创建时间": "2026-06-03", "开播账号": "抖音-EXEED星途", "本场主播": "刘花旗", "曝光人数": 5000},
        ]
    ).to_excel(seed_workbook, index=False)
    snapshot_path = tmp_path / "artifacts" / "snapshots" / "daily_performance_snapshot_latest_2026-06-03.csv"
    snapshot_path.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            _snapshot_row("account_total", "线索组汇总"),
            _snapshot_row("anchor", "丁俐佳", parent_account="抖音-星途汽车官方直播间"),
        ]
    ).to_csv(snapshot_path, index=False, encoding="utf-8-sig")
    fact = _fact_frame()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        feishu_report,
        "parse_args",
        lambda: SimpleNamespace(
            reports_dir=str(reports_dir),
            fact_csv=str(fact_path),
            manual_override_file="",
            live_file=str(live_path),
            leads_file=str(leads_path),
            deals_file=str(deals_path),
            topline_config=str(topline_config_path),
            report_date="2026-06-03",
            snapshot_csv=str(snapshot_path),
            ledger_csv="",
            analysis_snapshot_csv="",
            export_dir=str(export_dir),
            run_id="run-test",
            schema_version="schema",
            metric_version="metric",
            template_version="template",
            freeze_id="freeze",
            output_md=str(reports_dir / "feishu_report_latest_2026-06-03.md"),
            output_tsv=str(reports_dir / "feishu_table_latest_2026-06-03.tsv"),
            output_dashboard_source_tsv=str(reports_dir / "feishu_dashboard_source_latest_2026-06-03.tsv"),
            seed_targets_file=str(seed_targets_path),
            seed_workbook_file="",
        ),
    )
    monkeypatch.setattr(feishu_report, "load_fact_with_manual_overrides", lambda *args, **kwargs: fact)
    monkeypatch.setattr(feishu_report, "_attach_douyin_laike_order_metrics", lambda **kwargs: (kwargs["account_panel"], kwargs["anchor_panel"], 0.0))
    monkeypatch.setattr(feishu_report, "build_topline_summary", lambda **kwargs: _topline_summary())
    monkeypatch.setattr(feishu_report, "deal_accounts_text", lambda **kwargs: ("无", "无", "无"))
    monkeypatch.setattr(feishu_report, "pending_accounts_text", lambda **kwargs: (0, 0, "无", "无", "无"))
    monkeypatch.setattr(feishu_report, "lead_quality_text", lambda **kwargs: "")
    feishu_report.main()

    monkeypatch.setattr(
        tsv_verify,
        "parse_args",
        lambda: SimpleNamespace(
            reports_dir=str(reports_dir),
            fact_csv=str(fact_path),
            manual_override_file="",
            live_file=str(live_path),
            leads_file=str(leads_path),
            deals_file=str(deals_path),
            topline_config=str(topline_config_path),
            tsv=str(reports_dir / "feishu_table_latest_2026-06-03.tsv"),
            snapshot_csv=str(snapshot_path),
            seed_targets_file=str(seed_targets_path),
            seed_workbook_file="",
        ),
    )
    monkeypatch.setattr(tsv_verify, "load_fact_with_manual_overrides", lambda *args, **kwargs: fact)
    monkeypatch.setattr(tsv_verify, "build_topline_summary", lambda **kwargs: _topline_summary())
    monkeypatch.setattr(tsv_verify.g, "_attach_douyin_laike_order_metrics", lambda **kwargs: (kwargs["account_panel"], kwargs["anchor_panel"], 0.0))
    monkeypatch.setattr(tsv_verify.g, "deal_accounts_text", lambda **kwargs: ("无", "无", "无"))
    monkeypatch.setattr(tsv_verify.g, "pending_accounts_text", lambda **kwargs: (0, 0, "无", "无", "无"))
    monkeypatch.setattr(tsv_verify.g, "lead_quality_text", lambda **kwargs: "")

    assert tsv_verify.main() == 0


def _fact_frame() -> pd.DataFrame:
    fact = pd.DataFrame(
        [
            {
                "is_deal": 0,
                "成交时间": "",
                "标准账号": "抖音-星途汽车官方直播间",
                "线索ID_norm": "L1",
                "date": "2026-06-03",
                "归属状态": "直接归属",
            }
        ]
    )
    fact.attrs["manual_override_summary"] = {}
    return fact


def _snapshot_row(scope_type: str, scope_name: str, *, parent_account: str = "") -> dict[str, object]:
    return {
        "date": "2026-06-03",
        "scope_type": scope_type,
        "scope_name": scope_name,
        "parent_account": parent_account,
        "daily_leads": 0,
        "daily_lead_target": 1,
        "daily_lead_attain_pct": 0,
        "mtd_leads": 0,
        "lead_target_month": 1,
        "mtd_lead_attain_pct": 0,
        "daily_deals": 0,
        "daily_deal_target": 1,
        "daily_deal_attain_pct": 0,
        "mtd_deals": 0,
        "deal_target_month": 1,
        "mtd_deal_attain_pct": 0,
        "lead_cost_target_month": 0,
        "cpl_target": 0,
        "cps_target": 0,
        "mtd_spend": 0,
        "mtd_cpl": 0,
        "mtd_cps": 0,
        "order_target_month": 0,
        "run_id": "run-test",
        "schema_version": "schema",
        "metric_version": "metric",
        "freeze_id": "freeze",
    }


def _topline_summary() -> ToplineSummary:
    return ToplineSummary(
        full_account=FullAccountTopline(
            impression_target=10_000,
            impression_actual=0,
            impression_attain=0,
            lead_target=1,
            lead_actual=0,
            lead_attain=0,
            deal_target=1,
            deal_actual=0,
            deal_attain=0,
            cpl_target=0,
            cpl_actual=0,
            cps_target=0,
            cps_actual=0,
            pending_day=0,
            pending_cumulative=0,
        ),
        excluding_ex7=SegmentTopline(label="不含 EX7", leads=0, deals=0, cpl_actual=0, cps_actual=0),
        ex7=SegmentTopline(label="EX7 专项", leads=0, deals=0, cpl_actual=0, cps_actual=0),
    )
