"""Coordinator for Feishu markdown/TSV exports."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from oae.contracts.models import RunMetadata
from oae.exports.feishu_content import ReportContext, build_markdown_content, build_tsv_content
from oae.exports.feishu_manifest import write_feishu_manifests
from oae.exports.feishu_panels import (
    ACCOUNT_REQUIRED_COLUMNS,
    ANCHOR_REQUIRED_COLUMNS,
    FACT_REQUIRED_COLUMNS,
    account_table,
    account_table_tsv,
    anchor_table,
    anchor_table_tsv,
    deal_accounts_text,
    get_target_accounts,
    infer_run_id,
    lead_quality_text,
    load_panel_for_date,
    load_panel_from_snapshot,
    pending_accounts_text,
    pick_latest_live_file,
    resolve_report_date,
    validate_columns,
)
from oae.exports.feishu_topline import (
    build_topline_summary,
    load_deals_source,
    load_leads_source,
    load_topline_config,
    resolve_latest_source_file,
)
from oae.overrides import load_fact_with_manual_overrides
from oae.version import METRIC_VERSION, SCHEMA_VERSION, TEMPLATE_VERSION, build_run_id


def _expand_search_dirs(base_dirs: list[Path]) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for base in base_dirs:
        for candidate in [base, base / "源文件"]:
            key = str(candidate.resolve()) if candidate.exists() else str(candidate)
            if key not in seen:
                seen.add(key)
                out.append(candidate)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Feishu markdown + tsv report from latest csv outputs")
    parser.add_argument("--reports-dir", default="output/sql_reports", help="日报目录")
    parser.add_argument("--fact-csv", default="output/fact_attribution.csv", help="事实表csv")
    parser.add_argument("--manual-override-file", default="config/manual_attribution_overrides.csv", help="专项人工确认归属配置")
    parser.add_argument("--live-file", default="2026年直播进度表.xlsx", help="直播进度表（用于读取全场景线索人数）")
    parser.add_argument("--leads-file", default="", help="原始线索明细 CSV，用于顶部核心汇报的 EX7 口径拆分")
    parser.add_argument("--deals-file", default="", help="原始成交明细 CSV，用于顶部核心汇报的 EX7 / 待交车口径")
    parser.add_argument("--topline-config", default="config/report_topline_config.json", help="顶部核心汇报配置")
    parser.add_argument("--report-date", default="", help="报表日期 YYYY-MM-DD，默认昨天")
    parser.add_argument("--snapshot-csv", default="", help="正式日报快照 CSV，优先于 legacy daily_goal_*")
    parser.add_argument("--ledger-csv", default="", help="正式绩效台账 CSV，用于 manifest 标记")
    parser.add_argument("--analysis-snapshot-csv", default="", help="正式分析快照 CSV，用于 manifest 标记")
    parser.add_argument("--export-dir", default="artifacts/exports", help="导出契约 manifest 目录")
    parser.add_argument("--run-id", default="", help="运行编号，留空自动生成")
    parser.add_argument("--schema-version", default=SCHEMA_VERSION, help="导出 schema 版本")
    parser.add_argument("--metric-version", default=METRIC_VERSION, help="经营口径版本")
    parser.add_argument("--template-version", default=TEMPLATE_VERSION, help="Excel 模板版本")
    parser.add_argument("--freeze-id", default="", help="冻结编号")
    parser.add_argument("--output-md", default="", help="输出md文件；留空自动命名")
    parser.add_argument("--output-tsv", default="", help="输出tsv文件；留空自动命名")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    reports_dir = Path(args.reports_dir).expanduser().resolve()
    fact_path = Path(args.fact_csv).expanduser().resolve()
    manual_override_path = Path(args.manual_override_file).expanduser().resolve() if str(args.manual_override_file).strip() else None
    live_path = Path(args.live_file).expanduser().resolve()
    topline_config_path = Path(args.topline_config).expanduser().resolve()
    snapshot_path = Path(args.snapshot_csv).expanduser().resolve() if args.snapshot_csv else None
    ledger_path = Path(args.ledger_csv).expanduser().resolve() if args.ledger_csv else None
    analysis_snapshot_path = Path(args.analysis_snapshot_csv).expanduser().resolve() if args.analysis_snapshot_csv else None
    export_dir = Path(args.export_dir).expanduser().resolve()

    if not live_path.exists():
        search_dirs = [
            Path.cwd().resolve(),
            reports_dir.parent.resolve(),
            reports_dir.parent.parent.resolve() if reports_dir.parent.parent.exists() else reports_dir.parent.resolve(),
            fact_path.parent.resolve(),
        ]
        search_dirs = _expand_search_dirs(search_dirs)
        try:
            auto_live = pick_latest_live_file(search_dirs)
            print(f"[WARN] live file not found: {live_path}; auto use: {auto_live}")
            live_path = auto_live
        except FileNotFoundError:
            raise SystemExit(f"[ERROR] 日报顶部汇报需要直播进度表，但未找到可用文件: {live_path}")

    if not reports_dir.exists() or not reports_dir.is_dir():
        print(f"[ERROR] reports dir not exists: {reports_dir}")
        return
    if not fact_path.exists():
        print(f"[ERROR] fact csv not exists: {fact_path}")
        return

    report_date_str = resolve_report_date(args, snapshot_path, reports_dir)
    search_dirs = [
        Path.cwd().resolve(),
        reports_dir.parent.resolve(),
        reports_dir.parent.parent.resolve() if reports_dir.parent.parent.exists() else reports_dir.parent.resolve(),
        fact_path.parent.resolve(),
        fact_path.parent.parent.resolve() if fact_path.parent.parent.exists() else fact_path.parent.resolve(),
    ]
    search_dirs = _expand_search_dirs(search_dirs)
    leads_path = resolve_latest_source_file(args.leads_file, search_dirs, "总部新媒体线索*.csv", "原始线索明细")
    deals_path = resolve_latest_source_file(args.deals_file, search_dirs, "总部新媒体成交*.csv", "原始成交明细")

    if snapshot_path and snapshot_path.exists():
        acc = load_panel_from_snapshot(snapshot_path=snapshot_path, report_date_str=report_date_str, scope="account")
        anc = load_panel_from_snapshot(snapshot_path=snapshot_path, report_date_str=report_date_str, scope="anchor")
    else:
        acc = load_panel_for_date(reports_dir=reports_dir, report_date_str=report_date_str, scope="account")
        anc = load_panel_for_date(reports_dir=reports_dir, report_date_str=report_date_str, scope="anchor")
    fact = load_fact_with_manual_overrides(fact_path, manual_override_path=manual_override_path)
    manual_override_summary = fact.attrs.get("manual_override_summary", {})

    if not validate_columns(acc, ACCOUNT_REQUIRED_COLUMNS, "账号层日报"):
        return
    if not validate_columns(anc, ANCHOR_REQUIRED_COLUMNS, "主播层日报"):
        return
    if not validate_columns(fact, FACT_REQUIRED_COLUMNS, "事实表"):
        return

    live_df = pd.read_excel(live_path) if live_path.exists() else pd.DataFrame()
    leads_source = load_leads_source(leads_path)
    deals_source = load_deals_source(deals_path)
    topline_config = load_topline_config(topline_config_path)
    report_date = pd.to_datetime(report_date_str)
    month_start = pd.to_datetime(f"{report_date_str[:7]}-01")
    topline_summary = build_topline_summary(
        fact=fact,
        live_df=live_df,
        leads_source=leads_source,
        deals_source=deals_source,
        report_date=report_date,
        config=topline_config,
    )

    target_accounts = get_target_accounts(acc)
    day_target_deal_accounts, mtd_target_deal_accounts, mtd_all_deal_accounts = deal_accounts_text(
        fact=fact,
        report_date=report_date,
        month_start=month_start,
        target_accounts=target_accounts,
    )
    _, _, day_target_pending_accounts, mtd_target_pending_accounts, mtd_all_pending_accounts = pending_accounts_text(
        fact=fact,
        report_date=report_date,
        month_start=month_start,
        target_accounts=target_accounts,
        deals_source=deals_source,
        topline_config=topline_config,
    )
    lead_quality_line = lead_quality_text(
        fact=fact,
        live_df=live_df,
        report_date=report_date,
        month_start=month_start,
        live_file_label=(live_path.stem if live_path.exists() else "直播进度表"),
        manual_override_summary=manual_override_summary,
    )

    acc_out = account_table(acc, target_accounts=target_accounts)
    anc_out = anchor_table(anc)
    acc_tsv_out = account_table_tsv(acc, target_accounts=target_accounts)
    anc_tsv_out = anchor_table_tsv(anc)

    ctx = ReportContext(
        report_date_str=report_date_str,
        topline_summary=topline_summary,
        day_target_deal_accounts=day_target_deal_accounts,
        mtd_target_deal_accounts=mtd_target_deal_accounts,
        mtd_all_deal_accounts=mtd_all_deal_accounts,
        day_target_pending_accounts=day_target_pending_accounts,
        mtd_target_pending_accounts=mtd_target_pending_accounts,
        mtd_all_pending_accounts=mtd_all_pending_accounts,
        lead_quality_line=lead_quality_line,
        acc_out=acc_out,
        anc_out=anc_out,
        acc_tsv_out=acc_tsv_out,
        anc_tsv_out=anc_tsv_out,
    )

    md_content = build_markdown_content(ctx)
    tsv_content = build_tsv_content(ctx)
    md_path = Path(args.output_md).expanduser().resolve() if args.output_md else reports_dir / f"feishu_report_latest_{report_date_str}.md"
    tsv_path = Path(args.output_tsv).expanduser().resolve() if args.output_tsv else reports_dir / f"feishu_table_latest_{report_date_str}.tsv"
    md_path.parent.mkdir(parents=True, exist_ok=True)
    tsv_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(md_content, encoding="utf-8")
    tsv_path.write_text(tsv_content, encoding="utf-8")

    metadata = RunMetadata(
        run_id=args.run_id or infer_run_id(acc) or build_run_id(),
        schema_version=(acc["schema_version"].iloc[0] if "schema_version" in acc.columns else args.schema_version),
        metric_version=(acc["metric_version"].iloc[0] if "metric_version" in acc.columns else args.metric_version),
        template_version=args.template_version,
        freeze_id=(acc["freeze_id"].iloc[0] if "freeze_id" in acc.columns else args.freeze_id),
    )
    write_feishu_manifests(
        export_dir=export_dir,
        report_date=report_date_str,
        metadata=metadata,
        row_count=len(acc_tsv_out) + len(anc_tsv_out),
        snapshot_path=snapshot_path,
        ledger_path=ledger_path,
        analysis_snapshot_path=analysis_snapshot_path,
        fact_path=fact_path,
        md_path=md_path,
        tsv_path=tsv_path,
    )

    print(f"[OK] markdown file: {md_path}")
    print(f"[OK] tsv file: {tsv_path}")
    print(f"[OK] export manifest dir: {export_dir}")
    print(f"[INFO] leads source: {leads_path}")
    print(f"[INFO] deals source: {deals_path}")
    print(f"[INFO] topline config: {topline_config_path}")
    print(
        f"[INFO] manual overrides: applied={manual_override_summary.get('applied_override_count', 0)}, "
        f"affected_rows={manual_override_summary.get('applied_row_count', 0)}"
    )
    print("[OK] tsv preview:\n")
    print(tsv_content)


if __name__ == "__main__":
    raise SystemExit(main())
