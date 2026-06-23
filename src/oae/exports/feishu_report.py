"""Coordinator for Feishu markdown/TSV exports."""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

import pandas as pd

from oae.contracts.monthly_metric_contract import (
    contract_has_month,
    load_monthly_metric_contract,
    monthly_metric_contract_path_for,
    project_report_topline_config,
    project_seed_monthly_targets,
)
from oae.contracts.models import RunMetadata
from oae.exports.feishu_content import ReportContext, build_markdown_content, build_tsv_content
from oae.exports.feishu_dashboard_source import build_dashboard_source_rows, dashboard_source_tsv
from oae.exports.feishu_dashboard_visual import write_dashboard_visual_long_compact_files
from oae.exports.feishu_douyin_laike import build_douyin_laike_order_metrics
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
from oae.exports.feishu_seed_dashboard import (
    build_seed_dashboard_tables,
    load_seed_monthly_targets as load_legacy_seed_monthly_targets,
    load_seed_sessions_from_workbooks,
    resolve_seed_workbook_paths,
)
from oae.exports.feishu_topline import (
    build_topline_summary,
    load_deals_source,
    load_leads_source,
    load_topline_config as load_legacy_topline_config,
    resolve_latest_source_file,
)
from oae.overrides import load_fact_with_manual_overrides
from oae.services.dashboard_daily_service import DashboardDailyService
from oae.version import METRIC_VERSION, SCHEMA_VERSION, TEMPLATE_VERSION, build_run_id


def _with_seed_impressions(topline_summary, seed_account_table: pd.DataFrame):
    if seed_account_table.empty or "累计曝光" not in seed_account_table.columns:
        return topline_summary

    seed_impressions = pd.to_numeric(seed_account_table["累计曝光"], errors="coerce").fillna(0.0).sum()
    if float(seed_impressions) <= 0:
        return topline_summary

    full = topline_summary.full_account
    actual = float(full.impression_actual or 0.0) + float(seed_impressions)
    target = float(full.impression_target or 0.0)
    attain = actual / target if target > 0 else None
    return replace(
        topline_summary,
        full_account=replace(
            full,
            impression_actual=actual,
            impression_attain=attain,
        ),
    )


def _merge_metric_by_scope(panel: pd.DataFrame, metric_frame: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    if "mtd_douyin_laike_orders" not in out.columns:
        out["mtd_douyin_laike_orders"] = 0.0
    out["mtd_douyin_laike_orders"] = pd.to_numeric(out["mtd_douyin_laike_orders"], errors="coerce").fillna(0.0)
    if metric_frame.empty:
        return out

    metric = metric_frame.set_index("scope_name")["mtd_douyin_laike_orders"]
    matched = out["scope_name"].map(metric)
    out.loc[matched.notna(), "mtd_douyin_laike_orders"] = matched.loc[matched.notna()].astype(float)
    return out


def _attach_order_attain(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    target = (
        pd.to_numeric(out["order_target_month"], errors="coerce")
        if "order_target_month" in out.columns
        else pd.Series(float("nan"), index=out.index, dtype="float64")
    )
    actual = pd.to_numeric(out.get("mtd_douyin_laike_orders", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
    attain = pd.Series(float("nan"), index=out.index, dtype="float64")
    mask = target.notna() & (target > 0)
    attain.loc[mask] = actual.loc[mask] / target.loc[mask]
    out["mtd_douyin_laike_order_attain"] = attain
    return out


def _attach_douyin_laike_order_metrics(
    *,
    account_panel: pd.DataFrame,
    anchor_panel: pd.DataFrame,
    live_df: pd.DataFrame,
    leads_source: pd.DataFrame,
    report_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    total_orders, account_orders, anchor_orders = build_douyin_laike_order_metrics(
        live_df=live_df,
        leads_df=leads_source,
        report_date=report_date,
    )

    acc = _merge_metric_by_scope(account_panel, account_orders)
    summary_mask = acc["scope_name"].astype(str).eq("线索组汇总")
    if summary_mask.any():
        acc.loc[summary_mask, "mtd_douyin_laike_orders"] = float(total_orders)
    acc = _attach_order_attain(acc)

    anc = _merge_metric_by_scope(anchor_panel, anchor_orders)
    anc = _attach_order_attain(anc)
    return acc, anc, float(total_orders)


def _build_visit_dashboard_source_tables(
    *,
    fact: pd.DataFrame,
    leads_source: pd.DataFrame,
    deals_source: pd.DataFrame,
    report_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    visit_source_available = any(column in fact.columns for column in ("到店日期", "到店时间")) or any(
        column in leads_source.columns for column in ("到店日期", "到店时间")
    )
    if fact.empty or not visit_source_available:
        return pd.DataFrame(), pd.DataFrame()

    service = DashboardDailyService(repo_root=Path.cwd())
    report_date_str = report_date.strftime("%Y-%m-%d")
    prepared_fact = service._prepare_fact_for_trends(fact, raw_leads=leads_source, raw_deals=deals_source)
    window = service._month_to_date_window(report_date_str)
    context = service._business_window_context(
        fact=prepared_fact,
        raw_deals=deals_source,
        live_sessions=pd.DataFrame(),
        seed_sessions=pd.DataFrame(),
        start_date=window["start_date"],
        end_date=window["end_date"],
    )
    if context["lead_rows"].empty and context["visit_rows"].empty and context["deal_rows"].empty:
        return pd.DataFrame(), pd.DataFrame()

    empty_targets = pd.DataFrame()
    accounts = service._entity_summaries(
        "account",
        context["date_strings"],
        lead_rows=context["lead_rows"],
        visit_rows=context["visit_rows"],
        deal_rows=context["deal_rows"],
        spend_rows=pd.DataFrame(),
        targets=empty_targets,
        window=window,
    )
    anchors = service._entity_summaries(
        "anchor",
        context["date_strings"],
        lead_rows=context["lead_rows"],
        visit_rows=context["visit_rows"],
        deal_rows=context["deal_rows"],
        spend_rows=pd.DataFrame(),
        targets=empty_targets,
        window=window,
    )
    return _visit_table_from_entities(accounts, name_column="账号"), _visit_table_from_entities(
        anchors,
        name_column="主播",
        parent_column="归属账号",
    )


def _visit_table_from_entities(
    entities: list[dict[str, object]],
    *,
    name_column: str,
    parent_column: str = "",
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for entity in entities:
        name = str(entity.get("name") or "").strip()
        if not name:
            continue
        metrics = entity.get("metrics", {})
        if not isinstance(metrics, dict):
            continue
        row: dict[str, object] = {name_column: name}
        if parent_column:
            row[parent_column] = str(entity.get("parent_scope") or "").strip()
        has_visit_metric = False
        for column, metric_key in [
            ("到店数", "visits"),
            ("到店成交数", "visit_deals"),
            ("到店率", "visit_rate"),
            ("到店成交率", "visit_deal_rate"),
        ]:
            metric = metrics.get(metric_key)
            if not isinstance(metric, dict):
                continue
            actual = metric.get("actual")
            if actual is None:
                continue
            row[column] = actual
            has_visit_metric = True
        if has_visit_metric:
            rows.append(row)
    return pd.DataFrame(rows)


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


def load_report_topline_config(path: str | Path, *, month: str | None = None) -> dict:
    topline_config_path = Path(path).expanduser().resolve()
    contract_path = monthly_metric_contract_path_for(topline_config_path)
    if contract_path.exists():
        contract = load_monthly_metric_contract(contract_path)
        if contract_has_month(contract, month):
            return project_report_topline_config(contract, month)
    return load_legacy_topline_config(topline_config_path)


def load_report_seed_monthly_targets(path: str | Path | None, *, month: str | None = None) -> pd.DataFrame:
    if not path:
        return load_legacy_seed_monthly_targets(path)
    seed_targets_path = Path(path).expanduser().resolve()
    contract_path = monthly_metric_contract_path_for(seed_targets_path)
    if contract_path.exists():
        contract = load_monthly_metric_contract(contract_path)
        if contract_has_month(contract, month):
            return project_seed_monthly_targets(contract, month)
    return load_legacy_seed_monthly_targets(seed_targets_path)


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
    parser.add_argument("--output-dashboard-source-tsv", default="", help="输出 dashboard source TSV；留空自动命名")
    parser.add_argument("--output-dashboard-visual-svg", default="", help="输出数据驾驶舱长图 SVG；留空自动命名")
    parser.add_argument("--output-dashboard-visual-png", default="", help="输出数据驾驶舱长图 PNG；留空自动命名")
    parser.add_argument("--skip-dashboard-visual-png", action="store_true", help="只写 SVG，不尝试生成 PNG")
    parser.add_argument("--seed-targets-file", default="config/seed_monthly_targets.csv", help="种草曝光月目标配置")
    parser.add_argument("--seed-workbook-file", default="", help="种草台账文件；留空自动搜索 EXEED星途台账")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    reports_dir = Path(args.reports_dir).expanduser().resolve()
    fact_path = Path(args.fact_csv).expanduser().resolve()
    manual_override_path = Path(args.manual_override_file).expanduser().resolve() if str(args.manual_override_file).strip() else None
    live_path = Path(args.live_file).expanduser().resolve()
    topline_config_path = Path(args.topline_config).expanduser().resolve()
    seed_targets_path = Path(args.seed_targets_file).expanduser().resolve() if str(args.seed_targets_file).strip() else None
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
        live_path.parent.resolve(),
    ]
    search_dirs = _expand_search_dirs(search_dirs)
    leads_path = resolve_latest_source_file(args.leads_file, search_dirs, "总部新媒体线索*.csv", "原始线索明细")
    deals_path = resolve_latest_source_file(args.deals_file, search_dirs, "总部新媒体成交*.csv", "原始成交明细")
    search_dirs = _expand_search_dirs(
        [
            *search_dirs,
            leads_path.parent.resolve(),
            deals_path.parent.resolve(),
        ]
    )

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
    topline_config = load_report_topline_config(topline_config_path, month=report_date_str[:7])
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
    acc, anc, total_douyin_laike_orders = _attach_douyin_laike_order_metrics(
        account_panel=acc,
        anchor_panel=anc,
        live_df=live_df,
        leads_source=leads_source,
        report_date=report_date,
    )
    setattr(topline_summary, "douyin_laike_orders", total_douyin_laike_orders)
    if "order_target_month" in acc.columns:
        summary_target = pd.to_numeric(
            acc.loc[acc["scope_name"].astype(str).eq("线索组汇总"), "order_target_month"],
            errors="coerce",
        )
        if summary_target.notna().any():
            setattr(topline_summary, "douyin_laike_order_target", float(summary_target.dropna().iloc[0]))

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
    visit_acc_tsv_out, visit_anc_tsv_out = _build_visit_dashboard_source_tables(
        fact=fact,
        leads_source=leads_source,
        deals_source=deals_source,
        report_date=report_date,
    )

    acc_out = account_table(acc, target_accounts=target_accounts)
    anc_out = anchor_table(anc)
    acc_tsv_out = account_table_tsv(acc, target_accounts=target_accounts)
    anc_tsv_out = anchor_table_tsv(anc)
    seed_workbook_paths = resolve_seed_workbook_paths(args.seed_workbook_file, search_dirs)
    seed_targets = load_report_seed_monthly_targets(seed_targets_path, month=report_date_str[:7])
    seed_sessions = load_seed_sessions_from_workbooks(seed_workbook_paths)
    seed_acc_tsv_out, seed_anc_tsv_out = build_seed_dashboard_tables(
        report_date=report_date_str,
        seed_sessions=seed_sessions,
        seed_targets=seed_targets,
    )
    topline_summary = _with_seed_impressions(topline_summary, seed_acc_tsv_out)

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
        seed_anchor_tsv_out=seed_anc_tsv_out,
    )

    md_content = build_markdown_content(ctx)
    tsv_content = build_tsv_content(ctx)
    dashboard_source_rows = build_dashboard_source_rows(
        report_date=report_date_str,
        topline_summary=topline_summary,
        account_table=acc_tsv_out,
        anchor_table=anc_tsv_out,
        seed_account_table=seed_acc_tsv_out,
        seed_anchor_table=seed_anc_tsv_out,
        visit_account_table=visit_acc_tsv_out,
        visit_anchor_table=visit_anc_tsv_out,
        lead_quality_line=lead_quality_line,
    )
    dashboard_source_content = dashboard_source_tsv(dashboard_source_rows)
    md_path = Path(args.output_md).expanduser().resolve() if args.output_md else reports_dir / f"feishu_report_latest_{report_date_str}.md"
    tsv_path = Path(args.output_tsv).expanduser().resolve() if args.output_tsv else reports_dir / f"feishu_table_latest_{report_date_str}.tsv"
    dashboard_source_path = (
        Path(args.output_dashboard_source_tsv).expanduser().resolve()
        if args.output_dashboard_source_tsv
        else reports_dir / f"feishu_dashboard_source_latest_{report_date_str}.tsv"
    )
    dashboard_visual_svg_path = (
        Path(getattr(args, "output_dashboard_visual_svg", "")).expanduser().resolve()
        if getattr(args, "output_dashboard_visual_svg", "")
        else reports_dir / f"feishu_dashboard_visual_p1_p5_long_compact_latest_{report_date_str}.svg"
    )
    skip_dashboard_visual_png = bool(getattr(args, "skip_dashboard_visual_png", False))
    dashboard_visual_png_path = None
    if not skip_dashboard_visual_png:
        dashboard_visual_png_path = (
            Path(getattr(args, "output_dashboard_visual_png", "")).expanduser().resolve()
            if getattr(args, "output_dashboard_visual_png", "")
            else reports_dir / f"feishu_dashboard_visual_p1_p5_long_compact_latest_{report_date_str}.png"
        )
    md_path.parent.mkdir(parents=True, exist_ok=True)
    tsv_path.parent.mkdir(parents=True, exist_ok=True)
    dashboard_source_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(md_content, encoding="utf-8")
    tsv_path.write_text(tsv_content, encoding="utf-8")
    dashboard_source_path.write_text(dashboard_source_content, encoding="utf-8-sig")

    metadata = RunMetadata(
        run_id=args.run_id or infer_run_id(acc) or build_run_id(),
        schema_version=(acc["schema_version"].iloc[0] if "schema_version" in acc.columns else args.schema_version),
        metric_version=(acc["metric_version"].iloc[0] if "metric_version" in acc.columns else args.metric_version),
        template_version=args.template_version,
        freeze_id=(acc["freeze_id"].iloc[0] if "freeze_id" in acc.columns else args.freeze_id),
    )
    dashboard_visual_written = write_dashboard_visual_long_compact_files(
        pd.DataFrame(dashboard_source_rows),
        svg_path=dashboard_visual_svg_path,
        png_path=dashboard_visual_png_path,
        run_id=metadata.run_id,
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
        dashboard_source_path=dashboard_source_path,
        dashboard_source_row_count=len(dashboard_source_rows),
    )

    print(f"[OK] markdown file: {md_path}")
    print(f"[OK] tsv file: {tsv_path}")
    print(f"[OK] dashboard source tsv file: {dashboard_source_path}")
    print(f"[OK] dashboard visual svg file: {dashboard_visual_written['svg']}")
    if "png" in dashboard_visual_written:
        print(f"[OK] dashboard visual png file: {dashboard_visual_written['png']}")
    elif dashboard_visual_png_path is not None:
        print(f"[WARN] dashboard visual png not written: rsvg-convert not available")
    print(f"[OK] export manifest dir: {export_dir}")
    print(f"[INFO] leads source: {leads_path}")
    print(f"[INFO] deals source: {deals_path}")
    print(f"[INFO] topline config: {topline_config_path}")
    print(f"[INFO] seed targets: {seed_targets_path if seed_targets_path else ''}")
    print(f"[INFO] seed workbooks: {[str(path) for path in seed_workbook_paths]}")
    print(
        f"[INFO] manual overrides: applied={manual_override_summary.get('applied_override_count', 0)}, "
        f"affected_rows={manual_override_summary.get('applied_row_count', 0)}"
    )
    print("[OK] tsv preview:\n")
    print(tsv_content)


if __name__ == "__main__":
    raise SystemExit(main())
