"""Performance runtime coordinator."""

from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

import pandas as pd

from oae.contracts.models import RunMetadata
from oae.performance.formatters import finalize_format
from oae.performance.loaders import (
    load_anchor_accounts_from_live,
    load_fact,
    load_targets,
    month_start_end,
    pick_latest_live_file,
    pick_report_month,
    resolve_spend_data,
)
from oae.performance.panel_builders import build_account_panel, build_anchor_panel
from oae.performance.snapshots import (
    build_compensation_ledger,
    build_daily_performance_snapshot,
    write_compensation_ledger,
    write_daily_performance_snapshots,
)
from oae.version import METRIC_VERSION, SCHEMA_VERSION, TEMPLATE_VERSION, build_run_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出目标达成日报（含 CPL/CPS）")
    parser.add_argument("--fact-csv", default="output/fact_attribution.csv", help="归因事实表 CSV")
    parser.add_argument("--manual-override-file", default="config/manual_attribution_overrides.csv", help="专项人工确认归属配置")
    parser.add_argument("--targets-file", default="config/monthly_targets.csv", help="月目标配置 CSV")
    parser.add_argument("--spend-file", default="config/daily_spend.csv", help="当日实际消耗 CSV（回退源）")
    parser.add_argument("--live-file", default="2026年直播进度表.xlsx", help="直播进度表路径（可包含消耗/CPL字段）")
    parser.add_argument(
        "--spend-source",
        choices=["auto", "live", "csv"],
        default="auto",
        help="成本来源：auto=优先直播进度表否则spend-file；live=仅直播进度表；csv=仅spend-file",
    )
    parser.add_argument("--month", default="", help="报表月份 YYYY-MM，留空则取目标配置最新月份")
    parser.add_argument("--output-dir", default="output/sql_reports", help="输出目录")
    parser.add_argument("--snapshot-dir", default="artifacts/snapshots", help="正式快照/台账输出目录")
    parser.add_argument("--run-id", default="", help="产品化运行编号")
    parser.add_argument("--schema-version", default=SCHEMA_VERSION, help="快照 schema 版本")
    parser.add_argument("--metric-version", default=METRIC_VERSION, help="经营口径版本")
    parser.add_argument("--template-version", default=TEMPLATE_VERSION, help="消费模板版本")
    parser.add_argument("--freeze-id", default="", help="结算冻结编号，留空按 provisional 生成")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    fact_path = Path(args.fact_csv).expanduser().resolve()
    manual_override_path = Path(args.manual_override_file).expanduser().resolve() if str(args.manual_override_file).strip() else None
    targets_path = Path(args.targets_file).expanduser().resolve()
    spend_path = Path(args.spend_file).expanduser().resolve()
    live_path = Path(args.live_file).expanduser().resolve()
    out_dir = Path(args.output_dir).expanduser().resolve()
    snapshot_dir = Path(args.snapshot_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    if not live_path.exists():
        search_dirs = [
            Path.cwd().resolve(),
            fact_path.parent.resolve(),
            fact_path.parent.parent.resolve() if fact_path.parent.parent.exists() else fact_path.parent.resolve(),
            out_dir.parent.resolve(),
        ]
        try:
            auto_live = pick_latest_live_file(search_dirs)
            print(f"[WARN] live file not found: {live_path}; auto use: {auto_live}")
            live_path = auto_live
        except FileNotFoundError:
            print(f"[WARN] live file not found: {live_path}; continue without live fallback data")

    t0 = perf_counter()
    fact = load_fact(fact_path, manual_override_path=manual_override_path)
    print(f"[INFO] load_fact elapsed: {perf_counter() - t0:.3f}s")
    manual_override_summary = fact.attrs.get("manual_override_summary", {})

    targets = load_targets(targets_path)
    spend, spend_source_used = resolve_spend_data(
        spend_source=args.spend_source,
        spend_path=spend_path,
        live_path=live_path,
    )

    report_month = pick_report_month(args.month, targets)
    month_start, month_end = month_start_end(report_month)

    targets_month = targets[targets["month"] == report_month].copy()
    if targets_month.empty:
        raise SystemExit(f"[ERROR] targets 中没有 {report_month} 的配置")

    spend_month = spend[(spend["date"] >= month_start) & (spend["date"] <= month_end)].copy()
    live_anchor_accounts = load_anchor_accounts_from_live(live_path, month_start, month_end)

    t0 = perf_counter()
    account_panel = build_account_panel(
        fact=fact,
        targets_month=targets_month,
        spend_month=spend_month,
        month_start=month_start,
        month_end=month_end,
    )
    print(f"[INFO] build_account_panel elapsed: {perf_counter() - t0:.3f}s")

    t0 = perf_counter()
    anchor_panel = build_anchor_panel(
        fact=fact,
        targets_month=targets_month,
        spend_month=spend_month,
        month_start=month_start,
        month_end=month_end,
        live_anchor_accounts=live_anchor_accounts,
    )
    print(f"[INFO] build_anchor_panel elapsed: {perf_counter() - t0:.3f}s")

    t0 = perf_counter()
    account_out = finalize_format(account_panel, report_month)
    anchor_out = finalize_format(anchor_panel, report_month)
    print(f"[INFO] finalize_format elapsed: {perf_counter() - t0:.3f}s")

    account_file = out_dir / f"daily_goal_account_{report_month}.csv"
    anchor_file = out_dir / f"daily_goal_anchor_{report_month}.csv"
    account_out.to_csv(account_file, index=False, encoding="utf-8-sig")
    anchor_out.to_csv(anchor_file, index=False, encoding="utf-8-sig")

    latest_date = pd.to_datetime(month_start)
    if not account_out.empty:
        spend_any = pd.to_numeric(account_out["daily_spend"], errors="coerce").fillna(0) if "daily_spend" in account_out.columns else pd.Series(0, index=account_out.index)
        has_any = account_out[
            (account_out["daily_leads"] > 0)
            | (account_out["daily_deals"] > 0)
            | (spend_any > 0)
        ]
        if not has_any.empty:
            latest_date = pd.to_datetime(has_any["date"]).max()

    latest_tag = latest_date.strftime("%Y-%m-%d")
    account_latest = account_out[pd.to_datetime(account_out["date"]) == latest_date]
    anchor_latest = anchor_out[pd.to_datetime(anchor_out["date"]) == latest_date] if not anchor_out.empty else anchor_out

    account_latest_file = out_dir / f"daily_goal_account_latest_{latest_tag}.csv"
    anchor_latest_file = out_dir / f"daily_goal_anchor_latest_{latest_tag}.csv"
    account_latest.to_csv(account_latest_file, index=False, encoding="utf-8-sig")
    anchor_latest.to_csv(anchor_latest_file, index=False, encoding="utf-8-sig")

    freeze_id = args.freeze_id or f"provisional-{report_month}-{latest_tag}"
    metadata = RunMetadata(
        run_id=args.run_id or build_run_id(),
        schema_version=args.schema_version,
        metric_version=args.metric_version,
        template_version=args.template_version,
        freeze_id=freeze_id,
    )
    snapshot_full, snapshot_latest = build_daily_performance_snapshot(
        account_frame=account_out,
        anchor_frame=anchor_out,
        report_month=report_month,
        latest_date=latest_date,
        metadata=metadata,
        spend_source=spend_source_used,
    )
    snapshot_paths = write_daily_performance_snapshots(
        snapshot=snapshot_full,
        latest_snapshot=snapshot_latest,
        snapshot_dir=snapshot_dir,
        report_month=report_month,
        latest_date=latest_date,
    )
    compensation_ledger = build_compensation_ledger(
        latest_snapshot=snapshot_latest,
        settlement_period=report_month,
        snapshot_start=month_start,
        snapshot_end=latest_date,
        metadata=metadata,
    )
    ledger_path = write_compensation_ledger(
        compensation_ledger,
        snapshot_dir=snapshot_dir,
        settlement_period=report_month,
        snapshot_end=latest_date,
    )

    print(f"[INFO] report_month={report_month}")
    print(f"[OK] account full: rows={len(account_out)}, file={account_file}")
    print(f"[OK] anchor full: rows={len(anchor_out)}, file={anchor_file}")
    print(f"[OK] account latest: rows={len(account_latest)}, file={account_latest_file}")
    print(f"[OK] anchor latest: rows={len(anchor_latest)}, file={anchor_latest_file}")
    print(f"[OK] daily snapshot full: rows={len(snapshot_full)}, file={snapshot_paths['full']}")
    print(f"[OK] daily snapshot latest: rows={len(snapshot_latest)}, file={snapshot_paths['latest']}")
    print(f"[OK] compensation ledger: rows={len(compensation_ledger)}, file={ledger_path}")
    print(f"[INFO] targets file: {targets_path}")
    print(f"[INFO] spend source: {spend_source_used}")
    print(f"[INFO] live file: {live_path}")
    print(f"[INFO] spend file: {spend_path}")
    print(f"[INFO] run_id: {metadata.run_id}")
    print(f"[INFO] freeze_id: {metadata.freeze_id}")
    print(
        f"[INFO] manual overrides: source={manual_override_summary.get('source_path', '')}, "
        f"applied={manual_override_summary.get('applied_override_count', 0)} override(s), "
        f"affected_rows={manual_override_summary.get('applied_row_count', 0)}"
    )
    if live_anchor_accounts is not None and not live_anchor_accounts.empty:
        anchor_count = live_anchor_accounts["scope_name"].nunique() if "scope_name" in live_anchor_accounts.columns else 0
        print(f"[INFO] anchor-account source: live schedule by date ({anchor_count} anchors parsed)")
    else:
        print("[INFO] anchor-account source: targets config only")


if __name__ == "__main__":
    raise SystemExit(main())
