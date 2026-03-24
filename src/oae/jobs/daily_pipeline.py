"""Python-first daily pipeline that replaces the batch-file orchestration role."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from oae.contracts import dump_contract_schemas
from oae.ingest import discover_runtime_inputs
from oae.ingest.input_discovery import dump_input_manifest
from oae.overrides import (
    build_manual_override_check,
    build_manual_override_daily_digest,
    build_manual_override_daily_digest_view,
    build_manual_override_issue_manifest,
    build_manual_override_manifest,
    dump_manual_override_manifest,
    inspect_manual_attribution_overrides,
)
from oae.quality import build_quality_report, compare_files_against_manifest, run_business_quality_checks
from oae.version import METRIC_VERSION, SCHEMA_VERSION, TEMPLATE_VERSION, build_run_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Round-5 productized daily pipeline")
    parser.add_argument("--workspace", default=".", help="工程根目录")
    parser.add_argument("--data-dir", default="", help="动态源文件目录，留空则按输入契约默认目录扫描")
    parser.add_argument("--live-file", default="", help="直播进度表路径")
    parser.add_argument("--report-date", default="", help="强制日报日期 YYYY-MM-DD")
    parser.add_argument("--freeze-id", default="", help="结算冻结编号，默认 provisional")
    parser.add_argument("--input-config", default="config/input_sources.json", help="输入契约配置文件")
    parser.add_argument("--quality-thresholds", default="config/quality_thresholds.json", help="质量阈值配置文件")
    parser.add_argument(
        "--quality-threshold-profile",
        choices=["auto", "regression", "operational", "settlement"],
        default="auto",
        help="质量阈值 profile：auto=provisional 用 operational，其余 settlement",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    workspace = Path(args.workspace).expanduser().resolve()
    run_id = build_run_id()
    freeze_id = args.freeze_id or "provisional"
    quality_threshold_profile = args.quality_threshold_profile
    if quality_threshold_profile == "auto":
        quality_threshold_profile = "operational" if freeze_id.startswith("provisional") else "settlement"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(workspace / "src") + os.pathsep + env.get("PYTHONPATH", "")

    output_dir = workspace / "output"
    reports_dir = output_dir / "sql_reports"
    snapshot_dir = workspace / "artifacts" / "snapshots"
    exports_dir = workspace / "artifacts" / "exports"
    runs_dir = workspace / "artifacts" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    dump_contract_schemas(workspace / "src" / "oae" / "contracts" / "schemas")
    input_manifest, resolved_inputs = discover_runtime_inputs(
        workspace=workspace,
        run_id=run_id,
        config_path=(workspace / args.input_config).resolve(),
        dynamic_dir_override=args.data_dir,
        path_overrides={"live_schedule": args.live_file} if args.live_file else {},
    )
    manual_override_source_summary = inspect_manual_attribution_overrides(
        resolved_inputs["manual_attribution_overrides"],
        run_id=run_id,
    )
    input_manifest_path = runs_dir / f"input_manifest_{run_id}.json"
    dump_input_manifest(input_manifest_path, input_manifest)
    manual_override_issue_manifest_path = runs_dir / f"manual_override_issue_manifest_{run_id}.json"
    manual_override_daily_digest_path = runs_dir / f"manual_override_daily_digest_{run_id}.json"
    if int(manual_override_source_summary["issue_summary"].get("blocking_count", 0) or 0) > 0:
        preflight_issue_manifest = {
            "run_id": run_id,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source_path": manual_override_source_summary["source_path"],
            "issue_summary": manual_override_source_summary["issue_summary"],
            "issues": manual_override_source_summary["issues"],
            "validation_summary": {
                "summary": manual_override_source_summary.get("summary", ""),
                "warnings": manual_override_source_summary.get("warnings", []),
                "counts": manual_override_source_summary.get("counts", {}),
            },
            "runtime_summary": {
                "matched_override_count": 0,
                "matched_sample_count": 0,
                "latest_effective_date": "",
            },
        }
        dump_manual_override_manifest(manual_override_issue_manifest_path, preflight_issue_manifest)
        preflight_daily_digest = build_manual_override_daily_digest(
            run_id=run_id,
            issue_manifest=preflight_issue_manifest,
            override_manifest={
                "configured_rows": manual_override_source_summary.get("counts", {}).get("configured", 0),
                "active_rows": manual_override_source_summary.get("counts", {}).get("active", 0),
                "applied_override_count": 0,
                "applied_row_count": 0,
                "affected_accounts": [],
                "affected_hosts": [],
                "final_consumer_scope": ["daily_snapshot", "analysis_snapshot", "feishu_report", "tsv_verify"],
            },
        )
        dump_manual_override_manifest(manual_override_daily_digest_path, preflight_daily_digest)
        top_messages = preflight_issue_manifest["issue_summary"].get("top_blocking_messages", [])[:5]
        raise SystemExit(
            "[ERROR] 专项人工确认归属文件存在阻断问题，已生成异常清单；"
            f"清单={manual_override_issue_manifest_path}；问题={top_messages}"
        )

    build_fact_step = [
        sys.executable,
        "-m",
        "oae.cli.build_fact",
        "--workspace",
        str(workspace),
        "--data-dir",
        str(Path(input_manifest["dynamic_input_root"])),
        "--live-file",
        str(resolved_inputs["live_schedule"]),
        "--leads-file",
        str(resolved_inputs["leads_detail"]),
        "--deals-file",
        str(resolved_inputs["deals_detail"]),
        "--match-mode",
        "process_deal_data",
        "--output-dir",
        str(output_dir),
        "--run-id",
        run_id,
        "--schema-version",
        SCHEMA_VERSION,
        "--metric-version",
        METRIC_VERSION,
    ]
    downstream_steps = [
        [
            sys.executable,
            "-m",
            "oae.cli.build_sqlite_db",
            "--csv",
            str(output_dir / "fact_attribution.csv"),
            "--db",
            str(output_dir / "lead_daily.db"),
        ],
        [
            sys.executable,
            "-m",
            "oae.cli.export_sql_daily",
            "--db",
            str(output_dir / "lead_daily.db"),
            "--output-dir",
            str(reports_dir),
        ],
        [
            sys.executable,
            "-m",
            "oae.cli.export_target_daily",
            "--fact-csv",
            str(output_dir / "fact_attribution.csv"),
            "--manual-override-file",
            str(resolved_inputs["manual_attribution_overrides"]),
            "--targets-file",
            str(resolved_inputs["monthly_targets"]),
            "--spend-file",
            str(resolved_inputs["daily_spend"]),
            "--live-file",
            str(resolved_inputs["live_schedule"]),
            "--spend-source",
            "auto",
            "--output-dir",
            str(reports_dir),
            "--snapshot-dir",
            str(snapshot_dir),
            "--run-id",
            run_id,
            "--schema-version",
            SCHEMA_VERSION,
            "--metric-version",
            METRIC_VERSION,
            "--template-version",
            TEMPLATE_VERSION,
            "--freeze-id",
            freeze_id,
        ],
        [
            sys.executable,
            "-m",
            "oae.cli.run_analysis",
            "--analysis-mode",
            "unified-fact",
            "--fact-csv",
            str(output_dir / "fact_attribution.csv"),
            "--manual-override-file",
            str(resolved_inputs["manual_attribution_overrides"]),
            "--output-dir",
            str(workspace / "全量分析"),
            "--snapshot-dir",
            str(snapshot_dir),
            "--run-id",
            run_id,
            "--schema-version",
            SCHEMA_VERSION,
            "--metric-version",
            METRIC_VERSION,
        ],
    ]

    completed_steps = []
    subprocess.run(build_fact_step, cwd=workspace, check=True, env=env)
    completed_steps.append(build_fact_step)

    manual_override_issue_manifest = build_manual_override_issue_manifest(
        fact_path=output_dir / "fact_attribution.csv",
        manual_override_path=resolved_inputs["manual_attribution_overrides"],
        run_id=run_id,
    )
    dump_manual_override_manifest(manual_override_issue_manifest_path, manual_override_issue_manifest)
    if int(manual_override_issue_manifest["issue_summary"].get("blocking_count", 0) or 0) > 0:
        runtime_daily_digest = build_manual_override_daily_digest(
            run_id=run_id,
            issue_manifest=manual_override_issue_manifest,
            override_manifest={
                "configured_rows": manual_override_source_summary.get("counts", {}).get("configured", 0),
                "active_rows": manual_override_source_summary.get("counts", {}).get("active", 0),
                "applied_override_count": 0,
                "applied_row_count": 0,
                "affected_accounts": [],
                "affected_hosts": [],
                "final_consumer_scope": ["daily_snapshot", "analysis_snapshot", "feishu_report", "tsv_verify"],
            },
        )
        dump_manual_override_manifest(manual_override_daily_digest_path, runtime_daily_digest)
        top_messages = manual_override_issue_manifest["issue_summary"].get("top_blocking_messages", [])[:5]
        raise SystemExit(
            "[ERROR] 专项人工确认归属存在阻断问题，已生成异常清单；"
            f"清单={manual_override_issue_manifest_path}；问题={top_messages}"
        )

    for command in downstream_steps:
        subprocess.run(command, cwd=workspace, check=True, env=env)
        completed_steps.append(command)

    snapshot_csv = _pick_latest(snapshot_dir, "daily_performance_snapshot_latest_*.csv")
    ledger_csv = _pick_latest(snapshot_dir, "compensation_ledger_*.csv")
    analysis_snapshot_csv = _pick_latest_any(
        snapshot_dir,
        ["analysis_snapshot_unified-fact_latest_*.csv", "analysis_snapshot_latest_*.csv"],
    )
    snapshot_date = _extract_date(snapshot_csv)

    export_step = [
        sys.executable,
        "-m",
        "oae.cli.export_feishu_report",
        "--reports-dir",
        str(reports_dir),
        "--fact-csv",
        str(output_dir / "fact_attribution.csv"),
        "--manual-override-file",
        str(resolved_inputs["manual_attribution_overrides"]),
        "--live-file",
        str(resolved_inputs["live_schedule"]),
        "--leads-file",
        str(resolved_inputs["leads_detail"]),
        "--deals-file",
        str(resolved_inputs["deals_detail"]),
        "--topline-config",
        str(workspace / "config" / "report_topline_config.json"),
        "--snapshot-csv",
        str(snapshot_csv),
        "--ledger-csv",
        str(ledger_csv),
        "--analysis-snapshot-csv",
        str(analysis_snapshot_csv),
        "--export-dir",
        str(exports_dir),
        "--run-id",
        run_id,
        "--schema-version",
        SCHEMA_VERSION,
        "--metric-version",
        METRIC_VERSION,
        "--template-version",
        TEMPLATE_VERSION,
        "--freeze-id",
        freeze_id,
        "--report-date",
        args.report_date or snapshot_date,
    ]
    subprocess.run(export_step, cwd=workspace, check=True, env=env)
    completed_steps.append(export_step)

    verify_step = [
        sys.executable,
        "-m",
        "oae.cli.verify_report_tsv",
        "--reports-dir",
        str(reports_dir),
        "--fact-csv",
        str(output_dir / "fact_attribution.csv"),
        "--manual-override-file",
        str(resolved_inputs["manual_attribution_overrides"]),
        "--live-file",
        str(resolved_inputs["live_schedule"]),
        "--leads-file",
        str(resolved_inputs["leads_detail"]),
        "--deals-file",
        str(resolved_inputs["deals_detail"]),
        "--topline-config",
        str(workspace / "config" / "report_topline_config.json"),
        "--tsv",
        str(reports_dir / f"feishu_table_latest_{args.report_date or snapshot_date}.tsv"),
    ]
    subprocess.run(verify_step, cwd=workspace, check=True, env=env)
    completed_steps.append(verify_step)

    output_files = [
        output_dir / "fact_attribution.csv",
        _pick_latest(reports_dir, "daily_goal_account_latest_*.csv"),
        reports_dir / f"feishu_table_latest_{args.report_date or snapshot_date}.tsv",
        _pick_latest_any(
            workspace / "全量分析",
            ["analysis_workbook_unified-fact_latest_*.xlsx", "analysis_tables.xlsx"],
        ),
    ]
    baseline_manifest = workspace / "tests" / "baseline" / "reference_manifest.json"
    baseline_result = compare_files_against_manifest(baseline_manifest, output_files)
    manual_override_manifest = build_manual_override_manifest(
        fact_path=output_dir / "fact_attribution.csv",
        manual_override_path=resolved_inputs["manual_attribution_overrides"],
        run_id=run_id,
    )
    manual_override_manifest_path = runs_dir / f"manual_override_manifest_{run_id}.json"
    dump_manual_override_manifest(manual_override_manifest_path, manual_override_manifest)
    manual_override_daily_digest = build_manual_override_daily_digest(
        run_id=run_id,
        issue_manifest=manual_override_issue_manifest,
        override_manifest=manual_override_manifest,
    )
    dump_manual_override_manifest(manual_override_daily_digest_path, manual_override_daily_digest)
    manual_override_quality_summary = {
        **manual_override_manifest,
        "issues": manual_override_issue_manifest.get("issues", []),
        "issue_summary": manual_override_issue_manifest.get("issue_summary", {}),
    }
    manifest_paths = [
        exports_dir / f"feishu_report_latest_{args.report_date or snapshot_date}.manifest.json",
        exports_dir / f"feishu_table_latest_{args.report_date or snapshot_date}.manifest.json",
    ]
    analysis_manifest_dir = workspace / "artifacts" / "exports" / "analysis"
    analysis_naming_status_path = _pick_latest_any(
        analysis_manifest_dir,
        ["analysis_output_naming_status_unified-fact_latest_*.json"],
    )
    analysis_workbook_path = _pick_latest_any(
        workspace / "全量分析",
        ["analysis_workbook_unified-fact_latest_*.xlsx", "analysis_tables.xlsx"],
    )
    analysis_snapshot_manifest_path = _pick_latest_any(
        analysis_manifest_dir,
        ["analysis_snapshot_unified-fact_latest_*.manifest.json", "analysis_snapshot_latest_*.manifest.json"],
    )
    analysis_workbook_manifest_path = _pick_latest_any(
        analysis_manifest_dir,
        ["analysis_workbook_unified-fact_latest_*.manifest.json", "analysis_workbook_latest_*.manifest.json"],
    )
    business_checks = run_business_quality_checks(
        fact_path=output_dir / "fact_attribution.csv",
        snapshot_path=snapshot_csv,
        ledger_path=ledger_csv,
        analysis_snapshot_path=analysis_snapshot_csv,
        export_manifest_paths=manifest_paths,
        baseline_reference_dir=workspace / "tests" / "baseline" / "reference",
        expected_schema_version=SCHEMA_VERSION,
        expected_metric_version=METRIC_VERSION,
        expected_template_version=TEMPLATE_VERSION,
        expected_run_id=run_id,
        expected_freeze_id=freeze_id,
        quality_thresholds_path=(workspace / args.quality_thresholds).resolve(),
        quality_threshold_profile=quality_threshold_profile,
    )
    business_checks.append(build_manual_override_check(manual_override_quality_summary))
    business_checks.extend(_build_input_checks(input_manifest))
    quality_report = build_quality_report(
        run_id=run_id,
        output_files=output_files,
        baseline_result=baseline_result,
        extra_checks=business_checks,
        report_type="quality_report",
        report_scope="pipeline",
    )
    quality_report["inputs"] = {
        "manifest_path": str(input_manifest_path),
        "dynamic_input_root": input_manifest.get("dynamic_input_root", ""),
        "sources": input_manifest.get("sources", []),
    }
    quality_report["manual_overrides"] = {
        "manifest_path": str(manual_override_manifest_path),
        **manual_override_manifest,
    }
    quality_report["manual_override_issues"] = {
        "manifest_path": str(manual_override_issue_manifest_path),
        **manual_override_issue_manifest,
    }
    quality_report["manual_override_daily_digest"] = {
        "manifest_path": str(manual_override_daily_digest_path),
        **manual_override_daily_digest,
    }
    quality_report.setdefault("summary", {})["override_daily_digest"] = build_manual_override_daily_digest_view(
        manual_override_daily_digest
    )

    analysis_naming_status = json.loads(analysis_naming_status_path.read_text(encoding="utf-8"))

    run_manifest = {
        "run_id": run_id,
        "schema_version": SCHEMA_VERSION,
        "metric_version": METRIC_VERSION,
        "template_version": TEMPLATE_VERSION,
        "freeze_id": freeze_id,
        "quality_threshold_profile": quality_threshold_profile,
        "analysis_output_default_paths": {
            "snapshot": str(analysis_snapshot_csv),
            "workbook": str(analysis_workbook_path),
            "snapshot_manifest": str(analysis_snapshot_manifest_path),
            "workbook_manifest": str(analysis_workbook_manifest_path),
        },
        "analysis_naming_status_path": str(analysis_naming_status_path),
        "analysis_naming_status": analysis_naming_status,
        "analysis_disable_gate": analysis_naming_status.get("pre_disable_check", {}),
        "analysis_disable_dry_run": analysis_naming_status.get("dry_run_result", {}),
        "workspace": str(workspace),
        "input_manifest": str(input_manifest_path),
        "inputs": input_manifest.get("sources", []),
        "manual_override_input": {
            key: value
            for key, value in manual_override_source_summary.items()
            if key not in {"normalized", "overrides"}
        },
        "manual_override_manifest": str(manual_override_manifest_path),
        "manual_overrides": manual_override_manifest,
        "manual_override_issue_manifest": str(manual_override_issue_manifest_path),
        "manual_override_issues": manual_override_issue_manifest,
        "manual_override_daily_digest_manifest": str(manual_override_daily_digest_path),
        "manual_override_daily_digest": manual_override_daily_digest,
        "steps": completed_steps,
    }
    (runs_dir / f"run_manifest_{run_id}.json").write_text(json.dumps(run_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (runs_dir / f"quality_report_{run_id}.json").write_text(json.dumps(quality_report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "run_manifest": str(runs_dir / f"run_manifest_{run_id}.json"),
                "quality_report": str(runs_dir / f"quality_report_{run_id}.json"),
                "input_manifest": str(input_manifest_path),
                "manual_override_issue_manifest": str(manual_override_issue_manifest_path),
                "manual_override_daily_digest": str(manual_override_daily_digest_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _pick_latest(directory: Path, pattern: str) -> Path:
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        raise FileNotFoundError(f"{directory} 下未找到 {pattern}")
    return candidates[-1]


def _pick_latest_any(directory: Path, patterns: list[str]) -> Path:
    for pattern in patterns:
        candidates = sorted(directory.glob(pattern))
        if candidates:
            return candidates[-1]
    raise FileNotFoundError(f"{directory} 下未找到任何匹配: {patterns}")


def _extract_date(path: Path) -> str:
    matched = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if not matched:
        raise ValueError(f"无法从文件名提取日期: {path}")
    return matched.group(1)


def _build_input_checks(input_manifest: dict[str, object]) -> list[dict[str, object]]:
    sources = input_manifest.get("sources", [])
    warning_sources = [
        source["label"]
        for source in sources
        if source.get("naming_status") != "pass" or source.get("validation_warnings")
    ]
    return [
        {
            "name": "input.discovery",
            "category": "pass" if not warning_sources else "structural change",
            "status": "pass" if not warning_sources else "warning",
            "details": {
                "summary": "输入发现完成" if not warning_sources else f"输入发现完成，但存在需关注项：{warning_sources}",
                "source_count": len(sources),
                "dynamic_input_root": input_manifest.get("dynamic_input_root", ""),
            },
        },
        {
            "name": "input.manifest",
            "category": "pass",
            "status": "pass",
            "details": {
                "summary": "已写出 input manifest，可追溯本次运行实际消费的源文件",
            },
        },
    ]


if __name__ == "__main__":
    main()
