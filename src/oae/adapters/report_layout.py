"""Shared naming/layout helpers for report artifacts."""

from __future__ import annotations

from pathlib import Path


RUN_MANIFEST_GLOB = "run_manifest_*.json"
QUALITY_REPORT_GLOB = "quality_report_*.json"
INPUT_MANIFEST_GLOB = "input_manifest_*.json"
PREFLIGHT_MANIFEST_GLOB = "preflight_manifest_*.json"
DOCTOR_MANIFEST_GLOB = "doctor_manifest_*.json"
MANUAL_OVERRIDE_ISSUE_MANIFEST_GLOB = "manual_override_issue_manifest_*.json"
ANALYSIS_WORKBOOK_GLOBS = ("analysis_workbook_unified-fact_latest_*.xlsx", "analysis_tables.xlsx")


def daily_goal_account_month_filename(report_month: str) -> str:
    return f"daily_goal_account_{report_month}.csv"


def daily_goal_anchor_month_filename(report_month: str) -> str:
    return f"daily_goal_anchor_{report_month}.csv"


def daily_goal_account_latest_filename(report_date: str) -> str:
    return f"daily_goal_account_latest_{report_date}.csv"


def daily_goal_anchor_latest_filename(report_date: str) -> str:
    return f"daily_goal_anchor_latest_{report_date}.csv"


def daily_performance_snapshot_month_filename(report_month: str) -> str:
    return f"daily_performance_snapshot_{report_month}.csv"


def daily_performance_snapshot_latest_filename(report_date: str) -> str:
    return f"daily_performance_snapshot_latest_{report_date}.csv"


def compensation_ledger_filename(settlement_period: str, report_date: str) -> str:
    return f"compensation_ledger_{settlement_period}_{report_date}.csv"


def feishu_report_filename(report_date: str) -> str:
    return f"feishu_report_latest_{report_date}.md"


def feishu_table_filename(report_date: str) -> str:
    return f"feishu_table_latest_{report_date}.tsv"


def feishu_table_manifest_filename(report_date: str) -> str:
    return f"feishu_table_latest_{report_date}.manifest.json"


def feishu_report_manifest_filename(report_date: str) -> str:
    return f"feishu_report_latest_{report_date}.manifest.json"


def reports_dir(workspace: Path) -> Path:
    return workspace / "output" / "sql_reports"


def snapshots_dir(workspace: Path) -> Path:
    return workspace / "artifacts" / "snapshots"


def analysis_dir(workspace: Path) -> Path:
    return workspace / "全量分析"


def exports_dir(workspace: Path) -> Path:
    return workspace / "artifacts" / "exports"


def runs_dir(workspace: Path) -> Path:
    return workspace / "artifacts" / "runs"


def daily_goal_account_month_path(workspace: Path, report_month: str) -> Path:
    return reports_dir(workspace) / daily_goal_account_month_filename(report_month)


def daily_goal_anchor_month_path(workspace: Path, report_month: str) -> Path:
    return reports_dir(workspace) / daily_goal_anchor_month_filename(report_month)


def daily_goal_account_latest_path(workspace: Path, report_date: str) -> Path:
    return reports_dir(workspace) / daily_goal_account_latest_filename(report_date)


def daily_goal_anchor_latest_path(workspace: Path, report_date: str) -> Path:
    return reports_dir(workspace) / daily_goal_anchor_latest_filename(report_date)


def daily_performance_snapshot_month_path(workspace: Path, report_month: str) -> Path:
    return snapshots_dir(workspace) / daily_performance_snapshot_month_filename(report_month)


def daily_performance_snapshot_latest_path(workspace: Path, report_date: str) -> Path:
    return snapshots_dir(workspace) / daily_performance_snapshot_latest_filename(report_date)


def compensation_ledger_path(workspace: Path, settlement_period: str, report_date: str) -> Path:
    return snapshots_dir(workspace) / compensation_ledger_filename(settlement_period, report_date)


def feishu_report_path(workspace: Path, report_date: str) -> Path:
    return reports_dir(workspace) / feishu_report_filename(report_date)


def feishu_table_path(workspace: Path, report_date: str) -> Path:
    return reports_dir(workspace) / feishu_table_filename(report_date)


def feishu_table_manifest_path(export_dir: Path, report_date: str) -> Path:
    return export_dir / feishu_table_manifest_filename(report_date)


def feishu_report_manifest_path(export_dir: Path, report_date: str) -> Path:
    return export_dir / feishu_report_manifest_filename(report_date)


def manual_override_issue_manifest_path(workspace: Path, run_id: str) -> Path:
    return runs_dir(workspace) / f"manual_override_issue_manifest_{run_id}.json"


def manual_override_daily_digest_path(workspace: Path, run_id: str) -> Path:
    return runs_dir(workspace) / f"manual_override_daily_digest_{run_id}.json"


def manual_override_manifest_path(workspace: Path, run_id: str) -> Path:
    return runs_dir(workspace) / f"manual_override_manifest_{run_id}.json"


def preflight_manifest_path(workspace: Path, run_id: str) -> Path:
    return runs_dir(workspace) / f"preflight_manifest_{run_id}.json"


def doctor_manifest_path(workspace: Path, run_id: str) -> Path:
    return runs_dir(workspace) / f"doctor_manifest_{run_id}.json"


def infer_workspace_root_from_artifact_path(path: Path, repo_root: Path) -> Path | None:
    resolved = path.expanduser()
    if not resolved.exists() and not resolved.is_absolute():
        resolved = (repo_root / resolved).resolve()
    else:
        resolved = resolved.resolve()

    if len(resolved.parents) >= 3:
        parent_names = tuple(part.name for part in resolved.parents[:3])
        if parent_names[:2] == ("sql_reports", "output"):
            return resolved.parents[2]
        if parent_names[:2] == ("snapshots", "artifacts"):
            return resolved.parents[2]
        if parent_names[:2] == ("runs", "artifacts"):
            return resolved.parents[2]
    return None
