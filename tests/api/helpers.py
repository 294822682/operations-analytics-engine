from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from oae.exports.feishu_dashboard_interactive_html import (
    render_api_connected_dashboard_html,
    render_feishu_link_trial_dashboard_html,
    render_trend_dashboard_html,
)
from oae.services.dashboard_daily_service import DashboardDailyService
from oae.utils import ApiError, build_error_response


REPO_ROOT = Path(__file__).resolve().parents[2]
CORE_REPO_DIRS = ("src", "config", "docs", "templates", "transform")
CORE_REPO_FILES = ("read me.md",)
MINIMAL_MONTHLY_TARGETS_CSV = """month,scope_type,scope_name,parent_account,lead_target_month,deal_target_month,lead_cost_target_month,cpl_target,cps_target,target_pool
2026-04,account,抖音-星途汽车官方直播间,,100,5,10000,100,2000,官方目标池
2026-04,anchor,丁俐佳,抖音-星途汽车官方直播间,20,1,2000,100,2000,
"""
MINIMAL_DAILY_SPEND_CSV = "date,account,actual_spend\n"
MINIMAL_SEED_MONTHLY_TARGETS_CSV = """month,scope_type,scope_name,parent_scope,parent_account,impression_target_month,spend_target_month,cpm_target,target_pool
2026-04,account,种草组总池,,,10000,5000,500,种草目标池
"""
MINIMAL_MANUAL_OVERRIDES_CSV = (
    "override_id,business_subject_key,phone,lead_id,override_scope,target_account,target_host,reason,"
    "evidence_note,confirmed_by,confirmed_at,effective_from,effective_to,status,metric_version,run_id\n"
)


def build_temp_repo(tmp_path: Path, *, include_run_latest_sources: bool = False) -> tuple[Path, Path]:
    repo_root = tmp_path / "repo"
    runs_root = tmp_path / "runs"
    repo_root.mkdir(parents=True, exist_ok=True)
    runs_root.mkdir(parents=True, exist_ok=True)

    for name in CORE_REPO_DIRS:
        source = REPO_ROOT / name
        if source.exists():
            shutil.copytree(source, repo_root / name, dirs_exist_ok=True)

    baseline_src = REPO_ROOT / "tests" / "baseline"
    if baseline_src.exists():
        shutil.copytree(baseline_src, repo_root / "tests" / "baseline", dirs_exist_ok=True)

    for name in CORE_REPO_FILES:
        source = REPO_ROOT / name
        if source.exists():
            shutil.copy2(source, repo_root / name)

    for path in (
        repo_root / "源文件",
        repo_root / "artifacts" / "runs",
        repo_root / "artifacts" / "registry",
        repo_root / "artifacts" / "snapshots",
        repo_root / "artifacts" / "exports" / "analysis",
        repo_root / "output" / "sql_reports",
        repo_root / "全量分析",
    ):
        path.mkdir(parents=True, exist_ok=True)

    _ensure_minimal_fixed_configs(repo_root)
    if include_run_latest_sources:
        raise NotImplementedError("run latest source fixtures are not needed for dashboard tests")
    return repo_root, runs_root


def create_test_app(repo_root: Path, runs_root: Path) -> FastAPI:
    app = FastAPI()
    service = DashboardDailyService(repo_root=repo_root)
    app.state.runs_root = runs_root

    @app.exception_handler(ApiError)
    async def handle_api_error(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=build_error_response(exc.code, exc.message, exc.details),
        )

    @app.get("/dashboard/daily/trends/prototype", response_class=HTMLResponse)
    def get_trends_prototype(start_date: Optional[str] = None, end_date: Optional[str] = None) -> HTMLResponse:
        api_path = _trend_api_path(start_date=start_date, end_date=end_date)
        return HTMLResponse(render_trend_dashboard_html(api_path=api_path))

    @app.get("/dashboard/daily/trends")
    def get_trends(start_date: Optional[str] = None, end_date: Optional[str] = None) -> dict[str, Any]:
        return service.get_daily_dashboard_trends(start_date=start_date, end_date=end_date)

    @app.get("/dashboard/daily/latest/prototype", response_class=HTMLResponse)
    def get_latest_prototype() -> HTMLResponse:
        return HTMLResponse(render_api_connected_dashboard_html("latest", api_path="/dashboard/daily/latest"))

    @app.get("/dashboard/daily/latest/feishu-link", response_class=HTMLResponse)
    def get_latest_feishu_link() -> HTMLResponse:
        return HTMLResponse(render_feishu_link_trial_dashboard_html("latest", api_path="/dashboard/daily/latest"))

    @app.get("/dashboard/daily/latest")
    def get_latest() -> dict[str, Any]:
        return service.get_latest_daily_dashboard()

    @app.get("/dashboard/daily/{report_date}/prototype", response_class=HTMLResponse)
    def get_dated_prototype(report_date: str) -> HTMLResponse:
        api_path = f"/dashboard/daily/{report_date}"
        return HTMLResponse(render_api_connected_dashboard_html(report_date, api_path=api_path))

    @app.get("/dashboard/daily/{report_date}/feishu-link", response_class=HTMLResponse)
    def get_dated_feishu_link(report_date: str) -> HTMLResponse:
        api_path = f"/dashboard/daily/{report_date}"
        return HTMLResponse(render_feishu_link_trial_dashboard_html(report_date, api_path=api_path))

    @app.get("/dashboard/daily/{report_date}")
    def get_daily(report_date: str) -> dict[str, Any]:
        return service.get_daily_dashboard(report_date)

    return app


def _trend_api_path(*, start_date: Optional[str], end_date: Optional[str]) -> str:
    params = []
    if start_date:
        params.append(f"start_date={start_date}")
    if end_date:
        params.append(f"end_date={end_date}")
    return "/dashboard/daily/trends" + (f"?{'&'.join(params)}" if params else "")


def _ensure_minimal_fixed_configs(repo_root: Path) -> None:
    config_dir = repo_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    fixed_files = {
        "monthly_targets.csv": MINIMAL_MONTHLY_TARGETS_CSV,
        "seed_monthly_targets.csv": MINIMAL_SEED_MONTHLY_TARGETS_CSV,
        "daily_spend.csv": MINIMAL_DAILY_SPEND_CSV,
        "manual_attribution_overrides.csv": MINIMAL_MANUAL_OVERRIDES_CSV,
    }
    for filename, contents in fixed_files.items():
        (config_dir / filename).write_text(contents, encoding="utf-8-sig")


def _replace_fixture_workspace(value: Any, workspace: str) -> Any:
    if isinstance(value, str):
        return value.replace("__WORKSPACE__", workspace)
    if isinstance(value, list):
        return [_replace_fixture_workspace(item, workspace) for item in value]
    if isinstance(value, dict):
        return {key: _replace_fixture_workspace(item, workspace) for key, item in value.items()}
    return value


def stage_report_run_fixture(repo_root: Path, fixture_name: str = "current_review_candidate") -> Path:
    fixture_dir = REPO_ROOT / "tests" / "fixtures" / "report_runs" / fixture_name
    if not fixture_dir.exists():
        raise FileNotFoundError(f"Missing report run fixture directory: {fixture_dir}")

    target_runs_dir = repo_root / "artifacts" / "runs"
    target_runs_dir.mkdir(parents=True, exist_ok=True)
    run_manifest_path: Path | None = None
    for source_path in sorted(fixture_dir.glob("*.json")):
        target_path = target_runs_dir / source_path.name
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        payload = _replace_fixture_workspace(payload, str(repo_root))
        target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        if source_path.name.startswith("run_manifest_"):
            run_manifest_path = target_path
    if run_manifest_path is None:
        raise FileNotFoundError(f"No run_manifest fixture found in {fixture_dir}")
    return run_manifest_path
