from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from oae.exports.feishu_dashboard_interactive_html import (
    render_api_connected_dashboard_html,
    render_feishu_link_trial_dashboard_html,
    render_trend_dashboard_html,
)
from oae.services.dashboard_daily_service import DashboardDailyService
from oae.utils import ApiError, build_error_response


def create_dashboard_app(repo_root: Path | None = None, runs_root: Path | None = None) -> FastAPI:
    resolved_repo_root = _resolve_path(repo_root, env_key="OAE_REPO_ROOT", default=Path.cwd())
    resolved_runs_root = _resolve_path(
        runs_root,
        env_key="OAE_RUNS_ROOT",
        default=resolved_repo_root / "artifacts" / "runs",
    )
    service = DashboardDailyService(repo_root=resolved_repo_root)
    app = FastAPI(title="Operations Analytics Engine BI")
    app.state.repo_root = resolved_repo_root
    app.state.runs_root = resolved_runs_root

    @app.exception_handler(ApiError)
    async def handle_api_error(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=build_error_response(exc.code, exc.message, exc.details),
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/dashboard/daily/latest/feishu-link")

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


def _resolve_path(value: Path | None, *, env_key: str, default: Path) -> Path:
    raw_value = value if value is not None else os.environ.get(env_key)
    return Path(raw_value or default).expanduser().resolve()


def _trend_api_path(*, start_date: Optional[str], end_date: Optional[str]) -> str:
    params = {key: value for key, value in (("start_date", start_date), ("end_date", end_date)) if value}
    return "/dashboard/daily/trends" + (f"?{urlencode(params)}" if params else "")


app = create_dashboard_app()
