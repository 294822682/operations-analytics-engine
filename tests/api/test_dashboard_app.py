from __future__ import annotations

from fastapi.testclient import TestClient

from oae.api.dashboard_app import create_dashboard_app
from tests.api.helpers import build_temp_repo
from tests.test_feishu_dashboard_interactive_html import _sample_rows, _write_tsv


def test_dashboard_app_serves_feishu_embed_routes_from_repo_root(tmp_path) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-14.tsv", _sample_rows())
    app = create_dashboard_app(repo_root=repo_root, runs_root=runs_root)

    with TestClient(app) as client:
        health = client.get("/healthz")
        latest = client.get("/dashboard/daily/latest")
        feishu_link = client.get("/dashboard/daily/latest/feishu-link")

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert latest.status_code == 200
    assert latest.json()["report_date"] == "2026-05-14"
    assert feishu_link.status_code == 200
    assert 'id="business-range-query"' in feishu_link.text
    assert "/dashboard/daily/trends/prototype" in feishu_link.text


def test_dashboard_app_uses_environment_repo_root(tmp_path, monkeypatch) -> None:
    repo_root, runs_root = build_temp_repo(tmp_path)
    reports_dir = repo_root / "output" / "sql_reports"
    _write_tsv(reports_dir / "feishu_dashboard_source_latest_2026-05-14.tsv", _sample_rows())
    monkeypatch.setenv("OAE_REPO_ROOT", str(repo_root))
    monkeypatch.setenv("OAE_RUNS_ROOT", str(runs_root))
    app = create_dashboard_app()

    with TestClient(app) as client:
        response = client.get("/dashboard/daily/2026-05-14")

    assert response.status_code == 200
    assert response.json()["source"]["path"] == "output/sql_reports/feishu_dashboard_source_latest_2026-05-14.tsv"
