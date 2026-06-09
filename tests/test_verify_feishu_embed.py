from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_verifier():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "verify_feishu_embed.py"
    spec = importlib.util.spec_from_file_location("verify_feishu_embed", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_verify_feishu_embed_checks_health_html_and_trends(monkeypatch) -> None:
    verifier = _load_verifier()

    def fake_read_url(url: str, *, timeout: float):
        assert timeout == 3.0
        if url == "https://bi.example.com/healthz":
            return verifier.HttpResult(200, "application/json", '{"status":"ok"}')
        if url == "https://bi.example.com/dashboard/daily/latest":
            return verifier.HttpResult(200, "application/json", '{"report_date":"2026-06-07"}')
        if url == "https://bi.example.com/dashboard/daily/latest/feishu-link":
            html = '运营日报 BI <main data-dashboard-mode="business"><form id="business-range-query" action="/dashboard/daily/trends/prototype"></form></main>'
            return verifier.HttpResult(200, "text/html", html)
        if url == "https://bi.example.com/dashboard/daily/trends?end_date=2026-06-07":
            body = '{"date_range":{"start":"2026-03-31","end":"2026-06-07","days":69},"daily_trends":[{"key":"leads"}]}'
            return verifier.HttpResult(200, "application/json", body)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(verifier, "_read_url", fake_read_url)

    result = verifier.verify("https://bi.example.com/", timeout=3.0)

    assert result["status"] == "ok"
    assert result["report_date"] == "2026-06-07"
    assert result["feishu_embed_url"] == "https://bi.example.com/dashboard/daily/latest/feishu-link"
    assert result["daily_trend_count"] == 1


def test_verify_feishu_embed_fails_when_range_is_over_one_quarter(monkeypatch) -> None:
    verifier = _load_verifier()

    def fake_read_url(url: str, *, timeout: float):
        if url.endswith("/healthz"):
            return verifier.HttpResult(200, "application/json", '{"status":"ok"}')
        if url.endswith("/dashboard/daily/latest"):
            return verifier.HttpResult(200, "application/json", '{"report_date":"2026-06-07"}')
        if url.endswith("/dashboard/daily/latest/feishu-link"):
            html = '运营日报 BI data-dashboard-mode="business" id="business-range-query" /dashboard/daily/trends/prototype'
            return verifier.HttpResult(200, "text/html", html)
        if "/dashboard/daily/trends?" in url:
            body = '{"date_range":{"start":"2026-03-01","end":"2026-06-07","days":99},"daily_trends":[{"key":"leads"}]}'
            return verifier.HttpResult(200, "application/json", body)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(verifier, "_read_url", fake_read_url)

    try:
        verifier.verify("https://bi.example.com")
    except AssertionError as exc:
        assert "between 1 and 92 days" in str(exc)
    else:
        raise AssertionError("expected verifier to reject over-quarter range")
