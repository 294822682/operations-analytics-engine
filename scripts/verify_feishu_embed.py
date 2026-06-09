#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class HttpResult:
    status: int
    content_type: str
    body: str


def verify(base_url: str, *, start_date: str | None = None, end_date: str | None = None, timeout: float = 10.0) -> dict[str, Any]:
    base = base_url.rstrip("/")
    health = _json_get(f"{base}/healthz", timeout=timeout)
    _require(health.get("status") == "ok", "/healthz must return {'status': 'ok'}")

    latest = _json_get(f"{base}/dashboard/daily/latest", timeout=timeout)
    report_date = str(latest.get("report_date") or "")
    _require(report_date, "/dashboard/daily/latest must include report_date")

    html = _text_get(f"{base}/dashboard/daily/latest/feishu-link", timeout=timeout)
    for marker in (
        "运营日报 BI",
        'id="business-range-query"',
        "/dashboard/daily/trends/prototype",
        'data-dashboard-mode="business"',
    ):
        _require(marker in html, f"Feishu embed HTML missing marker: {marker}")

    trend_end = end_date or report_date
    params = {"end_date": trend_end}
    if start_date:
        params["start_date"] = start_date
    trends = _json_get(f"{base}/dashboard/daily/trends?{urlencode(params)}", timeout=timeout)
    date_range = trends.get("date_range") or {}
    selected_days = int(date_range.get("days") or trends.get("selected_range_days") or 0)
    _require(0 < selected_days <= 92, "trend range must be between 1 and 92 days")
    _require(trends.get("daily_trends"), "/dashboard/daily/trends must include daily_trends")

    return {
        "base_url": base,
        "report_date": report_date,
        "feishu_embed_url": f"{base}/dashboard/daily/latest/feishu-link",
        "trend_range": date_range,
        "daily_trend_count": len(trends.get("daily_trends") or []),
        "status": "ok",
    }


def _json_get(url: str, *, timeout: float) -> dict[str, Any]:
    body = _text_get(url, timeout=timeout)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"Expected JSON response from {url}") from exc
    _require(isinstance(payload, dict), f"Expected JSON object from {url}")
    return payload


def _text_get(url: str, *, timeout: float) -> str:
    result = _read_url(url, timeout=timeout)
    _require(200 <= result.status < 300, f"{url} returned HTTP {result.status}")
    return result.body


def _read_url(url: str, *, timeout: float) -> HttpResult:
    request = Request(url, headers={"User-Agent": "oae-feishu-embed-verifier/1.0"})
    with urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return HttpResult(
            status=response.status,
            content_type=response.headers.get("content-type", ""),
            body=body,
        )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify the OAE Feishu BI embed endpoint.")
    parser.add_argument("base_url", help="Public HTTPS or local base URL, for example https://example.onrender.com")
    parser.add_argument("--start-date", help="Optional trend start date, YYYY-MM-DD")
    parser.add_argument("--end-date", help="Optional trend end date, YYYY-MM-DD")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    args = parser.parse_args(argv)

    try:
        result = verify(
            args.base_url,
            start_date=args.start_date,
            end_date=args.end_date,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
