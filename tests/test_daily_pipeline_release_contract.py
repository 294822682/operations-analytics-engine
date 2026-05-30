from __future__ import annotations

from pathlib import Path

import pytest

from oae.jobs.daily_pipeline import _resolve_target_report_month


def _write_targets(path: Path, months: list[str]) -> None:
    rows = [
        "month,scope_type,scope_name,parent_account,lead_target_month,deal_target_month",
        *[f"{month},account,EXEED,,100,10" for month in months],
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_resolve_target_report_month_uses_dynamic_input_month_when_targets_include_it(tmp_path: Path) -> None:
    targets_path = tmp_path / "monthly_targets.csv"
    _write_targets(targets_path, ["2026-03", "2026-05"])

    report_month = _resolve_target_report_month(
        {
            "sources": [
                {"source_key": "live_schedule", "kind": "dynamic", "business_date": "2026-05"},
                {"source_key": "leads_detail", "kind": "dynamic", "business_date": "2026-05-29"},
                {"source_key": "deals_detail", "kind": "dynamic", "business_date": "2026-05-29"},
                {"source_key": "monthly_targets", "kind": "fixed", "business_date": ""},
            ]
        },
        targets_path,
        explicit_report_date="",
    )

    assert report_month == "2026-05"


def test_resolve_target_report_month_blocks_when_input_month_is_missing_from_targets(tmp_path: Path) -> None:
    targets_path = tmp_path / "monthly_targets.csv"
    _write_targets(targets_path, ["2026-03"])

    with pytest.raises(SystemExit, match="目标月份配置缺失.*输入业务月份=2026-05.*2026-03"):
        _resolve_target_report_month(
            {
                "sources": [
                    {"source_key": "live_schedule", "kind": "dynamic", "business_date": "2026-05"},
                    {"source_key": "leads_detail", "kind": "dynamic", "business_date": "2026-05-29"},
                    {"source_key": "deals_detail", "kind": "dynamic", "business_date": "2026-05-29"},
                ]
            },
            targets_path,
            explicit_report_date="",
        )


def test_resolve_target_report_month_blocks_mixed_dynamic_input_months(tmp_path: Path) -> None:
    targets_path = tmp_path / "monthly_targets.csv"
    _write_targets(targets_path, ["2026-05"])

    with pytest.raises(SystemExit, match="动态输入业务月份不一致.*live_schedule=2026-05.*leads_detail=2026-04"):
        _resolve_target_report_month(
            {
                "sources": [
                    {"source_key": "live_schedule", "kind": "dynamic", "business_date": "2026-05"},
                    {"source_key": "leads_detail", "kind": "dynamic", "business_date": "2026-04-30"},
                ]
            },
            targets_path,
            explicit_report_date="",
        )

