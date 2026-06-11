from __future__ import annotations

from pathlib import Path
from typing import Any

import oae.quality.business as business


def test_run_business_quality_checks_aggregates_layer_checks_in_pipeline_order(
    monkeypatch,
    tmp_path: Path,
) -> None:
    thresholds = {
        "fact": {"fact_rule": {"mode": "absolute"}},
        "snapshot": {"snapshot_rule": {"mode": "absolute"}},
        "ledger": {"ledger_rule": {"mode": "absolute"}},
        "analysis": {"analysis_rule": {"mode": "absolute"}},
    }
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def fake_load_quality_thresholds(path: Path | None, *, profile: str) -> tuple[dict[str, object], str, str]:
        calls.append(("thresholds", (path,), {"profile": profile}))
        return thresholds, "config/quality_thresholds.json", "settlement"

    def fake_fact_layer(*args: Any, **kwargs: Any) -> list[dict[str, object]]:
        calls.append(("fact", args, kwargs))
        return [{"name": "fact.structural", "category": "structural change", "status": "pass", "details": {}}]

    def fake_snapshot_layer(*args: Any, **kwargs: Any) -> list[dict[str, object]]:
        calls.append(("snapshot", args, kwargs))
        return [{"name": "snapshot.account_total_reconcile", "category": "structural change", "status": "pass", "details": {}}]

    def fake_ledger_layer(*args: Any, **kwargs: Any) -> list[dict[str, object]]:
        calls.append(("ledger", args, kwargs))
        return [{"name": "ledger.snapshot_reconcile", "category": "metric drift", "status": "pass", "details": {}}]

    def fake_export_contracts(*args: Any, **kwargs: Any) -> list[dict[str, object]]:
        calls.append(("export", args, kwargs))
        return [{"name": "contract.feishu_table", "category": "contract violation", "status": "pass", "details": {}}]

    def fake_analysis_snapshot(*args: Any, **kwargs: Any) -> list[dict[str, object]]:
        calls.append(("analysis", args, kwargs))
        return [{"name": "analysis.subject_areas", "category": "contract violation", "status": "pass", "details": {}}]

    monkeypatch.setattr(business, "load_quality_thresholds", fake_load_quality_thresholds)
    monkeypatch.setattr(business, "check_fact_layer", fake_fact_layer)
    monkeypatch.setattr(business, "check_snapshot_layer", fake_snapshot_layer)
    monkeypatch.setattr(business, "check_ledger_layer", fake_ledger_layer)
    monkeypatch.setattr(business, "check_export_contracts", fake_export_contracts)
    monkeypatch.setattr(business, "check_analysis_snapshot", fake_analysis_snapshot)

    fact_path = tmp_path / "fact_attribution.csv"
    snapshot_path = tmp_path / "daily_performance_snapshot.csv"
    ledger_path = tmp_path / "compensation_ledger.csv"
    analysis_snapshot_path = tmp_path / "analysis_snapshot.csv"
    export_manifest_paths = [tmp_path / "feishu_report.manifest.json", tmp_path / "feishu_table.manifest.json"]
    thresholds_path = tmp_path / "quality_thresholds.json"

    checks = business.run_business_quality_checks(
        fact_path=fact_path,
        snapshot_path=snapshot_path,
        ledger_path=ledger_path,
        analysis_snapshot_path=analysis_snapshot_path,
        export_manifest_paths=export_manifest_paths,
        baseline_reference_dir=tmp_path / "baseline",
        expected_schema_version="schema-v1",
        expected_metric_version="metric-v1",
        expected_template_version="template-v1",
        expected_run_id="run-20260608T010203Z",
        expected_freeze_id="provisional",
        quality_thresholds_path=thresholds_path,
        quality_threshold_profile="settlement",
    )

    assert [call[0] for call in calls] == ["thresholds", "fact", "snapshot", "ledger", "export", "analysis"]
    assert [item["name"] for item in checks] == [
        "fact.structural",
        "snapshot.account_total_reconcile",
        "ledger.snapshot_reconcile",
        "contract.feishu_table",
        "analysis.subject_areas",
    ]
    assert calls[0] == ("thresholds", (thresholds_path,), {"profile": "settlement"})
    assert calls[1][1] == (fact_path, tmp_path / "baseline" / "fact_attribution.csv")
    assert calls[1][2]["thresholds"] == thresholds["fact"]
    assert calls[1][2]["threshold_source"] == "config/quality_thresholds.json"
    assert calls[1][2]["threshold_profile"] == "settlement"
    assert calls[2][1] == (snapshot_path, tmp_path / "baseline" / "daily_goal_account_latest_2026-03-12.csv")
    assert calls[2][2]["thresholds"] == thresholds["snapshot"]
    assert calls[3][1] == (ledger_path, snapshot_path)
    assert calls[3][2]["thresholds"] == thresholds["ledger"]
    assert calls[4][1] == (export_manifest_paths,)
    assert calls[4][2] == {
        "expected_schema_version": "schema-v1",
        "expected_metric_version": "metric-v1",
        "expected_template_version": "template-v1",
        "expected_run_id": "run-20260608T010203Z",
        "expected_freeze_id": "provisional",
    }
    assert calls[5][1] == (analysis_snapshot_path,)
    assert calls[5][2]["thresholds"] == thresholds["analysis"]
