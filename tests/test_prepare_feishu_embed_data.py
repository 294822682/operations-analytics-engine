from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_packager():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "prepare_feishu_embed_data.py"
    spec = importlib.util.spec_from_file_location("prepare_feishu_embed_data", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_prepare_feishu_embed_data_copies_only_dashboard_sources(tmp_path) -> None:
    packager = _load_packager()
    repo_root = tmp_path / "repo"
    reports_dir = repo_root / "output" / "sql_reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "feishu_dashboard_source_latest_2026-06-07.tsv").write_text("report_date\tmetric\n2026-06-07\t1\n", encoding="utf-8")
    (reports_dir / "feishu_dashboard_source_latest_2026-06-08.tsv").write_text("report_date\tmetric\n2026-06-08\t2\n", encoding="utf-8")
    (reports_dir / "feishu_report_latest_2026-06-08.md").write_text("not copied", encoding="utf-8")
    (repo_root / "output" / "fact_attribution.csv").write_text("not copied", encoding="utf-8")

    result = packager.prepare_data_bundle(repo_root, tmp_path / "bundle")

    copied = sorted(path.name for path in (tmp_path / "bundle" / "output" / "sql_reports").glob("*"))
    assert copied == [
        "feishu_dashboard_source_latest_2026-06-07.tsv",
        "feishu_dashboard_source_latest_2026-06-08.tsv",
    ]
    assert result["file_count"] == 2
    assert result["latest_file"] == "feishu_dashboard_source_latest_2026-06-08.tsv"
    assert not (tmp_path / "bundle" / "output" / "fact_attribution.csv").exists()
    manifest = json.loads((tmp_path / "bundle" / "feishu_embed_data_manifest.json").read_text(encoding="utf-8"))
    assert manifest["source_pattern"] == "output/sql_reports/feishu_dashboard_source_latest_*.tsv"
    assert len(manifest["files"]) == 2


def test_prepare_feishu_embed_data_requires_dashboard_sources(tmp_path) -> None:
    packager = _load_packager()
    repo_root = tmp_path / "repo"
    (repo_root / "output" / "sql_reports").mkdir(parents=True)

    try:
        packager.prepare_data_bundle(repo_root, tmp_path / "bundle")
    except FileNotFoundError as exc:
        assert "No dashboard source TSV files found" in str(exc)
    else:
        raise AssertionError("expected missing dashboard source error")
