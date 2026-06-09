from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

from oae.ingest import discover_runtime_inputs


def test_discover_runtime_inputs_selects_seed_ledger_without_business_date(tmp_path: Path) -> None:
    source_dir = tmp_path / "源文件"
    source_dir.mkdir()
    older = source_dir / "EXEED星途台账（五月）.xlsx"
    newer = source_dir / "EXEED星途台账（六月）.xlsx"
    pd.DataFrame(
        [
            {
                "创建时间": "2026-06-07",
                "开播账号": "抖音-EXEED星途",
                "本场主播": "刘花旗",
                "曝光人数": 1000,
            }
        ]
    ).to_excel(older, index=False)
    pd.DataFrame(
        [
            {
                "创建时间": "2026-06-08",
                "开播账号": "抖音-EXEED星途",
                "本场主播": "桂婕",
                "曝光人数": 2000,
            }
        ]
    ).to_excel(newer, index=False)
    os.utime(older, (1_700_000_000, 1_700_000_000))
    os.utime(newer, (1_710_000_000, 1_710_000_000))

    config_path = tmp_path / "input_sources.json"
    config_path.write_text(
        json.dumps(
            {
                "default_dynamic_input_dir": "源文件",
                "sources": {
                    "seed_live_ledger": {
                        "label": "EXEED 星途台账",
                        "kind": "dynamic",
                        "directory": "源文件",
                        "glob_patterns": ["*EXEED星途台账*.xlsx"],
                        "naming_regex": "",
                        "file_types": [".xlsx"],
                        "business_date_type": "none",
                        "allow_multiple_versions": True,
                        "selection_rule": "按文件修改时间优先",
                        "required_alias_keys": ["live_date", "live_account", "live_host"],
                        "required_exact_fields": ["曝光人数"],
                        "optional_alias_keys": [],
                        "recommended_exact_fields": [],
                        "preferred_sheets": ["拆分", "Sheet1"],
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    manifest, resolved = discover_runtime_inputs(
        workspace=tmp_path,
        run_id="run-test",
        config_path=config_path,
    )

    assert resolved["seed_live_ledger"] == newer.resolve()
    assert manifest["selected_inputs"]["seed_live_ledger"] == str(newer.resolve())
    source = manifest["sources"][0]
    assert source["source_key"] == "seed_live_ledger"
    assert source["selected_by"] == "auto_discovery"
    assert source["business_date"] == ""
