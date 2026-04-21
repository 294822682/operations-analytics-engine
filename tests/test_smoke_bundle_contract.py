from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = REPO_ROOT / "tests" / "fixtures" / "pipeline" / "smoke_bundle_contract.json"


def test_smoke_bundle_matches_contract() -> None:
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    bundle_path = REPO_ROOT / contract["bundle_path"]
    rules = contract["rules"]

    assert bundle_path.exists()
    assert bundle_path.stat().st_size == contract["bundle_size_bytes"]
    assert hashlib.sha256(bundle_path.read_bytes()).hexdigest() == contract["bundle_sha256"]

    with zipfile.ZipFile(bundle_path) as archive:
        names = archive.namelist()

    assert names
    assert len(names) == rules["exact_file_count"]
    assert len(names) == len(set(names))

    if not rules["allow_directory_entries"]:
        assert all(not name.endswith("/") for name in names)

    forbidden_fragments = tuple(rules["forbidden_name_fragments"])
    assert all(fragment not in name for name in names for fragment in forbidden_fragments)

    disallowed_entries = set(rules["disallowed_entries"])
    assert disallowed_entries.isdisjoint(names)

    unmatched_names = set(names)
    for slot in rules["required_slots"]:
        matched = [
            name
            for name in names
            if name.endswith(slot["suffix"]) and slot["name_contains"] in name
        ]
        assert len(matched) == 1, slot["slot"]
        unmatched_names.discard(matched[0])

    assert not unmatched_names
