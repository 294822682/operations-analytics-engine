#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path


DASHBOARD_SOURCE_PATTERN = "feishu_dashboard_source_latest_*.tsv"


def prepare_data_bundle(repo_root: Path, output_dir: Path) -> dict[str, object]:
    repo_root = repo_root.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    source_dir = repo_root / "output" / "sql_reports"
    target_dir = output_dir / "output" / "sql_reports"
    source_paths = sorted(source_dir.glob(DASHBOARD_SOURCE_PATTERN))
    if not source_paths:
        raise FileNotFoundError(f"No dashboard source TSV files found under {source_dir}")

    target_dir.mkdir(parents=True, exist_ok=True)
    files: list[dict[str, object]] = []
    for source_path in source_paths:
        target_path = target_dir / source_path.name
        shutil.copy2(source_path, target_path)
        files.append(
            {
                "name": source_path.name,
                "relative_path": str(Path("output") / "sql_reports" / source_path.name),
                "bytes": target_path.stat().st_size,
                "sha256": _sha256(target_path),
            }
        )

    latest = files[-1]["name"] if files else ""
    manifest = {
        "bundle_type": "oae_feishu_embed_dashboard_sources",
        "source_pattern": "output/sql_reports/" + DASHBOARD_SOURCE_PATTERN,
        "file_count": len(files),
        "latest_file": latest,
        "files": files,
    }
    manifest_path = output_dir / "feishu_embed_data_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"output_dir": str(output_dir), "manifest_path": str(manifest_path), **manifest}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare dashboard source TSVs for Feishu embed deployment.")
    parser.add_argument("--repo-root", default=".", help="Repository root containing output/sql_reports")
    parser.add_argument("--output-dir", required=True, help="Destination bundle directory")
    args = parser.parse_args(argv)

    result = prepare_data_bundle(Path(args.repo_root), Path(args.output_dir))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
