"""Writers for productized export manifests."""

from __future__ import annotations

import json
from pathlib import Path

from oae.contracts.models import ExportManifestRecord, RunMetadata


def write_export_manifest(
    *,
    manifest_path: Path,
    export_name: str,
    snapshot_date: str,
    metadata: RunMetadata,
    source_tables: list[str],
    row_count: int,
    consumer: str,
    output_path: str | Path,
) -> Path:
    manifest = ExportManifestRecord.build(
        export_name=export_name,
        snapshot_date=snapshot_date,
        metadata=metadata,
        source_tables=source_tables,
        row_count=row_count,
        consumer=consumer,
        output_path=output_path,
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path
