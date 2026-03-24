"""Formal raw-analysis snapshot builders."""

from __future__ import annotations

import pandas as pd

from oae.analysis.snapshot import build_analysis_snapshot_frame
from oae.analysis.raw_evidence import RAW_EVIDENCE_TOPICS
from oae.contracts.models import RunMetadata


def build_raw_analysis_snapshot(
    *,
    topic_tables: dict[str, dict[str, pd.DataFrame]],
    snapshot_date: str,
    metadata: RunMetadata,
    source_scope: str,
) -> pd.DataFrame:
    topic_meta = {item["topic"]: item for item in RAW_EVIDENCE_TOPICS}
    rows: list[dict[str, object]] = []
    for topic_name, tables in topic_tables.items():
        meta = topic_meta.get(topic_name, {})
        for table_name, frame in tables.items():
            rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "analysis_mode": "raw-evidence",
                    "evidence_mode": "raw",
                    "subject_area": topic_name,
                    "topic_name": topic_name,
                    "table_name": table_name,
                    "grain": "table",
                    "dimension_key": "ALL",
                    "metric_name": "row_count",
                    "metric_value": float(len(frame)),
                    "raw_evidence_required": True,
                    "source_scope": source_scope,
                    "migration_status": meta.get("migration_status", "unknown"),
                }
            )
            rows.extend(_flatten_numeric_metrics(frame, topic_name=topic_name, table_name=table_name, source_scope=source_scope, migration_status=str(meta.get("migration_status", "unknown"))))
    return build_analysis_snapshot_frame(
        snapshot_rows=rows,
        snapshot_date=snapshot_date,
        metadata=metadata,
        analysis_mode="raw-evidence",
        evidence_mode="raw",
        default_source_scope=source_scope,
        default_raw_evidence_required=True,
        default_migration_status="fully_raw_required",
    )


def _flatten_numeric_metrics(
    frame: pd.DataFrame,
    *,
    topic_name: str,
    table_name: str,
    source_scope: str,
    migration_status: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if frame.empty:
        return rows

    if set(["指标", "数值"]).issubset(frame.columns):
        numeric = pd.to_numeric(frame["数值"], errors="coerce")
        for idx, (_, record) in enumerate(frame.loc[numeric.notna(), ["指标", "数值"]].iterrows()):
            rows.append(
                {
                    "snapshot_date": "",
                    "analysis_mode": "raw-evidence",
                    "evidence_mode": "raw",
                    "subject_area": topic_name,
                    "topic_name": topic_name,
                    "table_name": table_name,
                    "grain": "metric",
                    "dimension_key": str(record["指标"]),
                    "metric_name": "summary_value",
                    "metric_value": float(record["数值"]),
                    "raw_evidence_required": True,
                    "source_scope": source_scope,
                    "migration_status": migration_status,
                }
            )
        return rows

    dimension_col = str(frame.columns[0]) if len(frame.columns) > 0 else ""
    for column in frame.columns[1:]:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if numeric.notna().sum() == 0:
            continue
        for idx, value in numeric.dropna().items():
            dimension_key = str(frame.loc[idx, dimension_col]) if dimension_col else str(idx)
            rows.append(
                {
                    "snapshot_date": "",
                    "analysis_mode": "raw-evidence",
                    "evidence_mode": "raw",
                    "subject_area": topic_name,
                    "topic_name": topic_name,
                    "table_name": table_name,
                    "grain": "dimension",
                    "dimension_key": dimension_key,
                    "metric_name": str(column),
                    "metric_value": float(value),
                    "raw_evidence_required": True,
                    "source_scope": source_scope,
                    "migration_status": migration_status,
                }
            )
    return rows
