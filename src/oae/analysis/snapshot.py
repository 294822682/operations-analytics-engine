"""Shared analysis-snapshot builders for unified and raw analysis modes."""

from __future__ import annotations

import pandas as pd

from oae.contracts import attach_contract_metadata, validate_contract_frame
from oae.contracts.models import RunMetadata


def build_analysis_snapshot_frame(
    *,
    snapshot_rows: list[dict[str, object]],
    snapshot_date: str,
    metadata: RunMetadata,
    analysis_mode: str,
    evidence_mode: str,
    default_source_scope: str,
    default_raw_evidence_required: bool,
    default_migration_status: str,
) -> pd.DataFrame:
    frame = pd.DataFrame(snapshot_rows)
    if frame.empty:
        frame = pd.DataFrame(columns=[
            "snapshot_date",
            "analysis_mode",
            "evidence_mode",
            "subject_area",
            "topic_name",
            "table_name",
            "grain",
            "dimension_key",
            "metric_name",
            "metric_value",
            "raw_evidence_required",
            "source_scope",
            "migration_status",
        ])

    frame["snapshot_date"] = _series_or_scalar(frame, "snapshot_date", snapshot_date).replace("", pd.NA).fillna(snapshot_date)
    frame["analysis_mode"] = _series_or_scalar(frame, "analysis_mode", analysis_mode).replace("", pd.NA).fillna(analysis_mode)
    frame["evidence_mode"] = _series_or_scalar(frame, "evidence_mode", evidence_mode).replace("", pd.NA).fillna(evidence_mode)
    frame["subject_area"] = _series_or_scalar(frame, "subject_area", "").fillna("")
    frame["topic_name"] = _series_or_scalar(frame, "topic_name", frame["subject_area"]).replace("", pd.NA).fillna(frame["subject_area"])
    frame["table_name"] = _series_or_scalar(frame, "table_name", frame["topic_name"]).replace("", pd.NA).fillna(frame["topic_name"])
    frame["grain"] = _series_or_scalar(frame, "grain", "").fillna("")
    frame["dimension_key"] = _series_or_scalar(frame, "dimension_key", "").fillna("")
    frame["metric_name"] = _series_or_scalar(frame, "metric_name", "").fillna("")
    frame["metric_value"] = pd.to_numeric(_series_or_scalar(frame, "metric_value", 0.0), errors="coerce").fillna(0.0)
    frame["raw_evidence_required"] = _coerce_bool_series(
        _series_or_scalar(frame, "raw_evidence_required", default_raw_evidence_required),
        default_raw_evidence_required,
    )
    frame["source_scope"] = _series_or_scalar(frame, "source_scope", default_source_scope).replace("", pd.NA).fillna(default_source_scope)
    frame["migration_status"] = _series_or_scalar(frame, "migration_status", default_migration_status).replace("", pd.NA).fillna(default_migration_status)

    frame = attach_contract_metadata(frame, metadata)
    missing = validate_contract_frame(frame, "analysis_snapshot")
    if missing:
        raise ValueError(f"analysis_snapshot 缺少字段: {missing}")
    return frame


def _series_or_scalar(frame: pd.DataFrame, column: str, default: object) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    if isinstance(default, pd.Series):
        return default
    return pd.Series([default] * len(frame), index=frame.index, dtype="object")


def _coerce_bool_series(series: pd.Series, default: bool) -> pd.Series:
    normalized = series.replace("", pd.NA)
    mapped = normalized.map(
        lambda value: pd.NA
        if pd.isna(value)
        else (str(value).strip().lower() in {"1", "true", "yes"} if isinstance(value, str) else bool(value))
    )
    return mapped.fillna(default).astype(bool)
