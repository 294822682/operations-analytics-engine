"""Formal snapshot and ledger builders for the performance layer."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from oae.contracts import attach_contract_metadata, validate_contract_frame
from oae.contracts.models import RunMetadata


def _ensure_iso_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def build_daily_performance_snapshot(
    *,
    account_frame: pd.DataFrame,
    anchor_frame: pd.DataFrame,
    report_month: str,
    latest_date: pd.Timestamp,
    metadata: RunMetadata,
    spend_source: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    snapshot = pd.concat([account_frame.copy(), anchor_frame.copy()], ignore_index=True, sort=False)
    snapshot["date"] = _ensure_iso_date(snapshot["date"])
    snapshot["snapshot_date"] = latest_date.strftime("%Y-%m-%d")
    snapshot["report_month"] = report_month
    snapshot["spend_source"] = spend_source
    snapshot["freeze_id"] = metadata.freeze_id
    snapshot = attach_contract_metadata(snapshot, metadata, freeze_id=metadata.freeze_id)

    missing = validate_contract_frame(snapshot, "daily_performance_snapshot")
    if missing:
        raise ValueError(f"daily_performance_snapshot 缺少字段: {missing}")

    latest_rows = snapshot[snapshot["date"] == latest_date.strftime("%Y-%m-%d")].copy()
    return snapshot, latest_rows


def build_compensation_ledger(
    *,
    latest_snapshot: pd.DataFrame,
    settlement_period: str,
    snapshot_start: pd.Timestamp,
    snapshot_end: pd.Timestamp,
    metadata: RunMetadata,
    formula_name: str = "round1_mtd_attainment",
) -> pd.DataFrame:
    if latest_snapshot.empty:
        return attach_contract_metadata(pd.DataFrame(columns=[]), metadata, freeze_id=metadata.freeze_id)

    ledger = latest_snapshot.copy()
    ledger["settlement_period"] = settlement_period
    ledger["snapshot_start"] = snapshot_start.strftime("%Y-%m-%d")
    ledger["snapshot_end"] = snapshot_end.strftime("%Y-%m-%d")
    ledger["freeze_id"] = metadata.freeze_id
    ledger["lead_attain_pct"] = pd.to_numeric(ledger.get("mtd_lead_attain"), errors="coerce")
    ledger["deal_attain_pct"] = pd.to_numeric(ledger.get("mtd_deal_attain"), errors="coerce")
    ledger["formula_name"] = formula_name
    ledger["formula_inputs_json"] = ledger.apply(
        lambda row: json.dumps(
            {
                "mtd_leads": _safe_float(row.get("mtd_leads")),
                "mtd_deals": _safe_float(row.get("mtd_deals")),
                "mtd_spend": _safe_float(row.get("mtd_spend")),
                "lead_target_month": _safe_float(row.get("lead_target_month")),
                "deal_target_month": _safe_float(row.get("deal_target_month")),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        axis=1,
    )
    ledger["ledger_status"] = "frozen" if metadata.freeze_id and not metadata.freeze_id.startswith("provisional") else "provisional"
    keep = [
        "settlement_period",
        "freeze_id",
        "scope_type",
        "scope_name",
        "parent_account",
        "metric_version",
        "schema_version",
        "run_id",
        "snapshot_start",
        "snapshot_end",
        "mtd_leads",
        "mtd_deals",
        "mtd_spend",
        "mtd_cpl",
        "mtd_cps",
        "lead_target_month",
        "deal_target_month",
        "lead_attain_pct",
        "deal_attain_pct",
        "formula_name",
        "formula_inputs_json",
        "ledger_status",
    ]
    ledger = attach_contract_metadata(ledger, metadata, freeze_id=metadata.freeze_id)
    existing = [column for column in keep if column in ledger.columns]
    others = [column for column in ledger.columns if column not in existing]
    ledger = ledger[existing + others].copy()

    missing = validate_contract_frame(ledger, "compensation_ledger")
    if missing:
        raise ValueError(f"compensation_ledger 缺少字段: {missing}")
    return ledger


def write_daily_performance_snapshots(
    snapshot: pd.DataFrame,
    latest_snapshot: pd.DataFrame,
    *,
    snapshot_dir: Path,
    report_month: str,
    latest_date: pd.Timestamp,
) -> dict[str, Path]:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    full_path = snapshot_dir / f"daily_performance_snapshot_{report_month}.csv"
    latest_path = snapshot_dir / f"daily_performance_snapshot_latest_{latest_date.strftime('%Y-%m-%d')}.csv"
    snapshot.to_csv(full_path, index=False, encoding="utf-8-sig")
    latest_snapshot.to_csv(latest_path, index=False, encoding="utf-8-sig")
    return {"full": full_path, "latest": latest_path}


def write_compensation_ledger(
    ledger: pd.DataFrame,
    *,
    snapshot_dir: Path,
    settlement_period: str,
    snapshot_end: pd.Timestamp,
) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    path = snapshot_dir / f"compensation_ledger_{settlement_period}_{snapshot_end.strftime('%Y-%m-%d')}.csv"
    ledger.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def _safe_float(value: object) -> float | None:
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
