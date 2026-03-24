"""Shared product/version constants for productized outputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


PRODUCT_NAME = "new-media-operations-analytics-engine"
SCHEMA_VERSION = "1.0.0"
METRIC_VERSION = "metric-v1"
TEMPLATE_VERSION = "excel-tsv-v1"


@dataclass(frozen=True)
class RunContext:
    run_id: str
    metric_version: str = METRIC_VERSION
    schema_version: str = SCHEMA_VERSION
    template_version: str = TEMPLATE_VERSION
    freeze_id: str = ""


def build_run_id(now: datetime | None = None) -> str:
    ts = now or datetime.now(timezone.utc)
    return ts.strftime("run-%Y%m%dT%H%M%SZ")

