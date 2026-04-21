from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional


DATE_PATTERN = re.compile(r"(20\d{2}-\d{2}-\d{2})")
RUN_ID_PATTERN = re.compile(r"run-(\d{8})T(\d{6})Z")


def iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def extract_date_from_text(value: str) -> str:
    matched = DATE_PATTERN.search(value)
    return matched.group(1) if matched else ""


def normalize_report_date(value: str) -> str:
    if not value:
        raise ValueError("report_date 不能为空")
    return str(datetime.strptime(value, "%Y-%m-%d").date())


def parse_run_id_started_at(run_id: str) -> str:
    matched = RUN_ID_PATTERN.fullmatch(run_id)
    if not matched:
        return ""
    stamp = datetime.strptime("".join(matched.groups()), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return stamp.astimezone().isoformat(timespec="seconds")


def derive_report_date_from_source_dates(*source_dates: str) -> str:
    parsed_dates = [datetime.strptime(item, "%Y-%m-%d").date() for item in source_dates if item]
    if not parsed_dates:
        return ""
    target_date = min(parsed_dates) - timedelta(days=1)
    return target_date.isoformat()


def coerce_date(value: str) -> Optional[date]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()
