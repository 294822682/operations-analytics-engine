"""Host parsing/allocation rules."""

from __future__ import annotations

import re

import pandas as pd


HOST_SPLIT_PATTERN = re.compile(r"[，,、/；;]+")


def split_hosts_text(value: object) -> list[str]:
    if pd.isna(value):
        return []
    text = str(value).strip()
    if not text or text in {"【无主线索】", "【无主播】"}:
        return []
    return [part.strip() for part in HOST_SPLIT_PATTERN.split(text) if part.strip()]


def extract_hosts(series: pd.Series) -> list[str]:
    hosts: set[str] = set()
    for value in series.dropna().astype(str):
        for name in split_hosts_text(value):
            hosts.add(name)
    return sorted(hosts)


def count_hosts_in_text(value: object) -> int:
    text = str(value).strip()
    if text in {"", "【无主线索】", "【无主播】", "nan", "None"}:
        return 0
    return max(1, len(split_hosts_text(text)))
