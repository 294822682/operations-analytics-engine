"""Small shared helpers."""

from __future__ import annotations

import pandas as pd


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()
