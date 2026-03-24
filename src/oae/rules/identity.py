"""Identity and business-subject rules."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from .common import normalize_text


def normalize_phone(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return ""
        if float(value).is_integer():
            return str(int(value))
    text = normalize_text(value)
    if not text or text.lower() in {"nan", "none", "<na>", "null"}:
        return ""
    text = "".join(text.split())
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or text


def normalize_lead_id(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "none": "", "<NA>": "", "<na>": "", "null": ""})
    )


def build_matching_subject_key(lead_id: object, phone: object) -> str:
    phone_norm = normalize_phone(phone)
    lead_norm = normalize_text(lead_id)
    if phone_norm:
        return f"PHONE:{phone_norm}"
    if lead_norm:
        return f"LEAD:{lead_norm}"
    return ""


def vectorized_build_matching_subject_key(lead_id_series: pd.Series, phone_series: pd.Series) -> pd.Series:
    phone = phone_series.fillna("").astype(str).str.strip()
    lead = lead_id_series.fillna("").astype(str).str.strip()
    return pd.Series(
        np.where(
            phone.ne(""),
            "PHONE:" + phone,
            np.where(lead.ne(""), "LEAD:" + lead, ""),
        ),
        index=lead_id_series.index,
    )


def build_business_subject_key(
    lead_id: object,
    phone: object,
    create_dt: object | None = None,
    fallback_row: int | None = None,
) -> str:
    phone_norm = normalize_phone(phone)
    lead_norm = normalize_text(lead_id)
    if phone_norm:
        return f"PHONE:{phone_norm}"

    create_text = ""
    if create_dt is not None and not pd.isna(create_dt):
        if isinstance(create_dt, pd.Timestamp):
            create_text = create_dt.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(create_dt, datetime):
            create_text = create_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            create_text = normalize_text(create_dt)

    if lead_norm and create_text:
        return f"LEAD_CREATE:{lead_norm}|{create_text}"
    if lead_norm:
        return f"LEAD:{lead_norm}"
    if fallback_row is not None:
        return f"ROW:{fallback_row}"
    return ""


def vectorized_build_business_subject_key(
    lead_id_series: pd.Series,
    phone_series: pd.Series,
    create_dt_series: pd.Series | None = None,
    fallback_row_series: pd.Series | None = None,
) -> pd.Series:
    if create_dt_series is None:
        create_dt_series = pd.Series([None] * len(lead_id_series), index=lead_id_series.index)
    if fallback_row_series is None:
        fallback_row_series = pd.Series([None] * len(lead_id_series), index=lead_id_series.index)
    values = [
        build_business_subject_key(lead_id, phone, create_dt=create_dt, fallback_row=fallback_row)
        for lead_id, phone, create_dt, fallback_row in zip(
            lead_id_series.tolist(),
            phone_series.tolist(),
            create_dt_series.tolist(),
            fallback_row_series.tolist(),
        )
    ]
    return pd.Series(values, index=lead_id_series.index)
