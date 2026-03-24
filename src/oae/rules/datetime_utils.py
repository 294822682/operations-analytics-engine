"""Date/time parsing and repair rules shared across layers."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd


def parse_excel_mixed_datetime(series: pd.Series) -> pd.Series:
    values = series.copy()
    out = pd.to_datetime(values, errors="coerce")
    numeric = pd.to_numeric(values, errors="coerce")
    excel_serial_mask = (numeric >= 1) & (numeric <= 80000)
    if excel_serial_mask.any():
        excel_dt = pd.to_datetime(
            numeric.where(excel_serial_mask),
            unit="D",
            origin="1899-12-30",
            errors="coerce",
        )
        out = out.where(~excel_serial_mask, excel_dt)
    return out


def parse_time_to_timedelta(series: pd.Series) -> pd.Series:
    result = pd.Series(pd.NaT, index=series.index, dtype="timedelta64[ns]")
    numeric = pd.to_numeric(series, errors="coerce")
    numeric_mask = numeric.notna() & (numeric >= 0) & (numeric < 1)
    if numeric_mask.any():
        seconds = (numeric.loc[numeric_mask] * 86400).round().astype("int64")
        result.loc[numeric_mask] = pd.to_timedelta(seconds, unit="s")

    remaining = result.isna()
    if remaining.any():
        text = series.loc[remaining].astype(str).str.strip()
        text = text.replace({"": np.nan, "nan": np.nan, "None": np.nan, "<NA>": np.nan, "NaT": np.nan})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            parsed = pd.to_datetime(text, errors="coerce")
        result.loc[remaining] = parsed - parsed.dt.normalize()

    still_missing = result.isna()
    if still_missing.any():
        raw = series.loc[still_missing].astype(str).str.strip()
        hhmm = raw.str.extract(r"^(?P<h>\d{1,2})(?P<m>\d{2})$")
        valid = hhmm["h"].notna()
        if valid.any():
            hours = pd.to_numeric(hhmm.loc[valid, "h"], errors="coerce")
            minutes = pd.to_numeric(hhmm.loc[valid, "m"], errors="coerce")
            okay = hours.between(0, 23) & minutes.between(0, 59)
            if okay.any():
                idx = hhmm.loc[valid].index[okay]
                seconds = hours.loc[okay] * 3600 + minutes.loc[okay] * 60
                result.loc[idx] = pd.to_timedelta(seconds.astype("int64"), unit="s")
    return result


def combine_date_time_series(date_series: pd.Series, time_series: pd.Series) -> pd.Series:
    date_only = pd.to_datetime(date_series, errors="coerce").dt.normalize()
    return date_only + parse_time_to_timedelta(time_series)


def combine_date_and_time(
    date_series: pd.Series,
    dt_series: pd.Series,
    raw_time_series: pd.Series | None = None,
) -> pd.Series:
    out = dt_series.copy()
    date_floor = pd.to_datetime(date_series, errors="coerce").dt.floor("D")

    if raw_time_series is not None:
        raw = raw_time_series.fillna("").astype(str).str.strip()
        mask_hms = raw.str.match(r"^\d{1,2}:\d{2}(:\d{2})?$") & date_floor.notna()
        if mask_hms.any():
            out.loc[mask_hms] = pd.to_datetime(
                date_floor.loc[mask_hms].dt.strftime("%Y-%m-%d") + " " + raw.loc[mask_hms],
                errors="coerce",
            )

    mask_time_only = out.notna() & (out.dt.year <= 1901) & date_floor.notna()
    if mask_time_only.any():
        out.loc[mask_time_only] = pd.to_datetime(
            date_floor.loc[mask_time_only].dt.strftime("%Y-%m-%d")
            + " "
            + out.loc[mask_time_only].dt.strftime("%H:%M:%S"),
            errors="coerce",
        )

    mask_missing_time = out.isna() & date_floor.notna()
    if mask_missing_time.any():
        out.loc[mask_missing_time] = date_floor.loc[mask_missing_time]

    return out
