"""Shared business rules for the productized engine."""

from .account_mapping import ACCOUNT_MAP, NON_LIVE_ACCOUNTS, canonical_account_name, normalize_account
from .columns import COLUMN_ALIASES, pick_col
from .common import normalize_text
from .datetime_utils import (
    combine_date_and_time,
    combine_date_time_series,
    parse_excel_mixed_datetime,
    parse_time_to_timedelta,
)
from .file_discovery import (
    parse_date_from_filename,
    parse_year_month_from_live_filename,
    pick_latest_file,
    pick_latest_live_file,
    split_patterns,
)
from .hosts import count_hosts_in_text, extract_hosts, split_hosts_text
from .identity import (
    build_business_subject_key,
    build_matching_subject_key,
    normalize_lead_id,
    normalize_phone,
    vectorized_build_matching_subject_key,
)
from .io_utils import read_csv_auto, read_table_auto, resolve_path

__all__ = [
    "ACCOUNT_MAP",
    "NON_LIVE_ACCOUNTS",
    "COLUMN_ALIASES",
    "build_business_subject_key",
    "build_matching_subject_key",
    "canonical_account_name",
    "combine_date_and_time",
    "combine_date_time_series",
    "count_hosts_in_text",
    "extract_hosts",
    "normalize_account",
    "normalize_lead_id",
    "normalize_phone",
    "normalize_text",
    "parse_date_from_filename",
    "parse_excel_mixed_datetime",
    "parse_time_to_timedelta",
    "parse_year_month_from_live_filename",
    "pick_col",
    "pick_latest_file",
    "pick_latest_live_file",
    "read_csv_auto",
    "read_table_auto",
    "resolve_path",
    "split_hosts_text",
    "split_patterns",
    "vectorized_build_matching_subject_key",
]
