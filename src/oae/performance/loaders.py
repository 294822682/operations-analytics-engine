"""Thin aggregation layer for performance loaders."""

from __future__ import annotations

from oae.performance.fact_loader import load_fact
from oae.performance.live_loader import load_anchor_accounts_from_live
from oae.performance.loader_utils import (
    join_unique_accounts,
    month_start_end,
    normalize_account,
    normalize_text,
    parse_year_month_from_live_filename,
    pick_latest_live_file,
    pick_live_column,
    pick_report_month,
    split_accounts,
    split_hosts,
    to_number,
)
from oae.performance.spend_loader import ensure_spend_template, load_spend, load_spend_from_live, resolve_spend_data
from oae.performance.targets_loader import backfill_cpl_cps, ensure_targets_template, load_targets

__all__ = [
    "backfill_cpl_cps",
    "ensure_spend_template",
    "ensure_targets_template",
    "join_unique_accounts",
    "load_anchor_accounts_from_live",
    "load_fact",
    "load_spend",
    "load_spend_from_live",
    "load_targets",
    "month_start_end",
    "normalize_account",
    "normalize_text",
    "parse_year_month_from_live_filename",
    "pick_latest_live_file",
    "pick_live_column",
    "pick_report_month",
    "resolve_spend_data",
    "split_accounts",
    "split_hosts",
    "to_number",
]
