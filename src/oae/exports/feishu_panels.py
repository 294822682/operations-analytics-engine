"""Thin compatibility aggregator for Feishu panel helpers."""

from __future__ import annotations

from oae.exports.feishu_narrative import deal_accounts_text, format_deal_accounts, lead_quality_text, pending_accounts_text
from oae.exports.feishu_panel_utils import (
    ACCOUNT_LABEL_MAP,
    ACCOUNT_REQUIRED_COLUMNS,
    ANCHOR_ORDER,
    ANCHOR_REQUIRED_COLUMNS,
    DISPLAY_ACCOUNT_ORDER,
    FACT_REQUIRED_COLUMNS,
    PCT_DISPLAY_COLS,
    PCT_RENAME_MAP,
    find_latest_file,
    get_target_accounts,
    infer_run_id,
    load_panel_for_date,
    load_panel_from_snapshot,
    pick_latest_live_file,
    resolve_report_date,
    validate_columns,
)
from oae.exports.feishu_table_adapters import account_table, account_table_tsv, anchor_table, anchor_table_tsv

__all__ = [
    "ACCOUNT_LABEL_MAP",
    "ACCOUNT_REQUIRED_COLUMNS",
    "ANCHOR_ORDER",
    "ANCHOR_REQUIRED_COLUMNS",
    "DISPLAY_ACCOUNT_ORDER",
    "FACT_REQUIRED_COLUMNS",
    "PCT_DISPLAY_COLS",
    "PCT_RENAME_MAP",
    "account_table",
    "account_table_tsv",
    "anchor_table",
    "anchor_table_tsv",
    "deal_accounts_text",
    "find_latest_file",
    "format_deal_accounts",
    "get_target_accounts",
    "infer_run_id",
    "lead_quality_text",
    "load_panel_for_date",
    "load_panel_from_snapshot",
    "pending_accounts_text",
    "pick_latest_live_file",
    "resolve_report_date",
    "validate_columns",
]
