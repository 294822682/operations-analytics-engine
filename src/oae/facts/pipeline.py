"""High-level fact pipeline using shared fact-layer modules."""

from __future__ import annotations

import logging

import pandas as pd

from oae.facts.assembler import build_fact
from oae.facts.deals import build_deal_dedup
from oae.facts.leads import match_leads
from oae.facts.live_sessions import build_live_windows
from oae.facts.models import FactArtifacts


def build_fact_artifacts(
    *,
    live_raw: pd.DataFrame,
    leads_raw: pd.DataFrame,
    deals_raw: pd.DataFrame,
    logger: logging.Logger,
    buffer_minutes: int,
    default_duration_minutes: int,
    max_duration_hours: int,
    match_mode: str,
    column_aliases: dict[str, list[str]],
    allowed_channel3: set[str],
    fallback_channel2_value: str,
    non_live_accounts: set[str],
) -> FactArtifacts:
    live_windows = build_live_windows(
        live_raw=live_raw,
        logger=logger,
        buffer_minutes=buffer_minutes,
        default_duration_minutes=default_duration_minutes,
        max_duration_hours=max_duration_hours,
    )
    leads_attr, lead_key_by_id = match_leads(
        leads_raw,
        live_windows,
        logger,
        match_mode=match_mode,
        column_aliases=column_aliases,
        allowed_channel3=allowed_channel3,
        fallback_channel2_value=fallback_channel2_value,
        non_live_accounts=non_live_accounts,
    )
    deals_dedup = build_deal_dedup(
        deals_raw,
        logger,
        lead_key_by_id=lead_key_by_id,
        column_aliases=column_aliases,
    )
    fact = build_fact(leads_attr, deals_dedup, logger)
    return FactArtifacts(
        live_windows=live_windows,
        leads_attr=leads_attr,
        deals_dedup=deals_dedup,
        fact=fact,
    )
