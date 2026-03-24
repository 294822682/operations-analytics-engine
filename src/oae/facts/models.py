"""Core fact-layer data objects."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class MatchMaps:
    hosts: dict[int, str]
    weights: dict[int, float]
    counts: dict[int, int]
    matched_idx: set[int]


@dataclass
class FactArtifacts:
    live_windows: pd.DataFrame
    leads_attr: pd.DataFrame
    deals_dedup: pd.DataFrame
    fact: pd.DataFrame
