"""Lead-to-live attribution and unmatched classification."""

from __future__ import annotations

import heapq

import numpy as np
import pandas as pd

from oae.facts.models import MatchMaps
from oae.rules.account_mapping import canonical_account_name
from oae.rules.common import normalize_text
from oae.rules.hosts import extract_hosts, split_hosts_text


def find_matches_by_account(
    valid_leads: pd.DataFrame,
    live_windows: pd.DataFrame,
    match_mode: str,
) -> MatchMaps:
    hosts: dict[int, str] = {}
    weights: dict[int, float] = {}
    counts: dict[int, int] = {}
    matched_idx: set[int] = set()

    if valid_leads.empty or live_windows.empty:
        return MatchMaps(hosts=hosts, weights=weights, counts=counts, matched_idx=matched_idx)

    live_by_account = {account: grp for account, grp in live_windows.groupby("标准账号", sort=False)}

    for account, lead_grp in valid_leads.groupby("标准账号", sort=False):
        win = live_by_account.get(account)
        if win is None or win.empty:
            continue

        starts = win["Match_Start"].values.astype("datetime64[ns]").astype("int64")
        ends = win["Match_End"].values.astype("datetime64[ns]").astype("int64")
        mids = (
            win["Valid_Start"].values.astype("datetime64[ns]").astype("int64")
            + win["Valid_End"].values.astype("datetime64[ns]").astype("int64")
        ) // 2
        hosts_text = win["本场主播"].fillna("").astype(str).tolist()
        live_order = win["_live_order"].to_numpy(dtype=np.int64)
        order_by_start = np.argsort(starts, kind="stable")

        sorted_leads = lead_grp.sort_values(["线索创建时间", "_idx"], kind="stable")
        lead_times = sorted_leads["线索创建时间"].values.astype("datetime64[ns]").astype("int64")
        lead_indices = sorted_leads["_idx"].to_numpy(dtype=np.int64)

        active: set[int] = set()
        end_heap: list[tuple[int, int]] = []
        ptr = 0

        for lead_idx, lead_ns in zip(lead_indices, lead_times):
            while ptr < len(order_by_start) and starts[order_by_start[ptr]] <= lead_ns:
                pos = int(order_by_start[ptr])
                active.add(pos)
                heapq.heappush(end_heap, (int(ends[pos]), pos))
                ptr += 1

            while end_heap and end_heap[0][0] < lead_ns:
                _, expired = heapq.heappop(end_heap)
                if expired in active and ends[expired] < lead_ns:
                    active.remove(expired)

            if not active:
                continue

            if match_mode == "process_deal_data":
                best_pos = min(active, key=lambda p: (abs(int(lead_ns) - int(mids[p])), int(live_order[p])))
                keep_hosts = extract_hosts(pd.Series([hosts_text[best_pos]]))
                hosts[int(lead_idx)] = ",".join(keep_hosts) if keep_hosts else "未知主播"
                weights[int(lead_idx)] = 1.0
                counts[int(lead_idx)] = 1
            else:
                host_names: set[str] = set()
                for pos in active:
                    for host in split_hosts_text(hosts_text[pos]):
                        host_names.add(host)
                hit_count = len(active)
                hosts[int(lead_idx)] = ",".join(sorted(host_names)) if host_names else "未知"
                weights[int(lead_idx)] = 1.0 / hit_count
                counts[int(lead_idx)] = hit_count

            matched_idx.add(int(lead_idx))

    return MatchMaps(hosts=hosts, weights=weights, counts=counts, matched_idx=matched_idx)


def apply_match_result(
    dedup: pd.DataFrame,
    match_maps: MatchMaps,
    live_windows: pd.DataFrame,
    *,
    non_live_accounts: set[str],
) -> pd.DataFrame:
    out = dedup.copy()
    out["本场主播"] = out["_idx"].map(match_maps.hosts)
    out["权重"] = out["_idx"].map(match_maps.weights).fillna(0.0)
    out["命中场次数量"] = out["_idx"].map(match_maps.counts).fillna(0).astype(int)

    live_accounts = set(live_windows["标准账号"].dropna().astype(str))
    matched_mask = out["_idx"].isin(match_maps.matched_idx)
    account_in_live = out["标准账号"].isin(live_accounts)

    out["无匹配原因"] = np.select(
        [
            out["线索创建时间"].isna(),
            out["标准账号"].eq(""),
            ~account_in_live,
            ~matched_mask,
        ],
        [
            "线索时间缺失",
            "账号缺失",
            "账号无直播排期",
            "未命中直播时间窗",
        ],
        default="",
    )
    out["归属状态"] = np.where(matched_mask, "匹配成功", "无主线索")
    out["本场主播"] = out["本场主播"].fillna("【无主线索】")

    non_live_raw = set(non_live_accounts)
    non_live_canonical = {canonical_account_name(item) for item in non_live_accounts}
    account_norm = out["标准账号"].fillna("").map(normalize_text)
    account_canonical = account_norm.map(canonical_account_name)
    out["report_bucket"] = np.where(
        account_norm.isin(non_live_raw) | account_canonical.isin(non_live_canonical),
        "自然流入/沉淀",
        "直播归因",
    )
    return out
