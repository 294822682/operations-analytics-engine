"""Seed exposure tables for the Feishu dashboard source export."""

from __future__ import annotations

import calendar
from pathlib import Path
from typing import Iterable

import pandas as pd

from oae.performance.loader_utils import normalize_account, normalize_text, pick_live_column, split_hosts


SEED_ACCOUNT_COLUMNS = ["账号", "当日曝光", "当日曝光目标", "当日曝光达成率", "累计曝光", "曝光目标", "累计曝光达成率"]
SEED_ANCHOR_COLUMNS = [
    "主播",
    "归属账号",
    "当日曝光",
    "当日曝光目标",
    "当日曝光达成率",
    "累计曝光",
    "曝光目标",
    "累计曝光达成率",
    "累计A3人群增长",
]
SEED_SESSION_COLUMNS = ["date", "account", "hosts_raw", "impressions", "a3_growth", "source_file"]
SEED_TARGET_COLUMNS = [
    "month",
    "scope_type",
    "scope_name",
    "parent_scope",
    "parent_account",
    "impression_target_month",
]


def resolve_seed_workbook_paths(explicit_path: str | Path | None, search_dirs: Iterable[Path]) -> list[Path]:
    """Resolve EXEED seed ledger workbooks without making the report fail when absent."""

    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.exists():
            raise SystemExit(f"[ERROR] 种草台账文件不存在: {path}")
        return [path]

    paths: dict[str, Path] = {}
    for search_dir in search_dirs:
        root = Path(search_dir).expanduser()
        if not root.exists():
            continue
        for pattern in ("*EXEED星途台账*.xlsx", "*EXEED星途台账*.xls"):
            for path in root.rglob(pattern):
                if path.is_file() and not path.name.startswith("~$"):
                    resolved = path.resolve()
                    paths[str(resolved)] = resolved
    return list(paths.values())


def load_seed_monthly_targets(path: str | Path | None) -> pd.DataFrame:
    if not path:
        return pd.DataFrame(columns=SEED_TARGET_COLUMNS)
    target_path = Path(path).expanduser().resolve()
    if not target_path.exists():
        return pd.DataFrame(columns=SEED_TARGET_COLUMNS)
    try:
        out = pd.read_csv(target_path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame(columns=SEED_TARGET_COLUMNS)

    out = out.copy()
    out.columns = [str(column).strip() for column in out.columns]
    if "month" not in out.columns or "scope_type" not in out.columns or "scope_name" not in out.columns:
        return pd.DataFrame(columns=SEED_TARGET_COLUMNS)

    for column in SEED_TARGET_COLUMNS:
        if column not in out.columns:
            out[column] = ""
    out["month"] = out["month"].map(_clean_text)
    out["scope_type"] = out["scope_type"].map(_clean_text)
    out["scope_name"] = out["scope_name"].map(_clean_text)
    out["parent_scope"] = out["parent_scope"].map(_clean_text)
    out["parent_account"] = out["parent_account"].map(_clean_text)
    out["impression_target_month"] = pd.to_numeric(out["impression_target_month"], errors="coerce")
    return out[SEED_TARGET_COLUMNS].copy()


def load_seed_sessions_from_workbooks(paths: Iterable[str | Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for source in paths:
        path = Path(source).expanduser().resolve()
        if not path.exists() or path.name.startswith("~$"):
            continue
        try:
            workbook = pd.ExcelFile(path)
            raw = pd.read_excel(path, sheet_name=workbook.sheet_names[0])
        except Exception:
            continue
        frame = _seed_sessions_from_raw(raw, source_file=path.name)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=SEED_SESSION_COLUMNS)
    out = pd.concat(frames, ignore_index=True, sort=False)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["impressions"] = pd.to_numeric(out["impressions"], errors="coerce")
    out = out[out["date"].notna() & out["impressions"].notna()].copy()
    return out[SEED_SESSION_COLUMNS].copy()


def build_seed_dashboard_tables(
    *,
    report_date: str | pd.Timestamp,
    seed_sessions: pd.DataFrame,
    seed_targets: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    report_ts = pd.to_datetime(report_date, errors="coerce")
    if pd.isna(report_ts):
        return pd.DataFrame(columns=SEED_ACCOUNT_COLUMNS), pd.DataFrame(columns=SEED_ANCHOR_COLUMNS)
    report_ts = report_ts.normalize()
    month = f"{report_ts.year:04d}-{report_ts.month:02d}"
    month_start = pd.Timestamp(year=report_ts.year, month=report_ts.month, day=1)

    sessions = _sessions_for_month(seed_sessions, month_start, report_ts)
    targets = _targets_for_month(seed_targets, month)
    if sessions.empty and targets.empty:
        return pd.DataFrame(columns=SEED_ACCOUNT_COLUMNS), pd.DataFrame(columns=SEED_ANCHOR_COLUMNS)

    account_table = _build_seed_account_table(sessions=sessions, targets=targets, report_ts=report_ts)
    anchor_table = _build_seed_anchor_table(sessions=sessions, targets=targets, report_ts=report_ts)
    return account_table, anchor_table


def _seed_sessions_from_raw(raw: pd.DataFrame, *, source_file: str) -> pd.DataFrame:
    data = raw.copy()
    data.columns = [str(column).strip() for column in data.columns]
    date_col = pick_live_column(data, ["日期", "直播日期", "创建时间"], required=False)
    account_col = pick_live_column(data, ["开播账号", "账号", "直播账号", "账号名称"], required=False)
    host_col = pick_live_column(data, ["本场主播", "主播", "主播名称"], required=False)
    exposure_col = pick_live_column(data, ["曝光人数", "曝光次数", "曝光", "展现", "曝光量"], required=False)
    a3_col = pick_live_column(data, ["A3人群增长"], required=False)
    if not date_col or not exposure_col:
        return pd.DataFrame(columns=SEED_SESSION_COLUMNS)
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(data[date_col], errors="coerce").dt.normalize(),
            "account": data[account_col].map(normalize_account) if account_col else "",
            "hosts_raw": data[host_col].map(normalize_text) if host_col else "",
            "impressions": pd.to_numeric(data[exposure_col], errors="coerce"),
            "a3_growth": pd.to_numeric(data[a3_col], errors="coerce").fillna(0.0) if a3_col else 0.0,
            "source_file": source_file,
        }
    )
    frame = frame[frame["date"].notna() & frame["impressions"].notna()].copy()
    return frame[SEED_SESSION_COLUMNS].copy()


def _targets_for_month(targets: pd.DataFrame, month: str) -> pd.DataFrame:
    if targets.empty or "month" not in targets.columns:
        return pd.DataFrame(columns=SEED_TARGET_COLUMNS)
    out = targets[targets["month"].astype(str).str.strip().eq(month)].copy()
    for column in SEED_TARGET_COLUMNS:
        if column not in out.columns:
            out[column] = ""
    out["impression_target_month"] = pd.to_numeric(out["impression_target_month"], errors="coerce")
    return out[SEED_TARGET_COLUMNS].copy()


def _sessions_for_month(seed_sessions: pd.DataFrame, month_start: pd.Timestamp, report_ts: pd.Timestamp) -> pd.DataFrame:
    if seed_sessions.empty or "date" not in seed_sessions.columns:
        return pd.DataFrame(columns=SEED_SESSION_COLUMNS)
    out = seed_sessions.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["impressions"] = pd.to_numeric(out.get("impressions", pd.Series(dtype="float64")), errors="coerce")
    out["a3_growth"] = pd.to_numeric(out.get("a3_growth", pd.Series(dtype="float64")), errors="coerce").fillna(0.0)
    out = out[out["date"].between(month_start, report_ts) & out["impressions"].notna()].copy()
    if out.empty:
        return pd.DataFrame(columns=SEED_SESSION_COLUMNS)
    for column in SEED_SESSION_COLUMNS:
        if column not in out.columns:
            out[column] = ""
    return out[SEED_SESSION_COLUMNS].copy()


def _build_seed_account_table(
    *,
    sessions: pd.DataFrame,
    targets: pd.DataFrame,
    report_ts: pd.Timestamp,
) -> pd.DataFrame:
    account_targets = targets[targets["scope_type"].eq("account")].copy() if not targets.empty else pd.DataFrame()
    target_names = [_clean_text(name) for name in account_targets.get("scope_name", pd.Series(dtype="object")).tolist()]
    target_names = [name for name in target_names if name]
    if not target_names and not sessions.empty:
        target_names = sorted({account for account in sessions["account"].map(normalize_account).tolist() if account})

    rows: list[dict[str, float | str]] = []
    for name in target_names:
        scoped = _scope_sessions_for_account(sessions, name, single_target=len(target_names) == 1)
        daily = _sum_impressions(scoped, report_ts, report_ts)
        mtd = _sum_impressions(scoped, pd.Timestamp(year=report_ts.year, month=report_ts.month, day=1), report_ts)
        previous = _sum_impressions(scoped, pd.Timestamp(year=report_ts.year, month=report_ts.month, day=1), report_ts - pd.Timedelta(days=1))
        target = _target_value(account_targets, scope_type="account", scope_name=name)
        daily_target = _remaining_daily_target(target, previous, report_ts)
        rows.append(_seed_row(name=name, parent="", daily=daily, mtd=mtd, daily_target=daily_target, mtd_target=target))
    return pd.DataFrame(rows, columns=SEED_ACCOUNT_COLUMNS)


def _build_seed_anchor_table(
    *,
    sessions: pd.DataFrame,
    targets: pd.DataFrame,
    report_ts: pd.Timestamp,
) -> pd.DataFrame:
    host_targets = targets[targets["scope_type"].isin(["host", "anchor"])].copy() if not targets.empty else pd.DataFrame()
    target_names = [_clean_text(name) for name in host_targets.get("scope_name", pd.Series(dtype="object")).tolist()]
    target_names = [name for name in target_names if name]
    anchor_daily = _anchor_daily_impressions(sessions)
    anchor_daily_a3 = _anchor_daily_metric(sessions, "a3_growth")
    extra_names = sorted(name for name in anchor_daily if name and name not in set(target_names))
    names = target_names + extra_names

    parent_map = _anchor_parent_map(sessions)
    rows: list[dict[str, float | str]] = []
    for name in names:
        values = anchor_daily.get(name, {})
        daily = float(values.get(report_ts.strftime("%Y-%m-%d"), 0.0))
        mtd = float(sum(values.values())) if values else 0.0
        a3_values = anchor_daily_a3.get(name, {})
        mtd_a3 = float(sum(a3_values.values())) if a3_values else 0.0
        previous = float(sum(value for date_key, value in values.items() if date_key < report_ts.strftime("%Y-%m-%d")))
        target = _target_value(host_targets, scope_type="host", scope_name=name)
        daily_target = _remaining_daily_target(target, previous, report_ts)
        parent = _target_parent(host_targets, name) or parent_map.get(name, "")
        rows.append(
            _seed_row(
                name=name,
                parent=parent,
                daily=daily,
                mtd=mtd,
                daily_target=daily_target,
                mtd_target=target,
                mtd_a3_growth=mtd_a3,
            )
        )
    return pd.DataFrame(rows, columns=SEED_ANCHOR_COLUMNS)


def _scope_sessions_for_account(sessions: pd.DataFrame, target_name: str, *, single_target: bool) -> pd.DataFrame:
    if sessions.empty:
        return sessions
    if single_target:
        return sessions
    normalized_target = normalize_account(target_name)
    account = sessions["account"].map(normalize_account)
    scoped = sessions[account.eq(normalized_target)].copy()
    if scoped.empty and target_name:
        scoped = sessions[account.astype(str).str.contains(target_name, regex=False, na=False)].copy()
    return scoped


def _anchor_daily_impressions(sessions: pd.DataFrame) -> dict[str, dict[str, float]]:
    return _anchor_daily_metric(sessions, "impressions")


def _anchor_daily_metric(sessions: pd.DataFrame, value_col: str) -> dict[str, dict[str, float]]:
    if sessions.empty:
        return {}
    if value_col not in sessions.columns:
        return {}
    data = sessions[["date", "account", "hosts_raw", value_col]].copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce")
    data["hosts"] = data["hosts_raw"].apply(split_hosts)
    data["host_count"] = data["hosts"].apply(len)
    data = data[data["date"].notna() & data[value_col].notna() & (data["host_count"] > 0)].copy()
    if data.empty:
        return {}
    data["weighted_value"] = data[value_col] / data["host_count"]
    data = data.explode("hosts", ignore_index=True)
    data["host"] = data["hosts"].map(normalize_text)
    data = data[data["host"].ne("")]

    out: dict[str, dict[str, float]] = {}
    grouped = data.groupby(["host", "date"])["weighted_value"].sum()
    for (host, date_key), value in grouped.items():
        out.setdefault(str(host), {})[str(date_key)] = float(value)
    return out


def _anchor_parent_map(sessions: pd.DataFrame) -> dict[str, str]:
    if sessions.empty:
        return {}
    data = sessions[["account", "hosts_raw"]].copy()
    data["account"] = data["account"].map(normalize_account)
    data["hosts"] = data["hosts_raw"].apply(split_hosts)
    data = data.explode("hosts", ignore_index=True)
    data["host"] = data["hosts"].map(normalize_text)
    data = data[data["host"].ne("") & data["account"].ne("")]
    mapping: dict[str, list[str]] = {}
    for _, row in data.iterrows():
        mapping.setdefault(str(row["host"]), []).append(str(row["account"]))
    return {host: " / ".join(dict.fromkeys(accounts)) for host, accounts in mapping.items()}


def _target_value(targets: pd.DataFrame, *, scope_type: str, scope_name: str) -> float | None:
    if targets.empty:
        return None
    matched_type = {"anchor": "host"}.get(scope_type, scope_type)
    matched = targets[targets["scope_type"].eq(matched_type) & targets["scope_name"].eq(scope_name)]
    values = pd.to_numeric(matched.get("impression_target_month", pd.Series(dtype="float64")), errors="coerce").dropna()
    return float(values.iloc[-1]) if not values.empty else None


def _target_parent(targets: pd.DataFrame, scope_name: str) -> str:
    if targets.empty:
        return ""
    matched = targets[targets["scope_name"].eq(scope_name)]
    for column in ("parent_account", "parent_scope"):
        if column not in matched.columns:
            continue
        values = matched[column].dropna().map(_clean_text)
        values = values[values.ne("")]
        if not values.empty:
            return values.iloc[-1]
    return ""


def _sum_impressions(sessions: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> float:
    if sessions.empty or end < start:
        return 0.0
    data = sessions.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.normalize()
    data["impressions"] = pd.to_numeric(data["impressions"], errors="coerce")
    matched = data[data["date"].between(start, end) & data["impressions"].notna()]
    return float(matched["impressions"].sum()) if not matched.empty else 0.0


def _remaining_daily_target(month_target: float | None, previous_actual: float, report_ts: pd.Timestamp) -> float | None:
    if month_target is None:
        return None
    days_in_month = calendar.monthrange(report_ts.year, report_ts.month)[1]
    remaining_days = max(days_in_month - report_ts.day + 1, 1)
    return max(float(month_target) - float(previous_actual), 0.0) / remaining_days


def _seed_row(
    *,
    name: str,
    parent: str,
    daily: float,
    mtd: float,
    daily_target: float | None,
    mtd_target: float | None,
    mtd_a3_growth: float = 0.0,
) -> dict[str, float | str]:
    return {
        "账号": name if not parent else "",
        "主播": name if parent else "",
        "归属账号": parent,
        "当日曝光": daily,
        "当日曝光目标": daily_target,
        "当日曝光达成率": _safe_div(daily, daily_target),
        "累计曝光": mtd,
        "曝光目标": mtd_target,
        "累计曝光达成率": _safe_div(mtd, mtd_target),
        "累计A3人群增长": mtd_a3_growth,
    }


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or float(denominator) == 0:
        return None
    return float(numerator) / float(denominator)


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "nat"} else text
