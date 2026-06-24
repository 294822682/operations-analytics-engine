"""Historical source registry and SQLite initialization helpers."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from oae.exports.feishu_douyin_laike import build_douyin_laike_order_metrics
from oae.rules.datetime_utils import combine_date_time_series
from oae.rules.identity import normalize_lead_id, normalize_phone
from oae.rules.io_utils import read_table_header_auto


@dataclass(frozen=True)
class HistoricalSourceRecord:
    source_id: str
    month: str
    source_kind: str
    source_path: str
    sheet_name: str
    row_count: int
    required_columns_status: str
    missing_columns: list[str]
    included_in_rollup: bool
    note: str


@dataclass(frozen=True)
class SourceSpec:
    source_kind: str
    patterns: tuple[str, ...]
    required_column_groups: tuple[tuple[str, tuple[str, ...]], ...]
    preferred_sheets: tuple[str, ...] = ()


SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec(
        source_kind="live_progress",
        patterns=("*直播进度表*.xlsx", "*直播进度表*.xls"),
        required_column_groups=(
            ("日期", ("日期", "直播日期", "创建时间")),
            ("开播账号", ("开播账号", "直播账号", "账号")),
            ("本场主播", ("本场主播", "主播")),
            ("开播时间", ("开播时间",)),
            ("下播时间", ("下播时间",)),
            ("曝光人数", ("曝光人数", "曝光次数", "曝光", "展现", "曝光量")),
            ("全场景线索人数", ("全场景线索人数", "直播全场景商机量", "唯一线索")),
        ),
    ),
    SourceSpec(
        source_kind="seed_ledger",
        patterns=("EXEED星途台账*.xlsx", "EXEED星途台账*.xls"),
        required_column_groups=(
            ("创建时间", ("创建时间", "直播日期", "日期")),
            ("开播账号", ("开播账号", "直播账号", "账号")),
            ("开播时间", ("开播时间",)),
            ("下播时间", ("下播时间",)),
            ("曝光人数", ("曝光人数", "曝光次数", "曝光", "展现", "曝光量")),
            ("A3人群增长", ("A3人群增长",)),
        ),
        preferred_sheets=("拆分", "1月原始台账", "2月拆分", "Sheet1"),
    ),
    SourceSpec(
        source_kind="lead_csv",
        patterns=("总部新媒体线索*.csv", "总部新媒体线索*.xlsx", "总部新媒体线索*.xls"),
        required_column_groups=(
            ("线索ID", ("线索ID", "线索id", "ID")),
            ("手机号", ("手机号", "手机", "电话")),
            ("创建时间", ("创建时间", "线索创建时间", "线索时间")),
            ("渠道2", ("渠道2",)),
            ("渠道3", ("渠道3", "渠道_3", "三级渠道")),
        ),
        preferred_sheets=("总部新媒体线索", "线索", "Sheet1"),
    ),
    SourceSpec(
        source_kind="deal_csv",
        patterns=("总部新媒体成交*.csv", "总部新媒体成交*.xlsx", "总部新媒体成交*.xls"),
        required_column_groups=(
            ("线索ID", ("线索ID", "线索id", "ID")),
            ("订单编号", ("订单编号", "订单ID", "订单id")),
            ("订单状态", ("订单状态",)),
            ("下订时间", ("下订时间", "下订日期")),
            ("成交时间", ("成交时间", "成交日期")),
        ),
        preferred_sheets=("成交", "Sheet1"),
    ),
)


def build_historical_metrics_db(
    *,
    workspace: Path,
    db_path: Path,
    start_month: str,
    end_month: str,
    history_dir: str = "历史文件",
) -> list[HistoricalSourceRecord]:
    records = scan_historical_sources(
        workspace,
        start_month=start_month,
        end_month=end_month,
        history_dir=history_dir,
    )
    initialize_historical_metrics_db(db_path, records)
    build_historical_staging(db_path, workspace.expanduser().resolve(), records)
    return records


def scan_historical_sources(
    workspace: Path,
    *,
    start_month: str,
    end_month: str,
    history_dir: str = "历史文件",
) -> list[HistoricalSourceRecord]:
    workspace = workspace.expanduser().resolve()
    records: list[HistoricalSourceRecord] = []
    for month in _iter_months(start_month, end_month):
        month_dir = workspace / history_dir / _month_dir_name(month)
        if not month_dir.exists():
            continue
        for spec in SOURCE_SPECS:
            for path in _match_source_files(month_dir, spec.patterns):
                records.append(_build_source_record(workspace, month, path, spec))
    return records


def initialize_historical_metrics_db(db_path: Path, records: list[HistoricalSourceRecord]) -> None:
    db_path = db_path.expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hist_source_registry (
                source_id TEXT PRIMARY KEY,
                month TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_path TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                required_columns_status TEXT NOT NULL,
                missing_columns TEXT NOT NULL,
                included_in_rollup INTEGER NOT NULL,
                note TEXT NOT NULL
            )
            """
        )
        conn.execute("DELETE FROM hist_source_registry")
        conn.executemany(
            """
            INSERT INTO hist_source_registry (
                source_id,
                month,
                source_kind,
                source_path,
                sheet_name,
                row_count,
                required_columns_status,
                missing_columns,
                included_in_rollup,
                note
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    record.source_id,
                    record.month,
                    record.source_kind,
                    record.source_path,
                    record.sheet_name,
                    record.row_count,
                    record.required_columns_status,
                    ",".join(record.missing_columns),
                    1 if record.included_in_rollup else 0,
                    record.note,
                )
                for record in records
            ],
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_source_registry_month ON hist_source_registry (month)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hist_source_registry_kind ON hist_source_registry (source_kind)"
        )


def build_historical_staging(db_path: Path, workspace: Path, records: list[HistoricalSourceRecord]) -> None:
    db_path = db_path.expanduser().resolve()
    workspace = workspace.expanduser().resolve()
    live_rows: list[dict[str, object]] = []
    seed_rows: list[dict[str, object]] = []
    lead_rows: list[dict[str, object]] = []
    deal_rows: list[dict[str, object]] = []
    laike_rows: list[dict[str, object]] = []

    for record in records:
        if record.source_kind == "live_progress":
            live_rows.extend(_session_rows(workspace, record, source_kind="live_progress"))
        elif record.source_kind == "seed_ledger":
            seed_rows.extend(_session_rows(workspace, record, source_kind="seed_ledger"))
        elif record.source_kind == "lead_csv":
            lead_rows.extend(_lead_rows(workspace, record))
        elif record.source_kind == "deal_csv":
            deal_rows.extend(_deal_rows(workspace, record))

    for month in sorted({record.month for record in records}):
        live_record = _record_for(records, month, "live_progress")
        lead_record = _record_for(records, month, "lead_csv")
        if live_record is None or lead_record is None:
            continue
        total_orders, _, _ = build_douyin_laike_order_metrics(
            _read_record_frame(workspace, live_record),
            _read_record_frame(workspace, lead_record),
            _month_end(month),
        )
        laike_rows.append(
            {
                "month": month,
                "total_orders": float(total_orders),
                "method": "build_douyin_laike_order_metrics",
                "source_id_live": live_record.source_id,
                "source_id_leads": lead_record.source_id,
                "note": "直播进度表抖音-来客窗口 + 总部新媒体线索表手机号优先去重",
            }
        )

    with sqlite3.connect(db_path) as conn:
        _write_table(conn, "stg_live_sessions", live_rows, _LIVE_SESSION_COLUMNS)
        _write_table(conn, "stg_seed_sessions", seed_rows, _SEED_SESSION_COLUMNS)
        _write_table(conn, "stg_leads", lead_rows, _LEAD_COLUMNS)
        _write_table(conn, "stg_deals", deal_rows, _DEAL_COLUMNS)
        _write_table(conn, "stg_douyin_laike_orders", laike_rows, _DOUYIN_LAIKE_COLUMNS)
        _create_monthly_views(conn)


def _build_source_record(workspace: Path, month: str, path: Path, spec: SourceSpec) -> HistoricalSourceRecord:
    columns, sheet_name = read_table_header_auto(path, preferred_sheets=list(spec.preferred_sheets))
    missing = _missing_required_columns(columns, spec)
    status = "passed" if not missing else "missing"
    note = ""
    if spec.source_kind == "seed_ledger" and "A3人群增长" in missing:
        note = "A3 source field missing; keep NULL instead of filling 0"
    return HistoricalSourceRecord(
        source_id=f"{month}:{spec.source_kind}:{path.name}",
        month=month,
        source_kind=spec.source_kind,
        source_path=path.relative_to(workspace).as_posix(),
        sheet_name=sheet_name,
        row_count=_row_count(path, sheet_name=sheet_name),
        required_columns_status=status,
        missing_columns=missing,
        included_in_rollup=True,
        note=note,
    )


def _match_source_files(month_dir: Path, patterns: tuple[str, ...]) -> list[Path]:
    matched: list[Path] = []
    for pattern in patterns:
        matched.extend(path for path in month_dir.glob(pattern) if path.is_file() and not path.name.startswith("~$"))
    return sorted(set(matched), key=lambda path: path.name)


def _row_count(path: Path, *, sheet_name: str) -> int:
    if path.suffix.lower() == ".csv":
        return int(len(pd.read_csv(path, encoding=_detect_csv_encoding(path), low_memory=False)))
    return int(len(pd.read_excel(path, sheet_name=sheet_name)))


def _detect_csv_encoding(path: Path) -> str:
    for encoding in ("utf-8-sig", "gb18030", "gbk"):
        try:
            pd.read_csv(path, encoding=encoding, nrows=0)
            return encoding
        except UnicodeDecodeError:
            continue
    return "utf-8-sig"


def _iter_months(start_month: str, end_month: str) -> list[str]:
    start = pd.Period(start_month, freq="M")
    end = pd.Period(end_month, freq="M")
    if start > end:
        raise ValueError(f"start_month must be <= end_month: {start_month} > {end_month}")
    return [str(period) for period in pd.period_range(start, end, freq="M")]


def _month_dir_name(month: str) -> str:
    period = pd.Period(month, freq="M")
    return f"{period.year}年{period.month}月"


def _fold_column(value: object) -> str:
    return "".join(str(value).strip().split())


def _missing_required_columns(columns: list[str], spec: SourceSpec) -> list[str]:
    folded_columns = {_fold_column(column) for column in columns}
    missing: list[str] = []
    for canonical, aliases in spec.required_column_groups:
        if not any(_fold_column(alias) in folded_columns for alias in aliases):
            missing.append(canonical)
    return missing


_LIVE_SESSION_COLUMNS = [
    "source_id",
    "month",
    "date",
    "account",
    "platform_component",
    "host",
    "start_time",
    "end_time",
    "duration_hours",
    "impressions",
    "raw_live_leads",
]

_SEED_SESSION_COLUMNS = [
    "source_id",
    "month",
    "date",
    "account",
    "host",
    "start_time",
    "end_time",
    "duration_hours",
    "impressions",
    "raw_live_leads",
    "a3_growth",
    "a3_source_status",
]

_LEAD_COLUMNS = ["source_id", "month", "lead_id", "phone_key", "create_time", "channel2", "channel3", "account"]

_DEAL_COLUMNS = [
    "source_id",
    "month",
    "lead_id",
    "order_id",
    "order_status",
    "order_time",
    "deal_time",
    "deal_model",
]

_DOUYIN_LAIKE_COLUMNS = ["month", "total_orders", "method", "source_id_live", "source_id_leads", "note"]


def _session_rows(workspace: Path, record: HistoricalSourceRecord, *, source_kind: str) -> list[dict[str, object]]:
    raw = _read_record_frame(workspace, record)
    date_col = _pick_col(raw, ["日期", "直播日期", "创建时间"], required=True)
    account_col = _pick_col(raw, ["开播账号", "直播账号", "账号", "账号名称"], required=False)
    host_col = _pick_col(raw, ["本场主播", "主播", "主播名称"], required=False)
    start_col = _pick_col(raw, ["开播时间", "开始时间", "直播开始时间"], required=False)
    end_col = _pick_col(raw, ["下播时间", "结束时间", "直播结束时间"], required=False)
    duration_col = _pick_col(raw, ["时长", "直播时长"], required=False)
    impression_col = _pick_col(raw, ["曝光人数", "曝光次数", "曝光", "展现", "曝光量"], required=False)
    lead_col = _pick_col(raw, ["全场景线索人数", "直播全场景商机量", "唯一线索"], required=False)
    component_col = _pick_col(
        raw,
        ["平台&挂载组件", "平台& 挂载组件", "平台&挂载组建", "平台& 挂载组建"],
        required=False,
    )
    a3_col = _pick_col(raw, ["A3人群增长"], required=False)

    scoped = _filter_month(raw, date_col, record.month)
    starts, ends, durations = _session_times(scoped, date_col=date_col, start_col=start_col, end_col=end_col, duration_col=duration_col)
    dates = _date_strings(scoped[date_col])
    impressions = _numeric(scoped[impression_col]) if impression_col else pd.Series([0.0] * len(scoped), index=scoped.index)
    raw_leads = _numeric(scoped[lead_col]) if lead_col else pd.Series([0.0] * len(scoped), index=scoped.index)
    accounts = _text_values(scoped, account_col)
    hosts = _text_values(scoped, host_col)

    if source_kind == "live_progress":
        components = _text_values(scoped, component_col)
        return [
            {
                "source_id": record.source_id,
                "month": record.month,
                "date": dates.loc[index],
                "account": accounts.loc[index],
                "platform_component": components.loc[index],
                "host": hosts.loc[index],
                "start_time": _timestamp_text(starts.loc[index]),
                "end_time": _timestamp_text(ends.loc[index]),
                "duration_hours": float(durations.loc[index] or 0.0),
                "impressions": float(impressions.loc[index]),
                "raw_live_leads": float(raw_leads.loc[index]),
            }
            for index in scoped.index
        ]

    a3_values = _numeric(scoped[a3_col]) if a3_col else pd.Series([pd.NA] * len(scoped), index=scoped.index)
    a3_status = "available" if a3_col else "missing_source_field"
    return [
        {
            "source_id": record.source_id,
            "month": record.month,
            "date": dates.loc[index],
            "account": accounts.loc[index],
            "host": hosts.loc[index],
            "start_time": _timestamp_text(starts.loc[index]),
            "end_time": _timestamp_text(ends.loc[index]),
            "duration_hours": float(durations.loc[index] or 0.0),
            "impressions": float(impressions.loc[index]),
            "raw_live_leads": float(raw_leads.loc[index]),
            "a3_growth": None if pd.isna(a3_values.loc[index]) else float(a3_values.loc[index]),
            "a3_source_status": a3_status,
        }
        for index in scoped.index
    ]


def _lead_rows(workspace: Path, record: HistoricalSourceRecord) -> list[dict[str, object]]:
    raw = _read_record_frame(workspace, record)
    lead_id_col = _pick_col(raw, ["线索ID", "线索id", "ID"], required=True)
    phone_col = _pick_col(raw, ["手机号", "手机", "电话"], required=False)
    create_col = _pick_col(raw, ["创建时间", "线索创建时间", "线索时间"], required=True)
    channel2_col = _pick_col(raw, ["渠道2"], required=False)
    channel3_col = _pick_col(raw, ["渠道3", "渠道_3", "三级渠道"], required=False)
    scoped = _filter_month(raw, create_col, record.month)
    create_time = pd.to_datetime(scoped[create_col], errors="coerce")
    lead_ids = normalize_lead_id(scoped[lead_id_col])
    phones = scoped[phone_col].map(normalize_phone) if phone_col else pd.Series([""] * len(scoped), index=scoped.index)
    channel2 = _text_values(scoped, channel2_col)
    channel3 = _text_values(scoped, channel3_col)
    return [
        {
            "source_id": record.source_id,
            "month": record.month,
            "lead_id": str(lead_ids.loc[index]),
            "phone_key": _phone_key(phones.loc[index]),
            "create_time": _timestamp_text(create_time.loc[index]),
            "channel2": channel2.loc[index],
            "channel3": channel3.loc[index],
            "account": _lead_account(channel2.loc[index], channel3.loc[index]),
        }
        for index in scoped.index
    ]


def _deal_rows(workspace: Path, record: HistoricalSourceRecord) -> list[dict[str, object]]:
    raw = _read_record_frame(workspace, record)
    lead_id_col = _pick_col(raw, ["线索ID", "线索id", "ID"], required=True)
    order_id_col = _pick_col(raw, ["订单编号", "订单ID", "订单id"], required=False)
    status_col = _pick_col(raw, ["订单状态"], required=False)
    order_time_col = _pick_col(raw, ["下订时间", "下订日期"], required=False)
    deal_time_col = _pick_col(raw, ["成交时间", "成交日期"], required=True)
    model_col = _pick_col(raw, ["成交车型", "车系"], required=False)
    scoped = _filter_month(raw, deal_time_col, record.month)
    lead_ids = normalize_lead_id(scoped[lead_id_col])
    order_ids = _text_values(scoped, order_id_col)
    statuses = _text_values(scoped, status_col)
    order_times = pd.to_datetime(scoped[order_time_col], errors="coerce") if order_time_col else pd.Series([pd.NaT] * len(scoped), index=scoped.index)
    deal_times = pd.to_datetime(scoped[deal_time_col], errors="coerce")
    models = _text_values(scoped, model_col)
    return [
        {
            "source_id": record.source_id,
            "month": record.month,
            "lead_id": str(lead_ids.loc[index]),
            "order_id": order_ids.loc[index],
            "order_status": statuses.loc[index],
            "order_time": _timestamp_text(order_times.loc[index]),
            "deal_time": _timestamp_text(deal_times.loc[index]),
            "deal_model": models.loc[index],
        }
        for index in scoped.index
    ]


def _read_record_frame(workspace: Path, record: HistoricalSourceRecord) -> pd.DataFrame:
    path = workspace / record.source_path
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path, encoding=_detect_csv_encoding(path), low_memory=False)
    else:
        frame = pd.read_excel(path, sheet_name=record.sheet_name)
    frame = frame.copy()
    frame.columns = [str(column).strip() for column in frame.columns]
    return frame


def _filter_month(df: pd.DataFrame, date_col: str, month: str) -> pd.DataFrame:
    dates = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    month_start = pd.Period(month, freq="M").to_timestamp().normalize()
    month_end = _month_end(month)
    return df[dates.between(month_start, month_end)].copy()


def _session_times(
    df: pd.DataFrame,
    *,
    date_col: str,
    start_col: str | None,
    end_col: str | None,
    duration_col: str | None,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    index = df.index
    starts = pd.Series([pd.NaT] * len(df), index=index, dtype="datetime64[ns]")
    ends = pd.Series([pd.NaT] * len(df), index=index, dtype="datetime64[ns]")
    durations = pd.Series([0.0] * len(df), index=index, dtype="float64")
    if start_col and end_col:
        starts = combine_date_time_series(df[date_col], df[start_col])
        ends = combine_date_time_series(df[date_col], df[end_col])
        ends = ends.where(~(ends.notna() & starts.notna() & (ends < starts)), ends + pd.Timedelta(days=1))
        computed = (ends - starts).dt.total_seconds() / 3600.0
        durations = computed.where(computed.gt(0) & computed.le(24), 0.0).fillna(0.0)
    if duration_col:
        provided = pd.to_numeric(df[duration_col], errors="coerce")
        provided = provided.where(~(provided.gt(0) & provided.lt(1)), provided * 24)
        durations = provided.where(provided.gt(0) & provided.le(24), durations).fillna(durations)
    return starts, ends, durations


def _write_table(conn: sqlite3.Connection, name: str, rows: list[dict[str, object]], columns: list[str]) -> None:
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    conn.execute(f'DROP TABLE IF EXISTS "{name}"')
    conn.execute(f'CREATE TABLE "{name}" ({", ".join(f"{column} TEXT" for column in columns)})')
    if rows:
        placeholders = ", ".join("?" for _ in columns)
        conn.executemany(
            f'INSERT INTO "{name}" ({quoted_columns}) VALUES ({placeholders})',
            [tuple(row.get(column) for column in columns) for row in rows],
        )


def _create_monthly_views(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP VIEW IF EXISTS v_monthly_live_metrics;
        DROP VIEW IF EXISTS v_monthly_leads;
        DROP VIEW IF EXISTS v_monthly_deals;
        DROP VIEW IF EXISTS v_monthly_summary;

        CREATE VIEW v_monthly_live_metrics AS
        SELECT
          month,
          SUM(CAST(duration_hours AS REAL)) AS live_hours,
          SUM(CAST(impressions AS REAL)) AS impressions,
          SUM(CAST(raw_live_leads AS REAL)) AS raw_live_leads
        FROM (
          SELECT month, duration_hours, impressions, raw_live_leads FROM stg_live_sessions
          UNION ALL
          SELECT month, duration_hours, impressions, raw_live_leads FROM stg_seed_sessions
        )
        GROUP BY month;

        CREATE VIEW v_monthly_leads AS
        SELECT
          month,
          COUNT(*) AS lead_rows,
          COUNT(DISTINCT lead_id) AS unique_lead_ids,
          COUNT(DISTINCT NULLIF(phone_key, '')) AS unique_phone_keys
        FROM stg_leads
        GROUP BY month;

        CREATE VIEW v_monthly_deals AS
        SELECT
          month,
          COUNT(DISTINCT lead_id) AS delivered_deals
        FROM stg_deals
        WHERE order_status = '已交车'
          AND deal_time IS NOT NULL
          AND TRIM(deal_time) != ''
        GROUP BY month;

        CREATE VIEW v_monthly_summary AS
        SELECT
          live.month,
          live.live_hours,
          live.impressions,
          leads.lead_rows,
          CAST(laike.total_orders AS REAL) AS douyin_laike_orders,
          deals.delivered_deals,
          a3.a3_growth,
          a3.a3_source_status
        FROM v_monthly_live_metrics live
        LEFT JOIN v_monthly_leads leads ON leads.month = live.month
        LEFT JOIN stg_douyin_laike_orders laike ON laike.month = live.month
        LEFT JOIN v_monthly_deals deals ON deals.month = live.month
        LEFT JOIN (
          SELECT
            month,
            SUM(CASE WHEN a3_growth IS NOT NULL AND TRIM(a3_growth) != '' THEN CAST(a3_growth AS REAL) ELSE NULL END) AS a3_growth,
            CASE
              WHEN SUM(CASE WHEN a3_source_status = 'available' THEN 1 ELSE 0 END) > 0
              THEN 'available'
              ELSE 'missing_source_field'
            END AS a3_source_status
          FROM stg_seed_sessions
          GROUP BY month
        ) a3 ON a3.month = live.month;
        """
    )


def _pick_col(df: pd.DataFrame, aliases: list[str], *, required: bool) -> str | None:
    folded = {_fold_column(column): column for column in df.columns}
    for alias in aliases:
        hit = folded.get(_fold_column(alias))
        if hit:
            return hit
    if required:
        raise ValueError(f"缺少列: {aliases}; 当前列={list(df.columns)}")
    return None


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce").fillna(0.0)


def _text_values(df: pd.DataFrame, column: str | None) -> pd.Series:
    if column is None:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[column].fillna("").astype(str).str.strip()


def _date_strings(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d").fillna("")


def _timestamp_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")
    return str(value).strip()


def _phone_key(phone: object) -> str:
    normalized = normalize_phone(phone)
    return f"PHONE9:{normalized[:9]}" if normalized else ""


def _lead_account(channel2: str, channel3: str) -> str:
    return channel3 if channel2 == "抖音来客直播" and channel3 else channel2


def _record_for(records: list[HistoricalSourceRecord], month: str, source_kind: str) -> HistoricalSourceRecord | None:
    for record in records:
        if record.month == month and record.source_kind == source_kind:
            return record
    return None


def _month_end(month: str) -> pd.Timestamp:
    period = pd.Period(month, freq="M")
    return period.to_timestamp(how="end").normalize()
