"""SQLite loader/runtime extracted from the root compatibility script."""

from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path

import pandas as pd


NUMERIC_COLUMNS = [
    "权重",
    "同手机号线索数",
    "主播人数",
    "成交分摊权重",
    "is_order",
    "is_deal",
    "orders_contrib",
    "deals_contrib",
    "命中场次数量",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build SQLite db from fact_attribution.csv")
    parser.add_argument("--csv", default="output/fact_attribution.csv", help="Source csv path")
    parser.add_argument("--db", default="output/lead_daily.db", help="Target sqlite db path")
    parser.add_argument("--chunksize", type=int, default=0, help="Chunk size for incremental load")
    return parser.parse_args()


def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in NUMERIC_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "date" in out.columns:
        parsed_date = pd.to_datetime(out["date"], errors="coerce")
        out["date"] = parsed_date.dt.strftime("%Y-%m-%d").where(parsed_date.notna(), None)
    return out


def create_indexes_and_views(conn: sqlite3.Connection, table_columns: set[str], logger: logging.Logger) -> None:
    quote = lambda name: '"' + str(name).replace('"', '""') + '"'
    index_specs = [
        ("date", "idx_f_actual_detail_date"),
        ("标准账号", "idx_f_actual_detail_account"),
        ("本场主播", "idx_f_actual_detail_host"),
        ("线索ID_norm", "idx_f_actual_detail_lead_id_norm"),
        ("归属状态", "idx_f_actual_detail_attr_status"),
        ("is_order", "idx_f_actual_detail_is_order"),
        ("is_deal", "idx_f_actual_detail_is_deal"),
    ]
    for col_name, idx_name in index_specs:
        if col_name not in table_columns:
            logger.warning("索引跳过：列不存在 %s", col_name)
            continue
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS {quote(idx_name)} '
            f'ON {quote("f_actual_detail")} ({quote(col_name)})'
        )

    daily_required = {"date", "标准账号", "本场主播", "权重", "is_order", "is_deal", "orders_contrib", "deals_contrib"}
    if daily_required.issubset(table_columns):
        conn.execute(
            """
            CREATE VIEW IF NOT EXISTS "v_daily_summary" AS
            SELECT
                "date" AS "date",
                "标准账号" AS "标准账号",
                "本场主播" AS "本场主播",
                COUNT(*) AS "线索数",
                SUM("权重") AS "加权线索数",
                SUM("is_order") AS "订单数",
                SUM("is_deal") AS "成交数",
                SUM("orders_contrib") AS "加权订单数",
                SUM("deals_contrib") AS "加权成交数"
            FROM "f_actual_detail"
            GROUP BY "date", "标准账号", "本场主播"
            """
        )
    else:
        logger.warning("视图跳过：v_daily_summary 缺少列 %s", sorted(daily_required - table_columns))

    host_required = {"本场主播", "is_deal", "归属状态"}
    if host_required.issubset(table_columns):
        conn.execute(
            """
            CREATE VIEW IF NOT EXISTS "v_host_summary" AS
            SELECT
                "本场主播",
                COUNT(*) AS "线索数",
                SUM("is_deal") AS "成交数",
                ROUND(SUM("is_deal") * 1.0 / COUNT(*) * 100, 2) AS "成交率"
            FROM "f_actual_detail"
            WHERE "归属状态" = '匹配成功'
            GROUP BY "本场主播"
            """
        )
    else:
        logger.warning("视图跳过：v_host_summary 缺少列 %s", sorted(host_required - table_columns))


def main() -> int:
    args = parse_args()
    logger = logging.getLogger("build_sqlite_db")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)

    csv_path = Path(args.csv).expanduser().resolve()
    db_path = Path(args.db).expanduser().resolve()
    if not csv_path.exists():
        logger.error("CSV not found: %s", csv_path)
        return 1
    if args.chunksize < 0:
        logger.error("Invalid chunksize: %s (must be >= 0)", args.chunksize)
        return 1
    db_path.parent.mkdir(parents=True, exist_ok=True)
    required_fields = ["线索ID_norm", "date", "标准账号", "is_order", "is_deal"]
    expected_rows = 0

    try:
        with sqlite3.connect(db_path) as conn:
            if args.chunksize > 0:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=OFF")
                first_chunk = True
                for chunk in pd.read_csv(csv_path, chunksize=args.chunksize):
                    if first_chunk:
                        missing_fields = [c for c in required_fields if c not in chunk.columns]
                        for col_name in missing_fields:
                            logger.warning("关键字段缺失（不中断）: %s", col_name)
                    if chunk.empty:
                        continue
                    transformed = transform_chunk(chunk)
                    transformed.to_sql("f_actual_detail", conn, if_exists="replace" if first_chunk else "append", index=False)
                    expected_rows += len(transformed)
                    first_chunk = False
                if expected_rows == 0:
                    logger.error("Source CSV is empty: %s", csv_path)
                    return 1
            else:
                df = pd.read_csv(csv_path)
                missing_fields = [c for c in required_fields if c not in df.columns]
                for col_name in missing_fields:
                    logger.warning("关键字段缺失（不中断）: %s", col_name)
                if df.empty:
                    logger.error("Source CSV is empty: %s", csv_path)
                    return 1
                transformed = transform_chunk(df)
                expected_rows = len(transformed)
                transformed.to_sql("f_actual_detail", conn, if_exists="replace", index=False)

            row_count = conn.execute('SELECT COUNT(*) FROM "f_actual_detail"').fetchone()[0]
            col_info = conn.execute('PRAGMA table_info("f_actual_detail")').fetchall()
            table_columns = {item[1] for item in col_info}
            create_indexes_and_views(conn, table_columns, logger)
            if row_count != expected_rows:
                logger.warning("写入行数不一致：DataFrame=%s, SQLite=%s", expected_rows, row_count)
    except Exception as exc:
        logger.exception("执行失败: %s", exc)
        return 1

    logger.info("[OK] Source CSV: %s", csv_path)
    logger.info("[OK] SQLite DB: %s", db_path)
    logger.info("[OK] Table: f_actual_detail, rows=%s, cols=%s", row_count, len(col_info))
    logger.info("[OK] Columns:")
    for item in col_info:
        logger.info("  - %s (%s)", item[1], item[2])
    return 0
