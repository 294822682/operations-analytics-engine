"""SQL-based daily diagnostics export runtime."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出 SQL 日报诊断结果")
    parser.add_argument("--db", default="output/lead_daily.db", help="SQLite db 路径")
    parser.add_argument("--output-dir", default="output/sql_reports", help="导出目录")
    return parser.parse_args()


def fetch_one_value(conn: sqlite3.Connection, sql: str) -> str:
    row = conn.execute(sql).fetchone()
    return str(row[0]) if row and row[0] is not None else ""


def export_query(conn: sqlite3.Connection, sql: str, out_file: Path) -> int:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(cols)
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    out_dir = Path(args.output_dir).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f"[ERROR] DB not found: {db_path}")
    out_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        latest_date = fetch_one_value(conn, "SELECT MAX(date) FROM f_actual_detail")
        if not latest_date:
            raise SystemExit("[ERROR] f_actual_detail is empty")
        ym = latest_date[:7]
        month_start = f"{ym}-01"
        queries = {
            "daily_kpi_latest": f"""
                SELECT
                  f.date,
                  COUNT(DISTINCT f."线索ID") AS unique_leads,
                  SUM(COALESCE(f.is_order, 0)) AS orders_cnt,
                  ROUND(SUM(COALESCE(f.deals_contrib, 0)), 4) AS deals_contrib,
                  printf('%.2f%%', 100.0 * SUM(COALESCE(f.deals_contrib, 0)) / NULLIF(COUNT(DISTINCT f."线索ID"), 0)) AS deal_rate_pct,
                  printf('%.2f%%', 100.0 * SUM(CASE WHEN f."归属状态"='无主线索' THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT f."线索ID"), 0)) AS unowned_ratio_pct
                FROM f_actual_detail f
                WHERE f.date = '{latest_date}'
                GROUP BY f.date
            """,
            "mtd_kpi": f"""
                SELECT
                  '{month_start}' AS mtd_start,
                  '{latest_date}' AS mtd_end,
                  COUNT(DISTINCT f."线索ID") AS mtd_unique_leads,
                  SUM(COALESCE(f.is_order, 0)) AS mtd_orders_cnt,
                  ROUND(SUM(COALESCE(f.deals_contrib, 0)), 4) AS mtd_deals_contrib,
                  printf('%.2f%%', 100.0 * SUM(COALESCE(f.deals_contrib, 0)) / NULLIF(COUNT(DISTINCT f."线索ID"), 0)) AS mtd_deal_rate_pct
                FROM f_actual_detail f
                WHERE date(f.date) BETWEEN date('{month_start}') AND date('{latest_date}')
            """,
            "trend_14d_bucket": f"""
                SELECT
                  f.date,
                  f.report_bucket,
                  COUNT(DISTINCT f."线索ID") AS unique_leads,
                  ROUND(SUM(COALESCE(f.deals_contrib, 0)), 4) AS deals_contrib
                FROM f_actual_detail f
                WHERE date(f.date) BETWEEN date('{latest_date}', '-13 day') AND date('{latest_date}')
                GROUP BY f.date, f.report_bucket
                ORDER BY f.date DESC, f.report_bucket
            """,
            "top10_anchor_mtd": f"""
                WITH normalized AS (
                  SELECT
                    *,
                    CASE
                      WHEN "本场主播" IS NULL OR TRIM("本场主播") = '' THEN '["【无主播】"]'
                      ELSE '["' || replace(replace(replace(TRIM("本场主播"), '，', ','), '、', ','), ',', '","') || '"]'
                    END AS anchor_json
                  FROM f_actual_detail
                  WHERE date(date) BETWEEN date('{month_start}') AND date('{latest_date}')
                ),
                split AS (
                  SELECT
                    TRIM(j.value) AS anchor_name,
                    json_array_length(n.anchor_json) AS anchor_cnt,
                    n."线索ID",
                    COALESCE(n.is_order, 0) AS is_order,
                    COALESCE(n.is_deal, 0) AS is_deal
                  FROM normalized n
                  JOIN json_each(n.anchor_json) j
                )
                SELECT
                  ROW_NUMBER() OVER (
                    ORDER BY SUM(CASE WHEN is_deal=1 THEN 1.0/anchor_cnt ELSE 0 END) DESC, SUM(1.0/anchor_cnt) DESC
                  ) AS rank_no,
                  anchor_name,
                  ROUND(SUM(1.0/anchor_cnt), 2) AS mtd_leads_contrib,
                  ROUND(SUM(CASE WHEN is_order=1 THEN 1.0/anchor_cnt ELSE 0 END), 4) AS mtd_orders_contrib,
                  ROUND(SUM(CASE WHEN is_deal=1 THEN 1.0/anchor_cnt ELSE 0 END), 4) AS mtd_deals_contrib,
                  printf('%.2f%%', 100.0 * SUM(CASE WHEN is_deal=1 THEN 1.0/anchor_cnt ELSE 0 END) / NULLIF(SUM(1.0/anchor_cnt), 0)) AS mtd_deal_rate_pct
                FROM split
                GROUP BY anchor_name
                ORDER BY rank_no
                LIMIT 10
            """,
            "unowned_by_account_14d": f"""
                SELECT
                  f."标准账号",
                  COUNT(DISTINCT f."线索ID") AS unique_leads,
                  SUM(CASE WHEN f."归属状态"='无主线索' THEN 1 ELSE 0 END) AS unowned_leads,
                  printf('%.2f%%', 100.0 * SUM(CASE WHEN f."归属状态"='无主线索' THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT f."线索ID"), 0)) AS unowned_ratio_pct
                FROM f_actual_detail f
                WHERE date(f.date) BETWEEN date('{latest_date}', '-13 day') AND date('{latest_date}')
                GROUP BY f."标准账号"
                HAVING COUNT(DISTINCT f."线索ID") >= 30
                ORDER BY 100.0 * SUM(CASE WHEN f."归属状态"='无主线索' THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT f."线索ID"), 0) DESC, unowned_leads DESC
            """,
        }
        print(f"[INFO] latest_date={latest_date}, mtd_start={month_start}")
        for name, sql in queries.items():
            out_file = out_dir / f"{name}_{latest_date}.csv"
            row_count = export_query(conn, sql, out_file)
            print(f"[OK] {name}: rows={row_count}, file={out_file.resolve()}")
    return 0
