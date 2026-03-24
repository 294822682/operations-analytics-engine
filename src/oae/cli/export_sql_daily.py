"""CLI entrypoint for SQL daily exports."""

from __future__ import annotations

from oae.reports.sql_daily import main


if __name__ == "__main__":
    raise SystemExit(main())
