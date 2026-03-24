"""CLI entrypoint for Feishu markdown/TSV exports."""

from __future__ import annotations

from oae.exports.feishu_report import main


if __name__ == "__main__":
    raise SystemExit(main())
