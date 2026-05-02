#!/usr/bin/env python3
"""
Run Iceberg catalog operations using SQLAlchemy 2.x (isolated venv).

Airflow 2.9.x ships with SQLAlchemy 1.4 (constraints). PyIceberg SqlCatalog requires 2.0+.
The custom Airflow image installs this script's dependencies into /opt/airflow/iceberg-toolkit only.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def cmd_compaction_health() -> dict:
    from storage_plane.storage_kpis import get_all_tables_kpis

    all_kpis = get_all_tables_kpis()
    tables_needing_compaction = [
        (name, kpis.get("small_file_ratio", 0))
        for name, kpis in all_kpis.items()
        if "error" not in kpis and kpis.get("small_file_ratio", 0) > 0.5
    ]
    return {
        "total_tables": len(all_kpis),
        "tables_needing_compaction": len(tables_needing_compaction),
        "tables": tables_needing_compaction,
    }


def cmd_compact(table_name: str) -> dict:
    from storage_plane.compaction import CompactionRunner

    runner = CompactionRunner()
    return runner.run_table(table_name)


def main() -> None:
    p = argparse.ArgumentParser(description="Iceberg tasks for Airflow (toolkit venv)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("compaction-health", help="Summarize tables that may need compaction")

    pc = sub.add_parser("compact", help="Run bin-pack compaction on one table")
    pc.add_argument("table_name", help='e.g. "bronze.iot_rfid_stream"')

    args = p.parse_args()
    if args.cmd == "compaction-health":
        out = cmd_compaction_health()
    elif args.cmd == "compact":
        out = cmd_compact(args.table_name)
    else:
        raise SystemExit(2)
    print(json.dumps(out))


if __name__ == "__main__":
    main()
