#!/usr/bin/env python3
"""
Drop bloated inventory Iceberg tables and CDC watermarks (dev/demo reset).

Silver with 2M+ rows cannot be fixed by deleting "some" rows — drop and rebuild
with dedup (~10k unique transaction_id) or re-ingest a small Bronze slice.

Usage (repo root, with Part 1/2 stack up or local Python + storage mount):

  python scripts/reset_inventory_iceberg.py --silver-only
  python scripts/reset_inventory_iceberg.py --full

  docker compose -f docker-compose.airflow.yml run --rm transform-service \\
    python scripts/reset_inventory_iceberg.py --full

  # If the script is missing, recreate transform-service (compose mounts repo at /app):
  docker compose -f docker-compose.airflow.yml up -d transform-service
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from control_plane.service_registry import StorageLayer, table_name_for_layer
from storage_plane.iceberg_catalog import get_catalog
from storage_plane.iceberg_incremental import clear_bronze_silver_watermark

SOURCE_ID = "src_inventory_transactions"


def _drop(catalog, table_name: str) -> None:
    try:
        catalog.drop_table(table_name)
        print(f"Dropped Iceberg table: {table_name}")
    except Exception as exc:
        print(f"Skip drop {table_name}: {type(exc).__name__}: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset inventory Bronze/Silver Iceberg tables")
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Drop silver.inventory_transactions only (Bronze stays — rebuild still scans Bronze)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Drop Bronze + Silver + watermarks (fastest path: re-ingest ~10k, then Silver)",
    )
    args = parser.parse_args()
    if not args.silver_only and not args.full:
        parser.error("Choose --silver-only or --full")

    catalog = get_catalog()
    silver_table = table_name_for_layer(StorageLayer.SILVER, SOURCE_ID)
    bronze_table = table_name_for_layer(StorageLayer.BRONZE, SOURCE_ID)

    _drop(catalog, silver_table)
    clear_bronze_silver_watermark(SOURCE_ID)
    print("Cleared storage/checkpoints/silver_bronze_src_inventory_transactions.json")

    if args.full:
        _drop(catalog, bronze_table)
        db_cp = ROOT / "storage" / "checkpoints" / f"db_{SOURCE_ID}_checkpoint.json"
        if db_cp.is_file():
            db_cp.unlink()
            print(f"Removed {db_cp}")
        print()
        print("Full reset done. Next:")
        print("  1. docker compose -f docker-compose.yml up -d ingestion-api")
        print("  2. Trigger one CDC/db ingest (or wait for cdc-consumer)")
        print("  3. curl -X POST http://localhost:8001/transform/silver/src_inventory_transactions")
        print("     (expect ~10k Silver rows in minutes, not millions)")
    else:
        print()
        print("Silver dropped. Bronze still has all CDC history.")
        print("Rebuild Silver with dedup (slow — scans all Bronze):")
        print("  Set SILVER_REBUILD_DEDUP=1 on transform-service, then POST /transform/silver/...")
        print("Or run: python scripts/reset_inventory_iceberg.py --full  for fastest dev reset.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
