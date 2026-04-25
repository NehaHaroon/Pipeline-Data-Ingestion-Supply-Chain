"""Idempotently seeds the inventory_transactions table in PostgreSQL."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import List, Tuple

import psycopg2
from dotenv import load_dotenv

SEED_ROWS: List[Tuple[str, str, str, str, int, str, str | None, str]] = [
    ("TXN-001-2025-001", "ART-1001-MID-XS", "WAREHOUSE-LONDON", "IN", 500, "2025-11-01T08:00:00Z", "PO-2025-0001", "system"),
    ("TXN-001-2025-002", "ART-1001-MID-S", "WAREHOUSE-LONDON", "IN", 300, "2025-11-01T08:15:00Z", "PO-2025-0001", "system"),
    ("TXN-001-2025-003", "ART-1001-MID-M", "WAREHOUSE-LONDON", "OUT", -50, "2025-11-01T09:00:00Z", "SO-2025-0101", "warehouse_staff"),
    ("TXN-001-2025-004", "ART-1017-OLI-XS", "WAREHOUSE-DUBAI", "IN", 800, "2025-11-01T09:30:00Z", "PO-2025-0002", "system"),
    ("TXN-001-2025-005", "ART-1017-OLI-M", "WAREHOUSE-DUBAI", "OUT", -120, "2025-11-01T10:00:00Z", "SO-2025-0102", "warehouse_staff"),
    ("TXN-001-2025-006", "ART-1397-OLI-M", "WAREHOUSE-KARACHI", "IN", 450, "2025-11-01T10:45:00Z", "PO-2025-0003", "system"),
    ("TXN-001-2025-007", "ART-1253-NAV-S", "WAREHOUSE-LONDON", "OUT", -75, "2025-11-01T11:15:00Z", "SO-2025-0103", "warehouse_staff"),
    ("TXN-001-2025-008", "ART-1222-OLI-L", "WAREHOUSE-DUBAI", "ADJUSTMENT", 10, "2025-11-01T12:00:00Z", None, "quality_control"),
    ("TXN-001-2025-009", "ART-1439-CLA-XS", "WAREHOUSE-KARACHI", "IN", 600, "2025-11-01T13:30:00Z", "PO-2025-0004", "system"),
    ("TXN-001-2025-010", "ART-1064-NAV-XS", "WAREHOUSE-LONDON", "OUT", -200, "2025-11-01T14:00:00Z", "SO-2025-0104", "warehouse_staff"),
]


def _get_connection_with_retry(database_url: str, retries: int = 5, backoff_seconds: int = 3):
    for attempt in range(1, retries + 1):
        try:
            print(f"[SEED] Connecting to PostgreSQL (attempt={attempt}/{retries})...")
            return psycopg2.connect(database_url)
        except psycopg2.Error as exc:
            if attempt == retries:
                raise
            print(f"[SEED] Connection failed: {exc}. Retrying in {backoff_seconds}s...")
            time.sleep(backoff_seconds)
    raise RuntimeError("Failed to establish PostgreSQL connection after retries.")


def seed_inventory_transactions() -> None:
    """Seed inventory transactions once if the table is currently empty."""
    load_dotenv(override=False)
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required.")

    conn = _get_connection_with_retry(database_url=database_url)
    conn.autocommit = False
    try:
        with conn.cursor() as cursor:
            print("[SEED] Checking if table already contains data...")
            cursor.execute("SELECT COUNT(*) FROM inventory_transactions;")
            row_count = cursor.fetchone()[0]
            if row_count > 0:
                print(f"[SEED] Skipping seed; table already has {row_count} rows.")
                conn.rollback()
                return

            print("[SEED] Seeding inventory_transactions with baseline records...")
            created_at = datetime.now(timezone.utc)
            insert_sql = """
                INSERT INTO inventory_transactions (
                    transaction_id,
                    product_id,
                    warehouse_location,
                    transaction_type,
                    quantity_change,
                    timestamp,
                    reference_order_id,
                    created_by,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s::timestamptz, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING;
            """
            rows_to_insert = [
                (
                    transaction_id,
                    product_id,
                    warehouse_location,
                    transaction_type,
                    quantity_change,
                    ts_iso,
                    reference_order_id,
                    created_by,
                    created_at,
                )
                for (
                    transaction_id,
                    product_id,
                    warehouse_location,
                    transaction_type,
                    quantity_change,
                    ts_iso,
                    reference_order_id,
                    created_by,
                ) in SEED_ROWS
            ]
            cursor.executemany(insert_sql, rows_to_insert)
            conn.commit()
            print("[SEED] Done. Seed operation completed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    seed_inventory_transactions()
