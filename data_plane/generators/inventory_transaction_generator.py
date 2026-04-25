"""Generates synthetic inventory transactions into PostgreSQL for Debezium CDC."""

from __future__ import annotations

import argparse
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from uuid import uuid4

import psycopg2
from dotenv import load_dotenv


@dataclass
class Stats:
    """Tracks generated event counters and throughput."""

    records: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    started_at: float = time.time()


def _connect_with_retry(database_url: str, retries: int = 5, backoff_seconds: int = 3):
    for attempt in range(1, retries + 1):
        try:
            print(f"[GENERATOR] Connecting to PostgreSQL (attempt={attempt}/{retries})...")
            return psycopg2.connect(database_url)
        except psycopg2.Error as exc:
            if attempt == retries:
                raise
            print(f"[GENERATOR] Connection failed: {exc}. Retrying in {backoff_seconds}s...")
            time.sleep(backoff_seconds)
    raise RuntimeError("Unable to connect to PostgreSQL after retries.")


def _build_product_pool(pool_size: int = 50) -> List[str]:
    colors = ["MID", "OLI", "NAV", "CLA", "BLK"]
    sizes = ["XS", "S", "M", "L", "XL"]
    products: List[str] = []
    while len(products) < pool_size:
        base_code = random.randint(1000, 1500)
        product = f"ART-{base_code}-{random.choice(colors)}-{random.choice(sizes)}"
        if product not in products:
            products.append(product)
    return products


def _generate_quantity_change(transaction_type: str) -> int:
    if transaction_type == "IN":
        return int(max(50, min(1000, random.gauss(400, 150))))
    if transaction_type == "OUT":
        return int(max(-500, min(-10, random.gauss(-100, 60))))
    if transaction_type == "ADJUSTMENT":
        return random.randint(-50, 50)
    if transaction_type == "RETURN":
        return int(max(1, min(200, random.gauss(30, 15))))
    return random.randint(-20, -1)


def _generate_reference_order(transaction_type: str) -> Optional[str]:
    current_year = datetime.now(timezone.utc).year
    seq = random.randint(1, 9999)
    if transaction_type in {"IN", "RETURN"}:
        return f"PO-{current_year}-{seq:04d}"
    if transaction_type == "OUT":
        return f"SO-{current_year}-{seq:04d}"
    return None


def _generate_transaction_row(product_pool: List[str]) -> Tuple[str, str, str, str, int, str, Optional[str], str]:
    now = datetime.now(timezone.utc)
    transaction_id = f"TXN-{now.strftime('%Y%m%d')}-{str(uuid4())[:8].upper()}"
    transaction_type = random.choices(
        population=["IN", "OUT", "ADJUSTMENT", "RETURN", "WRITE_OFF"],
        weights=[35, 45, 15, 4, 1],
        k=1,
    )[0]
    return (
        transaction_id,
        random.choice(product_pool),
        random.choices(
            population=["WAREHOUSE-LONDON", "WAREHOUSE-DUBAI", "WAREHOUSE-KARACHI"],
            weights=[40, 35, 25],
            k=1,
        )[0],
        transaction_type,
        _generate_quantity_change(transaction_type),
        now.isoformat(),
        _generate_reference_order(transaction_type),
        random.choices(
            population=["system", "warehouse_staff", "quality_control", "manager"],
            weights=[50, 35, 10, 5],
            k=1,
        )[0],
    )


def _insert_one(cursor, row: Tuple[str, str, str, str, int, str, Optional[str], str]) -> int:
    cursor.execute(
        """
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
        ) VALUES (%s, %s, %s, %s, %s, %s::timestamptz, %s, %s, NOW())
        ON CONFLICT (transaction_id) DO NOTHING;
        """,
        row,
    )
    return cursor.rowcount


def _maybe_update(cursor) -> int:
    if random.random() > 0.05:
        return 0
    cursor.execute("SELECT transaction_id FROM inventory_transactions ORDER BY random() LIMIT 1;")
    selected = cursor.fetchone()
    if not selected:
        return 0
    cursor.execute(
        """
        UPDATE inventory_transactions
        SET quantity_change = quantity_change + %s,
            updated_at = NOW()
        WHERE transaction_id = %s;
        """,
        (random.choice([-10, 10]), selected[0]),
    )
    return cursor.rowcount


def _maybe_delete(cursor) -> int:
    if random.random() > 0.01:
        return 0
    cursor.execute("SELECT transaction_id FROM inventory_transactions ORDER BY random() LIMIT 1;")
    selected = cursor.fetchone()
    if not selected:
        return 0
    cursor.execute("DELETE FROM inventory_transactions WHERE transaction_id = %s;", (selected[0],))
    return cursor.rowcount


def _print_stats(mode: str, stats: Stats) -> None:
    elapsed = max(0.001, time.time() - stats.started_at)
    rate = stats.records / elapsed
    print(
        f"[GENERATOR] mode={mode} records={stats.records} inserts={stats.inserts} "
        f"updates={stats.updates} deletes={stats.deletes} elapsed={elapsed:.1f}s rate={rate:.1f} rec/s"
    )


def run_generator(mode: str) -> None:
    """Run steady or burst synthetic generation mode forever."""
    load_dotenv(override=False)
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required.")

    product_pool = _build_product_pool(pool_size=50)
    conn = _connect_with_retry(database_url=database_url)
    conn.autocommit = False
    stats = Stats()
    print(f"[GENERATOR] Starting generator in mode={mode}")

    try:
        with conn.cursor() as cursor:
            while True:
                cycle_target = 1 if mode == "steady" else 5000
                cycle_sleep = 0.1 if mode == "steady" else 10.0

                for _ in range(cycle_target):
                    inserted = _insert_one(cursor, _generate_transaction_row(product_pool))
                    stats.inserts += inserted
                    stats.records += inserted

                    updated = _maybe_update(cursor)
                    deleted = _maybe_delete(cursor)
                    stats.updates += updated
                    stats.deletes += deleted
                    stats.records += (updated + deleted)
                    conn.commit()

                    if stats.records > 0 and stats.records % 100 == 0:
                        _print_stats(mode=mode, stats=stats)

                    if mode == "steady":
                        time.sleep(0.1)

                if mode == "burst":
                    _print_stats(mode=mode, stats=stats)
                    time.sleep(cycle_sleep)
    except KeyboardInterrupt:
        print("[GENERATOR] Shutdown requested by user.")
    finally:
        conn.close()
        print("[GENERATOR] Connection closed.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic inventory transaction CDC generator.")
    parser.add_argument(
        "--mode",
        choices=["steady", "burst"],
        default="steady",
        help="Generation mode: steady (10 rec/s) or burst (5000 then pause).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_generator(mode=args.mode)
