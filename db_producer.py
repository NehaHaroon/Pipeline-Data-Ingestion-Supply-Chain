#!/usr/bin/env python3
"""
DB Producer: Continuous Database Record Generator

Simulates a database producer that continuously inserts new records into the
simulated database. This runs in the background and adds new inventory
transactions at a configurable rate.

Usage:
    python db_producer.py

The producer will:
1. Generate new inventory transaction records using InventoryTransactionsGenerator
2. Append them to the simulated DB (storage/simulated_db/inventory_transactions.jsonl)
3. Run continuously until interrupted
"""

import os
import sys
import time
import signal
import threading
from datetime import datetime, timezone

# Setup path and logging
sys.path.insert(0, os.path.dirname(__file__))
from common import setup_logging, ensure_storage_directories

log = setup_logging("db_producer")

from data_plane.generators.source_generators import InventoryTransactionsGenerator
from data_plane.ingestion.db_ingest import write_to_simulated_db

# Configuration
PRODUCTION_RATE_PER_MINUTE = 10  # Records per minute (adjust as needed)
INTERVAL_SECONDS = 60 / PRODUCTION_RATE_PER_MINUTE

# Global flag for graceful shutdown
running = True

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global running
    log.info("Shutdown signal received, stopping producer...")
    running = False

def produce_records():
    """Continuously generate and write new records to simulated DB."""
    log.info(f"Starting DB producer | rate: {PRODUCTION_RATE_PER_MINUTE} records/minute | interval: {INTERVAL_SECONDS:.1f}s")

    gen = InventoryTransactionsGenerator()

    record_count = 0
    start_time = time.time()

    try:
        while running:
            # Generate and write a new record
            record = gen.generate_one()
            write_to_simulated_db(record)

            record_count += 1

            # Log progress every 10 records
            if record_count % 10 == 0:
                elapsed = time.time() - start_time
                rate = record_count / elapsed * 60  # per minute
                log.info(f"Produced {record_count} records | current rate: {rate:.1f}/min")

            # Wait for next record
            time.sleep(INTERVAL_SECONDS)

    except Exception as e:
        log.error(f"Producer error: {e}", exc_info=True)
    finally:
        elapsed = time.time() - start_time
        rate = record_count / elapsed * 60 if elapsed > 0 else 0
        log.info(f"Producer stopped | total records: {record_count} | final rate: {rate:.1f}/min | duration: {elapsed:.1f}s")

def main():
    """Main entry point."""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    DB PRODUCER STARTED                      ║
║                                                              ║
║  Continuously generating inventory transactions for CDC     ║
║  ingestion testing. Records are appended to simulated DB.   ║
║                                                              ║
║  Rate: {PRODUCTION_RATE_PER_MINUTE} records/minute                          ║
║  Press Ctrl+C to stop                                       ║
╚══════════════════════════════════════════════════════════════╝
""".format(PRODUCTION_RATE_PER_MINUTE=PRODUCTION_RATE_PER_MINUTE)

    print(banner)

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start the producer
    producer_thread = threading.Thread(target=produce_records, daemon=True)
    producer_thread.start()

    # Wait for shutdown signal
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

    # Wait for producer thread to finish
    producer_thread.join(timeout=5)

    log.info("DB producer shutdown complete")

if __name__ == "__main__":
    main()