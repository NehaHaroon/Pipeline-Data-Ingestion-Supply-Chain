#!/usr/bin/env python3
# run_production.py — Production runner for the supply chain ingestion pipeline.
#
# Usage:  python run_production.py
#
# This script starts the API and optionally the real-time IoT consumer.

import os
import sys
import logging
import threading
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("run_production")

BASE = os.path.dirname(__file__)
sys.path.insert(0, BASE)

def start_api():
    """Start the FastAPI server."""
    log.info("Starting API server...")
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, log_level="info")

def start_iot_consumer():
    """Start the real-time IoT consumer."""
    log.info("Starting IoT consumer...")
    from data_plane.ingestion.real_time_iot_ingest import run_real_time_ingestion
    run_real_time_ingestion()

def main():
    log.info("Starting Supply Chain Ingestion Pipeline (Production Mode)")

    # Start API in a thread
    api_thread = threading.Thread(target=start_api, daemon=True)
    api_thread.start()

    # Wait a bit for API to start
    time.sleep(2)

    # Start IoT consumer
    start_iot_consumer()

if __name__ == "__main__":
    main()