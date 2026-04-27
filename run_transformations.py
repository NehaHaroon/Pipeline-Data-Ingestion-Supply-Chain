#!/usr/bin/env python3
"""
Run Transformations: Bronze → Silver → Gold

This script runs the complete transformation pipeline:
1. Silver transformations for all sources
2. Gold aggregation

Usage: python run_transformations.py
"""

import sys
import os
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("run_transformations")

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

def run_silver_transformations():
    """Run Silver transformations for all sources."""
    from data_plane.transformation.silver_transformer import SilverTransformer
    from data_plane.transformation.transformation_kpis import TransformationKPILogger

    sources = [
        "src_sales_history",
        "src_warehouse_master",
        "src_manufacturing_logs",
        "src_iot_rfid_stream",
        "src_inventory_transactions",
        "src_legacy_trends",
        "src_weather_api",
    ]

    log.info("Starting Silver transformations...")

    for source_id in sources:
        try:
            log.info(f"Transforming {source_id}...")
            transformer = SilverTransformer(source_id)
            result = transformer.transform()

            # Log KPIs
            TransformationKPILogger.log_kpi(result)

            log.info(f"✓ {source_id}: {result.records_read} read, {result.records_cleaned} cleaned")

        except Exception as e:
            log.error(f"✗ Failed to transform {source_id}: {e}")

def run_gold_aggregation():
    """Run Gold layer aggregation."""
    from data_plane.transformation.gold_aggregator import GoldAggregator
    from data_plane.transformation.transformation_kpis import TransformationKPILogger

    log.info("Starting Gold aggregation...")

    try:
        aggregator = GoldAggregator()
        result = aggregator.run()

        # Log KPIs (create a mock result for gold)
        from data_plane.transformation.transformation_kpis import TransformationKPIs
        from datetime import datetime

        gold_kpi = TransformationKPIs(
            run_id=f"gold_{datetime.utcnow().isoformat()}",
            source_id="gold_aggregation",
            layer="gold",
            run_at=datetime.utcnow().isoformat(),
            records_read=result.get("products_checked", 0),
            records_cleaned=result.get("records_written", 0),
            records_rejected=0,
            records_written=result.get("records_written", 0),
            transformation_latency_sec=0.0,  # Not tracked
        )
        TransformationKPILogger.log_kpi(gold_kpi)

        log.info(f"✓ Gold aggregation: {result.get('records_written', 0)} replenishment signals created")

    except Exception as e:
        log.error(f"✗ Failed Gold aggregation: {e}")

def main():
    log.info("🚀 Starting transformation pipeline...")

    start_time = datetime.now()

    # Run Silver transformations
    run_silver_transformations()

    # Run Gold aggregation
    run_gold_aggregation()

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    log.info(f"✅ Transformation pipeline completed in {duration:.2f}s")

if __name__ == "__main__":
    main()