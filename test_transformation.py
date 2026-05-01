#!/usr/bin/env python3
"""
Test script to verify Bronze table creation and Silver transformation.
Run this after ingestion to check if the pipeline is working correctly.
"""

import os
import sys
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from storage_plane.iceberg_catalog import get_catalog

def test_bronze_tables():
    """Test that Bronze tables exist and have data."""
    print("🔍 Testing Bronze tables...")

    catalog = get_catalog()
    sources = ["warehouse_master", "sales_history", "manufacturing_logs", "legacy_trends"]

    for source in sources:
        table_name = f"bronze.{source}"
        try:
            table = catalog.load_table(table_name)
            df = table.scan().to_pandas()
            print(f"✅ {table_name}: {len(df)} records")
        except Exception as e:
            print(f"❌ {table_name}: {e}")

def test_silver_transformation():
    """Test Silver transformation for one source."""
    print("\n🔄 Testing Silver transformation...")

    try:
        from data_plane.transformation.silver_transformer import SilverTransformer

        # Test with warehouse_master (smallest table)
        transformer = SilverTransformer("src_warehouse_master")
        result = transformer.transform()

        print("✅ Silver transformation successful:" )
        print(f"   - Records read: {result.records_read}")
        print(f"   - Records cleaned: {result.records_cleaned}")
        print(f"   - Records rejected: {result.records_rejected}")
        print(f"   - Latency: {result.transformation_latency_sec:.2f}s")

    except Exception as e:
        print(f"❌ Silver transformation failed: {e}")

if __name__ == "__main__":
    test_bronze_tables()
    test_silver_transformation()
    print("\n✨ Test complete!")