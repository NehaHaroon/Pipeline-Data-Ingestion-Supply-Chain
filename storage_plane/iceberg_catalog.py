
from pyiceberg.catalog.sql import SqlCatalog
import os
from config import STORAGE_ICEBERG_WAREHOUSE, STORAGE_ICEBERG_CATALOG_URI
from config import ICEBERG_NAMESPACE_BRONZE, ICEBERG_NAMESPACE_SILVER, ICEBERG_NAMESPACE_GOLD

_catalog = None

def get_catalog() -> SqlCatalog:
    global _catalog
    if _catalog is None:
        warehouse_path = os.path.abspath(STORAGE_ICEBERG_WAREHOUSE)
        os.makedirs(warehouse_path, exist_ok=True)
        _catalog = SqlCatalog(
            "supply_chain",
            **{
                "uri": STORAGE_ICEBERG_CATALOG_URI,
                "warehouse": f"file://{warehouse_path}",
            }
        )
        # Create namespaces
        for ns in [ICEBERG_NAMESPACE_BRONZE, ICEBERG_NAMESPACE_SILVER, ICEBERG_NAMESPACE_GOLD]:
            try:
                _catalog.create_namespace(ns)
            except Exception:
                pass   # already exists
    return _catalog