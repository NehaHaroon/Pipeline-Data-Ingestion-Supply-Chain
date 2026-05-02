"""
Iceberg bin-pack compaction — called from Airflow ``compaction_dag``.
Uses the same catalog lock as other writers to avoid SQLite contention failures.
"""

from __future__ import annotations

import logging

from storage_plane.iceberg_catalog import get_catalog
from storage_plane.iceberg_session_lock import iceberg_catalog_session, retry_catalog_mutation

log = logging.getLogger(__name__)


class CompactionRunner:
    """Runs PyIceberg rewrite_data_files (binpack) on one table."""

    def run_table(self, table_name: str) -> dict:
        def _compact() -> dict:
            catalog = get_catalog()
            if not catalog.table_exists(table_name):
                return {"skipped": True, "reason": "table does not exist"}

            table = catalog.load_table(table_name)
            files_before = len(table.inspect.files().to_pydict().get("file_path", []))

            table.rewrite_data_files(
                strategy="binpack",
                options={"target-file-size-bytes": str(128 * 1024 * 1024)},
            )

            files_after = len(table.inspect.files().to_pydict().get("file_path", []))
            log.info("Compacted %s: %s → %s files", table_name, files_before, files_after)
            return {
                "table": table_name,
                "files_before": files_before,
                "files_after": files_after,
                "files_compacted": max(0, files_before - files_after),
            }

        with iceberg_catalog_session():
            return retry_catalog_mutation(_compact)
