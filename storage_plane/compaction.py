"""
Iceberg bin-pack compaction — called from Airflow ``compaction_dag``.
Uses the same catalog lock as other writers to avoid SQLite contention failures.
"""

from __future__ import annotations

import logging

from pyiceberg.exceptions import NoSuchTableError

from storage_plane.iceberg_catalog import get_catalog
from storage_plane.iceberg_session_lock import iceberg_catalog_session, retry_catalog_mutation

log = logging.getLogger(__name__)


class CompactionRunner:
    """Runs PyIceberg rewrite_data_files (binpack) on one table."""

    @staticmethod
    def _file_count(table) -> int | None:
        """Best-effort file count compatible across PyIceberg versions."""
        try:
            inspect_obj = getattr(table, "inspect", None)
            if inspect_obj:
                files_tbl = inspect_obj.files().to_pydict()
                return len(files_tbl.get("file_path", []))
        except Exception:
            pass
        try:
            snap = table.current_snapshot()
            if not snap:
                return 0
            summary = snap.summary or {}
            if summary.get("total-data-files") is not None:
                return int(summary.get("total-data-files"))
            if summary.get("added-data-files") is not None:
                return int(summary.get("added-data-files"))
        except Exception:
            pass
        return None

    def run_table(self, table_name: str) -> dict:
        def _compact() -> dict:
            catalog = get_catalog()
            try:
                table = catalog.load_table(table_name)
            except NoSuchTableError:
                return {"skipped": True, "reason": "table does not exist"}
            if not hasattr(table, "rewrite_data_files"):
                return {
                    "table": table_name,
                    "skipped": True,
                    "reason": "rewrite_data_files not supported by installed PyIceberg version",
                }
            files_before = self._file_count(table)

            table.rewrite_data_files(
                strategy="binpack",
                options={"target-file-size-bytes": str(128 * 1024 * 1024)},
            )

            table = catalog.load_table(table_name)
            files_after = self._file_count(table)
            log.info("Compacted %s: %s → %s files", table_name, files_before, files_after)
            return {
                "table": table_name,
                "files_before": files_before,
                "files_after": files_after,
                "files_compacted": (
                    max(0, files_before - files_after)
                    if isinstance(files_before, int) and isinstance(files_after, int)
                    else None
                ),
            }

        with iceberg_catalog_session():
            return retry_catalog_mutation(_compact)
