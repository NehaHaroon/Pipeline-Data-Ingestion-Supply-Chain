
from pyiceberg.catalog import Catalog
from storage_plane.iceberg_catalog import get_catalog
import logging

log = logging.getLogger(__name__)

class CompactionRunner:
    """
    Runs Iceberg bin-pack compaction on tables that exceed the
    min_files_to_compact threshold in their CompactionPolicy.
    Called by compaction_dag.py on schedule.
    """

    def run_table(self, table_name: str) -> dict:
        catalog = get_catalog()
        if not catalog.table_exists(table_name):
            return {"skipped": True, "reason": "table does not exist"}

        t = catalog.load_table(table_name)
        files_before = len(t.inspect.files().to_pydict().get("file_path", []))
        # PyIceberg 0.7+ exposes table.rewrite_data_files()
        result = t.rewrite_data_files(
            strategy="binpack",
            options={"target-file-size-bytes": str(128 * 1024 * 1024)}
        )
        files_after = len(t.inspect.files().to_pydict().get("file_path", []))
        log.info(f"Compacted {table_name}: {files_before} → {files_after} files")
        return {
            "table": table_name,
            "files_before": files_before,
            "files_after": files_after,
            "files_compacted": files_before - files_after,
        }