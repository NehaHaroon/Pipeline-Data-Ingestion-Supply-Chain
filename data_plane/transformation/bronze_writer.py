
import pyarrow as pa
from storage_plane.iceberg_catalog import get_catalog
from storage_plane.iceberg_session_lock import iceberg_catalog_session, retry_catalog_mutation
from control_plane.entities import EventEnvelope
from control_plane.service_registry import StorageLayer, table_name_for_layer
from observability_plane.structured_logging import get_logger, log_pipeline_event

class BronzeWriter:
    """
    Accepts EventEnvelope records (already produced by your existing ingestion)
    and appends them to the appropriate Iceberg Bronze table.
    Called from your existing /ingest/{source_id} endpoint AFTER the
    current parquet write, as an additional sink.
    """

    def __init__(self, source_id: str):
        self.source_id = source_id
        self.table_name = table_name_for_layer(StorageLayer.BRONZE, source_id)
        self.catalog = get_catalog()
        self.log = get_logger("bronze_writer")

    def write_batch(self, envelopes: list[EventEnvelope]) -> dict:
        rows = [e.to_dict() for e in envelopes]
        if not rows:
            return {"snapshot_id": None, "files_added": 0, "records_written": 0}

        # PyIceberg cannot create schema fields with Arrow null type.
        # For tiny batches, optional metadata fields can be all None.
        all_keys = set().union(*(row.keys() for row in rows))
        all_null_keys = [
            key for key in all_keys
            if all(row.get(key) is None for row in rows)
        ]
        if all_null_keys:
            for row in rows:
                for key in all_null_keys:
                    row.pop(key, None)
            log_pipeline_event(
                self.log,
                "warning",
                "Dropped all-null columns from Bronze batch",
                layer=StorageLayer.BRONZE.value,
                source_id=self.source_id,
                dropped_columns=all_null_keys,
            )

        arrow_table = pa.Table.from_pylist(rows)
        log_pipeline_event(
            self.log,
            "info",
            "Writing batch into Bronze table",
            layer=StorageLayer.BRONZE.value,
            source_id=self.source_id,
            records=len(rows),
            table_name=self.table_name,
        )

        def _bronze_append():
            try:
                tbl = self.catalog.load_table(self.table_name)
            except Exception:
                tbl = self.catalog.create_table(
                    self.table_name,
                    schema=arrow_table.schema,
                )
            tbl.append(arrow_table)
            return tbl.current_snapshot()

        with iceberg_catalog_session():
            snap = retry_catalog_mutation(_bronze_append)
        return {
            "snapshot_id": snap.snapshot_id if snap else None,
            "files_added": snap.summary.get("added-data-files", 0) if snap else 0,
            "records_written": len(rows),
        }