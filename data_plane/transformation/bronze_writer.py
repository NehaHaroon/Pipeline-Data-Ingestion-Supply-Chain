
import pyarrow as pa
from storage_plane.iceberg_catalog import get_catalog
from control_plane.entities import EventEnvelope

class BronzeWriter:
    """
    Accepts EventEnvelope records (already produced by your existing ingestion)
    and appends them to the appropriate Iceberg Bronze table.
    Called from your existing /ingest/{source_id} endpoint AFTER the
    current parquet write, as an additional sink.
    """

    def __init__(self, source_id: str):
        self.source_id = source_id
        self.table_name = f"bronze.{source_id.replace('src_', '')}"
        self.catalog = get_catalog()

    def write_batch(self, envelopes: list[EventEnvelope]) -> dict:
        rows = [e.to_dict() for e in envelopes]
        arrow_table = pa.Table.from_pylist(rows)

        try:
            iceberg_table = self.catalog.load_table(self.table_name)
        except:
            iceberg_table = self.catalog.create_table(
                self.table_name,
                schema=arrow_table.schema,
            )

        iceberg_table.append(arrow_table)

        # Return storage KPIs for observability
        snap = iceberg_table.current_snapshot()
        return {
            "snapshot_id": snap.snapshot_id if snap else None,
            "files_added": snap.summary.get("added-data-files", 0) if snap else 0,
            "records_written": len(rows),
        }