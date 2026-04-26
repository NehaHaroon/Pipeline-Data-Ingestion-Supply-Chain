import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data_plane.ingestion import db_ingest
from common import get_ingestion_interval_for_source


def test_inventory_transactions_ingestion_interval():
    interval = get_ingestion_interval_for_source("src_inventory_transactions")
    assert interval == 3600


def test_read_from_simulated_db_sorts_records(tmp_path, monkeypatch):
    simulated_db = tmp_path / "inventory_transactions.jsonl"
    records = [
        {
            "transaction_id": "TXN-0002",
            "created_at": "2026-04-26T12:05:00+00:00"
        },
        {
            "transaction_id": "TXN-0001",
            "created_at": "2026-04-26T12:00:00+00:00"
        }
    ]
    simulated_db.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    monkeypatch.setattr(db_ingest, "SIMULATED_DB_PATH", str(simulated_db))

    loaded = db_ingest.read_from_simulated_db("2026-04-26T11:59:00+00:00")
    assert len(loaded) == 2
    assert loaded[0]["transaction_id"] == "TXN-0001"
    assert loaded[1]["transaction_id"] == "TXN-0002"
