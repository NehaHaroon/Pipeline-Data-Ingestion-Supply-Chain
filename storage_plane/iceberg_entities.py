
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

class TableLayer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD   = "gold"

class FileFormat(Enum):
    PARQUET = "parquet"
    AVRO    = "avro"
    ORC     = "orc"

@dataclass
class CompactionPolicy:
    size_threshold_mb: int = 128        # target file size
    frequency_minutes: int = 60         # how often to compact
    min_files_to_compact: int = 5       # don't compact fewer than this

@dataclass
class SnapshotRetention:
    max_snapshots: int = 10             # keep last N snapshots
    max_age_days: Optional[int] = None  # OR age-based

@dataclass
class IcebergTable:
    table_id:           str
    dataset_id:         str
    table_layer:        TableLayer
    partition_spec:     str             # e.g. "date(event_timestamp)" — human-readable
    file_format:        FileFormat = FileFormat.PARQUET
    compaction_policy:  CompactionPolicy = field(default_factory=CompactionPolicy)
    snapshot_retention: SnapshotRetention = field(default_factory=SnapshotRetention)
    description:        str = ""