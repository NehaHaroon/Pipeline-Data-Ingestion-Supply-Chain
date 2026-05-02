from enum import Enum
from typing import Dict

from control_plane.contracts import CONTRACT_REGISTRY
from control_plane.entities import ALL_DATASETS, ALL_SOURCES, DataSource, Dataset


class StorageLayer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


SOURCE_BY_ID: Dict[str, DataSource] = {source.source_id: source for source in ALL_SOURCES}
DATASET_BY_ID: Dict[str, Dataset] = {dataset.dataset_id: dataset for dataset in ALL_DATASETS}


def get_source(source_id: str) -> DataSource:
    source = SOURCE_BY_ID.get(source_id)
    if source is None:
        raise KeyError(f"Unknown source_id: {source_id}")
    return source


def get_dataset_for_source(source_id: str) -> Dataset:
    dataset_id = dataset_id_from_source(source_id)
    dataset = DATASET_BY_ID.get(dataset_id)
    if dataset is None:
        raise KeyError(f"Unknown dataset for source_id: {source_id}")
    return dataset


def get_contract(source_id: str):
    contract = CONTRACT_REGISTRY.get(source_id)
    if contract is None:
        raise KeyError(f"No contract configured for source_id: {source_id}")
    return contract


def dataset_id_from_source(source_id: str) -> str:
    return f"ds_{source_id.removeprefix('src_')}"


def source_id_from_dataset(dataset_id: str) -> str:
    return f"src_{dataset_id.removeprefix('ds_')}"


def table_name_for_layer(layer: StorageLayer, source_id: str) -> str:
    return f"{layer.value}.{source_id.removeprefix('src_')}"
