"""
Centralized ingestion frequency utilities.
"""

from control_plane.entities import IngestionFrequency, ALL_SOURCES


def frequency_to_seconds(frequency: IngestionFrequency) -> int:
    """
    Convert IngestionFrequency enum to seconds for scheduling.
    
    Args:
        frequency: IngestionFrequency enum value
    
    Returns:
        Number of seconds corresponding to the frequency
    """
    mapping = {
        IngestionFrequency.REAL_TIME: 1,  # Not really used for batch scheduling
        IngestionFrequency.EVERY_2_MINUTES: 120,
        IngestionFrequency.HOURLY: 3600,
        IngestionFrequency.DAILY: 86400,
        IngestionFrequency.WEEKLY: 604800,
        IngestionFrequency.ON_DEMAND: 0,  # Not scheduled
    }
    return mapping.get(frequency, 3600)  # Default to hourly


def get_ingestion_interval_for_source(source_id: str) -> int:
    """
    Get ingestion interval for any source by ID from entity definitions.
    
    Args:
        source_id: Source identifier (e.g., 'src_inventory_transactions')
    
    Returns:
        Interval in seconds, or 3600 (hourly) if source not found
    """
    source = next((s for s in ALL_SOURCES if s.source_id == source_id), None)
    if source:
        return frequency_to_seconds(source.ingestion_frequency)
    return 3600  # Default fallback to hourly
