"""
Common utility functions and configurations shared across the project.
"""

import os
import sys
from pathlib import Path
from typing import List


def ensure_project_in_path() -> str:
    """
    Ensure project root is in sys.path for imports.
    Returns the project root directory.
    """
    # Try different common patterns to find project root
    current_file = os.path.abspath(__file__)
    
    # If in common/ subdirectory, go up one level
    if "common" in current_file:
        project_root = os.path.dirname(os.path.dirname(current_file))
    else:
        project_root = os.path.dirname(current_file)
    
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    return project_root


def ensure_directories_exist(directories: List[str]) -> None:
    """
    Create multiple directories if they don't exist.
    
    Args:
        directories: List of directory paths to create
    """
    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def build_storage_paths() -> dict:
    """
    Create a dictionary of all standard storage paths used in the project.
    Returns a dict with keys like 'ingested', 'quarantine', 'cdc_log', etc.
    """
    paths = {
        'ingested': 'storage/ingested',
        'quarantine': 'storage/quarantine',
        'cdc_log': 'storage/cdc_log',
        'checkpoints': 'storage/checkpoints',
        'simulated_db': 'storage/simulated_db',
        'stream_buffer': 'storage/stream_buffer',
        'detail_logs': 'storage/ingested/detail_logs',
        'raw': 'storage/raw',
        'logs': 'logs'
    }
    return paths


def ensure_storage_directories() -> dict:
    """
    Ensure all standard storage directories exist and return their paths.
    """
    paths = build_storage_paths()
    ensure_directories_exist(list(paths.values()))
    return paths
