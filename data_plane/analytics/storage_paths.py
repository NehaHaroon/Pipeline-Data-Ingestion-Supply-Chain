"""
Resolve writable paths for Part 3 analytics artifacts on Docker bind mounts (Windows/Linux).
"""

from __future__ import annotations

import os
from functools import lru_cache


def _can_append(path: str) -> bool:
    try:
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "a", encoding="utf-8"):
            pass
        return True
    except OSError:
        return False


def _can_write_new(path: str) -> bool:
    try:
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write("")
        return True
    except OSError:
        return False


@lru_cache(maxsize=1)
def query_audit_log_path() -> str:
    primary = os.getenv("QUERY_AUDIT_LOG_PATH", "storage/analytics/query_audit.jsonl")
    if _can_append(primary):
        return primary
    fallback = os.getenv("QUERY_AUDIT_LOG_FALLBACK", "/tmp/query_audit.jsonl")
    os.makedirs(os.path.dirname(os.path.abspath(fallback)), exist_ok=True)
    return fallback


@lru_cache(maxsize=1)
def metrics_state_file_path() -> str:
    primary = os.getenv("ANALYTICS_METRICS_STATE", "storage/analytics/metrics_state.json")
    if os.path.exists(primary):
        if _can_append(primary):
            return primary
    elif _can_write_new(primary):
        return primary
    fallback = os.getenv("ANALYTICS_METRICS_STATE_FALLBACK", "/tmp/metrics_state.json")
    return fallback


def ensure_analytics_storage_dir() -> None:
    """Best-effort: create storage/analytics with permissive mode inside containers."""
    base = os.getenv("ANALYTICS_STORAGE_DIR", "storage/analytics")
    try:
        os.makedirs(base, exist_ok=True)
        os.chmod(base, 0o777)
    except OSError:
        pass
