"""
Resolve writable telemetry directory (Docker bind mounts on Windows).
"""

from __future__ import annotations

import os
from typing import Tuple

_RESOLVED_DIR: str | None = None


def _writable_dir(path: str) -> bool:
    try:
        os.makedirs(path, exist_ok=True)
        probe = os.path.join(path, ".write_probe")
        with open(probe, "a", encoding="utf-8"):
            pass
        os.remove(probe)
        return True
    except OSError:
        return False


def get_telemetry_dir() -> str:
    global _RESOLVED_DIR
    if _RESOLVED_DIR:
        return _RESOLVED_DIR
    primary = os.getenv("TELEMETRY_DIR", os.path.join("storage", "telemetry"))
    fallback = os.getenv("TELEMETRY_DIR_FALLBACK", "/tmp/telemetry")
    for candidate in (primary, fallback):
        if _writable_dir(candidate):
            _RESOLVED_DIR = candidate
            return candidate
    _RESOLVED_DIR = fallback
    os.makedirs(fallback, exist_ok=True)
    return fallback


def telemetry_paths() -> Tuple[str, str, str]:
    """Return (directory, jsonl log path, summary json path)."""
    base = get_telemetry_dir()
    return (
        base,
        os.path.join(base, "telemetry_records.jsonl"),
        os.path.join(base, "telemetry_summary.json"),
    )
