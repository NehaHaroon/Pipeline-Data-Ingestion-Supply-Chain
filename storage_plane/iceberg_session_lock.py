"""
Serialize mutating access to the PyIceberg SqlCatalog (SQLite metadata DB).

Concurrent Airflow tasks + ingestion API + compaction all touch the same SQLite file.
Use ``iceberg_catalog_session()`` around catalog mutations (append/overwrite/create/drop/
rewrite). Transformation logic (pandas) runs *outside* this lock where possible so tasks
queue only for metadata writes, not for CPU work.

Environment:
  ICEBERG_CATALOG_LOCK_TIMEOUT — seconds to wait for the lock once (default 900).
  ICEBERG_CATALOG_IO_RETRIES — retries for transient SQLite busy inside a mutation (default 12).
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Callable, Iterator, TypeVar

from config import STORAGE_ICEBERG_WAREHOUSE

T = TypeVar("T")

_DEFAULT_LOCK_WAIT_SEC = float(os.getenv("ICEBERG_CATALOG_LOCK_TIMEOUT", "900"))
_DEFAULT_IO_RETRIES = int(os.getenv("ICEBERG_CATALOG_IO_RETRIES", "12"))
_DEFAULT_BACKOFF_CAP = 45.0


def _lock_file_path() -> str:
    base = os.path.dirname(os.path.abspath(STORAGE_ICEBERG_WAREHOUSE))
    return os.path.join(base, ".iceberg_pyiceberg_catalog.lock")


def _transient_catalog_exception(exc: BaseException) -> bool:
    import sqlite3

    if isinstance(exc, sqlite3.OperationalError):
        return True
    msg = str(exc).lower()
    markers = (
        "locked",
        "busy",
        "database is locked",
        "unable to open database",
        "disk i/o error",
    )
    return any(m in msg for m in markers)


def retry_catalog_mutation(fn: Callable[[], T], max_attempts: int | None = None) -> T:
    """
    Retry a catalog mutation (overwrite/append/create/drop) on transient SQLite contention.
    Call inside ``iceberg_catalog_session`` for reserved catalog access.
    """
    attempts = max_attempts if max_attempts is not None else _DEFAULT_IO_RETRIES
    last: BaseException | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as e:
            last = e
            if not _transient_catalog_exception(e) or attempt == attempts - 1:
                raise
            delay = min(_DEFAULT_BACKOFF_CAP, 0.25 * (2**attempt))
            time.sleep(delay)
    assert last is not None
    raise last


@contextmanager
def iceberg_catalog_session(lock_timeout_sec: float | None = None) -> Iterator[None]:
    """
    Acquire exclusive file lock before SQLite-backed catalog mutations.

    Uses a single long ``acquire(timeout=…)`` so queued jobs wait for the writer instead of
    failing immediately. Tune ICEBERG_CATALOG_LOCK_TIMEOUT if Airflow HTTP timeouts occur.
    """
    path = _lock_file_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    try:
        from filelock import FileLock, Timeout as FileLockTimeout
    except ImportError:
        yield
        return

    wait = float(lock_timeout_sec if lock_timeout_sec is not None else _DEFAULT_LOCK_WAIT_SEC)
    lock = FileLock(path)
    acquired = False
    try:
        try:
            lock.acquire(timeout=wait)
        except FileLockTimeout as e:
            raise RuntimeError(
                f"Iceberg catalog busy: exclusive lock not acquired within {wait}s ({path}). "
                f"Another task may be writing to the SQLite catalog. Increase "
                f"ICEBERG_CATALOG_LOCK_TIMEOUT or TRANSFORM_POST_TIMEOUT_SECONDS, or use Postgres/etc. "
                f"for the Iceberg catalog."
            ) from e
        acquired = True
        yield
    finally:
        if acquired:
            try:
                lock.release()
            except Exception:
                pass
