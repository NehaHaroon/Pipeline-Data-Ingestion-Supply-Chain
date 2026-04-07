# Last Updated: 2026-04-05
# Phase 7 — Observability Plane: Telemetry collector.
# Every ingestion job emits metrics here. Without telemetry, pipelines fail silently.

import time
import threading
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class JobTelemetry:
    """
    Collects and reports metrics for a single ingestion job run.
    All times are in seconds; rates in records/sec.
    """
    job_id:   str
    source_id: str

    # Core counters
    records_ingested: int = 0
    records_failed:   int = 0
    records_quarantined: int = 0
    records_coerced:  int = 0

    # Timing
    _start_wall: float = field(default_factory=time.time, repr=False)
    _end_wall:   Optional[float] = field(default=None, repr=False)

    # Latency tracking (source event time → ingestion time)
    _latency_samples: list = field(default_factory=list, repr=False)

    # Bonus metrics
    file_count_per_partition: int = 0
    snapshot_count:           int = 0

    # ── Timing helpers ──────────────────────────────────────────────────

    def mark_start(self):
        self._start_wall = time.time()
        log.info(f"[TELEMETRY][{self.job_id}] ▶  Job started at "
                 f"{datetime.now(timezone.utc).isoformat()}")

    def mark_end(self):
        self._end_wall = time.time()
        log.info(f"[TELEMETRY][{self.job_id}] ⏹  Job ended at "
                 f"{datetime.now(timezone.utc).isoformat()} | "
                 f"Duration: {self.duration_seconds:.2f}s")

    @property
    def duration_seconds(self) -> float:
        end = self._end_wall if self._end_wall else time.time()
        return end - self._start_wall

    # ── Record tracking ─────────────────────────────────────────────────

    def record_ok(self, latency_sec: float = 0.0):
        self.records_ingested += 1
        if latency_sec:
            self._latency_samples.append(latency_sec)

    def record_fail(self):
        self.records_failed += 1

    def record_quarantine(self):
        self.records_quarantined += 1

    def record_coerce(self):
        self.records_coerced += 1

    # ── Derived metrics ─────────────────────────────────────────────────

    @property
    def throughput(self) -> float:
        """Records successfully ingested per second."""
        d = self.duration_seconds
        return self.records_ingested / d if d > 0 else 0.0

    @property
    def ingestion_latency(self) -> float:
        """Average source-to-ingestion latency in seconds."""
        return sum(self._latency_samples) / len(self._latency_samples) \
            if self._latency_samples else 0.0

    @property
    def processing_lag(self) -> float:
        """
        Difference between wall-clock 'now' and last processed record time.
        For batch jobs this equals duration; for streaming it is a live metric.
        """
        return self.duration_seconds

    # ── Reporting ────────────────────────────────────────────────────────

    def report(self) -> dict:
        r = {
            "job_id":               self.job_id,
            "source_id":            self.source_id,
            "start_time":           datetime.fromtimestamp(self._start_wall, timezone.utc).isoformat(),
            "end_time":             datetime.fromtimestamp(
                                        self._end_wall if self._end_wall else time.time(),
                                        timezone.utc).isoformat(),
            "duration_seconds":     round(self.duration_seconds, 4),
            "records_ingested":     self.records_ingested,
            "records_failed":       self.records_failed,
            "records_quarantined":  self.records_quarantined,
            "records_coerced":      self.records_coerced,
            "throughput_rec_sec":   round(self.throughput, 2),
            "avg_ingestion_latency_sec": round(self.ingestion_latency, 6),
            "processing_lag_sec":   round(self.processing_lag, 4),
            # Bonus
            "file_count_per_partition": self.file_count_per_partition,
            "snapshot_count":           self.snapshot_count,
        }
        return r

    def log_report(self):
        r = self.report()
        log.info(f"\n{'='*60}")
        log.info(f"  TELEMETRY REPORT — {r['job_id']}")
        log.info(f"{'='*60}")
        for k, v in r.items():
            log.info(f"  {k:<35} {v}")
        log.info(f"{'='*60}\n")
        return r


# ─────────────────────────────────────────────
# HEARTBEAT — logs every 5 seconds while a job runs
# ─────────────────────────────────────────────

class Heartbeat:
    """
    Background thread that emits a [HEARTBEAT] log every 5 seconds.
    Attach to any long-running job to prove the pipeline is alive.
    """
    def __init__(self, job_id: str, telemetry: JobTelemetry, interval: int = 5):
        self.job_id    = job_id
        self.telemetry = telemetry
        self.interval  = interval
        self._stop     = threading.Event()
        self._thread   = threading.Thread(target=self._beat, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=self.interval + 1)

    def _beat(self):
        while not self._stop.wait(self.interval):
            t = self.telemetry
            log.info(
                f"[HEARTBEAT][{self.job_id}] "
                f"elapsed={t.duration_seconds:.1f}s | "
                f"ingested={t.records_ingested} | "
                f"failed={t.records_failed} | "
                f"quarantined={t.records_quarantined} | "
                f"throughput={t.throughput:.1f} rec/s"
            )
