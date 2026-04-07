# Last Updated: 2026-04-05
# Phase 3 — Base generator with distribution profiling and synthetic generation.
# All source-specific generators inherit from this class.

import time
import logging
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


class BaseGenerator(ABC):
    """
    Abstract base for all data generators.

    Workflow:
      1. profile(df)  — learn statistical distribution from real data
      2. generate(n)  — produce n synthetic records matching that distribution
      3. upsample(n)  — alias for generate used when n > original row count

    Each generator captures:
      - Categorical columns → frequency distribution → numpy.random.choice
      - Numeric columns     → KDE (Gaussian kernel) or parametric fit
      - Timestamp columns   → min/max range → uniform random in range
    """

    def __init__(self, source_id: str):
        self.source_id  = source_id
        self._profiles: Dict[str, Any] = {}   # col_name → fitted profile
        self._col_types: Dict[str, str] = {}  # col_name → "categorical"|"numeric"|"timestamp"
        self._original_size = 0

    # ─── Abstract interface ───────────────────────────────────────────────

    @abstractmethod
    def _get_raw_path(self) -> str:
        """Return path to the raw CSV for this source."""
        ...

    @abstractmethod
    def _post_process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply source-specific business rules after generic generation."""
        ...

    # ─── Distribution profiling ───────────────────────────────────────────

    def profile(self, df: Optional[pd.DataFrame] = None) -> None:
        """
        Learn the statistical fingerprint of the dataset.
        If df is None, loads from the raw CSV path.
        """
        if df is None:
            df = pd.read_csv(self._get_raw_path())
        self._original_size = len(df)
        log.info(f"[GENERATOR:{self.source_id}] Profiling {len(df)} rows × {len(df.columns)} cols")

        for col in df.columns:
            series = df[col].dropna()
            if len(series) == 0:
                self._profiles[col] = {"type": "constant", "value": None}
                self._col_types[col] = "constant"
                continue

            # Detect column type
            if pd.api.types.is_datetime64_any_dtype(series) or self._looks_like_timestamp(col, series):
                self._profile_timestamp(col, series)
            elif pd.api.types.is_numeric_dtype(series) and series.nunique() > 20:
                self._profile_numeric(col, series)
            else:
                self._profile_categorical(col, series, df[col])

        log.info(f"[GENERATOR:{self.source_id}] Profiling complete. "
                 f"Columns: {list(self._col_types.items())}")

    def _looks_like_timestamp(self, col: str, series: pd.Series) -> bool:
        return "timestamp" in col.lower() or "date" in col.lower() or "period" in col.lower()

    def _profile_categorical(self, col: str, series: pd.Series, full: pd.Series):
        """Capture value frequencies + null rate."""
        null_rate = full.isna().mean()
        counts    = series.value_counts(normalize=True)
        self._profiles[col] = {
            "type":      "categorical",
            "values":    counts.index.tolist(),
            "probs":     counts.values.tolist(),
            "null_rate": null_rate,
        }
        self._col_types[col] = "categorical"

    def _profile_numeric(self, col: str, series: pd.Series):
        """Fit mean + std (Gaussian) for numeric columns."""
        null_rate = 1 - len(series) / (len(series) + series.isna().sum())
        self._profiles[col] = {
            "type":      "numeric",
            "mean":      float(series.mean()),
            "std":       float(series.std()),
            "min":       float(series.min()),
            "max":       float(series.max()),
            "null_rate": null_rate,
        }
        self._col_types[col] = "numeric"

    def _profile_timestamp(self, col: str, series: pd.Series):
        """Capture timestamp range for uniform sampling."""
        try:
            parsed = pd.to_datetime(series, errors="coerce").dropna()
            self._profiles[col] = {
                "type": "timestamp",
                "min":  parsed.min().timestamp(),
                "max":  parsed.max().timestamp(),
            }
            self._col_types[col] = "timestamp"
        except Exception:
            self._profile_categorical(col, series, series)

    # ─── Synthetic generation ─────────────────────────────────────────────

    def generate(self, n: int) -> List[Dict[str, Any]]:
        """
        Generate n synthetic records using fitted distributions.
        Calls _post_process on each record for source-specific rules.
        """
        if not self._profiles:
            raise RuntimeError("Call profile() before generate().")

        records = []
        for _ in range(n):
            record = {}
            for col, profile in self._profiles.items():
                record[col] = self._sample(profile)
            record = self._post_process(record)
            records.append(record)
        return records

    def upsample(self, n: int) -> List[Dict[str, Any]]:
        """Alias for generate — used when n > original dataset size."""
        log.info(f"[GENERATOR:{self.source_id}] Upsampling {n} records "
                 f"(original size = {self._original_size})")
        return self.generate(n)

    def _sample(self, profile: Dict[str, Any]) -> Any:
        """Draw one sample from a fitted profile."""
        ptype = profile["type"]

        if ptype == "constant":
            return profile["value"]

        if ptype == "categorical":
            if profile["null_rate"] > 0 and np.random.random() < profile["null_rate"]:
                return None
            # Normalize probabilities to handle floating-point precision issues
            probs = np.array(profile["probs"])
            probs = probs / probs.sum()  # Ensure sum = 1.0
            return np.random.choice(profile["values"], p=probs)

        if ptype == "numeric":
            if profile["null_rate"] > 0 and np.random.random() < profile["null_rate"]:
                return None
            val = np.random.normal(profile["mean"], profile["std"])
            val = float(np.clip(val, profile["min"], profile["max"]))
            return val

        if ptype == "timestamp":
            ts = np.random.uniform(profile["min"], profile["max"])
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

        return None

    # ─── Timing helpers ────────────────────────────────────────────────────

    @staticmethod
    def timed_run(fn, *args, label="run", **kwargs):
        """Wrap any callable with start/end/duration logging."""
        start = time.time()
        start_str = datetime.now(timezone.utc).isoformat()
        log.info(f"[TIMER] ▶  {label} started at {start_str}")
        result = fn(*args, **kwargs)
        end   = time.time()
        end_str = datetime.now(timezone.utc).isoformat()
        duration = end - start
        log.info(f"[TIMER] ⏹  {label} ended at {end_str} | duration={duration:.4f}s")
        return result, duration
