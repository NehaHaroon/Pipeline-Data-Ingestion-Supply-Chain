#!/usr/bin/env python3
"""
Phase 3 complement — compare synthetic generator output to reference CSV using
discrete KL divergence on categorical marginals (fast, report-friendly).

Usage (from repo root):
  python scripts/generator_distribution_check.py \\
      --reference storage/raw/warehouse_master.csv \\
      --column color \\
      --samples 5000

Requires: pandas, numpy
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def kl_divergence_discrete(p: np.ndarray, q: np.ndarray, eps: float = 1e-12) -> float:
    """KL(P || Q) for aligned discrete distributions."""
    p = np.clip(p, eps, 1.0)
    q = np.clip(q, eps, 1.0)
    p = p / p.sum()
    q = q / q.sum()
    return float(np.sum(p * np.log(p / q)))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))

    ap = argparse.ArgumentParser(description="KL check: reference CSV vs synthetic generator")
    ap.add_argument("--reference", required=True, help="Path to reference CSV")
    ap.add_argument("--column", required=True, help="Column name for marginal comparison")
    ap.add_argument("--samples", type=int, default=3000, help="Synthetic rows to draw")
    ap.add_argument("--source-id", default="src_warehouse_master", help="Generator source_id")
    args = ap.parse_args()

    ref_path = Path(args.reference)
    if not ref_path.is_file():
        print(f"Reference not found: {ref_path}", file=sys.stderr)
        return 1

    ref = pd.read_csv(ref_path)
    if args.column not in ref.columns:
        print(f"Column {args.column!r} not in reference", file=sys.stderr)
        return 1

    ref_counts = ref[args.column].astype(str).value_counts(normalize=True)

    from data_plane.generators import source_generators as sg

    gen_map = {
        "src_warehouse_master": sg.WarehouseMasterGenerator,
        "src_manufacturing_logs": sg.ManufacturingLogsGenerator,
        "src_sales_history": sg.SalesHistoryGenerator,
        "src_legacy_trends": sg.LegacyTrendsGenerator,
    }
    cls = gen_map.get(args.source_id)
    if cls is None:
        print(f"No mapped generator for {args.source_id}", file=sys.stderr)
        return 1

    gen = cls()
    synth_rows = gen.generate(args.samples)
    synth = pd.DataFrame(synth_rows)
    if args.column not in synth.columns:
        print(f"Column {args.column!r} not in synthetic output", file=sys.stderr)
        return 1

    syn_counts = synth[args.column].astype(str).value_counts(normalize=True)
    all_vals = sorted(set(ref_counts.index) | set(syn_counts.index))
    p = np.array([ref_counts.get(v, 0.0) for v in all_vals])
    q = np.array([syn_counts.get(v, 0.0) for v in all_vals])
    kl = kl_divergence_discrete(p, q)

    print("reference_rows", len(ref))
    print("synthetic_rows", len(synth))
    print("column", args.column)
    print("kl_divergence_discrete", round(kl, 6))
    print("interpretation", "lower is closer (0 = identical marginals)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
