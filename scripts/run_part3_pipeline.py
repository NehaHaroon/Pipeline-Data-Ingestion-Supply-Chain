#!/usr/bin/env python3
"""
Part 3 end-to-end orchestration (local or CI):
  1. Export Iceberg → parquet for dbt
  2. Run all 8 SQL workloads
  3. Optional dbt run + test
  4. Refresh analytics metrics

Usage:
  python scripts/run_part3_pipeline.py
  python scripts/run_part3_pipeline.py --skip-dbt
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Part 3 analytics + semantic pipeline")
    parser.add_argument("--skip-dbt", action="store_true")
    parser.add_argument("--skip-sql", action="store_true")
    args = parser.parse_args()

    print("=== Part 3: Export semantic parquet ===")
    from semantic_plane.export_for_dbt import export_all

    counts = export_all()
    print(json.dumps(counts, indent=2))

    if not args.skip_sql:
        print("\n=== Part 3: Execute SQL workloads (8 queries) ===")
        from data_plane.analytics.query_executor import run_all_queries

        results = run_all_queries()
        ok = sum(1 for r in results if r.get("status") == "ok")
        blocked = sum(1 for r in results if r.get("status") == "blocked")
        err = sum(1 for r in results if r.get("status") == "error")
        print(f"Completed: ok={ok} blocked={blocked} error={err}")
        for r in results:
            print(f"  {r['query_id']}: {r['status']} rows={r.get('row_count', 0)} "
                  f"elapsed={r.get('stats', {}).get('elapsed_sec')}")

    if not args.skip_dbt:
        print("\n=== Part 3: dbt run + test ===")
        try:
            import subprocess

            dbt_dir = ROOT / "semantic_plane" / "dbt" / "supply_chain_semantic"
            for cmd in (
                [sys.executable, "-m", "dbt", "run", "--project-dir", str(dbt_dir)],
                [sys.executable, "-m", "dbt", "test", "--project-dir", str(dbt_dir)],
                [sys.executable, "-m", "dbt", "docs", "generate", "--project-dir", str(dbt_dir)],
            ):
                proc = subprocess.run(cmd, cwd=str(dbt_dir), capture_output=True, text=True)
                print(f"{' '.join(cmd[2:4])}: returncode={proc.returncode}")
                if proc.returncode != 0:
                    print(proc.stderr[-1500:])
        except Exception as exc:
            print(f"dbt skipped or failed: {exc}")
            print("Install: pip install dbt-core dbt-duckdb")

    print("\n=== Part 3: Metrics state ===")
    from observability_plane.analytics_metrics import load_metrics_state

    print(json.dumps(load_metrics_state(), indent=2))
    print("\nDone. Open Grafana http://localhost:3000 (admin/admin)")
    print("Dashboards: Supply Chain Executive BI, Analytical Workload Monitoring")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
