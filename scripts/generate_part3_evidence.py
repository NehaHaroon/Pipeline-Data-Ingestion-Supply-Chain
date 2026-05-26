#!/usr/bin/env python3
"""Write SQL workload execution evidence for Part 3 report appendix."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OUT = ROOT / "storage" / "analytics" / "workload_execution_report.json"


def main() -> int:
    try:
        import duckdb  # noqa: F401
    except ImportError:
        print(
            "Missing dependency: duckdb\n"
            "Install Part 3 extras:\n"
            "  pip install -r requirements-part3-extras.txt\n"
            "Or run evidence inside Docker:\n"
            "  docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-export"
        )
        return 1

    from data_plane.analytics.query_executor import run_all_queries

    results = run_all_queries()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "executed": len(results),
        "ok": sum(1 for r in results if r.get("status") == "ok"),
        "blocked": sum(1 for r in results if r.get("status") == "blocked"),
        "error": sum(1 for r in results if r.get("status") == "error"),
        "queries": [
            {
                "query_id": r["query_id"],
                "name": r.get("name"),
                "workload_type": r.get("workload_type"),
                "complexity": r.get("complexity"),
                "status": r.get("status"),
                "row_count": r.get("row_count"),
                "elapsed_sec": (r.get("stats") or {}).get("elapsed_sec"),
                "characteristics": r.get("characteristics"),
                "optimization_plan": r.get("optimization_plan"),
            }
            for r in results
        ],
    }
    OUT.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
