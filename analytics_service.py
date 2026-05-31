"""
Part 3 — Analytics & semantic serving API (port 8002).

Exposes governance, contracts, SQL workloads, BI metrics, and Prometheus scrape endpoint.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

from control_plane.access_control import AnalyticsRole, resolve_role

from control_plane.advanced_contracts import contracts_catalog
from control_plane.governance_policies import policies_to_report_dict
from data_plane.analytics.query_executor import catalog_response, run_all_queries, run_query
from observability_plane.analytics_metrics import (
    QueryConcurrencyGuard,
    analytics_metrics_to_prometheus,
    load_metrics_state,
    top_expensive_queries,
    update_semantic_metrics,
)
from observability_plane.structured_logging import get_logger, log_pipeline_event

app = FastAPI(title="Supply Chain Analytics Service", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
log = get_logger("analytics_service")


@app.on_event("startup")
def _startup_seed_metrics() -> None:
    import threading

    from observability_plane.analytics_metrics import (
        refresh_storage_and_bi_metrics,
        seed_prometheus_metrics,
    )

    try:
        seed_prometheus_metrics()
        log_pipeline_event(log, "info", "Seeded default Prometheus metrics (fast)")
    except Exception as exc:
        log_pipeline_event(log, "warning", "Metric seed skipped", error=str(exc))

    def _refresh_bi_background() -> None:
        try:
            refresh_storage_and_bi_metrics(force=True, skip_resource=True)
            log_pipeline_event(log, "info", "Background executive BI metrics refresh completed")
        except Exception as exc:
            log_pipeline_event(
                log,
                "warning",
                "Background BI refresh failed — Grafana may show zeros until run-all",
                error=str(exc),
            )

    threading.Thread(target=_refresh_bi_background, daemon=True, name="bi-metrics-refresh").start()


PROJECT_ROOT = Path(__file__).resolve().parent
DBT_PROJECT = PROJECT_ROOT / "semantic_plane" / "dbt" / "supply_chain_semantic"
_DASHBOARD_HTML = PROJECT_ROOT / "data_plane" / "analytics" / "analytics_dashboard.html"


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")


@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def analytics_dashboard():
    """
    Part 3 web UI — consumes this service's REST APIs in the browser.
    Grafana (port 3000) is the separate BI/workload monitoring front-end via Prometheus.
    """
    if not _DASHBOARD_HTML.exists():
        raise HTTPException(status_code=503, detail="analytics_dashboard.html missing")
    return HTMLResponse(_DASHBOARD_HTML.read_text(encoding="utf-8"))


@app.get("/health")
def health():
    return {"status": "ok", "service": "analytics-service"}


@app.get("/governance/policies")
def governance_policies():
    return policies_to_report_dict()


@app.get("/contracts/enterprise")
def enterprise_contracts():
    return contracts_catalog()


@app.get("/analytics/catalog")
def analytics_catalog():
    return catalog_response()


@app.get("/semantic/bi-kpis")
def bi_kpis():
    from data_plane.analytics.bi_kpis import compute_bi_kpis
    from observability_plane.analytics_metrics import refresh_storage_and_bi_metrics

    data = compute_bi_kpis()
    refresh_storage_and_bi_metrics()
    return data


@app.get("/semantic/executive-dashboard")
def executive_dashboard():
    """
    Full executive payload: scorecards, top-10 priorities, monthly trend, warehouse table.
    Use for :8002/dashboard charts and stakeholder reports.
    """
    from data_plane.analytics.executive_dashboard import persist_executive_dashboard
    from observability_plane.analytics_metrics import refresh_storage_and_bi_metrics

    payload = persist_executive_dashboard()
    refresh_storage_and_bi_metrics()
    return payload


@app.post("/analytics/query/{query_id}")
def execute_workload_query(
    query_id: str,
    role: AnalyticsRole = Depends(resolve_role),
):
    with QueryConcurrencyGuard():
        try:
            log_pipeline_event(log, "info", "Executing SQL workload", query_id=query_id, role=role.value)
            return run_query(query_id, role=role.value)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            log_pipeline_event(log, "error", "Query failed", query_id=query_id, error=str(exc))
            raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/analytics/run-all")
def execute_all_workloads():
    import threading

    with QueryConcurrencyGuard():
        results = run_all_queries(include_rows=False)
        failed = [r for r in results if r.get("status") not in ("ok", "blocked")]
        if failed:
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "One or more SQL workloads failed",
                    "failed": [
                        {
                            "query_id": r.get("query_id"),
                            "status": r.get("status"),
                            "error": r.get("error"),
                        }
                        for r in failed
                    ],
                },
            )

        def _refresh_bi_background() -> None:
            from observability_plane.analytics_metrics import refresh_storage_and_bi_metrics

            try:
                refresh_storage_and_bi_metrics(force=True, skip_resource=True)
                log_pipeline_event(log, "info", "Post run-all BI metrics refresh completed")
            except Exception as exc:
                log_pipeline_event(
                    log,
                    "warning",
                    "Post run-all BI refresh failed",
                    error=str(exc),
                )

        threading.Thread(target=_refresh_bi_background, daemon=True, name="run-all-bi-refresh").start()
        return {
            "executed": len(results),
            "ok": sum(1 for r in results if r.get("status") == "ok"),
            "blocked": sum(1 for r in results if r.get("status") == "blocked"),
            "bi_refresh": "scheduled",
            "results": results,
        }


@app.post("/semantic/refresh-bi-metrics")
def refresh_bi_metrics_only():
    """
    Lightweight Grafana refresh — skips SQL workloads.
    Use this instead of /analytics/run-all when dashboards need updated Prometheus KPIs.
    """
    import threading
    from observability_plane.analytics_metrics import refresh_storage_and_bi_metrics

    def _work() -> None:
        try:
            refresh_storage_and_bi_metrics(force=True, skip_resource=True)
            log_pipeline_event(log, "info", "BI metrics-only refresh completed")
        except Exception as exc:
            log_pipeline_event(log, "warning", "BI metrics-only refresh failed", error=str(exc))

    threading.Thread(target=_work, daemon=True, name="bi-metrics-only-refresh").start()
    return {"status": "accepted", "message": "BI metrics refresh started in background"}


@app.get("/analytics/metrics")
def analytics_metrics():
    state = load_metrics_state()
    state["top_expensive_queries"] = top_expensive_queries()
    return state


@app.get("/metrics/prometheus")
def prometheus_metrics():
    return PlainTextResponse(analytics_metrics_to_prometheus())


@app.get("/semantic/prometheus-bi-catalog")
def prometheus_bi_catalog():
    """List BI metric names currently exported to Prometheus (debug Grafana 'No data')."""
    from observability_plane.analytics_metrics import current_bi_kpis

    bi = current_bi_kpis()
    return {"metrics": sorted(bi.keys()), "count": len(bi)}


@app.post("/semantic/dbt/run")
def run_dbt(
    select: str = Query(default="marts", description="dbt node selection"),
):
    """Run dbt models (requires dbt-core + dbt-duckdb installed)."""
    if not DBT_PROJECT.exists():
        raise HTTPException(status_code=503, detail=f"dbt project not found: {DBT_PROJECT}")

    env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_PROJECT)}
    t0 = __import__("time").perf_counter()
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "dbt", "run", "--project-dir", str(DBT_PROJECT), "--select", select],
            capture_output=True,
            text=True,
            timeout=int(os.getenv("DBT_RUN_TIMEOUT_SEC", "600")),
            cwd=str(DBT_PROJECT),
            env=env,
        )
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="dbt not installed. pip install dbt-core dbt-duckdb",
        ) from None
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="dbt run timed out")

    elapsed = __import__("time").perf_counter() - t0
    failed = 1 if proc.returncode != 0 else 0
    update_semantic_metrics(dbt_runtime_sec=elapsed, failed_tests=failed)

    return {
        "returncode": proc.returncode,
        "elapsed_sec": round(elapsed, 2),
        "stdout_tail": (proc.stdout or "")[-4000:],
        "stderr_tail": (proc.stderr or "")[-2000:],
    }


@app.post("/semantic/dbt/test")
def test_dbt():
    if not DBT_PROJECT.exists():
        raise HTTPException(status_code=503, detail="dbt project missing")
    proc = subprocess.run(
        [sys.executable, "-m", "dbt", "test", "--project-dir", str(DBT_PROJECT)],
        capture_output=True,
        text=True,
        timeout=300,
        cwd=str(DBT_PROJECT),
    )
    failed = 0 if proc.returncode == 0 else 1
    update_semantic_metrics(failed_tests=failed)
    return {"returncode": proc.returncode, "stderr_tail": (proc.stderr or "")[-2000:]}


@app.get("/semantic/metrics")
def semantic_metrics():
    """Semantic layer KPIs for BI dashboards."""
    from semantic_plane.metric_catalog import METRIC_CATALOG

    return {"metrics": METRIC_CATALOG, "observability": load_metrics_state().get("semantic_layer", {})}
