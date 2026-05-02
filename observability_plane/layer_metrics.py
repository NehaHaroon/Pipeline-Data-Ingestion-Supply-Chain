from typing import Any, Dict, List

from observability_plane.telemetry import JobTelemetry


def build_ingestion_layer_summary(jobs_db: Dict[str, Any]) -> Dict[str, Any]:
    telemetry_records = JobTelemetry.load_reports()
    running_jobs = sum(1 for j in jobs_db.values() if j.get("status") == "running")
    completed_jobs = sum(1 for j in jobs_db.values() if j.get("status") == "completed")
    failed_jobs = sum(1 for j in jobs_db.values() if j.get("status") == "failed")

    total_ingested = sum(r.get("records_ingested", 0) for r in telemetry_records)
    total_quarantined = sum(r.get("records_quarantined", 0) for r in telemetry_records)
    total_failed = sum(r.get("records_failed", 0) for r in telemetry_records)

    throughputs = [r.get("throughput_rec_sec", 0.0) for r in telemetry_records if r.get("throughput_rec_sec")]
    avg_throughput = (sum(throughputs) / len(throughputs)) if throughputs else 0.0

    return {
        "layer": "ingestion",
        "jobs_running": running_jobs,
        "jobs_completed": completed_jobs,
        "jobs_failed": failed_jobs,
        "records_ingested": total_ingested,
        "records_quarantined": total_quarantined,
        "records_failed": total_failed,
        "avg_throughput_rec_sec": round(avg_throughput, 3),
    }


def build_transformation_layer_summary() -> Dict[str, Any]:
    from data_plane.transformation.transformation_kpis import TransformationKPILogger

    silver = TransformationKPILogger.get_aggregate_stats("silver")
    gold = TransformationKPILogger.get_aggregate_stats("gold")

    return {
        "layer": "transformation",
        "silver_runs": silver.get("run_count", 0),
        "gold_runs": gold.get("run_count", 0),
        "silver_records_cleaned": silver.get("total_records_cleaned", 0),
        "gold_records_processed": gold.get("total_records_processed", 0),
        "silver_avg_latency_sec": round(silver.get("avg_transformation_latency_sec", 0.0), 3),
        "gold_avg_latency_sec": round(gold.get("avg_transformation_latency_sec", 0.0), 3),
        "silver_quality_ratio": round(silver.get("overall_quality_ratio", 0.0), 4),
        "gold_quality_ratio": round(gold.get("overall_quality_ratio", 0.0), 4),
    }


def build_storage_layer_summary() -> Dict[str, Any]:
    from storage_plane.storage_kpis import compute_storage_health, get_all_tables_kpis

    all_kpis = get_all_tables_kpis()
    health = compute_storage_health(all_kpis)
    valid_kpis = [k for k in all_kpis.values() if isinstance(k, dict) and not k.get("error")]

    total_files = sum(k.get("file_count", 0) for k in valid_kpis)
    total_storage_mb = sum(k.get("total_storage_mb", 0.0) for k in valid_kpis)
    total_snapshots = sum(k.get("snapshot_count", 0) for k in valid_kpis)
    needs_compaction = sum(1 for k in valid_kpis if k.get("needs_compaction"))

    return {
        "layer": "storage",
        "table_count": len(valid_kpis),
        "total_files": total_files,
        "total_storage_mb": round(total_storage_mb, 3),
        "total_snapshots": total_snapshots,
        "tables_needing_compaction": needs_compaction,
        "health_status": health.get("health_status", "unknown"),
    }


def build_serving_layer_summary(jobs_db: Dict[str, Any]) -> Dict[str, Any]:
    total_jobs = len(jobs_db)
    completed_jobs = sum(1 for j in jobs_db.values() if j.get("status") == "completed")
    success_ratio = (completed_jobs / total_jobs) if total_jobs else 1.0
    return {
        "layer": "serving",
        "api_health": 1,
        "active_jobs_in_memory": total_jobs,
        "job_success_ratio": round(success_ratio, 4),
    }


def build_layer_summaries(jobs_db: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        "ingestion": build_ingestion_layer_summary(jobs_db),
        "transformation": build_transformation_layer_summary(),
        "storage": build_storage_layer_summary(),
        "serving": build_serving_layer_summary(jobs_db),
    }


def layer_summaries_to_prometheus(layer_summaries: Dict[str, Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append("# HELP pipeline_layer_metric Generic layer metric")
    lines.append("# TYPE pipeline_layer_metric gauge")

    for layer_name, metrics in layer_summaries.items():
        for metric_name, value in metrics.items():
            if metric_name == "layer":
                continue
            if isinstance(value, str):
                value = 1 if value in ("healthy", "good", "ok") else 0
            lines.append(f'pipeline_layer_metric{{layer="{layer_name}",metric="{metric_name}"}} {float(value)}')

    return "\n".join(lines) + "\n"
