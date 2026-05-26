"""
Part 3 — Task 1: Data governance policies for the supply-chain lakehouse.

Each category defines two enforceable policies with business rationale.
Policies are referenced by analytics workload routing, retention jobs, and access control.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class PolicyCategory(str, Enum):
    DATA_ACCESS = "data_access"
    RETENTION = "retention"
    COST_GOVERNANCE = "cost_governance"
    WORKLOAD_ISOLATION = "workload_isolation"
    SCHEMA_EVOLUTION = "schema_evolution"
    DATA_QUALITY_SLA = "data_quality_sla"


@dataclass(frozen=True)
class GovernancePolicy:
    policy_id: str
    category: PolicyCategory
    name: str
    description: str
    rules: List[str]
    business_value: str
    operational_impact: str
    tradeoffs: str
    enforcement_hook: Optional[str] = None  # module or API that applies the policy


# ── Data Access ──────────────────────────────────────────────────────────────

DATA_ACCESS_POLICIES = [
    GovernancePolicy(
        policy_id="gov_access_001",
        category=PolicyCategory.DATA_ACCESS,
        name="Role-based lakehouse access",
        description=(
            "Finance and external auditors may query Gold aggregates only; "
            "PII-bearing Bronze fields (created_by, store_id) are masked at Silver."
        ),
        rules=[
            "role=finance → SELECT gold.*, silver.aggregates only",
            "role=analyst → SELECT silver.*, gold.* (no bronze PII columns)",
            "role=ops_streaming → SELECT bronze.* WHERE event_age < 24h",
            "role=admin → full catalog with audit logging",
        ],
        business_value="Protects customer and employee PII while enabling self-serve analytics.",
        operational_impact="Requires view-level grants in DuckDB/Trino and API token scopes.",
        tradeoffs="Stricter roles slow ad-hoc exploration; mitigated by pre-built semantic models.",
        enforcement_hook="analytics_service.verify_workload_access",
    ),
    GovernancePolicy(
        policy_id="gov_access_002",
        category=PolicyCategory.DATA_ACCESS,
        name="Streaming operational window",
        description=(
            "Real-time IoT and CDC Bronze tables are accessible to operations dashboards "
            "for 24 hours only; historical analysis must use Silver/Gold."
        ),
        rules=[
            "bronze.src_iot_rfid_stream retention for ops role = 24h",
            "bronze.src_inventory_transactions ops access requires late_arriving flag review",
            "cross-layer joins from Bronze blocked for BI roles",
        ],
        business_value="Reduces exposure of noisy raw events and limits blast radius of bad payloads.",
        operational_impact="Ops dashboards must refresh from Silver after T+24h for trends.",
        tradeoffs="24h window may miss forensic needs; quarantine store retains rejects 90d.",
        enforcement_hook="control_plane.governance_policies.check_bronze_access_window",
    ),
]

# ── Retention ────────────────────────────────────────────────────────────────

RETENTION_POLICIES = [
    GovernancePolicy(
        policy_id="gov_retention_001",
        category=PolicyCategory.RETENTION,
        name="Medallion retention tiers",
        description="Bronze 30d, Silver 365d, Gold indefinite with snapshot expiry on Bronze only.",
        rules=[
            "bronze.* snapshot expire → 30 days",
            "silver.* snapshot expire → 365 days",
            "gold.* retain all versions; compaction weekly",
            "quarantine/* retain 90 days for RCA",
        ],
        business_value="Balances storage cost with regulatory and trend analysis needs.",
        operational_impact="Compaction DAG must run before Bronze expiry to avoid orphan files.",
        tradeoffs="Shorter Bronze window complicates late CDC replay; mitigated by Silver CDC ordering.",
        enforcement_hook="storage_plane.compaction.apply_retention_policy",
    ),
    GovernancePolicy(
        policy_id="gov_retention_002",
        category=PolicyCategory.RETENTION,
        name="Telemetry and KPI log retention",
        description=(
            "Transformation KPI JSONL and query audit logs kept 180 days; "
            "Prometheus raw metrics 15 days, aggregated recording rules 1 year."
        ),
        rules=[
            "storage/ingested/detail_logs/*.jsonl → 180d",
            "storage/analytics/query_audit.jsonl → 180d",
            "prometheus TSDB → 15d with remote_write optional",
        ],
        business_value="Supports semester-long project evaluation and incident timelines.",
        operational_impact="Disk monitoring on storage/ path required in docker-compose volumes.",
        tradeoffs="Longer retention increases disk; sampled metrics reduce cardinality.",
        enforcement_hook="observability_plane.analytics_metrics.rotate_audit_log",
    ),
]

# ── Cost Governance ────────────────────────────────────────────────────────────

COST_GOVERNANCE_POLICIES = [
    GovernancePolicy(
        policy_id="gov_cost_001",
        category=PolicyCategory.COST_GOVERNANCE,
        name="Business-hours query guardrails",
        description=(
            "Cross-source joins >3 tables or estimated scan >500MB blocked 09:00–18:00 PKT "
            "unless workload_class=executive_bi."
        ),
        rules=[
            "09:00-18:00 PKT: ad_hoc joins max 3 tables",
            "query estimated_bytes > 500MB → require materialized mart",
            "streaming compaction: max 1 full rewrite per source per hour",
        ],
        business_value="Prevents analyst queries from starving CDC and transformation slots.",
        operational_impact="Analytics API returns 429 with suggested mart alternative.",
        tradeoffs="Executives may see delayed ad-hoc answers; pre-aggregated marts offset this.",
        enforcement_hook="data_plane.analytics.query_executor.enforce_cost_policy",
    ),
    GovernancePolicy(
        policy_id="gov_cost_002",
        category=PolicyCategory.COST_GOVERNANCE,
        name="Auto-terminate runaway queries",
        description="Queries exceeding 120s wall time or 2GB scanned auto-cancelled; logged to audit.",
        rules=[
            "max_wall_time_sec=120 for ad_hoc",
            "max_bytes_scanned=2GB for all workloads except strategic (4GB)",
            "failed OOM → alert observability_plane",
        ],
        business_value="Protects shared DuckDB/Iceberg catalog from single bad SQL.",
        operational_impact="Users receive query_id and partial stats in audit log.",
        tradeoffs="Large strategic rollups may need pre-scheduled batch; Airflow semantic DAG handles this.",
        enforcement_hook="data_plane.analytics.query_executor",
    ),
]

# ── Workload Isolation ─────────────────────────────────────────────────────────

WORKLOAD_ISOLATION_POLICIES = [
    GovernancePolicy(
        policy_id="gov_isolation_001",
        category=PolicyCategory.WORKLOAD_ISOLATION,
        name="Plane resource pools",
        description=(
            "Separate logical pools: cdc_ingestion (postgres+debezium), "
            "transformation (transform-service), bi_serving (analytics-service), ad_hoc_sql."
        ),
        rules=[
            "cdc_ingestion: dedicated kafka-connect + cdc-consumer containers",
            "transformation: transform-service + Airflow transformation DAG",
            "bi_serving: analytics-service port 8002 + Grafana",
            "ad_hoc_sql: DuckDB in-process with concurrency limit 2",
        ],
        business_value="Noisy neighbor isolation keeps replenishment SLA <5 min fresh.",
        operational_impact="docker-compose service boundaries; scale analytics replicas independently.",
        tradeoffs="More containers increase ops surface; acceptable for production demo stack.",
        enforcement_hook="docker-compose service labels",
    ),
    GovernancePolicy(
        policy_id="gov_isolation_002",
        category=PolicyCategory.WORKLOAD_ISOLATION,
        name="Semantic layer build isolation",
        description="dbt runs only after Gold success; never concurrent with Silver full rewrites.",
        rules=[
            "semantic_dag trigger: gold_task success",
            "dbt threads=2 max during business hours",
            "dbt full-refresh disallowed on marts.fct_replenishment_signals",
        ],
        business_value="Prevents BI cache from half-updated dimensions mid-transformation.",
        operational_impact="Airflow semantic_dag depends on transformation_dag external gate.",
        tradeoffs="Slightly stale semantic models during incident replay; incremental dbt preferred.",
        enforcement_hook="airflow/dags/semantic_dag.py",
    ),
]

# ── Schema Evolution ───────────────────────────────────────────────────────────

SCHEMA_EVOLUTION_POLICIES = [
    GovernancePolicy(
        policy_id="gov_schema_001",
        category=PolicyCategory.SCHEMA_EVOLUTION,
        name="Additive-first Iceberg evolution",
        description="New columns allowed as optional; type changes require contract version bump.",
        rules=[
            "additive column → auto append to Iceberg schema",
            "breaking type change → contract version v{n+1} + new table suffix",
            "dbt staging models must pass before mart deploy",
        ],
        business_value="Sources evolve without breaking dashboards tied to semantic layer.",
        operational_impact="Silver re-validates via CONTRACT_REGISTRY on every run.",
        tradeoffs="Multiple contract versions increase storage; old versions retired per retention.",
        enforcement_hook="control_plane.contracts.DataContract.version",
    ),
    GovernancePolicy(
        policy_id="gov_schema_002",
        category=PolicyCategory.SCHEMA_EVOLUTION,
        name="dbt-gated deployment",
        description="All semantic model changes require uniqueness, not_null, and accepted_values tests green.",
        rules=[
            "dbt test failure → block BI dashboard refresh",
            "exposures.yml documents dashboard lineage",
            "breaking rename requires deprecation period 7d in schema.yml",
        ],
        business_value="Semantic consistency across Grafana and future Power BI connectors.",
        operational_impact="CI step: dbt test && dbt docs generate before merge.",
        tradeoffs="Slower iteration for analysts; staging models absorb experimentation.",
        enforcement_hook="semantic_plane/dbt/tests",
    ),
]

# ── Data Quality SLAs ──────────────────────────────────────────────────────────

DATA_QUALITY_SLA_POLICIES = [
    GovernancePolicy(
        policy_id="gov_sla_001",
        category=PolicyCategory.DATA_QUALITY_SLA,
        name="Null and completeness SLA",
        description="Critical keys null rate <2%; quarantine rate <5% per source per hour.",
        rules=[
            "PK null% < 2% on silver layer",
            "quarantine rate < 5% rolling 1h",
            "contract violation → alert if >10 records/hour",
        ],
        business_value="Finance and ops KPIs remain trustworthy for replenishment decisions.",
        operational_impact="Transformation KPI JSONL feeds Grafana null% panels.",
        tradeoffs="Strict null SLA may reject valid sparse legacy data; legacy uses AUTO_COERCE.",
        enforcement_hook="data_plane.transformation.transformation_kpis",
    ),
    GovernancePolicy(
        policy_id="gov_sla_002",
        category=PolicyCategory.DATA_QUALITY_SLA,
        name="Freshness and latency SLA",
        description=(
            "Streaming dashboard freshness <5 min; Gold replenishment p95 query <3s; "
            "dbt semantic refresh <15 min after Gold."
        ),
        rules=[
            "iot→gold path max lag 300s (aligned with late_arriving flag)",
            "analytics executive queries p95 < 3s on marts",
            "dbt run completion < 15 min after gold_computed_at",
        ],
        business_value="Meets operational replenishment and executive morning briefing windows.",
        operational_impact="observability_plane.analytics_metrics records freshness timestamps.",
        tradeoffs="Sub-3s may require DuckDB in-memory cache of mart parquet; refreshed hourly.",
        enforcement_hook="observability_plane.analytics_metrics",
    ),
]

ALL_GOVERNANCE_POLICIES: List[GovernancePolicy] = (
    DATA_ACCESS_POLICIES
    + RETENTION_POLICIES
    + COST_GOVERNANCE_POLICIES
    + WORKLOAD_ISOLATION_POLICIES
    + SCHEMA_EVOLUTION_POLICIES
    + DATA_QUALITY_SLA_POLICIES
)

POLICIES_BY_CATEGORY = {
    PolicyCategory.DATA_ACCESS: DATA_ACCESS_POLICIES,
    PolicyCategory.RETENTION: RETENTION_POLICIES,
    PolicyCategory.COST_GOVERNANCE: COST_GOVERNANCE_POLICIES,
    PolicyCategory.WORKLOAD_ISOLATION: WORKLOAD_ISOLATION_POLICIES,
    PolicyCategory.SCHEMA_EVOLUTION: SCHEMA_EVOLUTION_POLICIES,
    PolicyCategory.DATA_QUALITY_SLA: DATA_QUALITY_SLA_POLICIES,
}


def check_bronze_access_window(table_name: str, role: str, record_age_hours: float) -> bool:
    """Enforce 24h Bronze access for ops_streaming role."""
    if not table_name.startswith("bronze."):
        return True
    if role != "ops_streaming":
        return role == "admin"
    return record_age_hours <= 24.0


def policies_to_report_dict() -> dict:
    """Serialize all policies for API / report generation."""
    out = {}
    for cat, policies in POLICIES_BY_CATEGORY.items():
        out[cat.value] = [
            {
                "policy_id": p.policy_id,
                "name": p.name,
                "description": p.description,
                "rules": p.rules,
                "business_value": p.business_value,
                "operational_impact": p.operational_impact,
                "tradeoffs": p.tradeoffs,
            }
            for p in policies
        ]
    return out
