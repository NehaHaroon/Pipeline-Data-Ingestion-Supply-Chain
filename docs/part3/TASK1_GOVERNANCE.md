# Task 1 — Data Governance Policies (Supply Chain Lakehouse)

Two policies per category with business value, operational impact, and tradeoffs. Implemented in `control_plane/governance_policies.py` and exposed via `GET http://localhost:8002/governance/policies`.

## Data Access

| ID | Policy | Summary |
|----|--------|---------|
| gov_access_001 | Role-based lakehouse access | Finance → Gold only; analysts → Silver/Gold; ops streaming → Bronze <24h |
| gov_access_002 | Streaming operational window | IoT/CDC Bronze limited to 24h for ops; historical analysis via Silver/Gold |

**Business value:** Protects PII while enabling self-serve analytics.  
**Tradeoff:** Stricter roles slow exploration → mitigated by dbt semantic marts.

## Retention

| gov_retention_001 | Medallion tiers | Bronze 30d, Silver 365d, Gold indefinite |
| gov_retention_002 | Telemetry retention | KPI JSONL 180d; Prometheus 15d |

## Cost Governance

| gov_cost_001 | Business-hours guardrails | Ad-hoc joins >3 tables blocked 09:00–18:00 PKT |
| gov_cost_002 | Auto-terminate runaway queries | >120s or >2GB scanned → cancel + audit |

## Workload Isolation

| gov_isolation_001 | Plane resource pools | CDC, transformation, BI serving, ad-hoc SQL separated in Docker |
| gov_isolation_002 | Semantic build isolation | dbt runs only after Gold success |

## Schema Evolution

| gov_schema_001 | Additive-first Iceberg | Breaking changes → contract version bump |
| gov_schema_002 | dbt-gated deployment | Tests must pass before BI refresh |

## Data Quality SLAs

| gov_sla_001 | Null/completeness | PK null% <2%; quarantine <5%/hour |
| gov_sla_002 | Freshness/latency | IoT→Gold <5min; executive queries p95 <3s |
