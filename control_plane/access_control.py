"""
Analytics query access control (Part 3 Task 1 — data access policies).

Roles align with governance_policies.DATA_ACCESS_POLICIES.
"""

from enum import Enum
from typing import List, Optional, Set

from fastapi import Header, HTTPException


class AnalyticsRole(str, Enum):
    ADMIN = "admin"
    ANALYST = "analyst"
    FINANCE = "finance"
    OPS_STREAMING = "ops_streaming"


# Workload types each role may execute via analytics-service
_ROLE_WORKLOADS: dict[AnalyticsRole, Set[str]] = {
    AnalyticsRole.ADMIN: {"operational", "strategic", "executive_bi", "ad_hoc"},
    AnalyticsRole.ANALYST: {"strategic", "executive_bi", "ad_hoc"},
    AnalyticsRole.FINANCE: {"executive_bi"},
    AnalyticsRole.OPS_STREAMING: {"operational"},
}

# Query IDs blocked for finance (no ad-hoc PII joins)
_FINANCE_ALLOWED_QUERIES = {"exec_001", "exec_002"}


def resolve_role(x_analytics_role: Optional[str] = Header(None, alias="X-Analytics-Role")) -> AnalyticsRole:
    if not x_analytics_role:
        return AnalyticsRole.ANALYST
    try:
        return AnalyticsRole(x_analytics_role.strip().lower())
    except ValueError:
        raise HTTPException(
            status_code=403,
            detail=f"Unknown role '{x_analytics_role}'. Use: admin, analyst, finance, ops_streaming",
        )


def assert_query_allowed(role: AnalyticsRole, query_id: str, workload_type: str) -> None:
    allowed = _ROLE_WORKLOADS.get(role, set())
    if workload_type not in allowed:
        raise HTTPException(
            status_code=403,
            detail=f"Role '{role.value}' cannot run workload '{workload_type}' per data access policy gov_access_001",
        )
    if role == AnalyticsRole.FINANCE and query_id not in _FINANCE_ALLOWED_QUERIES:
        raise HTTPException(
            status_code=403,
            detail="Finance role restricted to executive BI scorecard queries only",
        )
    if role == AnalyticsRole.OPS_STREAMING and workload_type != "operational":
        raise HTTPException(status_code=403, detail="Ops streaming role: operational queries only (24h Bronze window)")
