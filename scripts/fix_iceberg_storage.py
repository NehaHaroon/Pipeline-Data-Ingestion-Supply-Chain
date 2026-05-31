#!/usr/bin/env python3
"""
Fix Iceberg lock files and storage/analytics write permissions (no PowerShell required).

Usage (from repo root):
  python scripts/fix_iceberg_storage.py
  python scripts/fix_iceberg_storage.py --docker-chown
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _rm(path: Path) -> None:
    if not path.exists():
        return
    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        print(f"  removed {path}")
    except OSError as exc:
        print(f"  could not remove {path}: {exc}")


def fix_local() -> None:
    analytics = ROOT / "storage" / "analytics"
    analytics.mkdir(parents=True, exist_ok=True)
    for name in ("query_audit.jsonl", "metrics_state.json", "workload_execution_report.json"):
        _rm(analytics / name)
    tel = ROOT / "storage" / "telemetry"
    _rm(tel / "telemetry_summary.json")

    for name in ("iceberg_catalog.db.session.lock", ".iceberg_pyiceberg_catalog.lock"):
        _rm(ROOT / "storage" / name)
    _rm(ROOT / "storage" / ".locks")


def docker_chown(uid: int) -> None:
    cmd = (
        f"chown -R {uid}:0 /opt/airflow/project/storage /opt/airflow/storage 2>/dev/null || true; "
        f"chmod -R a+rwX /opt/airflow/project/storage/analytics 2>/dev/null || true; "
        "echo storage permissions updated"
    )
    compose = ROOT / "docker-compose.airflow.yml"
    if not compose.is_file():
        print("docker-compose.airflow.yml not found — skip --docker-chown")
        return
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose),
            "run",
            "--rm",
            "--user",
            "0:0",
            "airflow-init",
            "bash",
            "-c",
            cmd,
        ],
        cwd=str(ROOT),
        check=False,
    )


def docker_analytics_chmod() -> None:
    """Part 3 only — fix analytics dir via running container (no Airflow needed)."""
    compose = ROOT / "docker-compose.part3.yml"
    if not compose.is_file():
        return
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose),
            "run",
            "--rm",
            "--user",
            "0:0",
            "--entrypoint",
            "bash",
            "analytics-service",
            "-c",
            "mkdir -p /app/storage/analytics && chmod -R a+rwX /app/storage/analytics && ls -la /app/storage/analytics",
        ],
        cwd=str(ROOT),
        check=False,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Fix storage locks and analytics write permissions")
    parser.add_argument(
        "--docker-chown",
        action="store_true",
        help="Run chown via airflow-init container (Linux Docker)",
    )
    parser.add_argument(
        "--part3-chmod",
        action="store_true",
        default=True,
        help="Run chmod on storage/analytics via analytics-service (default: on)",
    )
    parser.add_argument(
        "--uid",
        type=int,
        default=int(os.getenv("AIRFLOW_UID", "50000")),
    )
    args = parser.parse_args()

    print("Fixing storage/analytics and Iceberg lock files ...")
    fix_local()
    if args.part3_chmod:
        print("\nDocker Part 3 analytics chmod ...")
        docker_analytics_chmod()
    if args.docker_chown:
        print(f"\nDocker chown storage to UID {args.uid} ...")
        docker_chown(args.uid)

    print("\nNext:")
    print("  docker compose -f docker-compose.part3.yml up -d --build")
    print("  docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-export")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
