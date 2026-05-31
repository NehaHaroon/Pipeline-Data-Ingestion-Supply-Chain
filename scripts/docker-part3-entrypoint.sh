#!/bin/sh
# Ensure bind-mounted dirs exist (no recursive chown — slow on Windows mounts).
set -e
mkdir -p /app/storage/analytics /app/storage/semantic/parquet
chmod a+rwX /app/storage/analytics 2>/dev/null || true
exec "$@"
