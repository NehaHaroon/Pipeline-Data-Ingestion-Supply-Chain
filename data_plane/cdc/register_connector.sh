#!/usr/bin/env sh
# Registers Debezium PostgreSQL connector for inventory CDC.

set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083/connectors}"

PAYLOAD='{
  "name": "inventory-transactions-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "etl_user",
    "database.password": "etl_password",
    "database.dbname": "supply_chain_db",
    "database.server.name": "supply_chain",
    "table.include.list": "public.inventory_transactions",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "supply_chain_pub",
    "topic.prefix": "supply_chain",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "string",
    "time.precision.mode": "connect",
    "snapshot.mode": "initial",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,source.ts_ms",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}'

echo "[DEBEZIUM] Registering connector at ${CONNECT_URL}..."
HTTP_CODE="$(curl -sS -o /tmp/debezium_register_response.json -w "%{http_code}" \
  -X POST "${CONNECT_URL}" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}")"

if [ "${HTTP_CODE}" = "201" ] || [ "${HTTP_CODE}" = "200" ]; then
  echo "[DEBEZIUM] Connector registered successfully."
  exit 0
fi

if [ "${HTTP_CODE}" = "409" ]; then
  echo "[DEBEZIUM][WARN] Connector already exists (HTTP 409). Continuing."
  exit 0
fi

echo "[DEBEZIUM][ERROR] Connector registration failed with HTTP ${HTTP_CODE}."
cat /tmp/debezium_register_response.json
exit 1
