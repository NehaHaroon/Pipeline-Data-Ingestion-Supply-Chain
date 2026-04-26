# Inventory Transactions DB + CDC

This module provisions PostgreSQL and Debezium CDC for `inventory_transactions`, then streams CDC events into Kafka topic `supply_chain.public.inventory_transactions`.

## Start full stack

```bash
docker-compose up -d
```

Windows automatic fallback starter (uses `5432`, falls back to `5433` if occupied):

```powershell
powershell -ExecutionPolicy Bypass -File data_plane/db/up_with_port_fallback.ps1
```

Note: `db-seeder` and `debezium-setup` are one-shot jobs. They typically show `Exited (0)` after success, which is expected.

## Verify PostgreSQL

```bash
docker exec -it <postgres_container> psql -U etl_user -d supply_chain_db -c "SELECT COUNT(*) FROM inventory_transactions;"
```

## Check Debezium connector status

```bash
curl http://localhost:8083/connectors/inventory-transactions-connector/status
```

## Watch CDC events in Kafka

```bash
docker exec -it <kafka_container> kafka-console-consumer --bootstrap-server localhost:9092 --topic supply_chain.public.inventory_transactions --from-beginning
```

## Run generator

Steady mode (10 inserts per second):

```bash
python data_plane/generators/inventory_transaction_generator.py --mode steady
```

Burst mode (5000 records rapidly, then 10 second pause):

```bash
python data_plane/generators/inventory_transaction_generator.py --mode burst
```

## Run CDC consumer

```bash
python data_plane/cdc/cdc_consumer.py
```

## Scheduled ingestion behavior

- Weather API ingestion (`src_weather_api`) runs every 2 minutes in production runner.
- Database ingestion (`src_inventory_transactions`) also runs every 2 minutes in production runner.
- Both emit telemetry records into:
  - `storage/telemetry/telemetry_records.jsonl`
  - source-specific telemetry logs in `storage/telemetry/`

Run production mode:

```bash
python run_production.py
```

Optional scheduler overrides:

- `WEATHER_INGESTION_INTERVAL_SECONDS` (default: auto-detected from WEATHER_API_SOURCE.ingestion_frequency)
- `DB_INGESTION_INTERVAL_SECONDS` (default: auto-detected from INVENTORY_TRANSACTIONS_SOURCE.ingestion_frequency)

## Kafka topic naming convention

Debezium topic naming follows:

`{server_name}.{schema}.{table}`

For this setup:

`supply_chain.public.inventory_transactions`

## Troubleshooting

- PostgreSQL log line `invalid length of startup packet`:
  - typically a harmless probe/noise from a non-Postgres client hitting port `5432`.
- Replication slot conflict: drop stale slot if required:
  - `SELECT pg_drop_replication_slot('debezium_slot');`
- Connector returns 409 on re-register:
  - this is expected when connector already exists; `data_plane/cdc/register_connector.sh` handles it gracefully.
- Verify WAL logical level:
  - `SHOW wal_level;` should return `logical`.
- If host cannot reach `localhost:5432`:
  - run `docker-compose ps` and verify `postgres` is healthy
  - check local port conflicts (`5432`) from another local PostgreSQL service
  - if conflict exists, stop local PostgreSQL service or set `POSTGRES_HOST_PORT` (e.g., `5433`)
  - compose now supports `POSTGRES_HOST_PORT` via `${POSTGRES_HOST_PORT:-5432}:5432`
