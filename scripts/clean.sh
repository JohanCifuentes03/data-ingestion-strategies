#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-generic}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}
SERVICES_TO_PAUSE=${SERVICES_TO_PAUSE:-generator probe}

echo "[clean] Resetting environment for scenario=$SCENARIO"

# Stop generator and probe to avoid writing during reset
docker compose stop ${SERVICES_TO_PAUSE} >/dev/null 2>&1 || true

# ── Kafka topic reset ──────────────────────────────────────────────
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 \
  --delete --topic events 2>/dev/null || true
sleep 2
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 \
  --create --topic events \
  --replication-factor ${KAFKA_REPLICATION_FACTOR:-1} \
  --partitions ${KAFKA_NUM_PARTITIONS:-6} \
  --if-not-exists

# ── PostgreSQL table reset ─────────────────────────────────────────
docker compose exec -T postgres psql -U ${POSTGRES_USER_NAME} \
  -d ${POSTGRES_DB_NAME} -c "TRUNCATE TABLE events" >/dev/null

# ── Clear Spark checkpoints (avoids offset conflicts) ──────────────
docker compose exec -T spark-master rm -rf /opt/spark/checkpoints/microbatch 2>/dev/null || true

# ── Restart services ───────────────────────────────────────────────
docker compose start ${SERVICES_TO_PAUSE} >/dev/null 2>&1 || true

echo "[clean] Environment reset complete"
