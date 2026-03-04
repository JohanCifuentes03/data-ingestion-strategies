#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-generic}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}
SERVICES_TO_PAUSE=${SERVICES_TO_PAUSE:-generator probe}
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

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

# ── Reset Flink consumer group (avoids stale offsets across runs) ──
# Suppress the non-fatal "group does not exist" error — that's fine.
docker compose exec -T kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --delete --group "flink-streaming-${SCENARIO}" 2>/dev/null || true

# ── PostgreSQL table reset ─────────────────────────────────────────
docker compose exec -T postgres psql -U ${POSTGRES_USER_NAME} \
  -d ${POSTGRES_DB_NAME} -c "TRUNCATE TABLE events" >/dev/null

# ── Clear Spark checkpoints (avoids offset conflicts) ──────────────
docker compose exec -T spark-master rm -rf /opt/spark/checkpoints/microbatch 2>/dev/null || true

# ── Clear results folder ───────────────────────────────────────────
# Only wipe if explicitly requested (CLEAN_RESULTS=true) or --all flag passed.
CLEAN_RESULTS=${CLEAN_RESULTS:-false}
if [[ "${1:-}" == "--all" || "$CLEAN_RESULTS" == "true" ]]; then
  echo "[clean] Removing results/ folder..."
  rm -rf "${ROOT_DIR}/results"
  mkdir -p "${ROOT_DIR}/results"
  echo "[clean] results/ cleared"
fi

# ── Restart services ───────────────────────────────────────────────
docker compose start ${SERVICES_TO_PAUSE} >/dev/null 2>&1 || true

echo "[clean] Environment reset complete"
