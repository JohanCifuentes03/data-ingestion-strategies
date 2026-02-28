#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-generic}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}

echo "[clean] Resetting Kafka topic and Postgres table for $SCENARIO"

docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic events || true
sleep 2
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic events --replication-factor ${KAFKA_REPLICATION_FACTOR:-1} --partitions ${KAFKA_NUM_PARTITIONS:-6} --if-not-exists

docker compose exec postgres psql -U ${POSTGRES_USER_NAME} -d ${POSTGRES_DB_NAME} -c "TRUNCATE TABLE events" >/dev/null
