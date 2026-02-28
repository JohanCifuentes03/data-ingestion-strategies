#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-low-load}
STARTING_OFFSETS=${2:-earliest}
ENDING_OFFSETS=${3:-latest}
MASTER_PORT=${SPARK_MASTER_PORT:-7077}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}
POSTGRES_PASSWORD_VALUE=${POSTGRES_PASSWORD:-benchmark}

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

bash "$ROOT_DIR/scripts/clean.sh" "$SCENARIO"

echo "[run_batch] Submitting Spark batch job for scenario $SCENARIO"
docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --class org.tesis.batch.SparkBatchJob \
  --master spark://spark-master:${MASTER_PORT} \
  /opt/bitnami/spark/jobs/batch/batch-job.jar \
    --scenario="$SCENARIO" \
    --kafka.bootstrap.servers=kafka:9092 \
    --kafka.topic=events \
    --kafka.startingOffsets="$STARTING_OFFSETS" \
    --kafka.endingOffsets="$ENDING_OFFSETS" \
    --postgres.url=jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
    --postgres.user=${POSTGRES_USER_NAME} \
    --postgres.password=${POSTGRES_PASSWORD_VALUE}
