#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-low-load}
TRIGGER_INTERVAL=${2:-5 seconds}
MASTER_PORT=${SPARK_MASTER_PORT:-7077}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}
POSTGRES_PASSWORD_VALUE=${POSTGRES_PASSWORD:-benchmark}

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

bash "$ROOT_DIR/scripts/clean.sh" "$SCENARIO"

echo "[run_microbatch] Starting Structured Streaming job for $SCENARIO"
MSYS_NO_PATHCONV=1 docker compose exec spark-master /opt/spark/bin/spark-submit \
  --class org.tesis.microbatch.SparkStructuredJob \
  --master spark://spark-master:${MASTER_PORT} \
  /opt/spark/jobs/microbatch/microbatch-job.jar \
    --scenario="$SCENARIO" \
    --trigger.interval="$TRIGGER_INTERVAL" \
    --kafka.bootstrap.servers=kafka:9092 \
    --kafka.topic=events \
    --checkpoint.location=/opt/spark/checkpoints/microbatch \
    --postgres.url=jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
    --postgres.user=${POSTGRES_USER_NAME} \
    --postgres.password=${POSTGRES_PASSWORD_VALUE}
