#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-low-load}
RUN_ID=${2:-run_1}
STARTING_OFFSETS=${3:-earliest}
ENDING_OFFSETS=${4:-latest}
MASTER_PORT=${SPARK_MASTER_PORT:-7077}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}
POSTGRES_PASSWORD_VALUE=${POSTGRES_PASSWORD:-benchmark}

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

bash "$ROOT_DIR/scripts/clean.sh" "$SCENARIO"

echo "────────────────────────────────────────────────────────────"
echo "[run_batch] strategy=batch  scenario=$SCENARIO  run_id=$RUN_ID"
echo "────────────────────────────────────────────────────────────"

MSYS_NO_PATHCONV=1 docker compose exec spark-master /opt/spark/bin/spark-submit \
  --class org.tesis.batch.SparkBatchJob \
  --master spark://spark-master:${MASTER_PORT} \
  /opt/spark/jobs/batch/batch-job.jar \
    --scenario="$SCENARIO" \
    --run.id="$RUN_ID" \
    --kafka.bootstrap.servers=kafka:9092 \
    --kafka.topic=events \
    --kafka.startingOffsets="$STARTING_OFFSETS" \
    --kafka.endingOffsets="$ENDING_OFFSETS" \
    --postgres.url=jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
    --postgres.user=${POSTGRES_USER_NAME} \
    --postgres.password=${POSTGRES_PASSWORD_VALUE}

echo "[run_batch] Completed — scenario=$SCENARIO run_id=$RUN_ID"
