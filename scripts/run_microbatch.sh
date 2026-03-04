#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-low-load}
RUN_ID=${2:-run_1}
TRIGGER_INTERVAL=${3:-5 seconds}
MASTER_PORT=${SPARK_MASTER_PORT:-7077}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}
POSTGRES_PASSWORD_VALUE=${POSTGRES_PASSWORD:-benchmark}
# How long the streaming query runs before auto-stopping (seconds). Default 20 min.
RUN_DURATION_SECONDS=${RUN_DURATION_SECONDS:-1200}

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

bash "$ROOT_DIR/scripts/clean.sh" "$SCENARIO"

echo "────────────────────────────────────────────────────────────"
echo "[run_microbatch] strategy=microbatch  scenario=$SCENARIO  run_id=$RUN_ID  trigger=$TRIGGER_INTERVAL"
echo "────────────────────────────────────────────────────────────"

MSYS_NO_PATHCONV=1 docker compose exec spark-master /opt/spark/bin/spark-submit \
  --class org.tesis.microbatch.SparkStructuredJob \
  --master spark://spark-master:${MASTER_PORT} \
  /opt/spark/jobs/microbatch/microbatch-job.jar \
    --scenario="$SCENARIO" \
    --run.id="$RUN_ID" \
    --trigger.interval="$TRIGGER_INTERVAL" \
    --kafka.bootstrap.servers=kafka:9092 \
    --kafka.topic=events \
    --checkpoint.location=/opt/spark/checkpoints/microbatch \
    --postgres.url=jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
    --postgres.user=${POSTGRES_USER_NAME} \
    --postgres.password=${POSTGRES_PASSWORD_VALUE} \
    --run.duration.seconds=${RUN_DURATION_SECONDS}

echo "[run_microbatch] Completed — scenario=$SCENARIO run_id=$RUN_ID"
