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

# ── How long to let the generator accumulate events before the batch runs ──
# This must equal run.duration.seconds used by microbatch and streaming,
# so all three strategies process the same observation window of data.
RUN_DURATION_SECONDS=${RUN_DURATION_SECONDS:-1200}

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

bash "$ROOT_DIR/scripts/clean.sh" "$SCENARIO"

echo "────────────────────────────────────────────────────────────"
echo "[run_batch] strategy=batch  scenario=$SCENARIO  run_id=$RUN_ID"
echo "[run_batch] Accumulation phase: generator runs for ${RUN_DURATION_SECONDS}s before batch job executes"
echo "────────────────────────────────────────────────────────────"

# ── Wait for the generator to accumulate events ────────────────────────────
# This mirrors the observation window of microbatch/streaming.
# Batch simulates a periodic bulk-load pattern: data accumulates in Kafka,
# then a single job reads and writes all of it to PostgreSQL.
ELAPSED=0
REPORT_INTERVAL=60
while [ "$ELAPSED" -lt "$RUN_DURATION_SECONDS" ]; do
    REMAINING=$((RUN_DURATION_SECONDS - ELAPSED))
    echo "[run_batch] Accumulating... ${ELAPSED}s elapsed, ${REMAINING}s remaining"
    SLEEP_FOR=$((REMAINING < REPORT_INTERVAL ? REMAINING : REPORT_INTERVAL))
    sleep "$SLEEP_FOR"
    ELAPSED=$((ELAPSED + SLEEP_FOR))
done

echo "[run_batch] Accumulation complete — launching Spark Batch job now"

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
