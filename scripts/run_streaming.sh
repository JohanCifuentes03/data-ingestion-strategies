#!/usr/bin/env bash
set -euo pipefail

SCENARIO=${1:-low-load}
RUN_ID=${2:-run_1}
FLINK_PARALLELISM_VALUE=${FLINK_PARALLELISM:-1}
FLINK_MAIN_CLASS=${FLINK_MAIN_CLASS:-org.tesis.streaming.FlinkStreamingJob}
FLINK_DETACHED=${FLINK_DETACHED:-false}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}
POSTGRES_PASSWORD_VALUE=${POSTGRES_PASSWORD:-benchmark}
# How long the Flink job runs before auto-stopping (seconds). Default 20 min.
RUN_DURATION_SECONDS=${RUN_DURATION_SECONDS:-1200}

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

bash "$ROOT_DIR/scripts/clean.sh" "$SCENARIO"

echo "────────────────────────────────────────────────────────────"
echo "[run_streaming] strategy=streaming  scenario=$SCENARIO  run_id=$RUN_ID"
echo "────────────────────────────────────────────────────────────"

FLINK_DETACH_FLAG=""
if [ "$FLINK_DETACHED" = "true" ]; then
  FLINK_DETACH_FLAG="-d"
fi

MSYS_NO_PATHCONV=1 docker compose exec flink-jobmanager /opt/flink/bin/flink run \
  ${FLINK_DETACH_FLAG} \
  -c ${FLINK_MAIN_CLASS} \
  -p ${FLINK_PARALLELISM_VALUE} \
  /opt/flink/usrlib/streaming-job.jar \
    --scenario "$SCENARIO" \
    --run.id "$RUN_ID" \
    --kafka.bootstrap.servers kafka:9092 \
    --kafka.topic events \
    --postgres.url jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
    --postgres.user ${POSTGRES_USER_NAME} \
    --postgres.password ${POSTGRES_PASSWORD_VALUE} \
    --run.duration.seconds ${RUN_DURATION_SECONDS}

echo "[run_streaming] Completed — scenario=$SCENARIO run_id=$RUN_ID"
