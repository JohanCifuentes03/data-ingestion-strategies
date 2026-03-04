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

# ── Ensure Flink containers are running before submitting ──────────
# After heavy or failed runs the jobmanager/taskmanager may be exited.
# Restart them unconditionally so each streaming run starts clean.
echo "[run_streaming] Restarting Flink (ensures clean JVM state)..."
docker compose restart flink-jobmanager flink-taskmanager >/dev/null 2>&1

# Wait until the flink-jobmanager REST port (8081) is bound.
# Uses /proc/net/tcp6 (always present in Linux containers; no nc/curl needed).
# Port 8081 in hex = 1F91. We grep for ":1F91 " in the listening sockets.
echo "[run_streaming] Waiting for Flink REST port 8081..."
for i in $(seq 1 45); do
  JM_STATE=$(docker inspect tesis-ingestion-flink-jobmanager-1 \
               --format '{{.State.Status}}' 2>/dev/null || echo "missing")
  if [ "$JM_STATE" = "running" ]; then
    # Check if port 8081 (0x1F91) is listening inside container
    if docker compose exec -T flink-jobmanager \
        sh -c "grep -qi ':1F91 ' /proc/net/tcp6 2>/dev/null || grep -qi ':1F91 ' /proc/net/tcp 2>/dev/null"; then
      echo "[run_streaming] Flink is ready (attempt $i)."
      break
    fi
  fi
  if [ "$i" -eq 45 ]; then
    echo "[run_streaming] ERROR: Flink not ready after 90s. Logs:"
    docker compose logs --tail=15 flink-jobmanager 2>&1 | tail -15
    exit 1
  fi
  sleep 2
done

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
