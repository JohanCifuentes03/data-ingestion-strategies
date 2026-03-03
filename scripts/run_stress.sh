#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

STRATEGY=${1:-microbatch}
SCENARIO=${2:-burst}
RUN_ID=${3:-run_stress}
TRIGGER_INTERVAL=${TRIGGER_INTERVAL:-"1 seconds"}
GENERATOR_RATE=${GENERATOR_RATE:-20000}
PROBE_POLL_MS=${PROBE_POLL_MS:-200}

echo "[stress] strategy=$STRATEGY scenario=$SCENARIO run_id=$RUN_ID"
echo "[stress] Recreating generator/probe with higher pressure"
GENERATOR_SCENARIO=$SCENARIO GENERATOR_EVENT_RATE=$GENERATOR_RATE PROBE_POLL_INTERVAL_MS=$PROBE_POLL_MS docker compose up -d --no-build --force-recreate generator probe kafka-exporter

bash "$ROOT_DIR/scripts/clean.sh" "$SCENARIO"

case "$STRATEGY" in
  batch)
    bash "$ROOT_DIR/scripts/run_batch.sh" "$SCENARIO" "$RUN_ID" earliest latest
    ;;
  microbatch)
    bash "$ROOT_DIR/scripts/run_microbatch.sh" "$SCENARIO" "$RUN_ID" "$TRIGGER_INTERVAL"
    ;;
  streaming)
    FLINK_DETACHED=true bash "$ROOT_DIR/scripts/run_streaming.sh" "$SCENARIO" "$RUN_ID"
    echo "[stress] Flink submitted in detached mode. Monitor at http://localhost:8081"
    ;;
  *)
    echo "[stress] Unknown strategy: $STRATEGY"
    exit 1
    ;;
esac

echo "[stress] Done. Open Grafana at http://localhost:3000"
