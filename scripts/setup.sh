#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[setup] Missing required command: $1"
    exit 1
  fi
}

echo "[setup] Verifying local requirements"
require_cmd docker
require_cmd bash
require_cmd java

if ! docker info >/dev/null 2>&1; then
  echo "[setup] Docker daemon is not available. Start Docker Desktop and retry."
  exit 1
fi

if [ ! -f "$ROOT_DIR/.env" ]; then
  cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
  echo "[setup] Created .env from .env.example"
fi

docker compose config -q

echo "[setup] Building Java jobs"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    "$ROOT_DIR/gradlew.bat" buildJobs
    ;;
  *)
    "$ROOT_DIR/gradlew" buildJobs
    ;;
esac

echo "[setup] Building Python images"
docker compose build generator probe

echo "[setup] Starting infrastructure"
docker compose up -d --no-build

echo "[setup] Resetting benchmark state"
bash "$ROOT_DIR/scripts/clean.sh"

echo "[setup] Stack status"
docker compose ps

echo ""
echo "[setup] Ready. Useful endpoints:"
echo "  - Grafana:    http://localhost:3000 (admin/admin)"
echo "  - Prometheus: http://localhost:9090"
echo "  - Spark UI:   http://localhost:8080"
echo "  - Flink UI:   http://localhost:8081"
echo ""
echo "[setup] Suggested next commands:"
echo "  - bash ./scripts/run_batch.sh low-load run_1"
echo "  - bash ./scripts/run_microbatch.sh medium-load run_1 \"5 seconds\""
echo "  - FLINK_DETACHED=true bash ./scripts/run_streaming.sh burst run_1"
