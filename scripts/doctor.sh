#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

echo "[doctor] Docker daemon"
docker info >/dev/null
echo "[doctor] OK"

echo "[doctor] Compose file"
docker compose config -q
echo "[doctor] OK"

echo "[doctor] Running services"
docker compose ps

echo "[doctor] HTTP endpoints"
for endpoint in http://localhost:3000/api/health http://localhost:9090/-/ready http://localhost:8000/metrics http://localhost:8001/metrics; do
  if curl -fsS "$endpoint" >/dev/null 2>&1; then
    echo "  OK   $endpoint"
  else
    echo "  WARN $endpoint"
  fi
done

echo "[doctor] Postgres row count"
docker compose exec -T postgres psql -U ${POSTGRES_USER:-benchmark} -d ${POSTGRES_DB:-benchmark} -c "SELECT COUNT(*) AS events_count FROM events;"

echo "[doctor] Prometheus targets (up/down)"
curl -fsS http://localhost:9090/api/v1/targets | python -c "import json,sys; data=json.load(sys.stdin); [print(f\"  {t['labels'].get('job','unknown'):14} {t['health']}\") for t in data['data']['activeTargets']]" || true

echo "[doctor] Completed"
