#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=
cd ""

REMOVE_VOLUMES=true
REMOVE_IMAGES=false

DOWN_ARGS=(down --remove-orphans)

if [ "" = "true" ]; then
  DOWN_ARGS+=( -v )
fi

if [ "" = "true" ]; then
  DOWN_ARGS+=( --rmi local )
fi

echo "[teardown] Stopping stack and cleaning resources"
docker compose ""

echo "[teardown] Done"
echo "  - volumes removed: "
echo "  - local images removed: "
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

REMOVE_VOLUMES=${REMOVE_VOLUMES:-true}
REMOVE_IMAGES=${REMOVE_IMAGES:-false}

DOWN_ARGS=(down --remove-orphans)

if [ "$REMOVE_VOLUMES" = "true" ]; then
  DOWN_ARGS+=( -v )
fi

if [ "$REMOVE_IMAGES" = "true" ]; then
  DOWN_ARGS+=( --rmi local )
fi

echo "[teardown] Stopping stack and cleaning resources"
docker compose "${DOWN_ARGS[@]}"

echo "[teardown] Done"
echo "  - volumes removed: $REMOVE_VOLUMES"
echo "  - local images removed: $REMOVE_IMAGES"
