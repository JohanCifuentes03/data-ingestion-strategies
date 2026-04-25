#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# scripts/collect-results.sh — Recolección de resultados desde VM-4
#
# Copia los resultados del experimento distribuido desde el nodo sink
# (VM-4 / node-sink) hacia ./results-distributed/ en la máquina local.
# Los resultados incluyen:
#   - latency_samples.csv (por corrada: strategy/scenario/run_id/)
#   - prometheus_snapshot.csv
#   - clock_offsets_*.csv (logs de sincronización NTP)
#
# USO:
#   bash scripts/collect-results.sh
#   bash scripts/collect-results.sh --dry-run  # solo muestra qué se copiaría
# ═══════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")/.." && pwd)

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN=true; shift ;;
        *) echo "Uso: $0 [--dry-run]"; exit 1 ;;
    esac
done

# ── Cargar IPs desde outputs de Terraform ──────────────────────
ENV_FILE="$SCRIPT_DIR/infra/terraform/outputs.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE no encontrado."
    echo "  Ejecuta primero: cd infra/terraform && terraform apply"
    exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

SSH_KEY=${CLOUD_SSH_KEY_PATH:-~/.ssh/benchmark_aws}
SSH_USER=${CLOUD_SSH_USER:-ubuntu}
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15"
REMOTE="${SSH_USER}@${CLOUD_VM_SINK_PUBLIC_IP}"
LOCAL_DEST="${RESULTS_BASE:-$SCRIPT_DIR/results-distributed}"
REMOTE_RESULTS="~/data-ingestion-strategies/$(basename "$LOCAL_DEST")/"
export LOCAL_DEST

echo ""
echo "┌──────────────────────────────────────────────────────────────┐"
echo "│  Recolectando resultados desde nodo sink                     │"
echo "└──────────────────────────────────────────────────────────────┘"
echo ""
echo "  Origen:  ${REMOTE}:${REMOTE_RESULTS}"
echo "  Destino: ${LOCAL_DEST}/"

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo -e "${YELLOW}[dry-run]${NC} Ejecutaría:"
    echo "  scp -r -i $SSH_KEY $SSH_OPTS ${REMOTE}:${REMOTE_RESULTS} ${LOCAL_DEST}/"
    exit 0
fi

# Verificar conectividad SSH antes de intentar la copia
echo ""
echo "Verificando conectividad SSH..."
if ! ssh $SSH_OPTS -i "$SSH_KEY" "$REMOTE" "echo OK" > /dev/null 2>&1; then
    echo "ERROR: No se puede conectar a $REMOTE"
    echo "  Verifica que la VM esté activa y que la SSH key sea correcta."
    exit 1
fi
echo -e "${GREEN}✓${NC} Conectividad SSH OK"

# Crear directorio destino local limpio para evitar mezclar corridas viejas
rm -rf "$LOCAL_DEST" 2>/dev/null || true
mkdir -p "$LOCAL_DEST"

# Copiar resultados
echo ""
echo "Copiando resultados (puede tardar según el volumen de datos)..."
scp -r $SSH_OPTS -i "$SSH_KEY" "${REMOTE}:${REMOTE_RESULTS}" "$LOCAL_DEST/"

# Normalizar estructura: results-distributed/<strategy>/<scenario>/<run_id>/...
if [ -d "$LOCAL_DEST/results" ]; then
    shopt -s dotglob nullglob
    mv "$LOCAL_DEST/results"/* "$LOCAL_DEST/" 2>/dev/null || true
    shopt -u dotglob nullglob
    rmdir "$LOCAL_DEST/results" 2>/dev/null || true
fi

# Calcular estadísticas de lo copiado
CSV_COUNT=$(find "$LOCAL_DEST" -name "latency_samples.csv" 2>/dev/null | wc -l | tr -d ' ')
TOTAL_LINES=$(
python3 - <<'PY'
import os
from pathlib import Path
root = Path(os.environ["LOCAL_DEST"])
total = 0
for f in root.rglob("latency_samples.csv"):
    try:
        with f.open("r", encoding="utf-8", errors="ignore") as h:
            lines = sum(1 for _ in h)
        total += max(lines - 1, 0)
    except Exception:
        pass
print(total)
PY
)
DISK_USAGE=$(du -sh "$LOCAL_DEST" 2>/dev/null | cut -f1)

echo ""
echo "┌──────────────────────────────────────────────────────────────┐"
echo "│  Resultados copiados exitosamente                            │"
echo "├──────────────────────────────────────────────────────────────┤"
echo "│  Archivos latency_samples.csv: $CSV_COUNT"
echo "│  Total de muestras de latencia: $TOTAL_LINES"
echo "│  Tamaño total:                  $DISK_USAGE"
echo "│  Directorio local:              $LOCAL_DEST/"
echo "└──────────────────────────────────────────────────────────────┘"
echo ""
echo "Siguiente paso — Análisis estadístico:"
echo "  .venv/bin/python -m benchmark.analysis.analyzer \\"
echo "    --results-dir $LOCAL_DEST"
echo ""
