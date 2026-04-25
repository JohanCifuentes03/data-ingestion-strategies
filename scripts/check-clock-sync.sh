#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# scripts/check-clock-sync.sh — Verificación de sincronización NTP
#
# Verifica que todos los nodos del experimento distribuido tengan
# un offset NTP < MAX_OFFSET_MS respecto al servidor NTP de AWS.
#
# CONTEXTO DE VALIDEZ EXPERIMENTAL:
# En modo distribuido, produced_at se genera en VM-1 (node-producers)
# y visible_at se asigna en VM-4 (node-sink). Si los relojes tienen
# una desviación > 5 ms, la métrica latencia = visible_at − produced_at
# puede ser negativa o incorrecta, invalidando los resultados.
#
# La configuración de Ansible (infra/ansible/roles/common) de
# chrony apuntando al NTP interno de AWS (169.254.169.123) garantiza
# un offset < 1 ms. Sincronización fallida o fuera de rango invalida
# cualquier medición de latencia del benchmark.
#
# USO:
#   bash scripts/check-clock-sync.sh
#   bash scripts/check-clock-sync.sh --max-offset 10  # umbral personalizado
#
# RETORNA:
#   0 — todos los nodos dentro del umbral → OK para correr experimentos
#   1 — algún nodo supera el umbral → ABORTAR experimento
# ═══════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")/.." && pwd)

# ── Parámetros ──────────────────────────────────────────────────
MAX_OFFSET_MS=${MAX_OFFSET_MS:-5}
SSH_KEY=${CLOUD_SSH_KEY_PATH:-~/.ssh/benchmark_aws}
SSH_USER=${CLOUD_SSH_USER:-ubuntu}
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"

# Colores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ── Parsear argumentos ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --max-offset) MAX_OFFSET_MS="$2"; shift 2 ;;
        --ssh-key)    SSH_KEY="$2";       shift 2 ;;
        *) echo "Uso: $0 [--max-offset <ms>] [--ssh-key <path>]"; exit 1 ;;
    esac
done

# ── Cargar IPs desde outputs de Terraform ──────────────────────
ENV_FILE="$SCRIPT_DIR/infra/terraform/outputs.env"
if [ -f "$ENV_FILE" ]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
else
    echo -e "${YELLOW}[clock-sync]${NC} outputs.env no encontrado en $ENV_FILE"
    echo "  Asegúrate de haber ejecutado: cd infra/terraform && terraform apply"
    exit 1
fi

COMPUTE_NODE_IP="${CLOUD_VM_COMPUTE_PUBLIC_IP:-}"
COMPUTE_NODE_NAME="node-compute   (VM-3)"

if [ "${CLOUD_COMPUTE_REGION_MODE:-primary}" = "brazil" ] && [ -n "${CLOUD_VM_COMPUTE_BRAZIL_PUBLIC_IP:-}" ]; then
    COMPUTE_NODE_IP="${CLOUD_VM_COMPUTE_BRAZIL_PUBLIC_IP}"
    COMPUTE_NODE_NAME="node-compute-br (sa-east-1)"
fi

NODES=(
    "${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
    "${CLOUD_VM_BROKER_PUBLIC_IP:-}"
    "${COMPUTE_NODE_IP:-}"
    "${CLOUD_VM_SINK_PUBLIC_IP:-}"
)

NODE_NAMES=(
    "node-producers (VM-1)"
    "node-broker    (VM-2)"
    "${COMPUTE_NODE_NAME}"
    "node-sink      (VM-4)"
)

# ── Preparar directorio de resultados ──────────────────────────
RESULTS_BASE="${RESULTS_BASE:-$SCRIPT_DIR/results}"
mkdir -p "$RESULTS_BASE"
CLOCK_LOG="$RESULTS_BASE/clock_offsets_$(date +%Y%m%d_%H%M%S).csv"
echo "node,node_name,offset_ms,status,timestamp" > "$CLOCK_LOG"

FAIL=0
CHECKED=0

echo ""
echo "┌──────────────────────────────────────────────────────────────┐"
echo "│  Verificación de sincronización NTP (umbral: ${MAX_OFFSET_MS}ms)        │"
echo "└──────────────────────────────────────────────────────────────┘"

for i in "${!NODES[@]}"; do
    NODE="${NODES[$i]}"
    NAME="${NODE_NAMES[$i]}"

    [ -z "$NODE" ] && continue
    CHECKED=$((CHECKED + 1))

    # Obtener offset via chronyc tracking en el nodo remoto
    OFFSET_SEC=$(ssh $SSH_OPTS -i "$SSH_KEY" "${SSH_USER}@${NODE}" \
        "chronyc tracking | grep 'System time' | awk '{print \$4}'" 2>/dev/null) || {
        echo -e "  ${RED}✗${NC} $NAME ($NODE) — SSH no disponible"
        echo "$NODE,$NAME,N/A,SSH_ERROR,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$CLOCK_LOG"
        FAIL=1
        continue
    }

    # Convertir a ms
    OFFSET_MS=$(echo "$OFFSET_SEC * 1000" | bc 2>/dev/null | \
        awk '{printf "%.2f", $1}') || OFFSET_MS="0.00"
    ABS_MS=$(echo "$OFFSET_MS" | tr -d '-' | awk '{printf "%d", $1+0.5}')

    if [ "$ABS_MS" -le "$MAX_OFFSET_MS" ]; then
        STATUS="OK"
        echo -e "  ${GREEN}✓${NC} $NAME ($NODE) → offset: ${OFFSET_MS}ms [OK]"
    else
        STATUS="FAIL"
        echo -e "  ${RED}✗${NC} $NAME ($NODE) → offset: ${OFFSET_MS}ms [FAIL — supera ${MAX_OFFSET_MS}ms]"
        FAIL=1
    fi

    echo "$NODE,$NAME,$OFFSET_MS,$STATUS,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$CLOCK_LOG"
done

echo ""
echo "Log guardado en: $CLOCK_LOG"

if [ "$CHECKED" -eq 0 ]; then
    echo -e "${RED}ERROR:${NC} No se encontraron IPs en outputs.env. ¿Ejecutaste terraform apply?"
    exit 1
fi

if [ "$FAIL" -eq 1 ]; then
    echo -e "${RED}ERROR:${NC} Uno o más nodos tienen clock skew fuera del umbral."
    echo "  Verificar: ssh ubuntu@<IP> 'chronyc tracking'"
    echo "  Si el problema persiste, esperar 30s y reintentar."
    exit 1
fi

echo -e "${GREEN}OK:${NC} Todos los nodos dentro del umbral de ${MAX_OFFSET_MS}ms."
echo "  El experimento puede proceder con seguridad estadística."
exit 0
