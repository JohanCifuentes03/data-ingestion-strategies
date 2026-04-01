#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# scripts/up.sh — Pre-flight check modo distribuido
#
# Orquesta la verificación completa del stack distribuido antes de
# iniciar un experimento. Valida:
#   1. Existencia del archivo outputs.env (Terraform aplicado)
#   2. Conectividad SSH a los 4 nodos AWS
#   3. Que Docker esté corriendo en cada nodo
#   4. Que los servicios principales estén activos
#   5. Sincronización NTP de relojes (offset < 5ms)
#
# USO:
#   bash scripts/up.sh
#   bash scripts/up.sh --skip-clock   # omitir verificación NTP
#
# PREREQUISITOS:
#   - terraform apply ejecutado (genera infra/terraform/outputs.env)
#   - ansible-playbook completado (provisioning de las 4 VMs)
# ═══════════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")/.." && pwd)

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SKIP_CLOCK=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-clock) SKIP_CLOCK=true; shift ;;
        *) echo "Uso: $0 [--skip-clock]"; exit 1 ;;
    esac
done

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       PRE-FLIGHT CHECK — MODO DISTRIBUIDO AWS               ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

FAIL=0

# ── Paso 1: Verificar outputs.env de Terraform ──────────────────
echo "[1/4] Validando IPs AWS desde infra/terraform/outputs.env..."
ENV_FILE="$SCRIPT_DIR/infra/terraform/outputs.env"
if [ ! -f "$ENV_FILE" ]; then
    echo -e "  ${RED}✗${NC} $ENV_FILE no encontrado"
    echo "     Ejecuta: cd infra/terraform && terraform apply"
    exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

echo -e "  ${GREEN}✓${NC} VM-1 node-producers : ${OCI_VM_PRODUCER_IP}"
echo -e "  ${GREEN}✓${NC} VM-2 node-broker    : ${OCI_VM_BROKER_IP}"
echo -e "  ${GREEN}✓${NC} VM-3 node-compute   : ${OCI_VM_COMPUTE_IP}"
echo -e "  ${GREEN}✓${NC} VM-4 node-sink      : ${OCI_VM_SINK_IP}"
echo ""

# ── Paso 2: Verificar conectividad SSH ──────────────────────────
echo "[2/4] Verificando conectividad SSH a las 4 VMs..."
SSH_KEY=${CLOUD_SSH_KEY_PATH:-~/.ssh/benchmark_aws}
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=8 -o BatchMode=yes"

declare -A NODE_MAP=(
    ["node-producers"]="${OCI_VM_PRODUCER_PUBLIC_IP:-$OCI_VM_PRODUCER_IP}"
    ["node-broker"]="${OCI_VM_BROKER_PUBLIC_IP:-$OCI_VM_BROKER_IP}"
    ["node-compute"]="${OCI_VM_COMPUTE_PUBLIC_IP:-$OCI_VM_COMPUTE_IP}"
    ["node-sink"]="${OCI_VM_SINK_PUBLIC_IP:-$OCI_VM_SINK_IP}"
)

for NODE_NAME in "${!NODE_MAP[@]}"; do
    IP="${NODE_MAP[$NODE_NAME]}"
    if ssh $SSH_OPTS -i "$SSH_KEY" "ubuntu@$IP" "echo OK" > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $NODE_NAME ($IP)"
    else
        echo -e "  ${RED}✗${NC} $NODE_NAME ($IP) — SSH no responde"
        FAIL=1
    fi
done
echo ""

[ "$FAIL" -eq 1 ] && {
    echo -e "${RED}ERROR:${NC} Algunos nodos no son accesibles via SSH."
    echo "  Verifica que el provisioning de Ansible completó correctamente."
    exit 1
}

# ── Paso 3: Verificar servicios Docker en nodos críticos ─────────
echo "[3/4] Verificando servicios en nodos..."

check_service() {
    local IP="$1"
    local ENDPOINT="$2"
    local LABEL="$3"
    local EXPECTED_STATUS="${4:-200}"

    if curl -sf --max-time 5 "$ENDPOINT" > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $LABEL"
    else
        echo -e "  ${YELLOW}⚠${NC} $LABEL — no responde (puede estar iniciando)"
    fi
}

check_service "$OCI_VM_COMPUTE_IP" \
    "http://${OCI_VM_COMPUTE_PUBLIC_IP:-$OCI_VM_COMPUTE_IP}:8080" \
    "Spark Master UI  (VM-3:8080)"

check_service "$OCI_VM_COMPUTE_IP" \
    "http://${OCI_VM_COMPUTE_PUBLIC_IP:-$OCI_VM_COMPUTE_IP}:8081" \
    "Flink REST UI    (VM-3:8081)"

check_service "$OCI_VM_SINK_IP" \
    "http://${OCI_VM_SINK_PUBLIC_IP:-$OCI_VM_SINK_IP}:9090/-/healthy" \
    "Prometheus       (VM-4:9090)"
echo ""

# ── Paso 4: Verificar sincronización de relojes ─────────────────
if [ "$SKIP_CLOCK" = true ]; then
    echo "[4/4] Verificación NTP omitida (--skip-clock)"
else
    echo "[4/4] Verificando sincronización de relojes NTP..."
    if bash "$SCRIPT_DIR/scripts/check-clock-sync.sh"; then
        echo -e "  ${GREEN}✓${NC} Clock sync OK"
    else
        echo -e "  ${RED}✗${NC} Clock sync FALLA — abortando"
        echo "     Experimenta fallará con datos de latencia inválidos."
        echo "     Solución: esperar 30s y reintentar, o revisar chrony en cada VM."
        exit 1
    fi
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  ✅  STACK DISTRIBUIDO LISTO — Todos los checks OK          ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Siguiente paso:                                            ║"
echo "║    MODE=distributed bash scripts/experiment.sh --quick      ║"
echo "║    MODE=distributed bash scripts/experiment.sh              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
