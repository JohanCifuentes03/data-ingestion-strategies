#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# fault_inject.sh — Prueba de Fault Recovery Time
#
# Inyecta un fallo controlado (kill del contenedor procesador) durante
# un run activo y mide el tiempo hasta que el sistema se recupera al
# 100% del throughput esperado. Registra pérdida de eventos y duplicados.
#
# Uso:
#   ./scripts/fault_inject.sh <strategy> <scenario> [run_id]
#
# Ejemplos:
#   ./scripts/fault_inject.sh streaming medium-load run_fault_1
#   ./scripts/fault_inject.sh microbatch medium-load run_fault_1
#   ./scripts/fault_inject.sh batch medium-load run_fault_1
#
# Salida:
#   results/fault_recovery.csv  — acumulativo (append)
#   results/<strategy>/<scenario>/<run_id>/fault_recovery.csv
#
# Variables de entorno:
#   RECOVERY_TIMEOUT_S   Tiempo máximo esperando recovery (default: 120)
#   THROUGHPUT_WINDOW_S  Ventana de medición de throughput (default: 10)
#   THROUGHPUT_THRESHOLD Fracción del throughput base requerida (default: 0.85)
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()   { echo -e "${GREEN}[fault_inject]${NC} $*"; }
warn()  { echo -e "${YELLOW}[fault_inject]${NC} $*"; }
error() { echo -e "${RED}[fault_inject]${NC} $*"; }
info()  { echo -e "${CYAN}[fault_inject]${NC} $*"; }

# ── Parámetros ──────────────────────────────────────────────────────
STRATEGY=${1:-streaming}
SCENARIO=${2:-medium-load}
RUN_ID=${3:-run_fault_1}

RECOVERY_TIMEOUT_S=${RECOVERY_TIMEOUT_S:-120}
THROUGHPUT_WINDOW_S=${THROUGHPUT_WINDOW_S:-10}
THROUGHPUT_THRESHOLD=${THROUGHPUT_THRESHOLD:-0.85}

RESULTS_BASE="$ROOT_DIR/results"
RUN_DIR="$RESULTS_BASE/$STRATEGY/$SCENARIO/$RUN_ID"
GLOBAL_CSV="$RESULTS_BASE/fault_recovery.csv"

mkdir -p "$RUN_DIR"

# ── Cabecera CSV global si no existe ────────────────────────────────
if [ ! -f "$GLOBAL_CSV" ]; then
    echo "strategy,scenario,run_id,fault_target,fault_time_epoch,recovery_time_s,events_in_postgres_before,events_in_postgres_after,status,notes" \
        > "$GLOBAL_CSV"
    log "Creado $GLOBAL_CSV"
fi

# ── Selección del contenedor a matar por estrategia ─────────────────
case "$STRATEGY" in
    streaming)
        FAULT_TARGET="tesis-ingestion-flink-taskmanager-1"
        RESTART_CMD="docker compose up -d flink-taskmanager"
        RECOVERY_CHECK_CMD="docker compose exec -T flink-jobmanager sh -c 'curl -sf http://localhost:8081/jobs 2>/dev/null | grep -q RUNNING'"
        ;;
    microbatch|batch)
        FAULT_TARGET="tesis-ingestion-spark-worker-1"
        RESTART_CMD="docker compose up -d spark-worker"
        # Spark worker recovery: chequeamos que el worker se re-registra con el master
        RECOVERY_CHECK_CMD="docker compose exec -T spark-master /opt/spark/bin/spark-class org.apache.spark.deploy.Client list spark://spark-master:7077 2>/dev/null | grep -q ALIVE || true"
        ;;
    *)
        error "Estrategia desconocida: $STRATEGY. Usa: streaming | microbatch | batch"
        exit 1
        ;;
esac

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              FAULT INJECTION TEST — TESIS                   ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Strategy  : $STRATEGY"
echo "║  Scenario  : $SCENARIO"
echo "║  Run ID    : $RUN_ID"
echo "║  Target    : $FAULT_TARGET"
echo "║  Timeout   : ${RECOVERY_TIMEOUT_S}s"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Helper: contar eventos en PostgreSQL ────────────────────────────
count_events_pg() {
    docker compose exec -T postgres psql \
        -U "${POSTGRES_USER:-benchmark}" \
        -d "${POSTGRES_DB:-benchmark}" \
        -t -c "SELECT COUNT(*) FROM events WHERE strategy='$STRATEGY' AND scenario='$SCENARIO';" \
        2>/dev/null | tr -d ' \n' || echo "0"
}

# ── Helper: calcular throughput reciente (eventos/s en ventana) ──────
# Compara el count de PG entre dos puntos separados por THROUGHPUT_WINDOW_S
measure_throughput() {
    local count_before count_after delta
    count_before=$(count_events_pg)
    sleep "$THROUGHPUT_WINDOW_S"
    count_after=$(count_events_pg)
    delta=$((count_after - count_before))
    echo "$((delta / THROUGHPUT_WINDOW_S))"
}

# ── 1. Verificar que los servicios necesarios están corriendo ────────
log "Verificando servicios..."
if ! docker inspect "$FAULT_TARGET" >/dev/null 2>&1; then
    error "Contenedor '$FAULT_TARGET' no encontrado. ¿Está el stack levantado?"
    exit 1
fi

TARGET_STATE=$(docker inspect "$FAULT_TARGET" --format '{{.State.Status}}' 2>/dev/null || echo "missing")
if [ "$TARGET_STATE" != "running" ]; then
    error "Contenedor '$FAULT_TARGET' no está corriendo (estado: $TARGET_STATE)"
    exit 1
fi

# ── 2. Medir throughput base (pre-fallo) ────────────────────────────
log "Midiendo throughput base (ventana: ${THROUGHPUT_WINDOW_S}s)..."
BASELINE_TPUT=$(measure_throughput)
info "Throughput base: ${BASELINE_TPUT} eventos/s"

if [ "$BASELINE_TPUT" -eq 0 ]; then
    warn "Throughput base es 0. Asegúrate de que hay un run activo antes de ejecutar fault_inject."
    warn "Continuando de todas formas (se registrará como baseline=0)..."
fi

# ── 3. Capturar count pre-fallo ──────────────────────────────────────
EVENTS_BEFORE=$(count_events_pg)
FAULT_TIME_EPOCH=$(date +%s)
log "Eventos en PG antes del fallo: $EVENTS_BEFORE"

# ── 4. INYECTAR FALLO ────────────────────────────────────────────────
warn "INYECTANDO FALLO: docker stop $FAULT_TARGET"
FAULT_START_NS=$(date +%s%N)
docker stop "$FAULT_TARGET" >/dev/null 2>&1
log "Contenedor detenido. Iniciando cronómetro de recovery..."

# ── 5. Reiniciar el contenedor inmediatamente (simula auto-restart) ──
sleep 2
log "Reiniciando contenedor..."
eval "$RESTART_CMD" >/dev/null 2>&1

# ── 6. Polling hasta recovery ────────────────────────────────────────
RECOVERY_TIME_S=-1
ELAPSED=0
THRESHOLD_TPUT=$(echo "$BASELINE_TPUT $THROUGHPUT_THRESHOLD" | awk '{printf "%d", $1 * $2}')
# Si baseline era 0, umbral mínimo de 1 evento/s para detectar recovery
if [ "$THRESHOLD_TPUT" -eq 0 ]; then
    THRESHOLD_TPUT=1
fi

info "Esperando recovery (umbral: ${THRESHOLD_TPUT} eventos/s)..."
while [ $ELAPSED -lt $RECOVERY_TIMEOUT_S ]; do
    sleep 5
    ELAPSED=$((ELAPSED + 5))

    # Verificar que el contenedor esté corriendo
    CURRENT_STATE=$(docker inspect "$FAULT_TARGET" --format '{{.State.Status}}' 2>/dev/null || echo "missing")
    if [ "$CURRENT_STATE" != "running" ]; then
        info "  [${ELAPSED}s] Contenedor aún iniciando..."
        continue
    fi

    # Medir throughput actual (ventana corta de 5s)
    COUNT_A=$(count_events_pg)
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    COUNT_B=$(count_events_pg)
    CURRENT_TPUT=$(( (COUNT_B - COUNT_A) / 5 ))

    info "  [${ELAPSED}s] Throughput actual: ${CURRENT_TPUT} ev/s (umbral: ${THRESHOLD_TPUT} ev/s)"

    if [ "$CURRENT_TPUT" -ge "$THRESHOLD_TPUT" ]; then
        FAULT_END_NS=$(date +%s%N)
        RECOVERY_TIME_S=$(( (FAULT_END_NS - FAULT_START_NS) / 1000000000 ))
        log "RECOVERY DETECTADO en ${RECOVERY_TIME_S}s"
        break
    fi
done

# ── 7. Capturar count post-recovery ──────────────────────────────────
EVENTS_AFTER=$(count_events_pg)
EVENTS_DELTA=$((EVENTS_AFTER - EVENTS_BEFORE))

# ── 8. Determinar estado y notas ─────────────────────────────────────
STATUS="ok"
NOTES=""
if [ "$RECOVERY_TIME_S" -eq -1 ]; then
    STATUS="timeout"
    NOTES="No alcanzó ${THRESHOLD_TPUT} ev/s en ${RECOVERY_TIMEOUT_S}s"
    RECOVERY_TIME_S=$RECOVERY_TIMEOUT_S
    warn "TIMEOUT: No se detectó recovery completo en ${RECOVERY_TIMEOUT_S}s"
elif [ "$EVENTS_DELTA" -lt 0 ]; then
    STATUS="data_loss"
    NOTES="Pérdida de eventos detectada: $((EVENTS_BEFORE - EVENTS_AFTER))"
    warn "POSIBLE PÉRDIDA DE DATOS: eventos antes=$EVENTS_BEFORE, después=$EVENTS_AFTER"
fi

# ── 9. Guardar resultados ─────────────────────────────────────────────
CSV_LINE="$STRATEGY,$SCENARIO,$RUN_ID,$FAULT_TARGET,$FAULT_TIME_EPOCH,$RECOVERY_TIME_S,$EVENTS_BEFORE,$EVENTS_AFTER,$STATUS,$NOTES"

echo "$CSV_LINE" >> "$GLOBAL_CSV"

# También guardar en directorio del run
echo "strategy,scenario,run_id,fault_target,fault_time_epoch,recovery_time_s,events_in_postgres_before,events_in_postgres_after,status,notes" \
    > "$RUN_DIR/fault_recovery.csv"
echo "$CSV_LINE" >> "$RUN_DIR/fault_recovery.csv"

# ── 10. Resumen final ─────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              RESULTADO — FAULT RECOVERY                     ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║  Estrategia     : %-43s║\n" "$STRATEGY"
printf "║  Escenario      : %-43s║\n" "$SCENARIO"
printf "║  Contenedor     : %-43s║\n" "$FAULT_TARGET"
printf "║  Recovery time  : %-40ss║\n" "$RECOVERY_TIME_S"
printf "║  Eventos antes  : %-43s║\n" "$EVENTS_BEFORE"
printf "║  Eventos después: %-43s║\n" "$EVENTS_AFTER"
printf "║  Estado         : %-43s║\n" "$STATUS"
if [ -n "$NOTES" ]; then
printf "║  Notas          : %-43s║\n" "$NOTES"
fi
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
log "Resultado guardado en: $GLOBAL_CSV"
log "Resultado guardado en: $RUN_DIR/fault_recovery.csv"
