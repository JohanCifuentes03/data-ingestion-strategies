#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# run.sh — Ejecutar una estrategia de ingestión
#
# Combina: run_batch, run_microbatch, run_streaming
#
# Uso:
#   ./scripts/run.sh batch low-load run_1
#   ./scripts/run.sh microbatch medium-load run_1 "5 seconds"
#   ./scripts/run.sh streaming high-load run_1
# ═══════════════════════════════════════════════════════════════════

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

# Colores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[run]${NC} $*"; }
warn() { echo -e "${YELLOW}[run]${NC} $*"; }

ensure_services() {
    log "Verificando servicios..."
    # Verificar que Kafka y PostgreSQL estén corriendo
    if ! docker compose ps kafka postgres 2>/dev/null | grep -q "Up"; then
        warn "Servicios no están levantados. Ejecuta ./scripts/manage.sh up primero."
        exit 1
    fi
    log "Servicios verificados"
}

STRATEGY=${1:-batch}
SCENARIO=${2:-low-load}
RUN_ID=${3:-run_1}
TRIGGER_INTERVAL=${4:-"5 seconds"}

MASTER_PORT=${SPARK_MASTER_PORT:-7077}
POSTGRES_DB_NAME=${POSTGRES_DB:-benchmark}
POSTGRES_USER_NAME=${POSTGRES_USER:-benchmark}
POSTGRES_PASSWORD_VALUE=${POSTGRES_PASSWORD:-benchmark}
RUN_DURATION_SECONDS=${RUN_DURATION_SECONDS:-300}
FLINK_PARALLELISM_VALUE=${FLINK_PARALLELISM:-1}
FLINK_DETACHED=${FLINK_DETACHED:-false}

# ═══════════════════════════════════════════════════════════════════
# RUN BATCH
# ═══════════════════════════════════════════════════════════════════
run_batch() {
    ensure_services
    
    log "Ejecutando BATCH: scenario=$SCENARIO run_id=$RUN_ID duration=${RUN_DURATION_SECONDS}s"
    
    # Limpiar antes (directo, sin llamar a manage.sh)
    log "Limpiando entorno..."
    docker compose exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
    docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true
    docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    
    echo "────────────────────────────────────────────────────────────"
    echo "[run_batch] strategy=batch  scenario=$SCENARIO  run_id=$RUN_ID"
    echo "────────────────────────────────────────────────────────────"
    
    ACCUMULATE_TIME=$RUN_DURATION_SECONDS
    log "Fase de acumulación: generator corre por ${ACCUMULATE_TIME}s antes del batch job"
    
    # Fase de acumulación
    echo "[run_batch] Accumulation phase: generator runs for ${ACCUMULATE_TIME}s before batch job executes"
    echo "[run_batch] Accumulating..."
    
    # Iniciar generator en background
    docker compose up -d generator
    
    # Esperar acumulación
    for i in $(seq 1 $((ACCUMULATE_TIME / 60))); do
        ELAPSED=$((i * 60))
        REMAINING=$((ACCUMULATE_TIME - ELAPSED))
        echo "[run_batch] Accumulating... ${ELAPSED}s elapsed, ${REMAINING}s remaining"
        sleep 60
    done
    
    # Detener generator
    docker compose stop generator
    
    # Ejecutar Spark Batch
    log "Ejecutando Spark Batch..."
    MSYS_NO_PATHCONV=1 docker compose exec spark-master /opt/spark/bin/spark-submit \
        --class org.tesis.batch.SparkBatchJob \
        --master spark://spark-master:${MASTER_PORT} \
        /opt/spark/jobs/batch/batch-job.jar \
        --scenario="$SCENARIO" \
        --run.id="$RUN_ID" \
        --kafka.bootstrap.servers=kafka:9092 \
        --kafka.topic=events \
        --postgres.url=jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
        --postgres.user=${POSTGRES_USER_NAME} \
        --postgres.password=${POSTGRES_PASSWORD_VALUE} \
        --run.duration.seconds=${RUN_DURATION_SECONDS}
    
    log "Completed: batch/$SCENARIO/$RUN_ID"
}

# ═══════════════════════════════════════════════════════════════════
# RUN MICROBATCH
# ═══════════════════════════════════════════════════════════════════
run_microbatch() {
    ensure_services
    
    log "Ejecutando MICROBATCH: scenario=$SCENARIO run_id=$RUN_ID trigger=$TRIGGER_INTERVAL"
    
    # Limpiar antes
    log "Limpiando entorno..."
    docker compose exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
    docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true
    docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    
    echo "────────────────────────────────────────────────────────────"
    echo "[run_microbatch] strategy=microbatch  scenario=$SCENARIO  run_id=$RUN_ID  trigger=$TRIGGER_INTERVAL"
    echo "────────────────────────────────────────────────────────────"
    
    # Iniciar generator y microbatch
    docker compose up -d generator
    
    MSYS_NO_PATHCONV=1 docker compose exec spark-master /opt/spark/bin/spark-submit \
        --class org.tesis.microbatch.SparkStructuredJob \
        --master spark://spark-master:${MASTER_PORT} \
        /opt/spark/jobs/microbatch/microbatch-job.jar \
        --scenario="$SCENARIO" \
        --run.id="$RUN_ID" \
        --trigger.interval="$TRIGGER_INTERVAL" \
        --kafka.bootstrap.servers=kafka:9092 \
        --kafka.topic=events \
        --checkpoint.location=/opt/spark/checkpoints/microbatch \
        --postgres.url=jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
        --postgres.user=${POSTGRES_USER_NAME} \
        --postgres.password=${POSTGRES_PASSWORD_VALUE} \
        --run.duration.seconds=${RUN_DURATION_SECONDS}
    
    # Detener generator
    docker compose stop generator
    
    log "Completed: microbatch/$SCENARIO/$RUN_ID"
}

# ═══════════════════════════════════════════════════════════════════
# RUN STREAMING
# ═══════════════════════════════════════════════════════════════════
run_streaming() {
    ensure_services
    
    log "Ejecutando STREAMING: scenario=$SCENARIO run_id=$RUN_ID"
    
    # Limpiar antes
    log "Limpiando entorno..."
    docker compose exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
    docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true
    docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    
    echo "────────────────────────────────────────────────────────────"
    echo "[run_streaming] strategy=streaming  scenario=$SCENARIO  run_id=$RUN_ID"
    echo "────────────────────────────────────────────────────────────"
    
    # Reiniciar Flink para estado limpio
    docker compose restart flink-jobmanager flink-taskmanager >/dev/null 2>&1
    
    # Esperar Flink
    log "Esperando Flink..."
    for i in $(seq 1 45); do
        JM_STATE=$(docker inspect tesis-ingestion-flink-jobmanager-1 --format '{{.State.Status}}' 2>/dev/null || echo "missing")
        if [ "$JM_STATE" = "running" ]; then
            if docker compose exec -T flink-jobmanager sh -c "grep -qi ':1F91 ' /proc/net/tcp6 2>/dev/null || grep -qi ':1F91 ' /proc/net/tcp 2>/dev/null"; then
                log "Flink listo"
                break
            fi
        fi
        if [ $i -eq 45 ]; then
            error "Flink no está listo"
            exit 1
        fi
        sleep 2
    done
    
    # Iniciar generator
    docker compose up -d generator
    
    FLINK_DETACH_FLAG=""
    if [ "$FLINK_DETACHED" = "true" ]; then
        FLINK_DETACH_FLAG="-d"
    fi
    
    MSYS_NO_PATHCONV=1 docker compose exec flink-jobmanager /opt/flink/bin/flink run \
        ${FLINK_DETACH_FLAG} \
        -c org.tesis.streaming.FlinkStreamingJob \
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
    
    # Detener generator
    docker compose stop generator
    
    log "Completed: streaming/$SCENARIO/$RUN_ID"
}

# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

case "$STRATEGY" in
    batch)
        run_batch
        ;;
    microbatch)
        run_microbatch
        ;;
    streaming)
        run_streaming
        ;;
    help|--help|-h)
        echo "╔══════════════════════════════════════════════════════════════╗"
        echo "║              EJECUTAR ESTRATEGIA DE INGESTIÓN              ║"
        echo "╚══════════════════════════════════════════════════════════════╝"
        echo ""
        echo "Uso: ./scripts/run.sh <estrategia> [escenario] [run_id] [trigger]"
        echo ""
        echo "Argumentos:"
        echo "  estrategia   batch | microbatch | streaming"
        echo "  escenario   low-load | medium-load | high-load | burst | extreme-load"
        echo "  run_id      Identificador de la corrida (default: run_1)"
        echo "  trigger     Intervalo para microbatch, ej: '5 seconds' (default)"
        echo ""
        echo "Variables de entorno:"
        echo "  RUN_DURATION_SECONDS   Duración en segundos (default: 300)"
        echo "  FLINK_PARALLELISM     Paralelismo Flink (default: 1)"
        echo ""
        echo "Ejemplos:"
        echo "  ./scripts/run.sh batch low-load run_1"
        echo "  ./scripts/run.sh microbatch medium-load run_1 '5 seconds'"
        echo "  ./scripts/run.sh streaming high-load run_1"
        echo "  RUN_DURATION_SECONDS=180 ./scripts/run.sh batch low-load run_1"
        ;;
    *)
        echo "Estrategia desconocida: $STRATEGY"
        echo "Usa: ./scripts/run.sh help"
        exit 1
        ;;
esac
