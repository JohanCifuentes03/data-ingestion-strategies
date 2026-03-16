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
PROMETHEUS_URL=${PROMETHEUS_URL:-http://localhost:9090}

# ═══════════════════════════════════════════════════════════════════
# PROMETHEUS SNAPSHOT
# ═══════════════════════════════════════════════════════════════════
# Consulta el endpoint /api/v1/query de Prometheus al finalizar cada
# corrida y guarda las métricas relevantes en prometheus_snapshot.csv
# dentro del directorio de resultados del run.
collect_prometheus_snapshot() {
    local run_dir="$1"      # directorio destino, ej: results/batch/low-load/run_1
    local strategy="$2"     # batch | microbatch | streaming
    local scenario="$3"
    local run_id="$4"

    local out_file="$run_dir/prometheus_snapshot.csv"

    log "Recolectando snapshot de Prometheus → $out_file"

    # Verificar que Prometheus esté disponible
    if ! curl -sf "${PROMETHEUS_URL}/-/healthy" >/dev/null 2>&1; then
        warn "Prometheus no disponible en ${PROMETHEUS_URL} — omitiendo snapshot"
        return 0
    fi

    # Función auxiliar: consulta una métrica PromQL y devuelve "value"
    # Uso: query_prometheus "<promql>"
    query_prometheus() {
        local promql="$1"
        local encoded
        encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$promql" 2>/dev/null \
            || printf '%s' "$promql" | sed 's/ /%20/g; s/{/%7B/g; s/}/%7D/g; s/"/%22/g; s/=/%3D/g; s/,/%2C/g')
        local result
        result=$(curl -sf "${PROMETHEUS_URL}/api/v1/query?query=${encoded}" 2>/dev/null || echo "")
        # Extraer primer valor numérico del JSON
        echo "$result" | python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
    v = d['data']['result']
    if v:
        print(v[0]['value'][1])
    else:
        print('NaN')
except:
    print('NaN')
" 2>/dev/null || echo "NaN"
    }

    # Encabezado CSV
    echo "strategy,scenario,run_id,metric,value,unit" > "$out_file"

    # ── CPU total del namespace (todos los contenedores tesis-ingestion-*) ──
    CPU_TOTAL=$(query_prometheus 'sum(rate(container_cpu_usage_seconds_total{name=~"tesis-ingestion-.*"}[2m]))')
    echo "${strategy},${scenario},${run_id},cpu_total_cores,${CPU_TOTAL},cores" >> "$out_file"

    # ── Memoria RSS total ──
    MEM_TOTAL=$(query_prometheus 'sum(container_memory_rss{name=~"tesis-ingestion-.*"})')
    echo "${strategy},${scenario},${run_id},mem_rss_bytes,${MEM_TOTAL},bytes" >> "$out_file"

    # ── Throughput: mensajes producidos por segundo (generator) ──
    TPUT_PRODUCED=$(query_prometheus 'rate(kafka_produced_messages_total[2m])')
    echo "${strategy},${scenario},${run_id},tput_produced_eps,${TPUT_PRODUCED},events/s" >> "$out_file"

    # ── Throughput escritura al sink: filas insertadas por segundo ──
    TPUT_SINK=$(query_prometheus 'rate(sink_rows_written_total[2m])')
    echo "${strategy},${scenario},${run_id},tput_sink_eps,${TPUT_SINK},events/s" >> "$out_file"

    # ── Kafka consumer lag (suma todos los grupos/particiones del topic events) ──
    KAFKA_LAG=$(query_prometheus 'sum(kafka_consumergroup_lag{topic="events"})')
    echo "${strategy},${scenario},${run_id},kafka_consumer_lag,${KAFKA_LAG},messages" >> "$out_file"

    # ── Latencia p50 / p95 / p99 en ms (desde histograma del probe si existe) ──
    LAT_P50=$(query_prometheus 'histogram_quantile(0.50, rate(probe_latency_ms_bucket[2m]))')
    LAT_P95=$(query_prometheus 'histogram_quantile(0.95, rate(probe_latency_ms_bucket[2m]))')
    LAT_P99=$(query_prometheus 'histogram_quantile(0.99, rate(probe_latency_ms_bucket[2m]))')
    echo "${strategy},${scenario},${run_id},latency_p50_ms,${LAT_P50},ms" >> "$out_file"
    echo "${strategy},${scenario},${run_id},latency_p95_ms,${LAT_P95},ms" >> "$out_file"
    echo "${strategy},${scenario},${run_id},latency_p99_ms,${LAT_P99},ms" >> "$out_file"

    log "Snapshot guardado: $(wc -l < "$out_file") métricas en $out_file"
}

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
    
    # Exportar snapshot de Prometheus al directorio del run
    RUN_DIR="${ROOT_DIR}/results/batch/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    collect_prometheus_snapshot "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"

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
    
    # Exportar snapshot de Prometheus al directorio del run
    RUN_DIR="${ROOT_DIR}/results/microbatch/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    collect_prometheus_snapshot "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"

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
    
    # Exportar snapshot de Prometheus al directorio del run
    RUN_DIR="${ROOT_DIR}/results/streaming/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    collect_prometheus_snapshot "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID"

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
