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

# Ensure all docker compose commands target the refactored stack.
REQUESTED_MODE=${MODE:-local}
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    # shellcheck source=/dev/null
    source "$ROOT_DIR/.env"
    set +a
fi
export COMPOSE_FILE="$ROOT_DIR/infra/docker/compose/docker-compose.yml"

MODE=$REQUESTED_MODE
SSH_KEY=${SSH_KEY:-$HOME/.ssh/benchmark_aws}
SSH_USER=${SSH_USER:-ubuntu}

remote_compose() {
    local host="$1"
    local compose_file="$2"
    local compose_args="$3"
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${SSH_USER}@${host}" \
        "cd ~/data-ingestion-strategies && docker compose --env-file .env -f ${compose_file} ${compose_args}"
}

remote_shell() {
    local host="$1"
    local command="$2"
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${SSH_USER}@${host}" "${command}"
}

sync_probe_csv_from_producer() {
    if [ "$MODE" != "distributed" ]; then
        return
    fi
    local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
    [ -z "$producer_ip" ] && return
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${SSH_USER}@${producer_ip}:~/data-ingestion-strategies/results/latency_samples.csv" \
        "$PROBE_GLOBAL" >/dev/null 2>&1 || true
}

archive_run_to_sink() {
    if [ "$MODE" != "distributed" ]; then
        return
    fi
    local run_dir="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    local sink_ip="${CLOUD_VM_SINK_PUBLIC_IP:-}"
    [ -z "$sink_ip" ] && return

    remote_shell "$sink_ip" "mkdir -p ~/data-ingestion-strategies/results/${strategy}/${scenario}/${run_id}" >/dev/null 2>&1 || true
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "$run_dir/latency_samples.csv" \
        "$run_dir/prometheus_snapshot.csv" \
        "${SSH_USER}@${sink_ip}:~/data-ingestion-strategies/results/${strategy}/${scenario}/${run_id}/" >/dev/null 2>&1 || true
}

RESULTS_BASE="$ROOT_DIR/results"
PROBE_GLOBAL="$RESULTS_BASE/latency_samples.csv"
PROBE_HEADER="event_id,produced_at,visible_at,latency_ms,strategy,scenario,run_id"
RUN_START_TS=0
RUN_END_TS=0
GENERATOR_DEFAULT_RATE=0
GENERATOR_DEFAULT_PAYLOAD=0
GENERATOR_DEFAULT_SCHEMA=""

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()   { echo -e "${GREEN}[run]${NC} $*"; }
warn()  { echo -e "${YELLOW}[run]${NC} $*"; }
error() { echo -e "${RED}[run]${NC} $*"; }

set_generator_defaults() {
    local scenario="$1"
    case "$scenario" in
        low-load)
            GENERATOR_DEFAULT_RATE=2000
            GENERATOR_DEFAULT_PAYLOAD=512
            GENERATOR_DEFAULT_SCHEMA="iot_sensor"
            ;;
        medium-load)
            GENERATOR_DEFAULT_RATE=10000
            GENERATOR_DEFAULT_PAYLOAD=512
            GENERATOR_DEFAULT_SCHEMA="financial_tick"
            ;;
        high-load)
            GENERATOR_DEFAULT_RATE=30000
            GENERATOR_DEFAULT_PAYLOAD=512
            GENERATOR_DEFAULT_SCHEMA="health_monitor"
            ;;
        extreme-load)
            GENERATOR_DEFAULT_RATE=100000
            GENERATOR_DEFAULT_PAYLOAD=512
            GENERATOR_DEFAULT_SCHEMA="iot_sensor"
            ;;
        burst)
            GENERATOR_DEFAULT_RATE=10000
            GENERATOR_DEFAULT_PAYLOAD=512
            GENERATOR_DEFAULT_SCHEMA="financial_tick"
            ;;
        mixed-payload)
            GENERATOR_DEFAULT_RATE=10000
            GENERATOR_DEFAULT_PAYLOAD=512
            GENERATOR_DEFAULT_SCHEMA="iot_sensor"
            ;;
        *)
            warn "Escenario '$scenario' no reconocido para generator; usando configuracion low-load"
            GENERATOR_DEFAULT_RATE=2000
            GENERATOR_DEFAULT_PAYLOAD=512
            GENERATOR_DEFAULT_SCHEMA="iot_sensor"
            ;;
    esac
}

start_generator_for_scenario() {
    local scenario="$1"
    set_generator_defaults "$scenario"
    log "Iniciando generator (scenario=${scenario}, rate=${GENERATOR_DEFAULT_RATE} ev/s, payload=${GENERATOR_DEFAULT_PAYLOAD}B)"
    (
        export GENERATOR_SCENARIO="$scenario"
        export GENERATOR_EVENT_RATE="$GENERATOR_DEFAULT_RATE"
        export GENERATOR_PAYLOAD_BYTES="$GENERATOR_DEFAULT_PAYLOAD"
        export GENERATOR_EVENT_SCHEMA="$GENERATOR_DEFAULT_SCHEMA"
        export RUN_ID="$RUN_ID"
        export STRATEGY="$STRATEGY"
        if [ "$MODE" = "distributed" ]; then
            local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
            remote_shell "$producer_ip" "docker rm -f tesis-generator >/dev/null 2>&1 || true"
            remote_shell "$producer_ip" \
                "cd ~/data-ingestion-strategies && \
                 GENERATOR_SCENARIO='${GENERATOR_SCENARIO}' \
                 GENERATOR_EVENT_RATE='${GENERATOR_EVENT_RATE}' \
                 GENERATOR_PAYLOAD_BYTES='${GENERATOR_PAYLOAD_BYTES}' \
                 GENERATOR_EVENT_SCHEMA='${GENERATOR_EVENT_SCHEMA}' \
                 RUN_ID='${RUN_ID}' \
                 STRATEGY='${STRATEGY}' \
                 docker compose --env-file .env -f infra/docker/compose/producer.yml up -d --no-deps generator"
        else
            docker compose up -d --no-deps --force-recreate generator
        fi
    )
}

clear_checkpoint_dir() {
    local subdir="$1"
    if [ -z "$subdir" ]; then
        return
    fi
    log "Limpiando checkpoint Spark (${subdir})"
    if [ "$MODE" = "distributed" ]; then
        local compute_ip="${CLOUD_VM_COMPUTE_PUBLIC_IP:-}"
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" \
            "exec -T spark-master sh -c 'rm -rf /opt/spark/checkpoints/${subdir} && mkdir -p /opt/spark/checkpoints/${subdir}'" >/dev/null 2>&1 || true
    else
        docker compose exec -T spark-master sh -c "rm -rf /opt/spark/checkpoints/${subdir} && mkdir -p /opt/spark/checkpoints/${subdir}" 2>/dev/null || true
    fi
}

start_run_timer() {
    RUN_START_TS=$(date +%s)
}

end_run_timer() {
    RUN_END_TS=$(date +%s)
}

reset_probe_csv() {
    mkdir -p "$RESULTS_BASE"
    # Resetear via el container del probe para evitar errores de permisos.
    # El probe escribe continuamente en /results/latency_samples.csv (montado
    # desde el host), por lo que el archivo puede estar bloqueado/en uso.
    if [ "$MODE" = "distributed" ]; then
        local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
        remote_shell "$producer_ip" "mkdir -p ~/data-ingestion-strategies/results && sudo -n chown -R ubuntu:ubuntu ~/data-ingestion-strategies/results >/dev/null 2>&1 || true"
        remote_shell "$producer_ip" "chmod 777 ~/data-ingestion-strategies/results >/dev/null 2>&1 || true; touch ~/data-ingestion-strategies/results/latency_samples.csv >/dev/null 2>&1 || true; chmod 666 ~/data-ingestion-strategies/results/latency_samples.csv >/dev/null 2>&1 || true"
        remote_shell "$producer_ip" "docker rm -f tesis-probe >/dev/null 2>&1 || true"
        remote_compose "$producer_ip" "infra/docker/compose/producer.yml" "up -d --no-deps probe" >/dev/null 2>&1 || true
        local ok=false
        for _ in $(seq 1 6); do
            if remote_compose "$producer_ip" "infra/docker/compose/producer.yml" \
                "exec -T probe sh -c \"echo '${PROBE_HEADER}' > /results/latency_samples.csv\"" 2>/dev/null; then
                ok=true
                break
            fi
            sleep 2
        done
        if [ "$ok" = true ]; then
            log "Probe CSV reseteado (via container remoto)"
        else
            warn "No se pudo resetear probe CSV remoto"
        fi
    elif docker compose exec -T probe sh -c \
        "echo '${PROBE_HEADER}' > /results/latency_samples.csv" 2>/dev/null; then
        log "Probe CSV reseteado (via container)"
    else
        # Fallback: escribir directamente si el container no esta disponible
        printf '%s\n' "$PROBE_HEADER" >"$PROBE_GLOBAL" 2>/dev/null || \
            warn "No se pudo resetear probe CSV"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# copy_probe_csv_to_run  <dest_dir> <strategy> <scenario> <run_id>
#
# FIX (2026-03-18): Extrae SOLO las filas del run actual desde el CSV global
# acumulativo del probe. Antes se copiaba el CSV completo, lo que contaminaba
# los resultados de cada run con datos de todos los runs anteriores.
# ─────────────────────────────────────────────────────────────────────────────
copy_probe_csv_to_run() {
    local dest="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    if [ -f "$PROBE_GLOBAL" ]; then
        # Encabezado + solo las filas de este run especifico
        head -1 "$PROBE_GLOBAL" > "$dest/latency_samples.csv"
        grep -F ",${strategy},${scenario},${run_id}" "$PROBE_GLOBAL" >> "$dest/latency_samples.csv" || true
        local lines
        lines=$(wc -l < "$dest/latency_samples.csv" 2>/dev/null || echo 0)
        log "copy_probe_csv: ${lines} lineas extraidas (${strategy}/${scenario}/${run_id})"
    fi
}

calc_latency_quantile_from_csv() {
    local file="$1"
    local quantile="$2"
    "$PYTHON_BIN" - "$file" "$quantile" <<'PY' 2>/dev/null || echo "NaN"
import csv
import math
import sys

file_path = sys.argv[1]
q = float(sys.argv[2])
values = []
try:
    with open(file_path, newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                values.append(float(row.get('latency_ms', 'nan')))
            except ValueError:
                continue
except FileNotFoundError:
    print('NaN')
    sys.exit(0)

if not values:
    print('NaN')
    sys.exit(0)

values.sort()
n = len(values)
k = (n - 1) * q
f = math.floor(k)
c = math.ceil(k)
if f == c:
    print(values[int(k)])
else:
    lower = values[f]
    upper = values[c]
    print(lower + (k - f) * (upper - lower))
PY
}

PYTHON_BIN=$(command -v python3 2>/dev/null || true)
if [ -n "$PYTHON_BIN" ]; then
    if ! "$PYTHON_BIN" -c "import sys" >/dev/null 2>&1; then
        PYTHON_BIN=""
    fi
fi
if [ -z "$PYTHON_BIN" ]; then
    PYTHON_BIN=$(command -v python 2>/dev/null || true)
fi
if [ -z "$PYTHON_BIN" ]; then
    warn "Python no encontrado en PATH; se intentara usar 'python3'"
    PYTHON_BIN=python3
fi

ensure_services() {
    log "Verificando servicios..."
    if [ "${MODE:-local}" = "distributed" ]; then
        # En modo distribuido, verificamos servicios remotamente via SSH
        local broker_ip="${CLOUD_VM_BROKER_PUBLIC_IP:-}"
        local compute_ip="${CLOUD_VM_COMPUTE_PUBLIC_IP:-}"
        
        if [ -z "$broker_ip" ] || [ -z "$compute_ip" ]; then
            warn "IPs de VMs no configuradas. Ejecuta: source infra/terraform/outputs.env"
            exit 1
        fi
        
        # Verificar Kafka en broker
        if ! remote_shell "$broker_ip" "docker ps | grep -q tesis-kafka" 2>/dev/null; then
            warn "Kafka no esta corriendo en ${broker_ip}"
            exit 1
        fi
        
        # Verificar Spark en compute
        if ! remote_shell "$compute_ip" "docker ps | grep -q tesis-spark-master" 2>/dev/null; then
            warn "Spark no esta corriendo en ${compute_ip}"
            exit 1
        fi
    else
        # Modo local: verificar docker compose local
        if ! docker compose ps kafka postgres 2>/dev/null | grep -q "Up"; then
            warn "Servicios no estan levantados. Ejecuta ./scripts/manage.sh up primero."
            exit 1
        fi
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

# ── Prometheus URL y modo de acceso ──
# En modo distribuido, usamos SSH para acceder a Prometheus (puerto 9090 no está expuesto públicamente)
OUTPUTS_ENV="$ROOT_DIR/infra/terraform/outputs.env"
if [ "${MODE:-local}" = "distributed" ]; then
    if [ -f "$OUTPUTS_ENV" ]; then
        # shellcheck source=/dev/null
        source "$OUTPUTS_ENV"
        # Prometheus se accede via SSH, usamos localhost dentro de la VM
        PROMETHEUS_URL="http://localhost:9090"
        PROMETHEUS_SSH_HOST="${CLOUD_VM_SINK_PUBLIC_IP:-}"
        PROMETHEUS_SSH_KEY="${SSH_KEY:-$HOME/.ssh/benchmark_aws}"
        PROMETHEUS_SSH_USER="${SSH_USER:-ubuntu}"
    else
        warn "outputs.env no encontrado en $OUTPUTS_ENV — Prometheus metrics may not be collected"
        PROMETHEUS_URL="http://localhost:9090"
        PROMETHEUS_SSH_HOST=""
    fi
else
    PROMETHEUS_URL=${PROMETHEUS_URL:-http://localhost:9090}
    PROMETHEUS_SSH_HOST=""
fi

# ═══════════════════════════════════════════════════════════════════
# PROMETHEUS SNAPSHOT
# ═══════════════════════════════════════════════════════════════════
collect_prometheus_snapshot() {
    local run_dir="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    local start_ts=${5:-0}
    local end_ts=${6:-0}

    local out_file="$run_dir/prometheus_snapshot.csv"
    local latency_file="$run_dir/latency_samples.csv"
    local duration=$((end_ts - start_ts))
    if [ "$duration" -le 0 ]; then
        duration=${RUN_DURATION_SECONDS:-60}
    fi
    local prom_window="${duration}s"
    local prom_time="$end_ts"

    log "Recolectando snapshot de Prometheus -> $out_file (ventana=${prom_window})"

    # Helper function to execute curl command (locally or via SSH)
    prom_curl() {
        local url="$1"
        if [ -n "${PROMETHEUS_SSH_HOST:-}" ]; then
            ssh -i "$PROMETHEUS_SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
                "${PROMETHEUS_SSH_USER}@${PROMETHEUS_SSH_HOST}" \
                "curl -sf '$url'" 2>/dev/null
        else
            curl -sf "$url" 2>/dev/null
        fi
    }

    # Check Prometheus health
    if ! prom_curl "${PROMETHEUS_URL}/-/healthy" >/dev/null 2>&1; then
        warn "Prometheus no disponible en ${PROMETHEUS_URL} (SSH=${PROMETHEUS_SSH_HOST:-none}) -- omitiendo snapshot"
        return 0
    fi

    query_prometheus() {
        local promql="$1"
        local eval_time="$2"
        local encoded_query
        encoded_query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$promql'''))" 2>/dev/null)
        local url="${PROMETHEUS_URL}/api/v1/query?query=${encoded_query}"
        if [ -n "$eval_time" ]; then
            url="${url}&time=${eval_time}"
        fi
        local response
        response=$(prom_curl "$url" 2>/dev/null) || { echo "NaN"; return; }
        # Extract the value from JSON response
        echo "$response" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    result = data.get('data', {}).get('result', [])
    if result:
        print(result[0]['value'][1])
    else:
        print('NaN')
except:
    print('NaN')
" 2>/dev/null || echo "NaN"
    }

    query_or_zero() {
        local promql="$1"
        local eval_time="$2"
        local value
        value=$(query_prometheus "$promql" "$eval_time")
        if [[ -z "$value" || "$value" == "NaN" || "$value" == "null" ]]; then
            echo 0
        else
            echo "$value"
        fi
    }

    echo "strategy,scenario,run_id,metric,value,unit" >"$out_file"

    # Container filter: match Docker containers by name prefix "tesis-" (our project containers)
    # The id format varies by cgroup driver: /docker/<id> (cgroupfs) or /system.slice/docker-<id>.scope (systemd)
    local container_filter='name=~"tesis-.*"'
    local cpu_query="sum(increase(container_cpu_usage_seconds_total{job=\"cadvisor\",${container_filter}}[${prom_window}])) / ${duration}"
    local mem_query="sum(avg_over_time(container_memory_rss{job=\"cadvisor\",${container_filter}}[${prom_window}]))"
    local prod_query="sum(increase(kafka_produced_messages_total{scenario=\"${scenario}\",run_id=\"${run_id}\",strategy=\"${strategy}\"}[${prom_window}])) / ${duration}"

    # Backward compatibility for historical metric labels (scenario-only)
    local prod_query_legacy="sum(increase(kafka_produced_messages_total{scenario=\"${scenario}\"}[${prom_window}])) / ${duration}"
    local kafka_lag_query='max_over_time(kafka_consumergroup_lag{topic="events"}['"${prom_window}"'])'

    CPU_TOTAL=$(query_or_zero "$cpu_query" "$prom_time")
    MEM_TOTAL=$(query_or_zero "$mem_query" "$prom_time")
    TPUT_PRODUCED=$(query_or_zero "$prod_query" "$prom_time")
    if [ "${TPUT_PRODUCED:-0}" = "0" ] || [ "${TPUT_PRODUCED:-0}" = "0.0" ]; then
        TPUT_PRODUCED=$(query_or_zero "$prod_query_legacy" "$prom_time")
    fi
    KAFKA_LAG=$(query_or_zero "$kafka_lag_query" "$prom_time")

    echo "${strategy},${scenario},${run_id},cpu_total_cores,${CPU_TOTAL},cores" >>"$out_file"
    echo "${strategy},${scenario},${run_id},mem_rss_bytes,${MEM_TOTAL},bytes" >>"$out_file"
    echo "${strategy},${scenario},${run_id},tput_produced_eps,${TPUT_PRODUCED},events/s" >>"$out_file"

    local sample_count=0
    local tput_sink=0
    local lat_p50=0
    local lat_p95=0
    local lat_p99=0

    if [ -f "$latency_file" ]; then
        sample_count=$(($(wc -l <"$latency_file") - 1))
        if [ "$sample_count" -lt 0 ]; then
            sample_count=0
        fi
        if [ "$sample_count" -gt 0 ] && [ "$duration" -gt 0 ]; then
            tput_sink=$("$PYTHON_BIN" - "$sample_count" "$duration" <<'PY'
import sys

rows = float(sys.argv[1])
duration = float(sys.argv[2])
if duration <= 0:
    print('0')
else:
    print(rows / duration)
PY
)
            lat_p50=$(calc_latency_quantile_from_csv "$latency_file" 0.50)
            lat_p95=$(calc_latency_quantile_from_csv "$latency_file" 0.95)
            lat_p99=$(calc_latency_quantile_from_csv "$latency_file" 0.99)
        fi
    fi

    echo "${strategy},${scenario},${run_id},tput_sink_eps,${tput_sink},events/s" >>"$out_file"
    echo "${strategy},${scenario},${run_id},kafka_consumer_lag,${KAFKA_LAG},messages" >>"$out_file"
    echo "${strategy},${scenario},${run_id},latency_p50_ms,${lat_p50},ms" >>"$out_file"
    echo "${strategy},${scenario},${run_id},latency_p95_ms,${lat_p95},ms" >>"$out_file"
    echo "${strategy},${scenario},${run_id},latency_p99_ms,${lat_p99},ms" >>"$out_file"

    log "Snapshot guardado: $(wc -l <"$out_file") metricas en $out_file"
}

# ═══════════════════════════════════════════════════════════════════
# RUN BATCH
# ═══════════════════════════════════════════════════════════════════
run_batch() {
    ensure_services

    local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
    local broker_ip="${CLOUD_VM_BROKER_PUBLIC_IP:-}"
    local compute_ip="${CLOUD_VM_COMPUTE_PUBLIC_IP:-}"
    local sink_ip="${CLOUD_VM_SINK_PUBLIC_IP:-}"

    log "Ejecutando BATCH: scenario=$SCENARIO run_id=$RUN_ID duration=${RUN_DURATION_SECONDS}s"

    # Limpiar antes
    log "Limpiando entorno y deteniendo jobs previos..."
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" "restart flink-jobmanager flink-taskmanager" >/dev/null 2>&1 || true
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" "exec -T spark-master sh -c 'pkill -f spark || true'" >/dev/null 2>&1 || true
        remote_compose "$broker_ip" "infra/docker/compose/broker.yml" "exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092" >/dev/null 2>&1 || true
        remote_compose "$broker_ip" "infra/docker/compose/broker.yml" "exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists" >/dev/null 2>&1 || true
        remote_compose "$sink_ip" "infra/docker/compose/sink.yml" "exec -T postgres psql -U benchmark -d benchmark -c \"TRUNCATE TABLE events RESTART IDENTITY CASCADE;\"" >/dev/null 2>&1 || true
    else
        docker compose restart flink-jobmanager flink-taskmanager >/dev/null 2>&1 || true
        docker compose exec -T spark-master sh -c 'pkill -f spark || true' >/dev/null 2>&1 || true
        docker compose exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
        docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true
        docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    fi
    reset_probe_csv
    start_run_timer

    echo "────────────────────────────────────────────────────────────"
    echo "[run_batch] strategy=batch  scenario=$SCENARIO  run_id=$RUN_ID"
    echo "────────────────────────────────────────────────────────────"

    ACCUMULATE_TIME=$RUN_DURATION_SECONDS
    log "Fase de acumulacion: generator corre por ${ACCUMULATE_TIME}s antes del batch job"

    echo "[run_batch] Accumulation phase: generator runs for ${ACCUMULATE_TIME}s before batch job executes"
    echo "[run_batch] Accumulating..."

    # Iniciar generator en background
    start_generator_for_scenario "$SCENARIO"

    # Esperar acumulacion
    for i in $(seq 1 $((ACCUMULATE_TIME / 60))); do
        ELAPSED=$((i * 60))
        REMAINING=$((ACCUMULATE_TIME - ELAPSED))
        echo "[run_batch] Accumulating... ${ELAPSED}s elapsed, ${REMAINING}s remaining"
        sleep 60
    done

    # Detener generator
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$producer_ip" "infra/docker/compose/producer.yml" "stop generator" >/dev/null 2>&1 || true
    else
        docker compose stop generator
    fi

    # Ejecutar Spark Batch
    log "Ejecutando Spark Batch..."
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" \
            "exec -T spark-master /opt/spark/bin/spark-submit --class org.tesis.batch.SparkBatchJob --master spark://spark-master:${MASTER_PORT} /opt/spark/jobs/batch/batch-job.jar --scenario=${SCENARIO} --run.id=${RUN_ID} --kafka.bootstrap.servers=${CLOUD_VM_BROKER_IP}:9092 --kafka.topic=events --postgres.url=jdbc:postgresql://${CLOUD_VM_SINK_IP}:5432/${POSTGRES_DB_NAME} --postgres.user=${POSTGRES_USER_NAME} --postgres.password=${POSTGRES_PASSWORD_VALUE} --run.duration.seconds=${RUN_DURATION_SECONDS}"
    else
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
    fi

    end_run_timer
    RUN_DIR="${ROOT_DIR}/results/batch/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    sync_probe_csv_from_producer
    copy_probe_csv_to_run "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"
    collect_prometheus_snapshot "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    archive_run_to_sink "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"

    log "Completed: batch/$SCENARIO/$RUN_ID"
}

# ═══════════════════════════════════════════════════════════════════
# RUN MICROBATCH
# ═══════════════════════════════════════════════════════════════════
run_microbatch() {
    ensure_services

    local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
    local broker_ip="${CLOUD_VM_BROKER_PUBLIC_IP:-}"
    local compute_ip="${CLOUD_VM_COMPUTE_PUBLIC_IP:-}"
    local sink_ip="${CLOUD_VM_SINK_PUBLIC_IP:-}"

    log "Ejecutando MICROBATCH: scenario=$SCENARIO run_id=$RUN_ID trigger=$TRIGGER_INTERVAL"

    # Limpiar antes
    log "Limpiando entorno y deteniendo jobs previos..."
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" "restart flink-jobmanager flink-taskmanager" >/dev/null 2>&1 || true
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" "exec -T spark-master sh -c 'pkill -f spark || true'" >/dev/null 2>&1 || true
        remote_compose "$broker_ip" "infra/docker/compose/broker.yml" "exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092" >/dev/null 2>&1 || true
        remote_compose "$broker_ip" "infra/docker/compose/broker.yml" "exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists" >/dev/null 2>&1 || true
        remote_compose "$sink_ip" "infra/docker/compose/sink.yml" "exec -T postgres psql -U benchmark -d benchmark -c \"TRUNCATE TABLE events RESTART IDENTITY CASCADE;\"" >/dev/null 2>&1 || true
    else
        docker compose restart flink-jobmanager flink-taskmanager >/dev/null 2>&1 || true
        docker compose exec -T spark-master sh -c 'pkill -f spark || true' >/dev/null 2>&1 || true
        docker compose exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
        docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true
        docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    fi
    reset_probe_csv
    clear_checkpoint_dir "microbatch"
    start_run_timer

    echo "────────────────────────────────────────────────────────────"
    echo "[run_microbatch] strategy=microbatch  scenario=$SCENARIO  run_id=$RUN_ID  trigger=$TRIGGER_INTERVAL"
    echo "────────────────────────────────────────────────────────────"

    # Iniciar generator y microbatch
    start_generator_for_scenario "$SCENARIO"

    if [ "$MODE" = "distributed" ]; then
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" \
            "exec -T spark-master /opt/spark/bin/spark-submit --class org.tesis.microbatch.SparkStructuredJob --master spark://spark-master:${MASTER_PORT} /opt/spark/jobs/microbatch/microbatch-job.jar --scenario=${SCENARIO} --run.id=${RUN_ID} --trigger.interval=\"${TRIGGER_INTERVAL}\" --kafka.bootstrap.servers=${CLOUD_VM_BROKER_IP}:9092 --kafka.topic=events --checkpoint.location=/opt/spark/checkpoints/microbatch --postgres.url=jdbc:postgresql://${CLOUD_VM_SINK_IP}:5432/${POSTGRES_DB_NAME} --postgres.user=${POSTGRES_USER_NAME} --postgres.password=${POSTGRES_PASSWORD_VALUE} --run.duration.seconds=$((RUN_DURATION_SECONDS + 20))"
    else
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
        --run.duration.seconds=$((RUN_DURATION_SECONDS + 20))
    fi

    # Detener generator
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$producer_ip" "infra/docker/compose/producer.yml" "stop generator" >/dev/null 2>&1 || true
    else
        docker compose stop generator
    fi

    RUN_DIR="${ROOT_DIR}/results/microbatch/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    end_run_timer
    sync_probe_csv_from_producer
    copy_probe_csv_to_run "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"
    collect_prometheus_snapshot "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    archive_run_to_sink "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"

    log "Completed: microbatch/$SCENARIO/$RUN_ID"
}

# ═══════════════════════════════════════════════════════════════════
# RUN STREAMING
# ═══════════════════════════════════════════════════════════════════
run_streaming() {
    ensure_services

    local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
    local broker_ip="${CLOUD_VM_BROKER_PUBLIC_IP:-}"
    local compute_ip="${CLOUD_VM_COMPUTE_PUBLIC_IP:-}"
    local sink_ip="${CLOUD_VM_SINK_PUBLIC_IP:-}"

    log "Ejecutando STREAMING: scenario=$SCENARIO run_id=$RUN_ID"

    # Limpiar antes
    log "Limpiando entorno..."
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$broker_ip" "infra/docker/compose/broker.yml" "exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092" >/dev/null 2>&1 || true
        remote_compose "$broker_ip" "infra/docker/compose/broker.yml" "exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists" >/dev/null 2>&1 || true
        remote_compose "$sink_ip" "infra/docker/compose/sink.yml" "exec -T postgres psql -U benchmark -d benchmark -c \"TRUNCATE TABLE events RESTART IDENTITY CASCADE;\"" >/dev/null 2>&1 || true
    else
        docker compose exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
        docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true
        docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    fi
    reset_probe_csv
    start_run_timer

    echo "────────────────────────────────────────────────────────────"
    echo "[run_streaming] strategy=streaming  scenario=$SCENARIO  run_id=$RUN_ID"
    echo "────────────────────────────────────────────────────────────"

    # Reiniciar Flink para estado limpio
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" "restart flink-jobmanager flink-taskmanager" >/dev/null 2>&1
    else
        docker compose restart flink-jobmanager flink-taskmanager >/dev/null 2>&1
    fi

    # Esperar Flink
    log "Esperando Flink..."
    for i in $(seq 1 45); do
        if [ "$MODE" = "distributed" ]; then
            JM_STATE=$(remote_shell "$compute_ip" "docker inspect tesis-flink-jobmanager --format '{{.State.Status}}'" 2>/dev/null || echo "missing")
        else
            JM_STATE=$(docker inspect tesis-ingestion-flink-jobmanager-1 --format '{{.State.Status}}' 2>/dev/null || echo "missing")
        fi
        if [ "$JM_STATE" = "running" ]; then
            if [ "$MODE" = "distributed" ]; then
                if remote_compose "$compute_ip" "infra/docker/compose/compute.yml" "exec -T flink-jobmanager sh -c 'grep -qi :1F91 /proc/net/tcp6 2>/dev/null || grep -qi :1F91 /proc/net/tcp 2>/dev/null'"; then
                    log "Flink listo"
                    break
                fi
            elif docker compose exec -T flink-jobmanager sh -c "grep -qi ':1F91 ' /proc/net/tcp6 2>/dev/null || grep -qi ':1F91 ' /proc/net/tcp 2>/dev/null"; then
                log "Flink listo"
                break
            fi
        fi
        if [ $i -eq 45 ]; then
            error "Flink no esta listo"
            exit 1
        fi
        sleep 2
    done

    # Iniciar generator
    start_generator_for_scenario "$SCENARIO"

    FLINK_DETACH_FLAG=""
    if [ "$FLINK_DETACHED" = "true" ]; then
        FLINK_DETACH_FLAG="-d"
    fi

    if [ "$MODE" = "distributed" ]; then
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" \
            "exec -T flink-jobmanager /opt/flink/bin/flink run ${FLINK_DETACH_FLAG} -c org.tesis.streaming.FlinkStreamingJob -p ${FLINK_PARALLELISM_VALUE} /opt/flink/usrlib/streaming-job.jar --scenario ${SCENARIO} --run.id ${RUN_ID} --kafka.bootstrap.servers ${CLOUD_VM_BROKER_IP}:9092 --kafka.topic events --postgres.url jdbc:postgresql://${CLOUD_VM_SINK_IP}:5432/${POSTGRES_DB_NAME} --postgres.user ${POSTGRES_USER_NAME} --postgres.password ${POSTGRES_PASSWORD_VALUE} --run.duration.seconds $((RUN_DURATION_SECONDS + 20))"
    else
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
        --run.duration.seconds $((RUN_DURATION_SECONDS + 20))
    fi

    # Detener generator
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$producer_ip" "infra/docker/compose/producer.yml" "stop generator" >/dev/null 2>&1 || true
    else
        docker compose stop generator
    fi

    RUN_DIR="${ROOT_DIR}/results/streaming/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    end_run_timer
    sync_probe_csv_from_producer
    copy_probe_csv_to_run "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID"
    collect_prometheus_snapshot "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    archive_run_to_sink "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID"

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
        echo "║              EJECUTAR ESTRATEGIA DE INGESTION              ║"
        echo "╚══════════════════════════════════════════════════════════════╝"
        echo ""
        echo "Uso: ./scripts/run.sh <estrategia> [escenario] [run_id] [trigger]"
        echo ""
        echo "Argumentos:"
        echo "  estrategia   batch | microbatch | streaming"
        echo "  escenario    low-load | medium-load | high-load | burst | extreme-load"
        echo "  run_id       Identificador de la corrida (default: run_1)"
        echo "  trigger      Intervalo para microbatch, ej: '5 seconds' (default)"
        echo ""
        echo "Variables de entorno:"
        echo "  RUN_DURATION_SECONDS   Duracion en segundos (default: 300)"
        echo "  FLINK_PARALLELISM      Paralelismo Flink (default: 1)"
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
