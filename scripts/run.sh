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
COMPUTE_REGION=${COMPUTE_REGION:-primary}
BRAZIL_NETWORK_MODE=${BRAZIL_NETWORK_MODE:-private}
LOAD_PROFILE=${LOAD_PROFILE:-constant}

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
        "${SSH_USER}@${producer_ip}:~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}/latency_samples.csv" \
        "$PROBE_GLOBAL" >/dev/null 2>&1 || true
}

sync_generator_summary_from_producer() {
    if [ "$MODE" != "distributed" ]; then
        return
    fi
    local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
    [ -z "$producer_ip" ] && return
    
    log "Sincronizando generator_summary.json desde producer ($producer_ip)..."
    
    # First check if file exists on remote
    remote_shell "$producer_ip" "ls -la ~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}/generator_summary.json 2>/dev/null || echo 'FILE_NOT_FOUND'" | head -1
    
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${SSH_USER}@${producer_ip}:~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}/generator_summary.json" \
        "$GENERATOR_SUMMARY_GLOBAL" 2>&1
    
    if [ $? -eq 0 ] && [ -f "$GENERATOR_SUMMARY_GLOBAL" ]; then
        log "Generator summary sincronizado exitosamente"
    else
        warn "Fallo al sincronizar generator_summary.json desde producer"
    fi
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

    remote_shell "$sink_ip" "mkdir -p ~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}/${strategy}/${scenario}/${run_id}" >/dev/null 2>&1 || true
    local candidates=(
        "$run_dir/latency_samples.csv"
        "$run_dir/prometheus_snapshot.csv"
        "$run_dir/generator_summary.json"
        "$run_dir/run_metadata.json"
        "$run_dir/run_summary.json"
        "$run_dir/kafka_lag_timeseries.csv"
        "$run_dir/resources_timeseries.csv"
    )
    local files=()
    local f
    for f in "${candidates[@]}"; do
        if [ -f "$f" ]; then
            files+=("$f")
        fi
    done
    [ -f "$run_dir/cloudwatch_snapshot.csv" ] && files+=("$run_dir/cloudwatch_snapshot.csv")
    if [ ${#files[@]} -eq 0 ]; then
        return
    fi
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${files[@]}" \
        "${SSH_USER}@${sink_ip}:~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}/${strategy}/${scenario}/${run_id}/" >/dev/null 2>&1 || true
}

RESULTS_BASE="${RESULTS_BASE:-$ROOT_DIR/results}"
case "$RESULTS_BASE" in
    /*) ;;
    *) RESULTS_BASE="$ROOT_DIR/$RESULTS_BASE" ;;
esac
REMOTE_RESULTS_BASE_NAME="$(basename "$RESULTS_BASE")"
PROBE_GLOBAL="$RESULTS_BASE/latency_samples.csv"
GENERATOR_SUMMARY_GLOBAL="$RESULTS_BASE/generator_summary.json"
PROBE_HEADER="event_id,strategy,scenario,run_id,produced_at,visible_at,latency_ms"
RUN_START_TS=0
RUN_END_TS=0
GENERATION_START_TS=0
GENERATION_END_TS=0
PROCESSING_START_TS=0
PROCESSING_END_TS=0
GENERATOR_DEFAULT_RATE=0
GENERATOR_DEFAULT_PAYLOAD=0
GENERATOR_DEFAULT_SCHEMA=""
RUN_TOPIC="events"

# ── Brazil / interregional resolvers ─────────────────────────────────

resolve_compute_public_ip() {
    if [ "${COMPUTE_REGION:-primary}" = "brazil" ]; then
        echo "${CLOUD_VM_COMPUTE_BRAZIL_PUBLIC_IP:-}"
    else
        echo "${CLOUD_VM_COMPUTE_PUBLIC_IP:-}"
    fi
}

resolve_broker_host() {
    if [ "${COMPUTE_REGION:-primary}" = "brazil" ]; then
        if [ "${BRAZIL_NETWORK_MODE:-private}" = "private" ]; then
            echo "${CLOUD_VM_BROKER_IP:-}"
        else
            echo "${CLOUD_VM_BROKER_PUBLIC_IP:-}"
        fi
    else
        echo "${CLOUD_VM_BROKER_IP:-}"
    fi
}

resolve_broker_port() {
    if [ "${COMPUTE_REGION:-primary}" = "brazil" ]; then
        if [ "${BRAZIL_NETWORK_MODE:-private}" = "private" ]; then
            echo "9092"
        else
            echo "19092"
        fi
    else
        echo "9092"
    fi
}

resolve_sink_host() {
    if [ "${COMPUTE_REGION:-primary}" = "brazil" ]; then
        if [ "${BRAZIL_NETWORK_MODE:-private}" = "private" ]; then
            echo "${CLOUD_VM_SINK_IP:-}"
        else
            echo "${CLOUD_VM_SINK_PUBLIC_IP:-}"
        fi
    else
        echo "${CLOUD_VM_SINK_IP:-}"
    fi
}

sanitize_topic_part() {
    printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '_'
}

set_run_topic() {
    local strategy_part scenario_part run_part
    strategy_part=$(sanitize_topic_part "$STRATEGY")
    scenario_part=$(sanitize_topic_part "$SCENARIO")
    run_part=$(sanitize_topic_part "$RUN_ID")
    RUN_TOPIC="events_${strategy_part}_${scenario_part}_${run_part}_$(date +%s)"
}

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
            GENERATOR_DEFAULT_PAYLOAD=1500
            GENERATOR_DEFAULT_SCHEMA="iot_sensor"
            ;;
        medium-load)
            GENERATOR_DEFAULT_RATE=10000
            GENERATOR_DEFAULT_PAYLOAD=1500
            GENERATOR_DEFAULT_SCHEMA="financial_tick"
            ;;
        high-load)
            GENERATOR_DEFAULT_RATE=30000
            GENERATOR_DEFAULT_PAYLOAD=1500
            GENERATOR_DEFAULT_SCHEMA="health_monitor"
            ;;
        bursty-load|bursty-load-br-compute)
            GENERATOR_DEFAULT_RATE=10000
            GENERATOR_DEFAULT_PAYLOAD=1500
            GENERATOR_DEFAULT_SCHEMA="financial_tick"
            ;;
        cyclic-load-br-compute)
            GENERATOR_DEFAULT_RATE=3000
            GENERATOR_DEFAULT_PAYLOAD=1500
            GENERATOR_DEFAULT_SCHEMA="financial_tick"
            ;;
        *)
            warn "Escenario '$scenario' no reconocido para generator; usando configuracion low-load"
            GENERATOR_DEFAULT_RATE=2000
            GENERATOR_DEFAULT_PAYLOAD=1500
            GENERATOR_DEFAULT_SCHEMA="iot_sensor"
            ;;
    esac
}

prepare_run_topic() {
    log "Preparando topic Kafka aislado para la corrida: ${RUN_TOPIC}"
    if [ "$MODE" = "distributed" ]; then
        local broker_ip="${CLOUD_VM_BROKER_PUBLIC_IP:-}"
        remote_compose "$broker_ip" "infra/docker/compose/broker.yml" \
            "exec -T kafka kafka-topics --create --topic ${RUN_TOPIC} --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists" >/dev/null
    else
        docker compose exec -T kafka kafka-topics --create --topic "$RUN_TOPIC" --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists >/dev/null 2>&1
    fi
}

start_generator_for_scenario() {
    local scenario="$1"
    set_generator_defaults "$scenario"
    case "$scenario" in
        low-load)    export GENERATOR_THREADS="${GENERATOR_THREADS:-4}" ;;
        medium-load) export GENERATOR_THREADS="${GENERATOR_THREADS:-8}" ;;
        high-load)   export GENERATOR_THREADS="${GENERATOR_THREADS:-16}" ;;
        *)           export GENERATOR_THREADS="${GENERATOR_THREADS:-4}" ;;
    esac
    log "Iniciando generator (scenario=${scenario}, rate=${GENERATOR_DEFAULT_RATE} ev/s, payload=${GENERATOR_DEFAULT_PAYLOAD}B, threads=${GENERATOR_THREADS})"
    (
        export GENERATOR_SCENARIO="$scenario"
        export GENERATOR_EVENT_RATE="$GENERATOR_DEFAULT_RATE"
        export GENERATOR_PAYLOAD_BYTES="$GENERATOR_DEFAULT_PAYLOAD"
        export GENERATOR_EVENT_SCHEMA="$GENERATOR_DEFAULT_SCHEMA"
        export TOPIC_NAME="$RUN_TOPIC"
        export RUN_ID="$RUN_ID"
        export STRATEGY="$STRATEGY"
        export GENERATOR_RUN_DURATION_SECONDS="${RUN_DURATION_SECONDS}"
        export GENERATOR_WARMUP_SECONDS=0
        export LOAD_PROFILE="${LOAD_PROFILE:-constant}"
        if [ "$MODE" = "distributed" ]; then
            local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
            remote_shell "$producer_ip" "docker rm -f tesis-generator >/dev/null 2>&1 || true"
            remote_shell "$producer_ip" \
                "cd ~/data-ingestion-strategies && \
                 GENERATOR_SCENARIO='${GENERATOR_SCENARIO}' \
                 GENERATOR_EVENT_RATE='${GENERATOR_EVENT_RATE}' \
                 GENERATOR_PAYLOAD_BYTES='${GENERATOR_PAYLOAD_BYTES}' \
                 GENERATOR_EVENT_SCHEMA='${GENERATOR_EVENT_SCHEMA}' \
                 TOPIC_NAME='${TOPIC_NAME}' \
                 RUN_ID='${RUN_ID}' \
                 STRATEGY='${STRATEGY}' \
                 GENERATOR_RUN_DURATION_SECONDS='${GENERATOR_RUN_DURATION_SECONDS}' \
                 GENERATOR_WARMUP_SECONDS='${GENERATOR_WARMUP_SECONDS}' \
                 GENERATOR_SUMMARY_PATH='/results/generator_summary.json' \
                 GENERATOR_THREADS='${GENERATOR_THREADS}' \
                 LOAD_PROFILE='${LOAD_PROFILE}' \
                 RESULTS_VOLUME_HOST='/home/ubuntu/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}' \
                 docker compose --env-file .env -f infra/docker/compose/producer.yml up -d --no-deps generator"
        else
            docker compose up -d --no-deps --force-recreate generator
        fi
    )
}

copy_generator_summary_to_run() {
    local run_dir="$1"
    if [ -f "$GENERATOR_SUMMARY_GLOBAL" ]; then
        cp "$GENERATOR_SUMMARY_GLOBAL" "$run_dir/generator_summary.json"
    else
        warn "generator_summary.json no encontrado en $GENERATOR_SUMMARY_GLOBAL"
    fi
}

stop_generator_if_running() {
    if [ "$MODE" = "distributed" ]; then
        local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
        remote_compose "$producer_ip" "infra/docker/compose/producer.yml" "stop generator" >/dev/null 2>&1 || true
    else
        docker compose stop generator >/dev/null 2>&1 || true
    fi
}

wait_for_generator_exit() {
    local timeout_seconds="${1:-90}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout_seconds" ]; do
        if [ "$MODE" = "distributed" ]; then
            local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
            local state
            state=$(remote_shell "$producer_ip" "docker inspect tesis-generator --format '{{.State.Status}}' 2>/dev/null || echo missing" 2>/dev/null || echo missing)
            if [ "$state" = "exited" ] || [ "$state" = "missing" ]; then
                return 0
            fi
        else
            local state
            state=$(docker inspect tesis-ingestion-generator-1 --format '{{.State.Status}}' 2>/dev/null || echo missing)
            if [ "$state" = "exited" ] || [ "$state" = "missing" ]; then
                return 0
            fi
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

require_generator_summary() {
    local run_dir="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    local summary_file="$run_dir/generator_summary.json"
    if [ ! -f "$summary_file" ]; then
        error "Falta generator_summary.json para ${strategy}/${scenario}/${run_id}"
        return 1
    fi
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

mark_generation_start() {
    GENERATION_START_TS=$(date +%s)
}

mark_generation_end() {
    GENERATION_END_TS=$(date +%s)
}

mark_processing_start() {
    PROCESSING_START_TS=$(date +%s)
}

mark_processing_end() {
    PROCESSING_END_TS=$(date +%s)
}

ensure_run_markers() {
    if [ "$GENERATION_START_TS" -le 0 ]; then
        GENERATION_START_TS=$RUN_START_TS
    fi
    if [ "$GENERATION_END_TS" -le 0 ]; then
        GENERATION_END_TS=$((GENERATION_START_TS + RUN_DURATION_SECONDS))
        if [ "$GENERATION_END_TS" -gt "$RUN_END_TS" ]; then
            GENERATION_END_TS=$RUN_END_TS
        fi
    fi
    if [ "$PROCESSING_START_TS" -le 0 ]; then
        PROCESSING_START_TS=$GENERATION_END_TS
    fi
    if [ "$PROCESSING_END_TS" -le 0 ]; then
        PROCESSING_END_TS=$RUN_END_TS
    fi
}

get_git_commit() {
    git rev-parse HEAD 2>/dev/null || echo "unknown"
}

get_git_branch() {
    git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown"
}

write_run_metadata() {
    local run_dir="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    local metadata_file="$run_dir/run_metadata.json"
    local official_duration="${RUN_DURATION_SECONDS:-300}"
    local git_commit
    local git_branch
    git_commit=$(get_git_commit)
    git_branch=$(get_git_branch)

    ensure_run_markers

    "$PYTHON_BIN" - "$metadata_file" <<PY
import json
from datetime import datetime, timezone

data = {
    "strategy": "${strategy}",
    "scenario": "${scenario}",
    "run_id": "${run_id}",
    "target_eps": int(${GENERATOR_DEFAULT_RATE:-0}),
    "official_duration_seconds": int(${official_duration}),
    "warmup_seconds": int(${WARMUP_SECONDS:-30}),
    "cooldown_seconds": int(${COOLDOWN_SECONDS:-30}),
    "payload_bytes": int(${GENERATOR_DEFAULT_PAYLOAD:-1500}),
    "kafka_topic": "${RUN_TOPIC}",
    "kafka_partitions": 12,
    "sink": "postgresql",
    "generation_start_ts": int(${GENERATION_START_TS}),
    "generation_end_ts": int(${GENERATION_END_TS}),
    "processing_start_ts": int(${PROCESSING_START_TS}),
    "processing_end_ts": int(${PROCESSING_END_TS}),
    "run_start_ts": int(${RUN_START_TS}),
    "run_end_ts": int(${RUN_END_TS}),
    "git_commit": "${git_commit}",
    "branch": "${git_branch}",
    "started_at_utc": datetime.fromtimestamp(int(${RUN_START_TS}), tz=timezone.utc).isoformat(),
    "load_profile": "${LOAD_PROFILE:-constant}",
    "compute_region": "${COMPUTE_REGION:-primary}",
    "brazil_network_mode": "${BRAZIL_NETWORK_MODE:-private}",
    "broker_endpoint_used": "$(resolve_broker_host):$(resolve_broker_port)",
    "sink_endpoint_used": "$(resolve_sink_host):5432",
}
with open("${metadata_file}", "w", encoding="utf-8") as handle:
    json.dump(data, handle, indent=2)
PY
}

count_visible_events_csv() {
    local latency_file="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"

    if [ ! -f "$latency_file" ]; then
        echo 0
        return
    fi

    "$PYTHON_BIN" - "$latency_file" "$strategy" "$scenario" "$run_id" <<'PY'
import csv
import sys

path, strategy, scenario, run_id = sys.argv[1:]
count = 0
with open(path, newline="", encoding="utf-8") as handle:
    reader = csv.DictReader(handle)
    for row in reader:
        if row.get("strategy") == strategy and row.get("scenario") == scenario and row.get("run_id") == run_id:
            count += 1
print(count)
PY
}

db_count_visible_events() {
    local strategy="$1"
    local scenario="$2"
    local run_id="$3"
    local query="SELECT COUNT(*) FROM events WHERE run_id = '${run_id}' AND strategy = '${strategy}' AND scenario = '${scenario}';"

    if [ "$MODE" = "distributed" ]; then
        local sink_ip="${CLOUD_VM_SINK_PUBLIC_IP:-}"
        remote_compose "$sink_ip" "infra/docker/compose/sink.yml" \
            "exec -T postgres psql -U ${POSTGRES_USER_NAME} -d ${POSTGRES_DB_NAME} -t -A -c \"${query}\"" 2>/dev/null | tr -d '[:space:]'
    else
        docker compose exec -T postgres psql -U "${POSTGRES_USER_NAME}" -d "${POSTGRES_DB_NAME}" -t -A -c "$query" 2>/dev/null | tr -d '[:space:]'
    fi
}

export_latency_samples_from_db() {
    local run_dir="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    local out_file="$run_dir/latency_samples.csv"
    local sql="COPY (
SELECT event_id::text,
       strategy,
       scenario,
       run_id,
       produced_at,
       visible_at,
       (visible_at - produced_at) AS latency_ms
FROM events
WHERE run_id = '${run_id}'
  AND strategy = '${strategy}'
  AND scenario = '${scenario}'
ORDER BY visible_at ASC, event_id ASC
) TO STDOUT WITH CSV HEADER"

    if [ "$MODE" = "distributed" ]; then
        local sink_ip="${CLOUD_VM_SINK_PUBLIC_IP:-}"
        remote_compose "$sink_ip" "infra/docker/compose/sink.yml" \
            "exec -T postgres psql -U ${POSTGRES_USER_NAME} -d ${POSTGRES_DB_NAME} -c \"${sql}\"" >"$out_file"
    else
        docker compose exec -T postgres psql -U "${POSTGRES_USER_NAME}" -d "${POSTGRES_DB_NAME}" -c "$sql" >"$out_file"
    fi
}

wait_for_visible_events_quiescence() {
    local strategy="$1"
    local scenario="$2"
    local run_id="$3"
    local stable_required="${4:-3}"
    local poll_seconds="${5:-2}"
    local max_wait="${6:-120}"
    local last_count=-1
    local stable=0
    local waited=0

    while [ "$waited" -lt "$max_wait" ]; do
        local current_count
        current_count=$(db_count_visible_events "$strategy" "$scenario" "$run_id")
        current_count=${current_count:-0}
        if [ "$current_count" = "$last_count" ]; then
            stable=$((stable + 1))
            if [ "$stable" -ge "$stable_required" ]; then
                return 0
            fi
        else
            stable=0
            last_count="$current_count"
        fi
        sleep "$poll_seconds"
        waited=$((waited + poll_seconds))
    done
    return 0
}

json_get_number() {
    local json_file="$1"
    local key="$2"
    local fallback="${3:-0}"
    if [ ! -f "$json_file" ]; then
        echo "$fallback"
        return
    fi
    "$PYTHON_BIN" - "$json_file" "$key" "$fallback" <<'PY'
import json
import sys

path, key, fallback = sys.argv[1:]
try:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    value = data.get(key, fallback)
    print(value)
except Exception:
    print(fallback)
PY
}

apply_generation_markers_from_summary() {
    local summary_file="$1"
    if [ ! -f "$summary_file" ]; then
        return
    fi
    local start_ms end_ms
    start_ms=$(json_get_number "$summary_file" "generation_start_epoch_ms" "0")
    end_ms=$(json_get_number "$summary_file" "generation_end_epoch_ms" "0")
    if [ "${start_ms%%.*}" -gt 0 ]; then
        GENERATION_START_TS=$(( ${start_ms%%.*} / 1000 ))
    fi
    if [ "${end_ms%%.*}" -gt 0 ]; then
        GENERATION_END_TS=$(( ${end_ms%%.*} / 1000 ))
    fi
}

build_run_summary() {
    local run_dir="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    local latency_file="$run_dir/latency_samples.csv"
    local prom_file="$run_dir/prometheus_snapshot.csv"
    local generator_summary_file="$run_dir/generator_summary.json"
    local run_summary_file="$run_dir/run_summary.json"
    local official_duration="${RUN_DURATION_SECONDS:-300}"

    local generated_events
    generated_events=$(json_get_number "$generator_summary_file" "generated_events" "0")
    local generated_eps_real
    generated_eps_real=$(json_get_number "$generator_summary_file" "generated_eps_real" "0")
    local generation_duration_seconds
    generation_duration_seconds=$(json_get_number "$generator_summary_file" "generation_duration_seconds" "$official_duration")
    local visible_events
    visible_events=$(db_count_visible_events "$strategy" "$scenario" "$run_id")
    visible_events=${visible_events:-0}

    local tput_sink lat_p50 lat_p95 lat_p99 kafka_lag kafka_lag_real_present cpu_cores mem_rss_bytes
    tput_sink=$(csv_metric_value "$prom_file" "tput_sink_eps")
    lat_p50=$(csv_metric_value "$prom_file" "latency_p50_ms")
    lat_p95=$(csv_metric_value "$prom_file" "latency_p95_ms")
    lat_p99=$(csv_metric_value "$prom_file" "latency_p99_ms")
    kafka_lag=$(csv_metric_value "$prom_file" "kafka_consumer_lag")
    kafka_lag_real_present=$(csv_metric_value "$prom_file" "kafka_consumer_lag_real_present")
    cpu_cores=$(csv_metric_value "$prom_file" "cpu_total_cores")
    mem_rss_bytes=$(csv_metric_value "$prom_file" "mem_rss_bytes")

    "$PYTHON_BIN" - "$run_summary_file" <<PY
import json

generated_events = float(${generated_events})
visible_events = float(${visible_events})
official_duration = float(${official_duration}) if float(${official_duration}) > 0 else 1.0
generation_duration = float(${generation_duration_seconds}) if float(${generation_duration_seconds}) > 0 else official_duration
generated_eps_real = float(${generated_eps_real}) if float(${generated_eps_real}) > 0 else (generated_events / generation_duration)
visible_eps_equivalent = visible_events / official_duration
delivery_ratio_pct = (visible_events / generated_events * 100.0) if generated_events > 0 else 0.0

data = {
    "strategy": "${strategy}",
    "scenario": "${scenario}",
    "run_id": "${run_id}",
    "target_eps": int(${GENERATOR_DEFAULT_RATE:-0}),
    "generated_events": int(generated_events),
    "visible_events": int(visible_events),
    "official_duration_seconds": int(official_duration),
    "generation_duration_seconds": round(generation_duration, 3),
    "generated_eps_real": round(generated_eps_real, 3),
    "visible_eps_equivalent": round(visible_eps_equivalent, 3),
    "delivery_ratio_pct": round(delivery_ratio_pct, 3),
    "latency_p50_ms": float(${lat_p50}),
    "latency_p95_ms": float(${lat_p95}),
    "latency_p99_ms": float(${lat_p99}),
    "kafka_lag_max": float(${kafka_lag}) if float(${kafka_lag_real_present}) >= 1.0 else None,
    "kafka_lag_mean": float(${kafka_lag}) if float(${kafka_lag_real_present}) >= 1.0 else None,
    "kafka_lag_real_present": bool(float(${kafka_lag_real_present}) >= 1.0),
    "cpu_cores_avg": float(${cpu_cores}),
    "mem_rss_mb_avg": round(float(${mem_rss_bytes}) / 1048576.0, 3),
    "processing_duration_seconds": max(0.0, float(${PROCESSING_END_TS}) - float(${PROCESSING_START_TS})),
    "estimated_backlog_events": max(0, int(generated_events - visible_events)),
}

with open("${run_summary_file}", "w", encoding="utf-8") as handle:
    json.dump(data, handle, indent=2)
PY
}

csv_metric_value() {
    local csv_file="$1"
    local metric_name="$2"
    if [ ! -f "$csv_file" ]; then
        echo 0
        return
    fi
    "$PYTHON_BIN" - "$csv_file" "$metric_name" <<'PY'
import csv
import sys

path, metric = sys.argv[1:]
value = 0.0
with open(path, newline="", encoding="utf-8") as handle:
    reader = csv.DictReader(handle)
    for row in reader:
        if row.get("metric") == metric:
            try:
                value = float(row.get("value", 0) or 0)
            except ValueError:
                value = 0.0
print(value)
PY
}

create_timeseries_from_snapshot() {
    local run_dir="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    local prom_file="$run_dir/prometheus_snapshot.csv"
    local lag_file="$run_dir/kafka_lag_timeseries.csv"
    local resources_file="$run_dir/resources_timeseries.csv"

    local ts
    ts=$(date +%s)
    local lag
    lag=$(csv_metric_value "$prom_file" "kafka_consumer_lag")
    local lag_present
    lag_present=$(csv_metric_value "$prom_file" "kafka_consumer_lag_real_present")
    local cpu
    cpu=$(csv_metric_value "$prom_file" "cpu_total_cores")
    local mem
    mem=$(csv_metric_value "$prom_file" "mem_rss_bytes")
    local consumer_group=""
    local lag_source="missing"

    if [ "$strategy" = "streaming" ] && [ "${lag_present}" = "1.0" -o "${lag_present}" = "1" ]; then
        consumer_group="flink-streaming-${scenario}-${run_id}"
        lag_source="real_prometheus"
    fi

    printf '%s\n' "timestamp,strategy,scenario,run_id,consumer_group,topic,lag,lag_source" >"$lag_file"
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$ts" "$strategy" "$scenario" "$run_id" "$consumer_group" "$RUN_TOPIC" "$lag" "$lag_source" >>"$lag_file"

    printf '%s\n' "timestamp,strategy,scenario,run_id,cpu_cores,mem_rss_bytes" >"$resources_file"
    printf '%s,%s,%s,%s,%s,%s\n' "$ts" "$strategy" "$scenario" "$run_id" "$cpu" "$mem" >>"$resources_file"
}

reset_probe_csv() {
    mkdir -p "$RESULTS_BASE"
    rm -f "$GENERATOR_SUMMARY_GLOBAL" 2>/dev/null || true
    # Resetear via el container del probe para evitar errores de permisos.
    # El probe escribe continuamente en /results/latency_samples.csv (montado
    # desde el host), por lo que el archivo puede estar bloqueado/en uso.
    if [ "$MODE" = "distributed" ]; then
        local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
        remote_shell "$producer_ip" "mkdir -p ~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME} && sudo -n chown -R ubuntu:ubuntu ~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME} >/dev/null 2>&1 || true"
        remote_shell "$producer_ip" "chmod 777 ~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME} >/dev/null 2>&1 || true; touch ~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}/latency_samples.csv >/dev/null 2>&1 || true; chmod 666 ~/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}/latency_samples.csv >/dev/null 2>&1 || true"
        remote_shell "$producer_ip" "docker rm -f tesis-probe >/dev/null 2>&1 || true"
        remote_shell "$producer_ip" \
            "cd ~/data-ingestion-strategies && \
             RUN_ID='${RUN_ID}' STRATEGY='${STRATEGY}' SCENARIO='${SCENARIO}' GENERATOR_SCENARIO='${SCENARIO}' RESULTS_VOLUME_HOST='/home/ubuntu/data-ingestion-strategies/${REMOTE_RESULTS_BASE_NAME}' \
             docker compose --env-file .env -f infra/docker/compose/producer.yml up -d --force-recreate --no-deps probe" >/dev/null 2>&1 || true
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
    else
        RUN_ID="$RUN_ID" STRATEGY="$STRATEGY" SCENARIO="$SCENARIO" GENERATOR_SCENARIO="$SCENARIO" \
            docker compose up -d --force-recreate --no-deps probe >/dev/null 2>&1 || true
        if docker compose exec -T probe sh -c \
        "echo '${PROBE_HEADER}' > /results/latency_samples.csv" 2>/dev/null; then
            log "Probe CSV reseteado (via container)"
        else
            # Fallback: escribir directamente si el container no esta disponible
            printf '%s\n' "$PROBE_HEADER" >"$PROBE_GLOBAL" 2>/dev/null || \
                warn "No se pudo resetear probe CSV"
        fi
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
            warn "Servicios no estan levantados. Ejecuta: bash scripts/thesis.sh run --mode local"
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
    # Fallback robusto para entornos donde el label 'name' no está presente
    local cpu_query_alt="sum(increase(container_cpu_usage_seconds_total{job=\"cadvisor\",container_label_com_docker_compose_project=~\"tesis-ingestion|tesis\"}[${prom_window}])) / ${duration}"
    local mem_query_alt="sum(avg_over_time(container_memory_rss{job=\"cadvisor\",container_label_com_docker_compose_project=~\"tesis-ingestion|tesis\"}[${prom_window}]))"
    local cpu_query_id="sum(increase(container_cpu_usage_seconds_total{job=\"cadvisor\",id=~\"/docker/.+|/system.slice/docker-.+\\.scope\"}[${prom_window}])) / ${duration}"
    local mem_query_id="sum(avg_over_time(container_memory_rss{job=\"cadvisor\",id=~\"/docker/.+|/system.slice/docker-.+\\.scope\"}[${prom_window}]))"
    # Fallback adicional host-level (node-exporter) en caso de cadvisor vacío
    local cpu_query_host="sum(increase(node_cpu_seconds_total{job=\"node-exporter\",mode!=\"idle\"}[${prom_window}])) / ${duration}"
    local mem_query_host="sum(avg_over_time(node_memory_MemTotal_bytes{job=\"node-exporter\"}[${prom_window}])) - sum(avg_over_time(node_memory_MemAvailable_bytes{job=\"node-exporter\"}[${prom_window}]))"
    local prod_query="sum(increase(kafka_produced_messages_total{scenario=\"${scenario}\",run_id=\"${run_id}\",strategy=\"${strategy}\"}[${prom_window}])) / ${duration}"

    # Backward compatibility for historical metric labels (scenario-only)
    local prod_query_legacy="sum(increase(kafka_produced_messages_total{scenario=\"${scenario}\"}[${prom_window}])) / ${duration}"
    local kafka_lag_query='0'
    local kafka_lag_real_present=0
    if [ "$strategy" = "streaming" ]; then
        kafka_lag_query='max_over_time(kafka_consumergroup_lag{topic="'"${RUN_TOPIC}"'",consumergroup="flink-streaming-'"${scenario}"'-'"${run_id}"'"}['"${prom_window}"'])'
    fi

    CPU_TOTAL=$(query_or_zero "$cpu_query" "$prom_time")
    MEM_TOTAL=$(query_or_zero "$mem_query" "$prom_time")
    if [ "${CPU_TOTAL:-0}" = "0" ] || [ "${CPU_TOTAL:-0}" = "0.0" ]; then
        CPU_TOTAL=$(query_or_zero "$cpu_query_alt" "$prom_time")
    fi
    if [ "${MEM_TOTAL:-0}" = "0" ] || [ "${MEM_TOTAL:-0}" = "0.0" ]; then
        MEM_TOTAL=$(query_or_zero "$mem_query_alt" "$prom_time")
    fi
    if [ "${CPU_TOTAL:-0}" = "0" ] || [ "${CPU_TOTAL:-0}" = "0.0" ]; then
        CPU_TOTAL=$(query_or_zero "$cpu_query_id" "$prom_time")
    fi
    if [ "${MEM_TOTAL:-0}" = "0" ] || [ "${MEM_TOTAL:-0}" = "0.0" ]; then
        MEM_TOTAL=$(query_or_zero "$mem_query_id" "$prom_time")
    fi
    if [ "${CPU_TOTAL:-0}" = "0" ] || [ "${CPU_TOTAL:-0}" = "0.0" ]; then
        CPU_TOTAL=$(query_or_zero "$cpu_query_host" "$prom_time")
    fi
    if [ "${MEM_TOTAL:-0}" = "0" ] || [ "${MEM_TOTAL:-0}" = "0.0" ]; then
        MEM_TOTAL=$(query_or_zero "$mem_query_host" "$prom_time")
    fi
    if [ "${CPU_TOTAL:-0}" = "0" ] || [ "${CPU_TOTAL:-0}" = "0.0" ] || [ "${MEM_TOTAL:-0}" = "0" ] || [ "${MEM_TOTAL:-0}" = "0.0" ]; then
        warn "Prometheus resources fallback unresolved (CPU=${CPU_TOTAL}, MEM=${MEM_TOTAL}) — keeping values for traceability"
    fi
    TPUT_PRODUCED=$(query_or_zero "$prod_query" "$prom_time")
    if [ "${TPUT_PRODUCED:-0}" = "0" ] || [ "${TPUT_PRODUCED:-0}" = "0.0" ]; then
        TPUT_PRODUCED=$(query_or_zero "$prod_query_legacy" "$prom_time")
    fi
    if [ "$strategy" = "streaming" ]; then
        local kafka_lag_raw
        kafka_lag_raw=$(query_prometheus "$kafka_lag_query" "$prom_time")
        if [[ -n "$kafka_lag_raw" && "$kafka_lag_raw" != "NaN" && "$kafka_lag_raw" != "null" ]]; then
            KAFKA_LAG="$kafka_lag_raw"
            kafka_lag_real_present=1
        else
            KAFKA_LAG=0
        fi
    else
        KAFKA_LAG=0
    fi

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
    echo "${strategy},${scenario},${run_id},kafka_consumer_lag_real_present,${kafka_lag_real_present},bool" >>"$out_file"
    echo "${strategy},${scenario},${run_id},latency_p50_ms,${lat_p50},ms" >>"$out_file"
    echo "${strategy},${scenario},${run_id},latency_p95_ms,${lat_p95},ms" >>"$out_file"
    echo "${strategy},${scenario},${run_id},latency_p99_ms,${lat_p99},ms" >>"$out_file"

    log "Snapshot guardado: $(wc -l <"$out_file") metricas en $out_file"
}

collect_cloudwatch_snapshot() {
    local run_dir="$1"
    local strategy="$2"
    local scenario="$3"
    local run_id="$4"
    local start_ts=${5:-0}
    local end_ts=${6:-0}

    local out_file="$run_dir/cloudwatch_snapshot.csv"
    local start_iso end_iso period
    start_iso=$(date -u -d "@${start_ts}" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || true)
    end_iso=$(date -u -d "@${end_ts}" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || true)
    period=60

    if [ -z "$start_iso" ] || [ -z "$end_iso" ]; then
        return 0
    fi

    if [ -z "${CLOUD_VM_PRODUCER_INSTANCE_ID:-}" ] || [ -z "${CLOUD_VM_BROKER_INSTANCE_ID:-}" ] || [ -z "${CLOUD_VM_COMPUTE_INSTANCE_ID:-}" ] || [ -z "${CLOUD_VM_SINK_INSTANCE_ID:-}" ]; then
        return 0
    fi

    local cw_runner_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
    local cw_region="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
    if [ -z "$cw_runner_ip" ]; then
        return 0
    fi

    echo "strategy,scenario,run_id,node,instance_id,metric,value,unit,start_utc,end_utc" >"$out_file"

    cw_append_metric() {
        local node="$1"
        local instance_id="$2"
        local namespace="$3"
        local metric_name="$4"
        local stat="$5"
        local unit="$6"
        local metric_period="$7"
        local metric_start_iso="$8"

        local val
        val=$(remote_shell "$cw_runner_ip" "AWS_DEFAULT_REGION='${cw_region}' aws cloudwatch get-metric-statistics \
            --namespace "$namespace" \
            --metric-name "$metric_name" \
            --dimensions "Name=InstanceId,Value=${instance_id}" \
            --start-time "$metric_start_iso" \
            --end-time "$end_iso" \
            --period "$metric_period" \
            --statistics "$stat" \
            --query 'Datapoints[-1].Average' \
            --output text 2>/dev/null || echo NaN" 2>/dev/null || echo "NaN")

        if [ -z "$val" ] || [ "$val" = "None" ] || [ "$val" = "null" ]; then
            val="NaN"
        fi

        echo "${strategy},${scenario},${run_id},${node},${instance_id},${metric_name},${val},${unit},${metric_start_iso},${end_iso}" >>"$out_file"
    }

    local ec2_start_iso
    ec2_start_iso=$(date -u -d "@$((start_ts - 1800))" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "$start_iso")

    # EC2 default metrics
    cw_append_metric "producer" "$CLOUD_VM_PRODUCER_INSTANCE_ID" "AWS/EC2" "CPUUtilization" "Average" "Percent" "300" "$ec2_start_iso"
    cw_append_metric "broker" "$CLOUD_VM_BROKER_INSTANCE_ID" "AWS/EC2" "CPUUtilization" "Average" "Percent" "300" "$ec2_start_iso"
    cw_append_metric "compute" "$CLOUD_VM_COMPUTE_INSTANCE_ID" "AWS/EC2" "CPUUtilization" "Average" "Percent" "300" "$ec2_start_iso"
    cw_append_metric "sink" "$CLOUD_VM_SINK_INSTANCE_ID" "AWS/EC2" "CPUUtilization" "Average" "Percent" "300" "$ec2_start_iso"

    cw_append_metric "producer" "$CLOUD_VM_PRODUCER_INSTANCE_ID" "AWS/EC2" "NetworkIn" "Average" "Bytes" "300" "$ec2_start_iso"
    cw_append_metric "broker" "$CLOUD_VM_BROKER_INSTANCE_ID" "AWS/EC2" "NetworkIn" "Average" "Bytes" "300" "$ec2_start_iso"
    cw_append_metric "compute" "$CLOUD_VM_COMPUTE_INSTANCE_ID" "AWS/EC2" "NetworkIn" "Average" "Bytes" "300" "$ec2_start_iso"
    cw_append_metric "sink" "$CLOUD_VM_SINK_INSTANCE_ID" "AWS/EC2" "NetworkIn" "Average" "Bytes" "300" "$ec2_start_iso"

    cw_append_metric "producer" "$CLOUD_VM_PRODUCER_INSTANCE_ID" "AWS/EC2" "NetworkOut" "Average" "Bytes" "300" "$ec2_start_iso"
    cw_append_metric "broker" "$CLOUD_VM_BROKER_INSTANCE_ID" "AWS/EC2" "NetworkOut" "Average" "Bytes" "300" "$ec2_start_iso"
    cw_append_metric "compute" "$CLOUD_VM_COMPUTE_INSTANCE_ID" "AWS/EC2" "NetworkOut" "Average" "Bytes" "300" "$ec2_start_iso"
    cw_append_metric "sink" "$CLOUD_VM_SINK_INSTANCE_ID" "AWS/EC2" "NetworkOut" "Average" "Bytes" "300" "$ec2_start_iso"

    # CloudWatch Agent custom host metrics
    cw_append_metric "producer" "$CLOUD_VM_PRODUCER_INSTANCE_ID" "TesisBenchmark/EC2" "mem_used_percent" "Average" "Percent" "60" "$start_iso"
    cw_append_metric "broker" "$CLOUD_VM_BROKER_INSTANCE_ID" "TesisBenchmark/EC2" "mem_used_percent" "Average" "Percent" "60" "$start_iso"
    cw_append_metric "compute" "$CLOUD_VM_COMPUTE_INSTANCE_ID" "TesisBenchmark/EC2" "mem_used_percent" "Average" "Percent" "60" "$start_iso"
    cw_append_metric "sink" "$CLOUD_VM_SINK_INSTANCE_ID" "TesisBenchmark/EC2" "mem_used_percent" "Average" "Percent" "60" "$start_iso"

    cw_append_metric "producer" "$CLOUD_VM_PRODUCER_INSTANCE_ID" "TesisBenchmark/EC2" "disk_used_percent" "Average" "Percent" "60" "$start_iso"
    cw_append_metric "broker" "$CLOUD_VM_BROKER_INSTANCE_ID" "TesisBenchmark/EC2" "disk_used_percent" "Average" "Percent" "60" "$start_iso"
    cw_append_metric "compute" "$CLOUD_VM_COMPUTE_INSTANCE_ID" "TesisBenchmark/EC2" "disk_used_percent" "Average" "Percent" "60" "$start_iso"
    cw_append_metric "sink" "$CLOUD_VM_SINK_INSTANCE_ID" "TesisBenchmark/EC2" "disk_used_percent" "Average" "Percent" "60" "$start_iso"

    log "CloudWatch snapshot guardado: $(wc -l <"$out_file") metricas en $out_file"
}

# ═══════════════════════════════════════════════════════════════════
# RUN BATCH
# ═══════════════════════════════════════════════════════════════════
run_batch() {
    ensure_services
    set_run_topic

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
        remote_compose "$sink_ip" "infra/docker/compose/sink.yml" "exec -T postgres psql -U benchmark -d benchmark -c \"TRUNCATE TABLE events RESTART IDENTITY CASCADE;\"" >/dev/null 2>&1 || true
    else
        docker compose restart flink-jobmanager flink-taskmanager >/dev/null 2>&1 || true
        docker compose exec -T spark-master sh -c 'pkill -f spark || true' >/dev/null 2>&1 || true
        docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    fi
    prepare_run_topic
    reset_probe_csv
    start_run_timer
    mark_generation_start

    echo "────────────────────────────────────────────────────────────"
    echo "[run_batch] strategy=batch  scenario=$SCENARIO  run_id=$RUN_ID"
    echo "────────────────────────────────────────────────────────────"

    ACCUMULATE_TIME=$RUN_DURATION_SECONDS
    log "Fase de acumulacion: generator corre por ${ACCUMULATE_TIME}s antes del batch job"

    echo "[run_batch] Accumulation phase: generator runs for ${ACCUMULATE_TIME}s before batch job executes"
    echo "[run_batch] Accumulating..."

    # Iniciar generator en background
    start_generator_for_scenario "$SCENARIO"

    # Esperar acumulacion completa del escenario oficial.
    sleep "$ACCUMULATE_TIME"

    if ! wait_for_generator_exit "$((RUN_DURATION_SECONDS + 30))"; then
        warn "Generator no termino solo dentro del tiempo esperado; forzando stop"
        stop_generator_if_running
    fi
    mark_processing_start

    # Ejecutar Spark Batch
    log "Ejecutando Spark Batch..."
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" \
            "exec -T spark-master /opt/spark/bin/spark-submit --class org.tesis.batch.SparkBatchJob --master spark://spark-master:${MASTER_PORT} --conf spark.executor.memory=1500m --conf spark.driver.memory=2g --conf spark.executor.cores=2 --conf spark.sql.shuffle.partitions=12 /opt/spark/jobs/batch/batch-job.jar --scenario=${SCENARIO} --run.id=${RUN_ID} --kafka.bootstrap.servers=$(resolve_broker_host):$(resolve_broker_port) --kafka.topic=${RUN_TOPIC} --postgres.url=jdbc:postgresql://$(resolve_sink_host):5432/${POSTGRES_DB_NAME} --postgres.user=${POSTGRES_USER_NAME} --postgres.password=${POSTGRES_PASSWORD_VALUE} --run.duration.seconds=${RUN_DURATION_SECONDS}"
    else
        MSYS_NO_PATHCONV=1 docker compose exec spark-master /opt/spark/bin/spark-submit \
        --class org.tesis.batch.SparkBatchJob \
        --master spark://spark-master:${MASTER_PORT} \
        --conf spark.executor.memory=1500m \
        --conf spark.driver.memory=2g \
        --conf spark.executor.cores=2 \
        --conf spark.sql.shuffle.partitions=12 \
        /opt/spark/jobs/batch/batch-job.jar \
        --scenario="$SCENARIO" \
        --run.id="$RUN_ID" \
        --kafka.bootstrap.servers=kafka:9092 \
        --kafka.topic="$RUN_TOPIC" \
        --postgres.url=jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
        --postgres.user=${POSTGRES_USER_NAME} \
        --postgres.password=${POSTGRES_PASSWORD_VALUE} \
        --run.duration.seconds=${RUN_DURATION_SECONDS}
    fi
    mark_processing_end

    end_run_timer
    RUN_DIR="${RESULTS_BASE}/batch/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    sync_generator_summary_from_producer
    copy_generator_summary_to_run "$RUN_DIR"
    require_generator_summary "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"
    apply_generation_markers_from_summary "$RUN_DIR/generator_summary.json"
    wait_for_visible_events_quiescence "batch" "$SCENARIO" "$RUN_ID" 3 2 60
    export_latency_samples_from_db "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"
    collect_prometheus_snapshot "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    create_timeseries_from_snapshot "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"
    write_run_metadata "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"
    build_run_summary "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"
    if [ "$MODE" = "distributed" ]; then
        collect_cloudwatch_snapshot "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    fi
    archive_run_to_sink "$RUN_DIR" "batch" "$SCENARIO" "$RUN_ID"

    log "Completed: batch/$SCENARIO/$RUN_ID"
}

# ═══════════════════════════════════════════════════════════════════
# RUN MICROBATCH
# ═══════════════════════════════════════════════════════════════════
run_microbatch() {
    ensure_services
    set_run_topic

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
        remote_compose "$sink_ip" "infra/docker/compose/sink.yml" "exec -T postgres psql -U benchmark -d benchmark -c \"TRUNCATE TABLE events RESTART IDENTITY CASCADE;\"" >/dev/null 2>&1 || true
    else
        docker compose restart flink-jobmanager flink-taskmanager >/dev/null 2>&1 || true
        docker compose exec -T spark-master sh -c 'pkill -f spark || true' >/dev/null 2>&1 || true
        docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    fi
    prepare_run_topic
    reset_probe_csv
    clear_checkpoint_dir "microbatch"
    start_run_timer
    mark_generation_start
    mark_processing_start

    echo "────────────────────────────────────────────────────────────"
    echo "[run_microbatch] strategy=microbatch  scenario=$SCENARIO  run_id=$RUN_ID  trigger=$TRIGGER_INTERVAL"
    echo "────────────────────────────────────────────────────────────"

    # Iniciar generator y microbatch
    start_generator_for_scenario "$SCENARIO"

    if [ "$MODE" = "distributed" ]; then
        remote_compose "$compute_ip" "infra/docker/compose/compute.yml" \
            "exec -T spark-master /opt/spark/bin/spark-submit --class org.tesis.microbatch.SparkStructuredJob --master spark://spark-master:${MASTER_PORT} /opt/spark/jobs/microbatch/microbatch-job.jar --scenario=${SCENARIO} --run.id=${RUN_ID} --trigger.interval=\"${TRIGGER_INTERVAL}\" --kafka.bootstrap.servers=$(resolve_broker_host):$(resolve_broker_port) --kafka.topic=${RUN_TOPIC} --checkpoint.location=/opt/spark/checkpoints/microbatch --postgres.url=jdbc:postgresql://$(resolve_sink_host):5432/${POSTGRES_DB_NAME} --postgres.user=${POSTGRES_USER_NAME} --postgres.password=${POSTGRES_PASSWORD_VALUE} --run.duration.seconds=$((RUN_DURATION_SECONDS + 20))"
    else
        MSYS_NO_PATHCONV=1 docker compose exec spark-master /opt/spark/bin/spark-submit \
        --class org.tesis.microbatch.SparkStructuredJob \
        --master spark://spark-master:${MASTER_PORT} \
        /opt/spark/jobs/microbatch/microbatch-job.jar \
        --scenario="$SCENARIO" \
        --run.id="$RUN_ID" \
        --trigger.interval="$TRIGGER_INTERVAL" \
        --kafka.bootstrap.servers=kafka:9092 \
        --kafka.topic="$RUN_TOPIC" \
        --checkpoint.location=/opt/spark/checkpoints/microbatch \
        --postgres.url=jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
        --postgres.user=${POSTGRES_USER_NAME} \
        --postgres.password=${POSTGRES_PASSWORD_VALUE} \
        --run.duration.seconds=$((RUN_DURATION_SECONDS + 20))
    fi

    mark_processing_end

    RUN_DIR="${RESULTS_BASE}/microbatch/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    end_run_timer
    if ! wait_for_generator_exit "$((RUN_DURATION_SECONDS + 30))"; then
        warn "Generator no termino solo dentro del tiempo esperado; forzando stop"
        stop_generator_if_running
    fi
    sync_generator_summary_from_producer
    copy_generator_summary_to_run "$RUN_DIR"
    require_generator_summary "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"
    apply_generation_markers_from_summary "$RUN_DIR/generator_summary.json"
    wait_for_visible_events_quiescence "microbatch" "$SCENARIO" "$RUN_ID" 4 2 90
    export_latency_samples_from_db "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"
    collect_prometheus_snapshot "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    create_timeseries_from_snapshot "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"
    write_run_metadata "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"
    build_run_summary "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"
    if [ "$MODE" = "distributed" ]; then
        collect_cloudwatch_snapshot "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    fi
    archive_run_to_sink "$RUN_DIR" "microbatch" "$SCENARIO" "$RUN_ID"

    log "Completed: microbatch/$SCENARIO/$RUN_ID"
}

# ═══════════════════════════════════════════════════════════════════
# RUN STREAMING
# ═══════════════════════════════════════════════════════════════════
run_streaming() {
    ensure_services
    set_run_topic

    local producer_ip="${CLOUD_VM_PRODUCER_PUBLIC_IP:-}"
    local broker_ip="${CLOUD_VM_BROKER_PUBLIC_IP:-}"
    local compute_ip="${CLOUD_VM_COMPUTE_PUBLIC_IP:-}"
    local sink_ip="${CLOUD_VM_SINK_PUBLIC_IP:-}"

    log "Ejecutando STREAMING: scenario=$SCENARIO run_id=$RUN_ID"

    # Limpiar antes
    log "Limpiando entorno..."
    if [ "$MODE" = "distributed" ]; then
        remote_compose "$sink_ip" "infra/docker/compose/sink.yml" "exec -T postgres psql -U benchmark -d benchmark -c \"TRUNCATE TABLE events RESTART IDENTITY CASCADE;\"" >/dev/null 2>&1 || true
    else
        docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    fi
    prepare_run_topic
    reset_probe_csv
    start_run_timer
    mark_generation_start
    mark_processing_start

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
            "exec -T flink-jobmanager /opt/flink/bin/flink run ${FLINK_DETACH_FLAG} -c org.tesis.streaming.FlinkStreamingJob -p ${FLINK_PARALLELISM_VALUE} /opt/flink/usrlib/streaming-job.jar --scenario ${SCENARIO} --run.id ${RUN_ID} --kafka.bootstrap.servers $(resolve_broker_host):$(resolve_broker_port) --kafka.topic ${RUN_TOPIC} --postgres.url jdbc:postgresql://$(resolve_sink_host):5432/${POSTGRES_DB_NAME} --postgres.user ${POSTGRES_USER_NAME} --postgres.password ${POSTGRES_PASSWORD_VALUE} --run.duration.seconds $((RUN_DURATION_SECONDS + 20))"
    else
        MSYS_NO_PATHCONV=1 docker compose exec flink-jobmanager /opt/flink/bin/flink run \
        ${FLINK_DETACH_FLAG} \
        -c org.tesis.streaming.FlinkStreamingJob \
        -p ${FLINK_PARALLELISM_VALUE} \
        /opt/flink/usrlib/streaming-job.jar \
        --scenario "$SCENARIO" \
        --run.id "$RUN_ID" \
        --kafka.bootstrap.servers kafka:9092 \
        --kafka.topic "$RUN_TOPIC" \
        --postgres.url jdbc:postgresql://postgres:5432/${POSTGRES_DB_NAME} \
        --postgres.user ${POSTGRES_USER_NAME} \
        --postgres.password ${POSTGRES_PASSWORD_VALUE} \
        --run.duration.seconds $((RUN_DURATION_SECONDS + 20))
    fi

    mark_processing_end

    RUN_DIR="${RESULTS_BASE}/streaming/${SCENARIO}/${RUN_ID}"
    mkdir -p "$RUN_DIR"
    end_run_timer
    if ! wait_for_generator_exit "$((RUN_DURATION_SECONDS + 30))"; then
        warn "Generator no termino solo dentro del tiempo esperado; forzando stop"
        stop_generator_if_running
    fi
    sync_generator_summary_from_producer
    copy_generator_summary_to_run "$RUN_DIR"
    require_generator_summary "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID"
    apply_generation_markers_from_summary "$RUN_DIR/generator_summary.json"
    wait_for_visible_events_quiescence "streaming" "$SCENARIO" "$RUN_ID" 4 2 90
    export_latency_samples_from_db "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID"
    collect_prometheus_snapshot "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    create_timeseries_from_snapshot "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID"
    write_run_metadata "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID"
    build_run_summary "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID"
    if [ "$MODE" = "distributed" ]; then
        collect_cloudwatch_snapshot "$RUN_DIR" "streaming" "$SCENARIO" "$RUN_ID" "$RUN_START_TS" "$RUN_END_TS"
    fi
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
        echo "  escenario    low-load | medium-load | high-load"
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
