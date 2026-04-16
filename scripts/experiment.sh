#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# experiment.sh — Experimento automatizado
#
# Itera sobre estrategias × escenarios × repeticiones:
# clean → warmup → run → cooldown → export
#
# Uso:
#   ./scripts/experiment.sh --smoke        # ~5 min (1 estrategia, 1 escenario)
#   ./scripts/experiment.sh --quick        # ~30 min (3 estrategias, 2 escenarios)
#   ./scripts/experiment.sh --standard     # ~2 horas (default)
#   ./scripts/experiment.sh --full         # matriz tesis completa
#   ./scripts/experiment.sh --reps 3       # 3 repeticiones
#   ./scripts/experiment.sh --duration 240 # 4 min por corrida
#   ./scripts/experiment.sh --strategies batch # solo batch
#   ./scripts/experiment.sh --fault-inject # inyectar fallos después de cada run
#   ./scripts/experiment.sh --scaling-test # medir eficiencia de escalado (1→2→3 workers)
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
RESULTS_BASE="$ROOT_DIR/results"

# ── Defaults (override via arguments) ──────────────────────────────
# Standard thesis experiment: 5 reps × 3 scenarios × 3 strategies = 45 runs
STRATEGIES=${STRATEGIES:-"batch microbatch streaming"}
SCENARIOS=${SCENARIOS:-"low-load medium-load high-load"}
REPETITIONS=${REPETITIONS:-5}
TRIGGER_INTERVAL=${TRIGGER_INTERVAL:-"5 seconds"}
COOLDOWN_SECONDS=${COOLDOWN_SECONDS:-30}   # >= Flink checkpoint interval + margin
EXPORT_METRICS=${EXPORT_METRICS:-true}
EXPORT_WINDOW=${EXPORT_WINDOW:-"5m"}
EVENT_SCHEMA=${EVENT_SCHEMA:-""}
export RUN_DURATION_SECONDS=${RUN_DURATION_SECONDS:-300}
WARMUP_SECONDS=${WARMUP_SECONDS:-30}    # 30 s per protocol spec (JVM JIT warm-up)

# ── Feature flags ──────────────────────────────────────────────────
FAULT_INJECT=false      # --fault-inject : ejecutar fault_inject.sh por estrategia
SCALING_TEST=false      # --scaling-test : medir eficiencia de escalado (1→2→3 workers)
FAULT_SCENARIO=${FAULT_SCENARIO:-"medium-load"}   # escenario sobre el que se inyecta fallo

# ── Quick mode presets ─────────────────────────────────────────────
QUICK_MODE=false
FAST_MODE=false
SMOKE_MODE=false

# ── Parse CLI flags ───────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_MODE=true
            STRATEGIES="batch microbatch streaming"
            SCENARIOS="low-load medium-load"
            REPETITIONS=1
            export RUN_DURATION_SECONDS=180
            WARMUP_SECONDS=10
            shift
            ;;
        --fast)
            FAST_MODE=true
            STRATEGIES="batch microbatch streaming"
            SCENARIOS="low-load medium-load"
            REPETITIONS=1
            export RUN_DURATION_SECONDS=60
            WARMUP_SECONDS=5
            EXPORT_WINDOW="2m"
            shift
            ;;
        --smoke)
            SMOKE_MODE=true
            STRATEGIES="batch"
            SCENARIOS="low-load"
            REPETITIONS=1
            export RUN_DURATION_SECONDS=60
            WARMUP_SECONDS=5
            EXPORT_METRICS=false
            shift
            ;;
        --fault-inject)   FAULT_INJECT=true;          shift   ;;
        --fault-scenario) FAULT_SCENARIO="$2";        shift 2 ;;
        --scaling-test)   SCALING_TEST=true;           shift   ;;
        --strategies)  STRATEGIES="$2";       shift 2 ;;
        --scenarios)   SCENARIOS="$2";        shift 2 ;;
        --reps)        REPETITIONS="$2";      shift 2 ;;
        --trigger)     TRIGGER_INTERVAL="$2"; shift 2 ;;
        --cooldown)    COOLDOWN_SECONDS="$2"; shift 2 ;;
        --no-export)   EXPORT_METRICS=false;  shift   ;;
        --window)      EXPORT_WINDOW="$2";    shift 2 ;;
        --schema)      EVENT_SCHEMA="$2";     shift 2 ;;
        --duration)    export RUN_DURATION_SECONDS="$2"; shift 2 ;;
        --warmup)      WARMUP_SECONDS="$2";   shift 2 ;;
        *) echo "[experiment] Unknown flag: $1"; exit 1 ;;
    esac
done

# Apply warmup setting to environment for generator
export WARMUP_SECONDS

TOTAL_RUNS=0
for _ in $STRATEGIES; do for _ in $SCENARIOS; do TOTAL_RUNS=$((TOTAL_RUNS + REPETITIONS)); done; done
CURRENT_RUN=0

# Estimate total time
ESTIMATED_MINUTES=$((TOTAL_RUNS * (RUN_DURATION_SECONDS + WARMUP_SECONDS + COOLDOWN_SECONDS) / 60))

MODE_NAME="standard"
if [ "$QUICK_MODE" = true ]; then
    MODE_NAME="quick"
elif [ "$FAST_MODE" = true ]; then
    MODE_NAME="fast"
elif [ "$SMOKE_MODE" = true ]; then
    MODE_NAME="smoke"
fi

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              EXPERIMENT RUNNER — TESIS BENCHMARK            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Mode        : $MODE_NAME"
echo "║  Strategies  : $STRATEGIES"
echo "║  Scenarios   : $SCENARIOS"
echo "║  Repetitions : $REPETITIONS"
echo "║  Duration    : ${RUN_DURATION_SECONDS}s per run"
echo "║  Warmup      : ${WARMUP_SECONDS}s"
echo "║  Total runs  : $TOTAL_RUNS"
echo "║  Est. time   : ~${ESTIMATED_MINUTES} minutes"
echo "║  Export win. : $EXPORT_WINDOW"
echo "║  Schema      : ${EVENT_SCHEMA:-<scenario default>}"
echo "║  Fault inject: $FAULT_INJECT"
echo "║  Scaling test: $SCALING_TEST"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Pre-flight: verificar sincronización NTP en modo distribuido ──
# Si los relojes de VM-1 (produced_at) y VM-4 (visible_at) tienen un
# offset > 5ms, la métrica latencia puede ser negativa o incorrecta.
# Este check bloquea el experimento si el skew es inaceptable.
if [ "${MODE:-local}" = "distributed" ]; then
    echo "[experiment] Modo distribuido: verificando sincronización de relojes NTP..."
    CLOCK_SYNC_SCRIPT="$(dirname "$0")/check-clock-sync.sh"
    if [ -f "$CLOCK_SYNC_SCRIPT" ]; then
        if ! bash "$CLOCK_SYNC_SCRIPT"; then
            echo "[experiment] ABORT: Relojes desincronizados. Ver log en results/clock_offsets_*.csv"
            echo "[experiment] Solución: esperar 30s para que chrony converja y reintentar."
            exit 1
        fi
    else
        echo "[experiment] WARN: check-clock-sync.sh no encontrado — omitiendo verificación NTP"
    fi
    echo ""
fi

for STRATEGY in $STRATEGIES; do
    for SCENARIO in $SCENARIOS; do
        for REP in $(seq 1 "$REPETITIONS"); do
            RUN_ID="run_${REP}"
            CURRENT_RUN=$((CURRENT_RUN + 1))
            RUN_DIR="$RESULTS_BASE/$STRATEGY/$SCENARIO/$RUN_ID"
            mkdir -p "$RUN_DIR"

            echo ""
            echo "┌──────────────────────────────────────────────────────────┐"
            echo "│  [$CURRENT_RUN/$TOTAL_RUNS]  $STRATEGY / $SCENARIO / $RUN_ID"
            echo "└──────────────────────────────────────────────────────────┘"

            # ── Reset probe CSV before each run ───────────────────
            echo "[experiment] Truncating probe CSV for fresh run..."
            if [ "${MODE:-local}" = "local" ]; then
                docker compose exec -T probe sh -c \
                    "echo 'event_id,produced_at,visible_at,latency_ms,strategy,scenario,run_id' > /results/latency_samples.csv" 2>/dev/null || true
            fi

            # ── Run the strategy ───────────────────────────────────
            case "$STRATEGY" in
                batch)
                    bash "$ROOT_DIR/scripts/run.sh" batch "$SCENARIO" "$RUN_ID"
                    ;;
                microbatch)
                    bash "$ROOT_DIR/scripts/run.sh" microbatch "$SCENARIO" "$RUN_ID" "$TRIGGER_INTERVAL"
                    ;;
                streaming)
                    bash "$ROOT_DIR/scripts/run.sh" streaming "$SCENARIO" "$RUN_ID"
                    ;;
                *)
                    echo "[experiment] Unknown strategy: $STRATEGY"; exit 1
                    ;;
            esac

            # ── Cooldown ───────────────────────────────────────────
            echo "[experiment] Cooldown ${COOLDOWN_SECONDS}s ..."
            sleep "$COOLDOWN_SECONDS"

            # Nota: la copia del CSV de latencias al directorio del run la realiza
            # run.sh a traves de copy_probe_csv_to_run, filtrando solo las filas
            # de este run (strategy/scenario/run_id). No es necesario copiar aqui.
            SAMPLE_LINES=$(wc -l < "$RUN_DIR/latency_samples.csv" 2>/dev/null || echo 0)
            if [ "$SAMPLE_LINES" -lt 10 ]; then
                echo "[experiment] WARNING: run $STRATEGY/$SCENARIO/$RUN_ID produjo solo $SAMPLE_LINES lineas de muestra"
            else
                echo "[experiment] Results en $RUN_DIR/ ($SAMPLE_LINES lineas)"
            fi



        done
    done
done

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          EXPERIMENT COMPLETE — $TOTAL_RUNS runs finished           ║"
echo "║  Results directory: $RESULTS_BASE/                          "
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ══════════════════════════════════════════════════════════════════
# FAULT INJECTION PHASE  (--fault-inject)
# ══════════════════════════════════════════════════════════════════
# Ejecuta fault_inject.sh por cada estrategia en el escenario
# seleccionado (default: medium-load). Los resultados se acumulan
# en results/fault_recovery.csv.
if [ "$FAULT_INJECT" = true ]; then
    echo ""
    echo "┌──────────────────────────────────────────────────────────┐"
    echo "│  FAULT INJECTION — escenario: $FAULT_SCENARIO"
    echo "└──────────────────────────────────────────────────────────┘"

    FAULT_INJECT_SCRIPT="$ROOT_DIR/scripts/fault_inject.sh"
    if [ ! -f "$FAULT_INJECT_SCRIPT" ]; then
        echo "[experiment] ERROR: fault_inject.sh no encontrado en scripts/"
        exit 1
    fi

    for STRATEGY in $STRATEGIES; do
        echo "[experiment] Inyectando fallo en estrategia: $STRATEGY / $FAULT_SCENARIO"
        bash "$FAULT_INJECT_SCRIPT" "$STRATEGY" "$FAULT_SCENARIO" \
            || echo "[experiment] WARN: fault_inject.sh retornó error para $STRATEGY — continuando"
    done

    echo "[experiment] Fault injection completada. Ver results/fault_recovery.csv"
fi

# ══════════════════════════════════════════════════════════════════
# SCALING TEST PHASE  (--scaling-test)
# ══════════════════════════════════════════════════════════════════
# Ejecuta cada estrategia en low-load con 1, 2 y 3 Spark workers
# usando el profile Docker Compose "scaling". Los resultados se
# guardan en results/<strategy>/scaling_<N>w/ para que analyze.py
# calcule la eficiencia de escalado.
if [ "$SCALING_TEST" = true ]; then
    echo ""
    echo "┌──────────────────────────────────────────────────────────┐"
    echo "│  SCALING TEST — 1 → 2 → 3 Spark workers"
    echo "└──────────────────────────────────────────────────────────┘"

    SCALING_SCENARIO="low-load"
    SCALING_DURATION=120   # 2 min es suficiente para medir throughput estable

    for STRATEGY in $STRATEGIES; do
        for N_WORKERS in 1 2 3; do
            SCALING_RUN_ID="scaling_${N_WORKERS}w"
            SCALING_RUN_DIR="$RESULTS_BASE/$STRATEGY/${SCALING_SCENARIO}/${SCALING_RUN_ID}"
            mkdir -p "$SCALING_RUN_DIR"

            echo "[experiment] Scaling: $STRATEGY / ${N_WORKERS} worker(s)"

            # Bajar workers adicionales, luego levantar solo los necesarios
            docker compose --profile scaling down spark-worker-2 spark-worker-3 2>/dev/null || true

            if [ "$N_WORKERS" -ge 2 ]; then
                docker compose --profile scaling up -d spark-worker-2
            fi
            if [ "$N_WORKERS" -ge 3 ]; then
                docker compose --profile scaling up -d spark-worker-3
            fi

            # Pequeña pausa para que los workers se registren en el master
            sleep 10

            # Ejecutar run con duración reducida
            RUN_DURATION_SECONDS=$SCALING_DURATION \
                bash "$ROOT_DIR/scripts/run.sh" "$STRATEGY" "$SCALING_SCENARIO" "$SCALING_RUN_ID"

            # Nota: run.sh ya copio el CSV filtrado al directorio del run via copy_probe_csv_to_run.

            # Añadir metadato de workers al snapshot de Prometheus
            if [ -f "$SCALING_RUN_DIR/prometheus_snapshot.csv" ]; then
                echo "${STRATEGY},${SCALING_SCENARIO},${SCALING_RUN_ID},n_workers,${N_WORKERS},count" \
                    >> "$SCALING_RUN_DIR/prometheus_snapshot.csv"
            fi
        done
    done

    # Restaurar a 1 worker (bajar los extras)
    docker compose --profile scaling down spark-worker-2 spark-worker-3 2>/dev/null || true
    echo "[experiment] Scaling test completado. Ver results/*/scaling_*w/"
fi
