#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# experiment.sh — Experimento automatizado
#
# Itera sobre estrategias × escenarios × repeticiones:
# clean → warmup → run → cooldown → export
#
# Rol operativo:
#   Orquestador interno de matrices. thesis.sh configura variables de entorno y
#   delega aquí; este script luego llama scripts/run.sh por cada corrida.
#
# Riesgos:
#   No sabe reanudar automáticamente desde el último run exitoso: siempre itera
#   desde run_1 hasta run_N para las estrategias/escenarios indicados.
#   Para continuar una prueba parcial, limitar --strategies/--scenarios o llamar
#   scripts/run.sh manualmente con cuidado.
#
# Uso:
#   ./scripts/experiment.sh --smoke        # ~5 min (1 estrategia, 1 escenario)
#   ./scripts/experiment.sh --quick        # ~30 min (3 estrategias, 2 escenarios)
#   ./scripts/experiment.sh                # matriz tesis completa
#   ./scripts/experiment.sh --reps 3       # 3 repeticiones
#   ./scripts/experiment.sh --duration 240 # 4 min por corrida
#   ./scripts/experiment.sh --strategies batch # solo batch
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
RESULTS_BASE="${RESULTS_BASE:-$ROOT_DIR/results}"

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
SCOPE=${SCOPE:-official}
LOAD_PROFILE=${LOAD_PROFILE:-constant}
COMPUTE_REGION=${COMPUTE_REGION:-primary}
export RUN_DURATION_SECONDS=${RUN_DURATION_SECONDS:-300}
WARMUP_SECONDS=${WARMUP_SECONDS:-30}    # 30 s per protocol spec (JVM JIT warm-up)

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
export LOAD_PROFILE
export COMPUTE_REGION
export RESULTS_BASE
export SCOPE

# Guardrail: en ejecuciones oficiales solo permitir escenarios oficiales
OFFICIAL_SCENARIOS="low-load medium-load high-load"
for SCN in $SCENARIOS; do
    case "$SCN" in
        low-load|medium-load|high-load) ;;
        *)
            if [ "$SCOPE" = "official" ]; then
                echo "[experiment] WARN: escenario no-oficial detectado: $SCN"
            fi
            ;;
    esac
done

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
echo "║  Scope       : $SCOPE"
echo "║  Load profile: $LOAD_PROFILE"
echo "║  Compute reg.: $COMPUTE_REGION"
echo "║  Results dir : $RESULTS_BASE"
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
        if ! RESULTS_BASE="$RESULTS_BASE" bash "$CLOCK_SYNC_SCRIPT"; then
            echo "[experiment] ABORT: Relojes desincronizados. Ver log en ${RESULTS_BASE}/clock_offsets_*.csv"
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
                    "echo 'event_id,strategy,scenario,run_id,produced_at,visible_at,latency_ms' > /results/latency_samples.csv" 2>/dev/null || true
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
