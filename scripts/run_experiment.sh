#!/usr/bin/env bash
# ───────────────────────────────────────────────────────────────────
# run_experiment.sh — Automated experiment runner
#
# Iterates over strategies × scenarios × repetitions following the
# protocol: clean → warmup → run → cooldown → export
#
# Usage:
#   ./scripts/run_experiment.sh                   # all defaults
#   ./scripts/run_experiment.sh --reps 3          # 3 repetitions
#   ./scripts/run_experiment.sh --duration 240    # 4 minutes per run
#   ./scripts/run_experiment.sh --strategies batch # single strategy
# ───────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
RESULTS_BASE="$ROOT_DIR/results"

# ── Defaults (override via arguments) ──────────────────────────────
STRATEGIES=${STRATEGIES:-"batch microbatch streaming"}
SCENARIOS=${SCENARIOS:-"low-load medium-load high-load burst"}
REPETITIONS=${REPETITIONS:-5}
TRIGGER_INTERVAL=${TRIGGER_INTERVAL:-"5 seconds"}
COOLDOWN_SECONDS=${COOLDOWN_SECONDS:-10}
EXPORT_METRICS=${EXPORT_METRICS:-true}
EXPORT_WINDOW=${EXPORT_WINDOW:-"5m"}    # Prometheus lookback window for metric export
EVENT_SCHEMA=${EVENT_SCHEMA:-""}        # leave empty to use scenario default
export RUN_DURATION_SECONDS=${RUN_DURATION_SECONDS:-1200} # Default 20 mins per run

# ── Parse CLI flags ────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --strategies)  STRATEGIES="$2";       shift 2 ;;
        --scenarios)   SCENARIOS="$2";        shift 2 ;;
        --reps)        REPETITIONS="$2";      shift 2 ;;
        --trigger)     TRIGGER_INTERVAL="$2"; shift 2 ;;
        --cooldown)    COOLDOWN_SECONDS="$2"; shift 2 ;;
        --no-export)   EXPORT_METRICS=false;  shift   ;;
        --window)      EXPORT_WINDOW="$2";    shift 2 ;;
        --schema)      EVENT_SCHEMA="$2";     shift 2 ;;
        --duration)    export RUN_DURATION_SECONDS="$2"; shift 2 ;;
        *) echo "[experiment] Unknown flag: $1"; exit 1 ;;
    esac
done

TOTAL_RUNS=0
for _ in $STRATEGIES; do for _ in $SCENARIOS; do TOTAL_RUNS=$((TOTAL_RUNS + REPETITIONS)); done; done
CURRENT_RUN=0

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              EXPERIMENT RUNNER — TESIS BENCHMARK            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Strategies  : $STRATEGIES"
echo "║  Scenarios   : $SCENARIOS"
echo "║  Repetitions : $REPETITIONS"
echo "║  Duration    : ${RUN_DURATION_SECONDS}s per run"
echo "║  Total runs  : $TOTAL_RUNS"
echo "║  Export win. : $EXPORT_WINDOW"
echo "║  Schema      : ${EVENT_SCHEMA:-<scenario default>}"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

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
            docker compose exec -T probe sh -c \
                "echo 'event_id,produced_at,visible_at,latency_ms,strategy,scenario,run_id' > /results/latency_samples.csv"

            # ── Run the strategy ───────────────────────────────────
            case "$STRATEGY" in
                batch)
                    bash "$ROOT_DIR/scripts/run_batch.sh" "$SCENARIO" "$RUN_ID"
                    ;;
                microbatch)
                    bash "$ROOT_DIR/scripts/run_microbatch.sh" "$SCENARIO" "$RUN_ID" "$TRIGGER_INTERVAL"
                    ;;
                streaming)
                    bash "$ROOT_DIR/scripts/run_streaming.sh" "$SCENARIO" "$RUN_ID"
                    ;;
                *)
                    echo "[experiment] Unknown strategy: $STRATEGY"; exit 1
                    ;;
            esac

            # ── Cooldown ───────────────────────────────────────────
            echo "[experiment] Cooldown ${COOLDOWN_SECONDS}s ..."
            sleep "$COOLDOWN_SECONDS"

            # ── Copy results ───────────────────────────────────────
            if [ -f "$RESULTS_BASE/latency_samples.csv" ]; then
                cp "$RESULTS_BASE/latency_samples.csv" "$RUN_DIR/latency_samples.csv"
                # Validate that the run produced enough data (more than just the header)
                SAMPLE_LINES=$(wc -l < "$RUN_DIR/latency_samples.csv" 2>/dev/null || echo 0)
                if [ "$SAMPLE_LINES" -lt 10 ]; then
                    echo "[experiment] WARNING: run $STRATEGY/$SCENARIO/$RUN_ID produced only $SAMPLE_LINES sample lines — possible silent failure"
                else
                    echo "[experiment] Results saved to $RUN_DIR/ ($SAMPLE_LINES lines)"
                fi
            else
                echo "[experiment] WARNING: no results CSV found for $STRATEGY/$SCENARIO/$RUN_ID"
            fi

            # ── Export Prometheus metrics ───────────────────────────
            if [ "$EXPORT_METRICS" = true ] && [ -f "$ROOT_DIR/scripts/export_metrics.py" ]; then
                python3 "$ROOT_DIR/scripts/export_metrics.py" \
                    --strategy "$STRATEGY" \
                    --scenario "$SCENARIO" \
                    --run-id "$RUN_ID" \
                    --window "$EXPORT_WINDOW" \
                    --output "$RUN_DIR/prometheus_snapshot.csv" 2>/dev/null || \
                    echo "[experiment] Warning: Prometheus export failed (non-fatal)"
            fi

        done
    done
done

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          EXPERIMENT COMPLETE — $TOTAL_RUNS runs finished           ║"
echo "║  Results directory: $RESULTS_BASE/                          "
echo "╚══════════════════════════════════════════════════════════════╝"
