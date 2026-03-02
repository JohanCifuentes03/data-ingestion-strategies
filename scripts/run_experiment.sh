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

# ── Parse CLI flags ────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --strategies) STRATEGIES="$2";        shift 2 ;;
        --scenarios)  SCENARIOS="$2";         shift 2 ;;
        --reps)       REPETITIONS="$2";       shift 2 ;;
        --trigger)    TRIGGER_INTERVAL="$2";  shift 2 ;;
        --cooldown)   COOLDOWN_SECONDS="$2";  shift 2 ;;
        --no-export)  EXPORT_METRICS=false;   shift   ;;
        *) echo "[experiment] Unknown flag: $1"; exit 1 ;;
    esac
done

TOTAL_RUNS=0
for _ in $STRATEGIES; do for _ in $SCENARIOS; do TOTAL_RUNS=$((TOTAL_RUNS + REPETITIONS)); done; done
CURRENT_RUN=0

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              EXPERIMENT RUNNER — TESIS BENCHMARK            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Strategies : $STRATEGIES"
echo "║  Scenarios  : $SCENARIOS"
echo "║  Repetitions: $REPETITIONS"
echo "║  Total runs : $TOTAL_RUNS"
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
                echo "[experiment] Results saved to $RUN_DIR/"
            fi

            # ── Export Prometheus metrics ───────────────────────────
            if [ "$EXPORT_METRICS" = true ] && [ -f "$ROOT_DIR/scripts/export_metrics.py" ]; then
                python3 "$ROOT_DIR/scripts/export_metrics.py" \
                    --strategy "$STRATEGY" \
                    --scenario "$SCENARIO" \
                    --run-id "$RUN_ID" \
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
