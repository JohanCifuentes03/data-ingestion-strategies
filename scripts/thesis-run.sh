#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

MODE=""
PROFILE="quick"
PROVISION=false
SKIP_SETUP=false
SKIP_DEPLOY=false
SKIP_ANALYZE=false
SKIP_VALIDATE=false
TEARDOWN=false

CUSTOM_ARGS=()

usage() {
    cat <<'EOF'
Usage:
  bash scripts/thesis-run.sh --mode <local|distributed> [options]

Options:
  --profile <smoke|quick|full>   Experiment profile (default: quick)
  --provision                    Run `make provision` first (distributed only)
  --skip-setup                   Skip local setup/build/up
  --skip-deploy                  Skip distributed deploy
  --skip-analyze                 Skip analysis step
  --skip-validate                Skip validation step
  --teardown                     Destroy infra at end (distributed) / stop stack (local)

Advanced experiment args (passed to scripts/experiment.sh):
  --strategies "..."
  --scenarios "..."
  --reps <N>
  --duration <seconds>
  --warmup <seconds>

Examples:
  bash scripts/thesis-run.sh --mode local --profile quick
  bash scripts/thesis-run.sh --mode distributed --profile smoke
  bash scripts/thesis-run.sh --mode distributed --profile quick --strategies "batch microbatch" --reps 2 --duration 180
EOF
}

log() {
    echo -e "${GREEN}[thesis-run]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[thesis-run]${NC} $*"
}

err() {
    echo -e "${RED}[thesis-run]${NC} $*" >&2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="${2:-}"
            shift 2
            ;;
        --profile)
            PROFILE="${2:-}"
            shift 2
            ;;
        --provision)
            PROVISION=true
            shift
            ;;
        --skip-setup)
            SKIP_SETUP=true
            shift
            ;;
        --skip-deploy)
            SKIP_DEPLOY=true
            shift
            ;;
        --skip-analyze)
            SKIP_ANALYZE=true
            shift
            ;;
        --skip-validate)
            SKIP_VALIDATE=true
            shift
            ;;
        --teardown)
            TEARDOWN=true
            shift
            ;;
        --strategies|--scenarios|--reps|--duration|--warmup)
            CUSTOM_ARGS+=("$1" "${2:-}")
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            err "Unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

if [[ "$MODE" != "local" && "$MODE" != "distributed" ]]; then
    err "--mode must be 'local' or 'distributed'"
    usage
    exit 1
fi

if [[ "$PROFILE" != "smoke" && "$PROFILE" != "quick" && "$PROFILE" != "full" ]]; then
    err "--profile must be smoke|quick|full"
    exit 1
fi

run_experiment() {
    local mode="$1"
    local profile="$2"

    if [[ ${#CUSTOM_ARGS[@]} -gt 0 ]]; then
        log "Running custom experiment in ${mode} mode"
        MODE="$mode" bash scripts/experiment.sh "${CUSTOM_ARGS[@]}"
        return
    fi

    case "$profile" in
        smoke)
            MODE="$mode" bash scripts/experiment.sh --smoke
            ;;
        quick)
            MODE="$mode" bash scripts/experiment.sh --quick
            ;;
        full)
            if [[ "$mode" = "distributed" ]]; then
                make distributed-experiment
            else
                make experiment MODE=local
            fi
            ;;
    esac
}

if [[ "$MODE" = "local" ]]; then
    log "Mode=local | profile=${PROFILE}"
    if [[ "$SKIP_SETUP" = false ]]; then
        log "Running local setup/build/up"
        make setup
        make build
        make up MODE=local
    fi

    run_experiment local "$PROFILE"

    if [[ "$SKIP_ANALYZE" = false ]]; then
        log "Analyzing local results"
        make analyze
    fi
    if [[ "$SKIP_VALIDATE" = false ]]; then
        log "Validating local results"
        make validate
    fi

    if [[ "$TEARDOWN" = true ]]; then
        warn "Stopping local stack"
        make down MODE=local || true
    fi

    log "Done. Local outputs: results/ and results/figures/"
    exit 0
fi

log "Mode=distributed | profile=${PROFILE}"

if [[ "$PROVISION" = true ]]; then
    log "Provisioning cloud infrastructure"
    make provision
fi

if [[ ! -f "infra/terraform/outputs.env" ]]; then
    err "infra/terraform/outputs.env not found. Run 'make provision' or pass --provision"
    exit 1
fi

if [[ "$SKIP_DEPLOY" = false ]]; then
    log "Deploying distributed services"
    make distributed-deploy
fi

run_experiment distributed "$PROFILE"

log "Collecting distributed outputs"
make aws-collect

if [[ "$SKIP_ANALYZE" = false ]]; then
    log "Analyzing distributed results"
    .venv/bin/python -m benchmark.analysis.analyzer --results-dir results-distributed --output results-distributed/figures
fi
if [[ "$SKIP_VALIDATE" = false ]]; then
    log "Validating distributed results"
    .venv/bin/python -m benchmark.validation.validator --results-dir results-distributed
fi

if [[ "$TEARDOWN" = true ]]; then
    warn "Destroying distributed infrastructure"
    printf 'y\n' | make distributed-teardown || true
fi

log "Done. Distributed outputs: results-distributed/ and results-distributed/figures/"
