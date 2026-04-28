#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

DC=(docker compose --env-file .env -f infra/docker/compose/docker-compose.yml)

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

MODE="distributed"
SUBCOMMAND=""
SCOPE="official"
LOAD_PROFILE="constant"
COMPUTE_REGION="primary"
RESULTS_DIR=""
DO_PROVISION=false
DO_DEPLOY=false
SKIP_SETUP=false
SKIP_COLLECT=false
SKIP_ANALYZE=false
SKIP_VALIDATE=false
DESTROY_INFRA=false

STRATEGIES="batch microbatch streaming"
SCENARIOS="low-load medium-load high-load"
REPS=5
DURATION=300
WARMUP=30
COOLDOWN=30
TRIGGER="5 seconds"

log() {
    echo -e "${GREEN}[thesis]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[thesis]${NC} $*"
}

err() {
    echo -e "${RED}[thesis]${NC} $*" >&2
}

usage() {
    cat <<'EOF'
Uso:
  bash scripts/thesis.sh <subcommand> [options]

Fase 1. Infraestructura:
  provision  Provisiona IaC distribuida
  deploy     Despliega servicios
  destroy    Baja local o destruye IaC distribuida

Fase 2. Ejecución:
  run        Ejecuta solo el experimento

Fase 3. Resultados y análisis:
  collect    Recolecta resultados distribuidos
  analyze    Genera figuras
  validate   Valida resultados

Conveniencia:
  full       Ejecuta pipeline completo
  help       Muestra esta ayuda

Opciones:
  --mode <local|distributed>     Modo de ejecución (default: distributed)
  --scope <official|advanced>    Alcance del benchmark (default: official)
  --load-profile <constant|bursty|cyclic>
                                  Perfil de carga del generator (default: constant)
  --compute-region <primary|brazil>
                                  Ubicación del nodo compute (default: primary)
  --results-dir <path>           Directorio base de resultados
  --provision                    Ejecuta provision antes de run/full/deploy
  --deploy                       Ejecuta deploy antes de run/full
  --skip-setup                   En local, no hace setup/build/up automáticamente
  --skip-collect                 En full, omite collect
  --skip-analyze                 En full, omite análisis
  --skip-validate                En full, omite validación
  --reps <N>                     Repeticiones (default: 5)
  --duration <s>                 Duración por run (default: 300)
  --warmup <s>                   Warmup por run (default: 30)
  --cooldown <s>                 Cooldown entre runs (default: 30)
  --trigger <interval>           Trigger microbatch (default: 5 seconds)
  --strategies "..."            Estrategias (default: batch microbatch streaming)
  --scenarios "..."             Escenarios (default: low-load medium-load high-load)

Ejemplos:
  # Flujo oficial recomendado
  bash scripts/thesis.sh provision --mode distributed
  bash scripts/thesis.sh deploy --mode distributed
  bash scripts/thesis.sh run --mode distributed
  bash scripts/thesis.sh collect --mode distributed
  bash scripts/thesis.sh analyze --mode distributed
  bash scripts/thesis.sh validate --mode distributed

  # Conveniencia
  bash scripts/thesis.sh full --mode distributed --deploy

  # Debug local
  bash scripts/thesis.sh run --mode local --reps 1 --duration 60 --warmup 5 --cooldown 5
  bash scripts/thesis.sh full --mode local --reps 1 --duration 60 --warmup 5 --cooldown 5
EOF
}

ensure_env_file() {
    if [[ ! -f ".env" ]]; then
        if [[ -f ".env.example" ]]; then
            cp ".env.example" ".env"
            log "Creado .env desde .env.example"
        else
            err ".env.example no existe"
            exit 1
        fi
    fi
}

ensure_python_env() {
    if [[ ! -d ".venv" ]]; then
        log "Creando entorno virtual Python"
        python3 -m venv .venv
        .venv/bin/pip install --upgrade pip
        .venv/bin/pip install -e src/python/
    fi
}

compile_jobs() {
    log "Compilando jobs Java"
    if [[ -f "./gradlew" ]]; then
        ./gradlew buildJobs
    else
        gradle buildJobs
    fi
}

build_local_images() {
    log "Construyendo imágenes locales necesarias"
    "${DC[@]}" build generator probe
}

ensure_local_results_permissions() {
    mkdir -p results
    if [[ -w results ]]; then
        return 0
    fi

    warn "Corrigiendo permisos de results/ creados por contenedores"
    docker run --rm \
        -v "$ROOT_DIR/results:/results" \
        alpine:3.20 \
        sh -c "chown -R $(id -u):$(id -g) /results && chmod -R u+rwX /results" >/dev/null

    if [[ ! -w results ]]; then
        err "No se pudieron corregir permisos de results/"
        exit 1
    fi
}

start_local_stack() {
    log "Levantando stack local"
    local results_host
    results_host="$(realpath -m "$RESULTS_DIR")"
    mkdir -p "$results_host"
    sudo chown -R "$(id -un):$(id -gn)" "$results_host" >/dev/null 2>&1 || true
    chmod 0777 "$results_host" >/dev/null 2>&1 || true
    RESULTS_VOLUME_HOST="$results_host" "${DC[@]}" up -d
}

stop_local_stack() {
    log "Bajando stack local"
    "${DC[@]}" down
}

require_outputs_env() {
    if [[ "$MODE" != "distributed" ]]; then
        return 0
    fi
    if [[ ! -f "infra/terraform/outputs.env" ]]; then
        err "infra/terraform/outputs.env no existe. Ejecuta: bash scripts/thesis.sh provision"
        exit 1
    fi
    # shellcheck source=/dev/null
    source "infra/terraform/outputs.env"
}

setup_local_stack() {
    if [[ "$MODE" != "local" ]]; then
        return 0
    fi
    if [[ "$SKIP_SETUP" == true ]]; then
        return 0
    fi
    log "Preparando stack local"
    ensure_local_results_permissions
    ensure_env_file
    ensure_python_env
    compile_jobs
    build_local_images
    start_local_stack
}

run_provision() {
    if [[ "$MODE" != "distributed" ]]; then
        err "provision solo aplica a modo distributed"
        exit 1
    fi
    log "Provisionando infraestructura distribuida"
    terraform -chdir=infra/terraform init
    if [[ "$COMPUTE_REGION" == "brazil" ]]; then
        terraform -chdir=infra/terraform apply -auto-approve -var="enable_brazil_compute=true"
    else
        terraform -chdir=infra/terraform apply -auto-approve
    fi
}

run_deploy() {
    if [[ "$MODE" != "distributed" ]]; then
        err "deploy solo aplica a modo distributed"
        exit 1
    fi
    if [[ "$DO_PROVISION" == true ]]; then
        run_provision
    fi
    require_outputs_env
    log "Desplegando servicios distribuidos"
    (
        # shellcheck source=/dev/null
        source infra/terraform/outputs.env
        cd infra/ansible
        if [[ "$COMPUTE_REGION" == "brazil" ]]; then
            ansible-playbook -i inventory.ini site-advanced-brazil.yml
        else
            ansible-playbook -i inventory.ini site.yml
        fi
    )
}

run_experiment() {
    if [[ "$MODE" == "local" ]]; then
        setup_local_stack
    else
        if [[ "$DO_PROVISION" == true ]]; then
            run_provision
        fi
        require_outputs_env
        if [[ "$DO_DEPLOY" == true ]]; then
            run_deploy
        fi
    fi

    log "Ejecutando experimento ${MODE}"
    MODE="$MODE" \
    SCOPE="$SCOPE" \
    LOAD_PROFILE="$LOAD_PROFILE" \
    COMPUTE_REGION="$COMPUTE_REGION" \
    RESULTS_BASE="$RESULTS_DIR" \
    bash scripts/experiment.sh \
        --strategies "$STRATEGIES" \
        --scenarios "$SCENARIOS" \
        --reps "$REPS" \
        --duration "$DURATION" \
        --warmup "$WARMUP" \
        --cooldown "$COOLDOWN" \
        --trigger "$TRIGGER"
}

run_collect() {
    if [[ "$MODE" != "distributed" ]]; then
        warn "collect no aplica a modo local"
        return 0
    fi
    require_outputs_env
    log "Recolectando resultados distribuidos"
    RESULTS_BASE="$RESULTS_DIR" bash scripts/collect-results.sh
}

results_dir() {
    printf '%s\n' "$RESULTS_DIR"
}

run_analyze() {
    local rdir
    rdir=$(results_dir)
    if [[ "$MODE" == "local" ]]; then
        ensure_local_results_permissions
    fi
    log "Analizando resultados en ${rdir}"
    .venv/bin/python -m benchmark.analysis.analyzer --results-dir "$rdir" --output "$rdir/figures" --scope official --validate
}

run_validate() {
    local rdir
    rdir=$(results_dir)
    if [[ "$MODE" == "local" ]]; then
        ensure_local_results_permissions
    fi
    log "Validando resultados en ${rdir}"
    .venv/bin/python -m benchmark.validation.validator --results-dir "$rdir"
}

run_full() {
    run_experiment
    if [[ "$SKIP_COLLECT" == false ]]; then
        run_collect
    fi
    if [[ "$SKIP_ANALYZE" == false ]]; then
        run_analyze
    fi
    if [[ "$SKIP_VALIDATE" == false ]]; then
        run_validate
    fi
}

run_destroy() {
    if [[ "$MODE" == "local" ]]; then
        log "Bajando stack local"
        stop_local_stack
        return 0
    fi
    require_outputs_env
    log "Destruyendo infraestructura distribuida"
    terraform -chdir=infra/terraform destroy -auto-approve
}

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

SUBCOMMAND="$1"
shift

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="${2:-}"
            shift 2
            ;;
        --scope)
            SCOPE="${2:-}"
            shift 2
            ;;
        --load-profile)
            LOAD_PROFILE="${2:-}"
            shift 2
            ;;
        --compute-region)
            COMPUTE_REGION="${2:-}"
            shift 2
            ;;
        --results-dir)
            RESULTS_DIR="${2:-}"
            shift 2
            ;;
        --provision)
            DO_PROVISION=true
            shift
            ;;
        --deploy)
            DO_DEPLOY=true
            shift
            ;;
        --skip-setup)
            SKIP_SETUP=true
            shift
            ;;
        --skip-collect)
            SKIP_COLLECT=true
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
        --reps)
            REPS="${2:-}"
            shift 2
            ;;
        --duration)
            DURATION="${2:-}"
            shift 2
            ;;
        --warmup)
            WARMUP="${2:-}"
            shift 2
            ;;
        --cooldown)
            COOLDOWN="${2:-}"
            shift 2
            ;;
        --trigger)
            TRIGGER="${2:-}"
            shift 2
            ;;
        --strategies)
            STRATEGIES="${2:-}"
            shift 2
            ;;
        --scenarios)
            SCENARIOS="${2:-}"
            shift 2
            ;;
        help|-h|--help)
            usage
            exit 0
            ;;
        *)
            err "Argumento desconocido: $1"
            usage
            exit 1
            ;;
    esac
done

if [[ "$MODE" != "local" && "$MODE" != "distributed" ]]; then
    err "--mode debe ser local o distributed"
    exit 1
fi

if [[ "$SCOPE" != "official" && "$SCOPE" != "advanced" ]]; then
    err "--scope debe ser official o advanced"
    exit 1
fi

if [[ "$LOAD_PROFILE" != "constant" && "$LOAD_PROFILE" != "bursty" && "$LOAD_PROFILE" != "cyclic" ]]; then
    err "--load-profile debe ser constant, bursty o cyclic"
    exit 1
fi

if [[ "$COMPUTE_REGION" != "primary" && "$COMPUTE_REGION" != "brazil" ]]; then
    err "--compute-region debe ser primary o brazil"
    exit 1
fi

if [[ "$SCOPE" == "official" ]]; then
    LOAD_PROFILE="constant"
fi

if [[ -z "$RESULTS_DIR" ]]; then
    if [[ "$SCOPE" == "advanced" ]]; then
        RESULTS_DIR="results-advanced"
    elif [[ "$MODE" == "distributed" ]]; then
        RESULTS_DIR="results-distributed"
    else
        RESULTS_DIR="results"
    fi
fi

case "$SUBCOMMAND" in
    run)
        run_experiment
        ;;
    collect)
        run_collect
        ;;
    analyze)
        run_analyze
        ;;
    validate)
        run_validate
        ;;
    full)
        run_full
        ;;
    provision)
        run_provision
        ;;
    deploy)
        run_deploy
        ;;
    destroy)
        run_destroy
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        err "Subcommand desconocido: $SUBCOMMAND"
        usage
        exit 1
        ;;
esac
