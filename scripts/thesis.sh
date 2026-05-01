#!/usr/bin/env bash
# Public benchmark entrypoint.
#
# Use this script for normal local/distributed workflows. Lower-level scripts
# under scripts/ are internal helpers and may mutate runtime state directly.
# In particular, run/collect/destroy paths can truncate database tables,
# replace local results, or destroy infrastructure depending on subcommand.

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

#######################################
# Prints an informational thesis workflow message.
# Arguments:
#   $* - Message text.
# Outputs:
#   Writes colored text to stdout.
#######################################
log() {
    echo -e "${GREEN}[thesis]${NC} $*"
}

#######################################
# Prints a non-fatal thesis workflow warning.
# Arguments:
#   $* - Warning text.
# Outputs:
#   Writes colored text to stdout.
#######################################
warn() {
    echo -e "${YELLOW}[thesis]${NC} $*"
}

#######################################
# Prints a fatal/error thesis workflow message.
# Arguments:
#   $* - Error text.
# Outputs:
#   Writes colored text to stderr.
#######################################
err() {
    echo -e "${RED}[thesis]${NC} $*" >&2
}

#######################################
# Shows the public thesis workflow CLI help.
# Outputs:
#   Writes usage examples and options to stdout.
#######################################
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

#######################################
# Ensures local Docker/Python configuration exists.
# Side effects:
#   Creates .env from .env.example when missing.
# Returns:
#   Exits non-zero if no template exists.
#######################################
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

#######################################
# Ensures the editable Python benchmark package is installed locally.
# Side effects:
#   Creates .venv and installs src/python when .venv is absent.
#######################################
ensure_python_env() {
    if [[ ! -d ".venv" ]]; then
        log "Creando entorno virtual Python"
        python3 -m venv .venv
        .venv/bin/pip install --upgrade pip
        .venv/bin/pip install -e src/python/
    fi
}

#######################################
# Builds Java Spark/Flink benchmark job artifacts.
# Side effects:
#   Runs Gradle buildJobs using the wrapper when available.
#######################################
compile_jobs() {
    log "Compilando jobs Java"
    if [[ -f "./gradlew" ]]; then
        ./gradlew buildJobs
    else
        gradle buildJobs
    fi
}

#######################################
# Builds local generator and probe Docker images.
# Globals:
#   DC - Docker Compose command array.
#######################################
build_local_images() {
    log "Construyendo imágenes locales necesarias"
    "${DC[@]}" build generator probe
}

#######################################
# Ensures the local results directory is writable by the host user.
# Side effects:
#   Creates results/ and may use an Alpine container to repair ownership.
# Returns:
#   Exits non-zero if permissions cannot be repaired.
#######################################
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

#######################################
# Starts the local Docker Compose benchmark stack.
# Globals:
#   RESULTS_DIR, DC, ROOT_DIR.
# Side effects:
#   Creates the result directory and launches containers.
#######################################
start_local_stack() {
    log "Levantando stack local"
    local results_host
    results_host="$(realpath -m "$RESULTS_DIR")"
    mkdir -p "$results_host"
    sudo chown -R "$(id -un):$(id -gn)" "$results_host" >/dev/null 2>&1 || true
    chmod 0777 "$results_host" >/dev/null 2>&1 || true
    RESULTS_VOLUME_HOST="$results_host" "${DC[@]}" up -d
}

#######################################
# Stops the local Docker Compose benchmark stack.
# Globals:
#   DC - Docker Compose command array.
#######################################
stop_local_stack() {
    log "Bajando stack local"
    "${DC[@]}" down
}

#######################################
# Loads Terraform outputs for distributed operations.
# Globals:
#   MODE.
# Side effects:
#   Sources infra/terraform/outputs.env in distributed mode.
# Returns:
#   Exits non-zero if distributed outputs are missing.
#######################################
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

#######################################
# Performs all setup needed before local experiments.
# Globals:
#   MODE, SKIP_SETUP.
# Side effects:
#   May create .env/.venv, compile jobs, build images, and start containers.
#######################################
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

#######################################
# Provisions distributed AWS infrastructure with Terraform.
# Globals:
#   MODE, COMPUTE_REGION.
# Returns:
#   Exits non-zero if called outside distributed mode.
#######################################
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

#######################################
# Deploys distributed benchmark services with Ansible.
# Globals:
#   MODE, DO_PROVISION, COMPUTE_REGION.
# Side effects:
#   May run Terraform first, then executes the appropriate Ansible playbook.
#######################################
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

#######################################
# Runs the experiment phase for local or distributed mode.
# Globals:
#   MODE, SCOPE, LOAD_PROFILE, COMPUTE_REGION, RESULTS_DIR, strategies and timing options.
# Side effects:
#   Delegates to scripts/experiment.sh and may provision/deploy first.
#######################################
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

#######################################
# Collects distributed results from the sink node.
# Globals:
#   MODE, RESULTS_DIR.
# Side effects:
#   Delegates to scripts/collect-results.sh, which replaces local destination contents.
#######################################
run_collect() {
    if [[ "$MODE" != "distributed" ]]; then
        warn "collect no aplica a modo local"
        return 0
    fi
    require_outputs_env
    log "Recolectando resultados distribuidos"
    RESULTS_BASE="$RESULTS_DIR" bash scripts/collect-results.sh
}

#######################################
# Prints the resolved result root for downstream commands.
# Outputs:
#   Writes RESULTS_DIR to stdout.
#######################################
results_dir() {
    printf '%s\n' "$RESULTS_DIR"
}

#######################################
# Runs the Python analysis/figure generation pipeline.
# Globals:
#   MODE, SCOPE, RESULTS_DIR.
# Side effects:
#   Writes figures and summary CSV files under the result directory.
#######################################
run_analyze() {
    local rdir
    local analyzer_scope
    rdir=$(results_dir)
    analyzer_scope="official"
    if [[ "$SCOPE" == "advanced" ]]; then
        analyzer_scope="all"
    fi
    if [[ "$MODE" == "local" ]]; then
        ensure_local_results_permissions
    fi
    log "Analizando resultados en ${rdir}"
    .venv/bin/python -m benchmark.analysis.analyzer --results-dir "$rdir" --output "$rdir/figures" --scope "$analyzer_scope" --validate
}

#######################################
# Runs the Python result validation pipeline.
# Globals:
#   MODE, RESULTS_DIR.
# Returns:
#   Non-zero if validation reports errors.
#######################################
run_validate() {
    local rdir
    rdir=$(results_dir)
    if [[ "$MODE" == "local" ]]; then
        ensure_local_results_permissions
    fi
    log "Validando resultados en ${rdir}"
    .venv/bin/python -m benchmark.validation.validator --results-dir "$rdir"
}

#######################################
# Runs experiment, collection, analysis, and validation as a convenience flow.
# Globals:
#   SKIP_COLLECT, SKIP_ANALYZE, SKIP_VALIDATE.
# Side effects:
#   Performs every non-skipped workflow phase in order.
#######################################
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

#######################################
# Tears down local containers or distributed Terraform infrastructure.
# Globals:
#   MODE.
# Side effects:
#   Stops local stack or destroys cloud infrastructure.
#######################################
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
