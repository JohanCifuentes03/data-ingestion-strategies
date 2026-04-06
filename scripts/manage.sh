#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# manage.sh — Gestión del entorno de experimentos
#
# Combina: setup, setup_minimal, teardown, clean, doctor, reset
#
# Uso (modo local — default):
#   ./scripts/manage.sh up        # Levantar infraestructura
#   ./scripts/manage.sh build     # Compilar jobs Java
#   ./scripts/manage.sh status     # Ver estado de servicios
#   ./scripts/manage.sh clean      # Limpiar Kafka, PostgreSQL, checkpoints
#   ./scripts/manage.sh down      # Bajar contenedores
#   ./scripts/manage.sh reset     # Reset completo (borra todo)
#
# Uso (modo distribuido AWS):
#   MODE=distributed ./scripts/manage.sh up     # Pre-flight check + instrucciones
#   MODE=distributed ./scripts/manage.sh status # Estado de las 4 VMs remotas
# ═══════════════════════════════════════════════════════════════════

# ── Modo de ejecución ────────────────────────────────────────────
# local      : todo en Docker Compose en esta máquina (default)
# distributed: 4 VMs reales en AWS gestionadas via IaC
MODE=${MODE:-local}

if [ "$MODE" = "distributed" ]; then
    # Cargar IPs generadas por terraform apply
    OUTPUTS_ENV="$(dirname "$0")/../infra/terraform/outputs.env"
    if [ -f "$OUTPUTS_ENV" ]; then
        # shellcheck source=/dev/null
        source "$OUTPUTS_ENV"
        # Variable crítica: hace que el broker anuncie su IP privada AWS
        # en lugar de 'kafka:9092' (que solo funciona dentro de Docker)
        export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${CLOUD_VM_BROKER_IP}:9092"
        export KAFKA_BOOTSTRAP_SERVERS="${CLOUD_VM_BROKER_IP}:9092"
    else
        echo "[manage] WARN: outputs.env no encontrado. Ejecuta: cd infra/terraform && terraform apply"
    fi
fi

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
LIB_DIR="$ROOT_DIR/lib"
cd "$ROOT_DIR"

# Docker Compose command with proper paths
DC="docker compose --env-file .env -f infra/docker/compose/docker-compose.yml"

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[manage]${NC} $*"; }
warn() { echo -e "${YELLOW}[manage]${NC} $*"; }
error() { echo -e "${RED}[manage]${NC} $*"; }

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        error "Se requiere '$1' pero no está instalado."
        exit 1
    fi
}

cmd_up() {
    log "Levantando infraestructura..."

    # Verificar Docker
    require_cmd docker
    if ! docker info >/dev/null 2>&1; then
        error "Docker no está ejecutándose. Inicia Docker Desktop."
        exit 1
    fi

    # Verificar/crear .env
    if [ ! -f "$ROOT_DIR/.env" ]; then
        if [ -f "$ROOT_DIR/.env.example" ]; then
            cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
            log "Creado .env desde .env.example"
        fi
    fi

    # Compilar JARs siempre para tener la última versión
    if command -v java >/dev/null 2>&1 && { command -v gradle >/dev/null 2>&1 || [ -f "$ROOT_DIR/gradlew" ]; }; then
        cmd_build
    else
        if [ ! -d "$LIB_DIR" ] || [ -z "$(ls -A "$LIB_DIR"/*.jar 2>/dev/null)" ]; then
            error "Necesitas Java 17+ y Gradle para compilar, o descarga los JARs pre-compilados en lib/."
            exit 1
        else
            warn "No se encontraron herramientas de compilación. Usando JARs pre-compilados existentes."
        fi
    fi

    # NOTE: With Docker-only builds, this step is no longer needed
    # Jobs are built inside Docker images via multi-stage Dockerfiles

    # Construir imágenes
    log "Construyendo imágenes Docker..."
    $DC build generator probe

    # Levantar servicios
    log "Levantando servicios..."
    $DC up -d

    # Esperar Kafka
    log "Esperando a Kafka..."
    for i in $(seq 1 30); do
        if $DC exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
            log "Kafka disponible"
            break
        fi
        sleep 2
    done

    # Crear tópico
    $DC exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true

    log "Infraestructura lista!"
    echo ""
    echo "Servicios:"

    echo "  • Spark UI:     http://localhost:8080"
    echo "  • Flink UI:     http://localhost:8081"
    echo "  • Prometheus:   http://localhost:9090"
    echo "  • cAdvisor:     http://localhost:8083"
    echo "  • Kafka Metrics: http://localhost:9308/metrics"
}

cmd_build() {
    log "Compilando jobs Java..."
    
    require_cmd java
    
    if [ -f "$ROOT_DIR/gradlew" ]; then
        ./gradlew buildJobs
    else
        require_cmd gradle
        gradle buildJobs
    fi
    
    # NOTE: Jobs are now in src/jobs/ and built via Docker multi-stage
    log "Jobs compilados (se usarán las imágenes Docker multi-stage)"
}

cmd_status() {
    log "Estado de servicios:"
    $DC ps
    
    echo ""
    log "Verificando endpoints..."
    
    # Kafka
    if $DC exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        echo "  ✅ Kafka:9092"
    else
        echo "  ❌ Kafka:9092"
    fi
    
    # PostgreSQL
    if $DC exec -T postgres pg_isready -U benchmark >/dev/null 2>&1; then
        echo "  ✅ PostgreSQL:5432"
    else
        echo "  ❌ PostgreSQL:5432"
    fi

    # Prometheus
    if curl -sf http://localhost:9090/-/healthy >/dev/null 2>&1; then
        echo "  ✅ Prometheus:9090"
    else
        echo "  ❌ Prometheus:9090"
    fi

    # cAdvisor
    if curl -sf http://localhost:8083/healthz >/dev/null 2>&1; then
        echo "  ✅ cAdvisor:8083"
    else
        echo "  ❌ cAdvisor:8083"
    fi

    # Kafka Exporter
    if curl -sf http://localhost:9308/metrics >/dev/null 2>&1; then
        echo "  ✅ kafka-exporter:9308"
    else
        echo "  ❌ kafka-exporter:9308"
    fi
}

cmd_clean() {
    local SCENARIO=${1:-generic}
    log "Limpiando entorno para escenario: $SCENARIO"
    
    # Limpiar tópico Kafka
    $DC exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
    $DC exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true
    log "Tópico 'events' recreado"
    
    # Limpiar PostgreSQL
    $DC exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    log "Tabla 'events' truncada"
    
    # Limpiar checkpoints
    docker exec tesis-ingestion-spark-worker-1 rm -rf /opt/spark/checkpoints/* 2>/dev/null || true
    log "Checkpoints limpiados"
    
    log "Limpieza completa"
}

cmd_down() {
    log "Bajando contenedores..."
    $DC down
    log "Contenedores detenidos"
}

cmd_reset() {
    echo -e "${RED}╔══════════════════════════════════════════════════════════════╗"
    echo -e "║           RESET COMPLETO — TODOS LOS DATOS SE PERDERÁN        ║"
    echo -e "╚══════════════════════════════════════════════════════════════╝${NC}"
    
    read -p "¿Escribe 'yes' para confirmar: " CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Cancelado."
        exit 0
    fi
    
    log "Deteniendo contenedores y eliminando volúmenes..."
    $DC --profile scaling down -v --remove-orphans 2>/dev/null || true
    
    log "Eliminando imágenes generadas..."
    docker rmi tesis-ingestion-generator tesis-ingestion-probe 2>/dev/null || true
    
    log "Limpiando resultados..."
    rm -rf "$ROOT_DIR/results"/*
    
    log "Restaurando .env..."
    cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env" 2>/dev/null || true
    
    log "Reset completo"
}

# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

COMMAND=${1:-help}

case "$COMMAND" in
    up)
        cmd_up
        ;;
    build)
        cmd_build
        ;;
    status)
        cmd_status
        ;;
    clean)
        cmd_clean "${2:-generic}"
        ;;
    down)
        cmd_down
        ;;
    reset)
        cmd_reset
        ;;
    help|--help|-h)
        echo "╔══════════════════════════════════════════════════════════════╗"
        echo "║                    GESTIÓN DEL ENTORNO                      ║"
        echo "╚══════════════════════════════════════════════════════════════╝"
        echo ""
        echo "Uso: ./scripts/manage.sh <comando> [opciones]"
        echo ""
        echo "Comandos:"
        echo "  up          Levantar infraestructura completa"
        echo "  build       Compilar jobs Java (requiere Java + Gradle)"
        echo "  status      Ver estado de servicios"
        echo "  clean       Limpiar Kafka, PostgreSQL y checkpoints"
        echo "  down        Bajar contenedores"
        echo "  reset       Reset completo (borra TODO)"
        echo ""
        echo "Ejemplos:"
        echo "  ./scripts/manage.sh up"
        echo "  ./scripts/manage.sh status"
        echo "  ./scripts/manage.sh clean low-load"
        echo "  ./scripts/manage.sh reset"
        ;;
    *)
        error "Comando desconocido: $COMMAND"
        echo "Usa: ./scripts/manage.sh help"
        exit 1
        ;;
esac
