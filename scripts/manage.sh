#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# manage.sh — Gestión del entorno de experimentos
#
# Combina: setup, setup_minimal, teardown, clean, doctor, reset
#
# Uso:
#   ./scripts/manage.sh up        # Levantar infraestructura
#   ./scripts/manage.sh build     # Compilar jobs Java
#   ./scripts/manage.sh status     # Ver estado de servicios
#   ./scripts/manage.sh clean      # Limpiar Kafka, PostgreSQL, checkpoints
#   ./scripts/manage.sh down      # Bajar contenedores
#   ./scripts/manage.sh reset     # Reset completo (borra todo)
# ═══════════════════════════════════════════════════════════════════

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
LIB_DIR="$ROOT_DIR/lib"
cd "$ROOT_DIR"

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

    # Verificar JARs
    if [ ! -d "$LIB_DIR" ] || [ -z "$(ls -A "$LIB_DIR"/*.jar 2>/dev/null)" ]; then
        warn "No se encontraron JARs pre-compilados."
        if command -v gradle >/dev/null 2>&1 || [ -f "$ROOT_DIR/gradlew" ]; then
            log "Compilando jobs..."
            ./gradlew buildJobs
        else
            error "Necesitas Java 17+ y Gradle para compilar, o descarga los JARs pre-compilados."
            exit 1
        fi
    fi

    # Copiar JARs a ubicaciones esperadas
    mkdir -p "$ROOT_DIR/batch/out/libs" "$ROOT_DIR/microbatch/out/libs" "$ROOT_DIR/streaming/out/libs"
    cp "$LIB_DIR"/batch-job.jar "$ROOT_DIR/batch/out/libs/" 2>/dev/null || true
    cp "$LIB_DIR"/microbatch-job.jar "$ROOT_DIR/microbatch/out/libs/" 2>/dev/null || true
    cp "$LIB_DIR"/streaming-job.jar "$ROOT_DIR/streaming/out/libs/" 2>/dev/null || true

    # Construir imágenes
    log "Construyendo imágenes Docker..."
    docker compose build generator probe

    # Levantar servicios
    log "Levantando servicios..."
    docker compose up -d

    # Esperar Kafka
    log "Esperando a Kafka..."
    for i in $(seq 1 30); do
        if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
            log "Kafka disponible"
            break
        fi
        sleep 2
    done

    # Crear tópico
    docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true

    log "Infraestructura lista!"
    echo ""
    echo "Servicios:"
    echo "  • Grafana:    http://localhost:3000 (admin/admin)"
    echo "  • Prometheus: http://localhost:9090"
    echo "  • Spark UI:   http://localhost:8080"
    echo "  • Flink UI:   http://localhost:8081"
}

cmd_build() {
    log "Compilando jobs Java..."
    
    require_cmd java
    require_cmd gradle
    
    ./gradlew buildJobs
    
    # Copiar a lib/
    mkdir -p "$LIB_DIR"
    cp "$ROOT_DIR/batch/out/libs/batch-job.jar" "$LIB_DIR/" 2>/dev/null || true
    cp "$ROOT_DIR/microbatch/out/libs/microbatch-job.jar" "$LIB_DIR/" 2>/dev/null || true
    cp "$ROOT_DIR/streaming/out/libs/streaming-job.jar" "$LIB_DIR/" 2>/dev/null || true
    
    log "Jobs compilados y copiados a lib/"
}

cmd_status() {
    log "Estado de servicios:"
    docker compose ps
    
    echo ""
    log "Verificando endpoints..."
    
    # Kafka
    if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        echo "  ✅ Kafka:9092"
    else
        echo "  ❌ Kafka:9092"
    fi
    
    # PostgreSQL
    if docker compose exec -T postgres pg_isready -U benchmark >/dev/null 2>&1; then
        echo "  ✅ PostgreSQL:5432"
    else
        echo "  ❌ PostgreSQL:5432"
    fi
}

cmd_clean() {
    local SCENARIO=${1:-generic}
    log "Limpiando entorno para escenario: $SCENARIO"
    
    # Limpiar tópico Kafka
    docker compose exec -T kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
    docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists 2>/dev/null || true
    log "Tópico 'events' recreado"
    
    # Limpiar PostgreSQL
    docker compose exec -T postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events RESTART IDENTITY CASCADE;" 2>/dev/null || true
    log "Tabla 'events' truncada"
    
    # Limpiar checkpoints
    docker exec tesis-ingestion-spark-worker-1 rm -rf /opt/spark/checkpoints/* 2>/dev/null || true
    log "Checkpoints limpiados"
    
    log "Limpieza completa"
}

cmd_down() {
    log "Bajando contenedores..."
    docker compose down
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
    docker compose down -v --remove-orphans 2>/dev/null || true
    
    log "Eliminando imágenes generadas..."
    docker rmi tesis-ingestion-generator tesis-ingestion-probe 2>/dev/null || true
    
    log "Limpiando resultados..."
    rm -rf "$ROOT_DIR/results"/*
    
    log "Restaurando .env..."
    cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env" 2>/dev/null || true
    
    log "Reset completo"
}

cmd_analyze() {
    log "Configurando entorno Python para análisis..."
    VENV="$ROOT_DIR/analysis/.venv"
    PYTHON="$VENV/bin/python"
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || -n "${WINDIR:-}" ]]; then
        PYTHON="$VENV/Scripts/python.exe"
    fi

    if [ ! -f "$PYTHON" ]; then
        log "Creando venv en analysis/.venv ..."
        python3 -m venv "$VENV" 2>/dev/null || python -m venv "$VENV"
    fi

    log "Instalando/actualizando dependencias Python..."
    "$PYTHON" -m pip install -q -r "$ROOT_DIR/analysis/requirements.txt"

    RESULTS_DIR="${1:-$ROOT_DIR/results}"
    OUTPUT_DIR="${2:-$ROOT_DIR/results/figures}"

    log "Ejecutando análisis: results=$RESULTS_DIR  output=$OUTPUT_DIR"
    "$PYTHON" "$ROOT_DIR/analysis/analyze.py" \
        --results-dir "$RESULTS_DIR" \
        --output "$OUTPUT_DIR"

    log "Análisis completo. Gráficas en: $OUTPUT_DIR"
}

# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

COMMAND=${1:-help}

case "$COMMAND" in
    analyze)
        cmd_analyze "${2:-}" "${3:-}"
        ;;
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
        echo "  analyze     Crear venv Python + ejecutar análisis de resultados"
        echo ""
        echo "Ejemplos:"
        echo "  ./scripts/manage.sh up"
        echo "  ./scripts/manage.sh status"
        echo "  ./scripts/manage.sh clean low-load"
        echo "  ./scripts/manage.sh analyze"
        echo "  ./scripts/manage.sh reset"
        ;;
    *)
        error "Comando desconocido: $COMMAND"
        echo "Usa: ./scripts/manage.sh help"
        exit 1
        ;;
esac
