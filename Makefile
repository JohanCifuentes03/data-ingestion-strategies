# ══════════════════════════════════════════════════════════════════
# Makefile — Data Ingestion Strategies Benchmark
# 
# Comandos simplificados para reproducibilidad científica.
# Uso: make <target>
# Ver ayuda: make help
# ══════════════════════════════════════════════════════════════════

.PHONY: help setup build up down status clean smoke-test experiment analyze \
        experiment-quick fault-inject scaling-test archive \
        provision distributed-deploy distributed-experiment distributed-teardown \
        aws-collect dev-compile test-integration validate logs reset \
        local-experiment local-smoke

.DEFAULT_GOAL := help

# ══════════════════════════════════════════════════════════════════
# Variables
# ══════════════════════════════════════════════════════════════════
MODE ?= local
SHELL := /bin/bash
ROOT_DIR := $(shell pwd)
RESULTS_DIR := results
FIGURES_DIR := $(RESULTS_DIR)/figures
ARCHIVE_PREFIX := benchmark-results
TIMESTAMP := $(shell date +%Y%m%d-%H%M%S)

# Python
VENV_DIR := $(ROOT_DIR)/.venv
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip

# Docker Compose
DC := docker compose
DC_LOCAL := $(DC) --env-file .env -f infra/docker/compose/docker-compose.yml

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

# ══════════════════════════════════════════════════════════════════
# Help
# ══════════════════════════════════════════════════════════════════
help: ## Mostrar esta ayuda
	@echo ""
	@echo "══════════════════════════════════════════════════════════════"
	@echo "  Data Ingestion Strategies Benchmark — Makefile"
	@echo "══════════════════════════════════════════════════════════════"
	@echo ""
	@echo "$(GREEN)Setup & Build:$(NC)"
	@grep -E '^(build|up|down|status|clean|reset):.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Experiments:$(NC)"
	@grep -E '^(smoke-test|experiment|experiment-quick|fault-inject|scaling-test):.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(GREEN)Analysis:$(NC)"
	@grep -E '^(analyze|archive|validate):.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Quick Commands:$(NC)"
	@grep -E '^(setup|local-experiment|local-smoke):.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(RED)Distributed Mode (AWS):$(NC)"
	@grep -E '^(provision|distributed-deploy|distributed-experiment|distributed-teardown|aws-collect):.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(RED)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(GREEN)Development:$(NC)"
	@grep -E '^(dev-compile|test-integration|logs):.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "Modo actual: $(MODE) (cambiar con MODE=distributed make <comando>)"
	@echo ""

# ══════════════════════════════════════════════════════════════════
# Setup & Build
# ══════════════════════════════════════════════════════════════════
setup: ## Setup inicial (instalar dependencias, crear venv)
	@echo "$(GREEN)📦 Setup inicial...$(NC)"
	@command -v docker >/dev/null 2>&1 || { echo "$(RED)❌ Docker no instalado$(NC)"; exit 1; }
	@command -v docker compose >/dev/null 2>&1 || { echo "$(RED)❌ Docker Compose no instalado$(NC)"; exit 1; }
	@if [ ! -f ".env" ]; then cp .env.example .env; echo "$(YELLOW)⚠️  Archivo .env creado - revisa la configuración$(NC)"; fi
	@$(MAKE) .venv
	@echo "$(GREEN)✅ Setup completado$(NC)"

build: ## Construir imágenes Docker (multi-stage, incluye compilación)
	@echo "$(GREEN)🐳 Construyendo imágenes Docker (multi-stage)...$(NC)"
	$(DC_LOCAL) build --no-cache
	@echo "$(GREEN)✅ Build completado$(NC)"

up: ## Levantar infraestructura completa
	@echo "$(GREEN)🚀 Levantando infraestructura (modo: $(MODE))...$(NC)"
ifeq ($(MODE),local)
	bash scripts/manage.sh up
else
	bash scripts/up.sh
endif
	@echo "$(GREEN)✅ Infraestructura levantada$(NC)"

down: ## Bajar todos los contenedores
	@echo "$(YELLOW)🛑 Bajando infraestructura...$(NC)"
ifeq ($(MODE),local)
	bash scripts/manage.sh down
else
	@echo "$(RED)⚠️  Modo distribuido: usa 'make aws-destroy' para destruir VMs$(NC)"
endif

status: ## Ver estado de servicios
ifeq ($(MODE),local)
	@bash scripts/manage.sh status
else
	@bash scripts/up.sh --status-only 2>/dev/null || echo "Usa: ssh ubuntu@<VM_IP> docker ps"
endif

clean: ## Limpiar Kafka, PostgreSQL y checkpoints
	@echo "$(YELLOW)🧹 Limpiando estado...$(NC)"
	bash scripts/manage.sh clean
	@echo "$(GREEN)✅ Limpieza completada$(NC)"

reset: ## Reset completo (borra contenedores, volúmenes, resultados)
	@echo "$(RED)⚠️  Esto borrará TODO (contenedores, volúmenes, resultados)$(NC)"
	@read -p "¿Continuar? [y/N]: " confirm && [ "$$confirm" = "y" ] || exit 1
	bash scripts/manage.sh reset
	rm -rf $(RESULTS_DIR)/*
	@echo "$(GREEN)✅ Reset completado$(NC)"

# ══════════════════════════════════════════════════════════════════
# Experiments
# ══════════════════════════════════════════════════════════════════
smoke-test: ## Smoke test rápido (~5 min)
	@echo "$(YELLOW)🔬 Ejecutando smoke test...$(NC)"
	MODE=$(MODE) bash scripts/experiment.sh --smoke
	@echo "$(GREEN)✅ Smoke test completado$(NC)"

experiment: ## Experimento completo (~2-3 horas)
	@echo "$(YELLOW)🧪 Ejecutando experimento completo...$(NC)"
	@echo "$(YELLOW)⏱️  Duración estimada: 2-3 horas$(NC)"
	MODE=$(MODE) bash scripts/experiment.sh
	@echo "$(GREEN)✅ Experimento completado$(NC)"

experiment-quick: ## Experimento rápido (~30 min)
	@echo "$(YELLOW)⚡ Ejecutando experimento rápido...$(NC)"
	MODE=$(MODE) bash scripts/experiment.sh --quick
	@echo "$(GREEN)✅ Experimento rápido completado$(NC)"

fault-inject: ## Test de recuperación ante fallos
	@echo "$(YELLOW)💥 Ejecutando fault injection test...$(NC)"
	MODE=$(MODE) bash scripts/experiment.sh --fault-inject
	@echo "$(GREEN)✅ Fault injection completado$(NC)"

scaling-test: ## Test de eficiencia de escalado horizontal
	@echo "$(YELLOW)📈 Ejecutando scaling efficiency test...$(NC)"
	MODE=$(MODE) bash scripts/experiment.sh --scaling-test
	@echo "$(GREEN)✅ Scaling test completado$(NC)"

# ══════════════════════════════════════════════════════════════════
# Analysis
# ══════════════════════════════════════════════════════════════════
.venv: ## Crear entorno virtual Python (interno)
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "$(YELLOW)📦 Creando entorno virtual Python...$(NC)"; \
		python3 -m venv $(VENV_DIR); \
		$(PIP) install --upgrade pip; \
		$(PIP) install -e src/python/; \
		echo "$(GREEN)✅ Entorno virtual creado$(NC)"; \
	fi

analyze: .venv ## Generar gráficas y estadísticas (9 charts)
	@echo "$(GREEN)📊 Generando análisis estadístico...$(NC)"
	$(PYTHON) -m benchmark.analysis.analyzer --results-dir $(RESULTS_DIR) --output $(FIGURES_DIR)
	@echo "$(GREEN)✅ Análisis completado — ver $(FIGURES_DIR)/$(NC)"

archive: ## Archivar resultados con timestamp y checksum
	@echo "$(GREEN)📦 Archivando resultados...$(NC)"
	@mkdir -p archives
	tar -czf archives/$(ARCHIVE_PREFIX)-$(TIMESTAMP).tar.gz \
		$(RESULTS_DIR)/ \
		infra/config/prometheus/prometheus.local.yml \
		.env 2>/dev/null || true
	cd archives && sha256sum $(ARCHIVE_PREFIX)-$(TIMESTAMP).tar.gz > $(ARCHIVE_PREFIX)-$(TIMESTAMP).sha256
	@echo "$(GREEN)✅ Archivo creado: archives/$(ARCHIVE_PREFIX)-$(TIMESTAMP).tar.gz$(NC)"
	@cat archives/$(ARCHIVE_PREFIX)-$(TIMESTAMP).sha256

validate: .venv ## Validar integridad de resultados
	@echo "$(GREEN)🔍 Validando resultados...$(NC)"
	$(PYTHON) -m benchmark.validation.validator --results-dir $(RESULTS_DIR)
	@echo "$(GREEN)✅ Validación completada$(NC)"

# ══════════════════════════════════════════════════════════════════
# Distributed Mode (AWS)
# ══════════════════════════════════════════════════════════════════
provision: ## Provisionar infraestructura AWS (Terraform)
	@echo "$(RED)☁️  Provisionando infraestructura AWS...$(NC)"
	cd infra/terraform && terraform init && terraform apply
	@echo "$(GREEN)✅ Infraestructura provisionada - ver infra/terraform/outputs.env$(NC)"

distributed-deploy: ## Desplegar servicios via Ansible
	@echo "$(RED)📦 Desplegando servicios en AWS...$(NC)"
	@if [ ! -f "infra/terraform/outputs.env" ]; then \
		echo "$(RED)❌ Primero ejecuta 'make provision'$(NC)"; \
		exit 1; \
	fi
	source infra/terraform/outputs.env && cd infra/ansible && ansible-playbook -i inventory.ini site.yml
	@echo "$(GREEN)✅ Servicios desplegados$(NC)"

distributed-experiment: ## Ejecutar experimento completo en AWS
	@echo "$(YELLOW)🧪 Ejecutando experimento distribuido...$(NC)"
	MODE=distributed $(MAKE) experiment
	@echo "$(GREEN)✅ Experimento distribuido completado$(NC)"

distributed-teardown: ## Destruir infraestructura AWS (¡CUIDADO!)
	@echo "$(RED)💣 Destruyendo infraestructura AWS...$(NC)"
	@read -p "¿Estás SEGURO? Esto destruirá todas las VMs [y/N]: " confirm && [ "$$confirm" = "y" ] || exit 1
	$(MAKE) aws-collect || true
	cd infra/terraform && terraform destroy
	@echo "$(GREEN)✅ Infraestructura AWS destruida$(NC)"

aws-collect: ## Copiar resultados desde AWS a local
	@echo "$(GREEN)📥 Recolectando resultados de AWS...$(NC)"
	bash scripts/collect-results.sh
	@echo "$(GREEN)✅ Resultados copiados$(NC)"

# ══════════════════════════════════════════════════════════════════
# Local Mode (Convenience Aliases)
# ══════════════════════════════════════════════════════════════════
local-experiment: setup build up ## Experimento completo local (one-command)
	@$(MAKE) experiment MODE=local
	@$(MAKE) analyze
	@echo "$(GREEN)✅ Experimento local completado - ver results/figures/$(NC)"

local-smoke: setup build up ## Smoke test local (one-command)
	@$(MAKE) smoke-test MODE=local
	@echo "$(GREEN)✅ Smoke test completado$(NC)"

# ══════════════════════════════════════════════════════════════════
# Development
# ══════════════════════════════════════════════════════════════════
dev-compile: ## Compilar JARs localmente (desarrollo)
	@echo "$(GREEN)🔨 Compilando JARs...$(NC)"
	./gradlew clean build -x test --no-daemon
	@echo "$(GREEN)✅ JARs compilados:$(NC)"
	@find src/jobs/*/build/libs -name "*-all.jar" -o -name "*-job.jar" 2>/dev/null || echo "No JARs found"

test-integration: ## Ejecutar tests de integración
	@echo "$(YELLOW)🧪 Ejecutando tests de integración...$(NC)"
	@if [ -f "tests/integration/smoke_test.sh" ]; then \
		bash tests/integration/smoke_test.sh; \
	else \
		echo "Tests de integración pendientes de implementar"; \
	fi

logs: ## Ver logs de todos los servicios
	$(DC_LOCAL) logs -f --tail=100
