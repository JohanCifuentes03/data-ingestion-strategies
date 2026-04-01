# Protocolo Experimental

Este documento describe el procedimiento paso a paso para ejecutar el banco de
pruebas de la tesis. Cada corrida sigue un protocolo riguroso para garantizar
**reproducibilidad** y **validez estadística**.

---

## 1. Prerrequisitos

- [ ] Docker Desktop con ≥ 8 GB de RAM asignados (recomendado: 12 GB para `extreme-load`).
- [ ] Docker Compose v2 instalado.
- [ ] Java 17+ (Gradle wrapper incluido).
- [ ] Python 3.10+ con pip (para `export_metrics.py` y `analyze.py`).
- [ ] WSL2 habilitado en Windows (para los scripts Bash).
- [ ] Clonar el repositorio y copiar `.env.example` a `.env`.

## 2. Preparación (una sola vez)

```bash
# 1. Compilar los JARs de los tres jobs
./gradlew buildJobs          # Linux / macOS / WSL2
.\gradlew.bat buildJobs      # Windows PowerShell

# 2. Construir imágenes Docker y levantar la infraestructura
./scripts/manage.sh up

# 3. Verificar que todos los servicios estén saludables
./scripts/manage.sh status

# 4. Crear entorno virtual Python e instalar dependencias
python -m venv analysis/.venv
# En Windows:
analysis\.venv\Scripts\pip install -r analysis\requirements.txt
# En Linux/macOS:
analysis/.venv/bin/pip install -r analysis/requirements.txt
```

## 3. Protocolo por corrida

Cada corrida experimental consta de 5 fases:

```mermaid
graph LR
    A[CLEAN] --> B[WARMUP<br/>30 s]
    B --> C[RUN<br/>5 min default]
    C --> D[COOLDOWN<br/>30 s]
    D --> E[EXPORT<br/>CSV]
```

### 3.1 CLEAN
- Borrar el tópico `events` de Kafka y recrearlo (12 particiones).
- Truncar la tabla `events` en PostgreSQL.
- Limpiar checkpoints de Spark.
- Resetear el CSV del probe a solo la cabecera.
- **Script:** `./scripts/manage.sh clean`

### 3.2 WARMUP (30 segundos)
- El generador comienza a producir eventos; los primeros 30 s
  se marcan como "warmup" (`generator_warmup_active = 1`).
- Propósito: estabilizar la JVM (JIT compiler), llenar caches L1/L2,
  inicializar conexiones JDBC y dejar que el consumer lag alcance estado estacionario.
- **El script `analyze.py` filtra estos 30 s automáticamente** (parámetro `--warmup-ms 30000`)
  de todos los runs de estrategias no-batch (streaming y microbatch).
- Para Batch, el warmup está estructuralmente incorporado en la fase de acumulación.
- **Configuración:** `WARMUP_SECONDS=30` (por defecto en `experiment.sh`).

### 3.3 RUN (duración configurable, default 5 min)
- Producción sostenida a la tasa definida por el escenario.
- El generador usa múltiples threads (1 por cada 20k ev/s) para
  mantener tasas altas de forma sostenida.
- El probe registra latencias en `results/latency_samples.csv`.
- **Configuración:** `RUN_DURATION_SECONDS=300` (default) o `--duration` en `experiment.sh`.

### 3.4 COOLDOWN (30 segundos)
- Esperar a que los buffers se drenen (Flink JDBC Sink, Kafka producer queue).
- No se producen nuevos eventos.
- **30 s garantiza un checkpoint completo de Flink** (intervalo de checkpoint = 10 s).

### 3.5 EXPORT
- Copiar `latency_samples.csv` al directorio de la corrida (`results/<strategy>/<scenario>/<run_id>/`).


## 4. Ejecución manual (corrida individual)

```bash
# Batch
./scripts/run.sh batch low-load run_1

# Micro-batch (trigger 5 segundos)
./scripts/run.sh microbatch medium-load run_1 "5 seconds"

# Streaming
./scripts/run.sh streaming high-load run_1

# Variables de entorno opcionales
RUN_DURATION_SECONDS=600 ./scripts/run.sh batch high-load run_1
FLINK_DETACHED=true ./scripts/run.sh streaming burst run_1
```

## 5. Ejecución automatizada (experimento completo)

```bash
# Todas las estrategias, todos los escenarios estándar, 5 repeticiones
./scripts/experiment.sh

# Experimento rápido (1 rep, 3 min por corrida):
./scripts/experiment.sh --quick

# Solo streaming, escenarios extremos, 3 repeticiones
./scripts/experiment.sh --strategies streaming --scenarios "high-load extreme-load" --reps 3

# Todas las estrategias, trigger de 10s para micro-batch, ventana de export 10m
./scripts/experiment.sh --trigger "10 seconds" --window 10m

# Override de schema para todos los runs
./scripts/experiment.sh --schema financial_tick

# Especificar warmup y cooldown explícitamente
./scripts/experiment.sh --warmup 30 --cooldown 30

# Inyectar fallos después de los runs estándar (mide fault recovery time)
./scripts/experiment.sh --fault-inject

# Medir eficiencia de escalado horizontal (1→2→3 Spark workers)
./scripts/experiment.sh --scaling-test

# Experimento completo con fault injection y scaling test
./scripts/experiment.sh --fault-inject --scaling-test
```

## 6. Matriz experimental estándar (tesis)

| Estrategia | Escenarios | Repeticiones | Total corridas |
|-----------|-----------|-------------|----------------|
| Batch | low, medium, high, burst | 5 | 20 |
| Micro-batch | low, medium, high, burst | 5 | 20 |
| Streaming | low, medium, high, burst | 5 | 20 |
| **Subtotal** | | | **60** |
| Batch | extreme-load, mixed-payload | 3 | 6 |
| Micro-batch | extreme-load, mixed-payload | 3 | 6 |
| Streaming | extreme-load, mixed-payload | 3 | 6 |
| **Total completo** | | | **78** |

### Variables controladas

| Variable | Valor fijo | Justificación |
|----------|-----------|---------------|
| Particiones Kafka | 12 | Soporte de paralelismo hasta extreme-load |
| Factor de replicación | 1 | Entorno single-node; elimina overhead de réplica |
| Payload base | 512 B | Representativo de eventos IoT/telemetría |
| Sink | PostgreSQL 15 | Único sink para las 3 estrategias (fair comparison) |
| Workers Spark | 1 (4 cores, 2 GB) | Control de recursos; consistente con Flink |
| TaskSlots Flink | 4 | Equivalente en paralelismo a Spark |
| Checkpointing Flink | 10 s, exactly-once | Configuración de producción realista |
| Trigger micro-batch | 5 s (default) | Balance latencia/eficiencia documentado en literatura |
| Warmup excluido | 30 s por run | Filtrado automático en `analyze.py` (post-procesamiento) |
| Cooldown | 30 s | Margen sobre intervalo de checkpoint Flink (10 s) |

## 7. Fase de Análisis

Una vez completadas las corridas, se consolidan y generan las gráficas estadísticas.

### 7.1 Generación de gráficas y estadísticas

```bash
# Windows
analysis\.venv\Scripts\python.exe analysis\analyze.py

# Linux / macOS / WSL2
analysis/.venv/bin/python analysis/analyze.py

# Parámetros opcionales
analysis/.venv/bin/python analysis/analyze.py \
    --results-dir ./results \
    --output ./results/figures
```

### 7.2 Gráficas generadas (`results/figures/`) — 9 charts

| # | Archivo | Descripción | Fuente |
|---|---------|-------------|--------|
| 01 | `01_boxplot_latencia_e2e.png` | Boxplot anotado de Latencia E2E con p50/p95/p99/IQR/CV% | `latency_samples.csv` |
| 02 | `02_throughput_dual.png` | Throughput E2E vs escritura al Sink (barras duales, ev/s) | `latency_samples.csv` + Prometheus |
| 03 | `03_fault_recovery.png` | Tiempo de recuperación ante fallos (barras horizontales, media ± std) | `fault_recovery.csv` |
| 04 | `04_scaling_efficiency.png` | Eficiencia de escalado 1→2→3 workers (% de ideal) | runs `scaling_*w` |
| 05 | `05_resource_utilization.png` | Scatter CPU cores vs MB/evento por estrategia × run | `prometheus_snapshot.csv` |
| 06 | `06_kafka_lag.png` | Kafka Consumer Lag con umbral crítico de 10.000 mensajes | `prometheus_snapshot.csv` |
| 07 | `07_tabla_resumen.csv/.png` | Tabla completa: p50/p95/p99/IQR/CV%/Min/Max | `latency_samples.csv` |
| 08 | `08_heatmap_escalabilidad.png` | Heatmap latencia p95: degradación por carga y estrategia | `latency_samples.csv` |
| 09 | `09_ranking_table.csv/.png` | Ranking objetivo (pesos: p95=35%, tput=30%, recovery=20%, CV=15%) | todos |

### 7.3 Protocolo de inyección de fallos (Fault Recovery)

**Propósito:** Medir cuánto tarda cada estrategia en recuperar el 85% del throughput base tras un fallo de contenedor.

**Procedimiento:**

```bash
# Ejecutar para una estrategia y escenario específico
./scripts/fault_inject.sh streaming medium-load

# O automáticamente para todas las estrategias via experiment.sh:
./scripts/experiment.sh --fault-inject --fault-scenario medium-load
```

**Pasos internos de `fault_inject.sh`:**
1. Medir throughput base (polling de 30 s) desde el probe (`/results/latency_samples.csv`).
2. Matar el contenedor target (`docker stop tesis-ingestion-<strategy>-*`).
3. Esperar recuperación espontánea (Docker restart policy: `unless-stopped`).
4. Medir tiempo hasta que throughput ≥ 85% del base (timeout: 120 s).
5. Guardar resultado en `results/fault_recovery.csv`:

```csv
strategy,scenario,run_id,recovery_time_s,status
streaming,medium-load,fault_1,15.2,recovered
```

**Valores de estado posibles:** `recovered` | `timeout` | `data_loss`.

### 7.4 Protocolo de eficiencia de escalado (Scaling Efficiency)

**Propósito:** Medir si el throughput aumenta linealmente al añadir workers Spark.

**Procedimiento:**

```bash
# Ejecutar scaling test (requiere perfil Docker Compose "scaling"):
./scripts/experiment.sh --scaling-test
```

**Pasos internos:**
1. Desactivar `spark-worker-2` y `spark-worker-3`.
2. Para N = 1, 2, 3 workers:
   - Activar los workers adicionales con `docker compose --profile scaling up -d spark-worker-N`.
   - Esperar 10 s de registro en Spark master.
   - Ejecutar run de 2 min en `low-load` con `run_id = scaling_Nw`.
   - Copiar `latency_samples.csv` a `results/<strategy>/low-load/scaling_Nw/`.
3. `analyze.py` calcula: `Eficiencia(N) = (tput_N / tput_1) / N × 100%`.

Los workers adicionales se definen con el perfil Docker Compose `scaling` en `docker-compose.yml` y no se activan en corridas normales.

### 7.5 Recolección de métricas Prometheus

Al finalizar cada run, `run.sh` llama automáticamente a `collect_prometheus_snapshot()` que:
- Consulta `localhost:9090/api/v1/query` con las siguientes métricas PromQL:
  - `sum(rate(container_cpu_usage_seconds_total{name=~"tesis-ingestion-.*"}[2m]))` → CPU total
  - `sum(container_memory_rss{name=~"tesis-ingestion-.*"})` → Memoria RSS
  - `rate(kafka_produced_messages_total[2m])` → Throughput producido
  - `rate(sink_rows_written_total[2m])` → Throughput sink
  - `sum(kafka_consumergroup_lag{topic="events"})` → Consumer lag
  - Histograma de latencia del probe (si expuesto)
- Guarda el resultado en `results/<strategy>/<scenario>/<run_id>/prometheus_snapshot.csv`.
- Si Prometheus no está disponible, genera un warning y omite el snapshot sin abortar el run.

**Verificar que Prometheus está activo:**
```bash
./scripts/manage.sh status
# Debe mostrar: ✅ Prometheus:9090
```

### 7.6 Interpretación de la tabla de significancia

- **H (KW)**: estadístico de Kruskal-Wallis. Valores altos indican diferencias mayores.
- **p-valor**: < 0.05 indica diferencias estadísticamente significativas entre las 3 estrategias.
- **p-valor (Bonf.)**: p-valor ajustado por corrección Bonferroni para comparaciones pairwise.
- Las diferencias encontradas tienen alta significancia (p → 0) dada la magnitud de las muestras.

### 7.7 Filtro de warmup (metodología)

El script excluye automáticamente los primeros **30 segundos** de cada run para estrategias
streaming y microbatch, basado en el timestamp `produced_at` de cada evento:

- **Streaming / Microbatch:** Se filtran eventos con `produced_at < run_start + 30 000 ms`.
  Esto elimina latencias anómalas durante la inicialización JVM (JIT compiler).
- **Batch:** Exento del filtro — su warmup es estructural (toda la fase de acumulación
  ocurre antes de que el job de Spark ejecute, por lo que los datos ya reflejan
  el estado estacionario del sistema cuando ingresan a PostgreSQL).

---

## 8. Modo distribuido (AWS)

El protocolo experimental puede ejecutarse en modo distribuido donde cada capa del
stack reside en una instancia dedicada EC2 de AWS. Los **procedimientos
de medición y análisis son idénticos** al modo local; solo cambian el entorno de
despliegue y algunos comandos de orquestación.

### 8.1 Prerrequisitos adicionales (modo distribuido)

- [ ] Terraform ≥ 1.5 instalado en la máquina local.
- [ ] Ansible ≥ 2.14 instalado en la máquina local.
- [ ] Credenciales AWS programáticas configuradas en `terraform.tfvars`.
- [ ] Par de claves SSH en `~/.ssh/oci_rsa` y `~/.ssh/oci_rsa.pub`.
- [ ] `infra/terraform/terraform.tfvars` completado con OCIDs reales.
  Ver instrucciones en `infra/terraform/terraform.tfvars.example`.

```bash
# Generar SSH key si no existe
ssh-keygen -t rsa -b 4096 -f ~/.ssh/oci_rsa -N ""
cat ~/.ssh/oci_rsa.pub  # copiar en terraform.tfvars → ssh_public_key
```

### 8.2 Preparación (una sola vez)

```bash
# 1. Crear infraestructura AWS (~3 min)
cd infra/terraform
terraform init && terraform apply
# Genera: infra/ansible/inventory.ini y infra/terraform/outputs.env

# 2. Provisionar las 4 VMs: Docker, chrony, git clone, compilación (~10 min)
cd ../ansible
ansible-playbook -i inventory.ini site.yml

# 3. Pre-flight check completo (SSH + servicios + NTP)
bash scripts/up.sh
```

### 8.3 Diferencias con el modo local

| Aspecto | Modo local | Modo distribuido |
|---|---|---|
| Infraestructura | 1 máquina, Docker Compose | 4 instancias EC2, cada una con su compose file |
| Compose file | `docker-compose.yml` (raíz) | `docker/broker.yml`, `docker/compute.yml`, etc. |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka:9092` | `PLAINTEXT://10.0.1.20:9092` |
| `POSTGRES_HOST` | `postgres` (nombre Docker) | `10.0.1.40` (IP privada VM-4) |
| Clock skew | N/A (mismo host) | Verificado con `check-clock-sync.sh` antes de cada experimento |
| Resultados | `./results/` local | `./results-distributed/` (copiados via scp desde VM-4) |
| Análisis | `analyze.py` | `analyze.py --results-dir results-distributed/` |

### 8.4 Ejecución del experimento

```bash
# Pre-flight: verifica IPs, SSH, servicios y NTP
bash scripts/up.sh

# Smoke test (~5 min)
MODE=distributed bash scripts/experiment.sh --smoke

# Experimento rápido (3 × 2 escenarios × 1 rep, ~30 min)
MODE=distributed bash scripts/experiment.sh --quick

# Experimento estándar completo (~2-3 horas)
MODE=distributed bash scripts/experiment.sh

# Con fault injection y scaling test
MODE=distributed bash scripts/experiment.sh --fault-inject --scaling-test
```

### 8.5 Recolección y análisis

```bash
# 1. Copiar resultados desde VM-4 al directorio local
bash scripts/collect-results.sh
# Destino: ./results-distributed/

# 2. Análisis estadístico: las 9 gráficas + Kruskal-Wallis + Bonferroni
analysis/.venv/bin/python analysis/analyze.py \
    --results-dir results-distributed/

# 3. DESTRUIR las VMs para no gastar saldo AWS
cd infra/terraform && terraform destroy
```

### 8.6 Protocolo de sincronización NTP

La validez de la métrica `latencia = visible_at − produced_at` depende de que
VM-1 (`produced_at`) y VM-4 (`visible_at`) tengan relojes sincronizados.

**Mecanismo:**
1. Ansible configura chrony con `server 169.254.169.254 iburst prefer` en todos los nodos
   (Amazon Time Sync, latencia ~0.1 ms).
2. `scripts/up.sh` ejecuta `check-clock-sync.sh` como parte del pre-flight.
3. `experiment.sh` ejecuta `check-clock-sync.sh` antes del primer run en modo distribuido.
4. Si el offset de cualquier nodo supera **5 ms**, el experimento se aborta automáticamente.

**Interpretación de resultados:** Los offsets < 5 ms son estadísticamente despreciables:
- Batch: p50 en el orden de segundos (ratio señal/ruido > 1000×)
- Micro-batch: p50 típicamente en decenas de ms (ratio > 10×)
- Streaming: p50 en 20–60 ms (ratio > 4×)

Los logs de sincronización se guardan en `results/clock_offsets_YYYYMMDD_HHmmSS.csv`
como evidencia metodológica para la documentación de la tesis.
