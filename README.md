# Data Ingestion Strategies Benchmark

Banco de pruebas reproducible para comparar tres estrategias de ingestión de datos
a gran escala sobre la misma infraestructura:

| Estrategia | Motor | Modo |
|---|---|---|
| `batch` | Apache Spark 3.5 | Lectura completa de Kafka → append a PostgreSQL |
| `microbatch` | Spark Structured Streaming + Kafka | Trigger periódico (1/5/10 s) |
| `streaming` | Apache Flink 1.18 + Kafka | Flujo continuo exactly-once |

Todas consumen de **Apache Kafka** (12 particiones) y escriben en **PostgreSQL**,
con observabilidad completa en Prometheus y Grafana.

---

## Qué incluye

- **Generador multi-thread** en Python: schemas realistas IoT/Financiero/Salud, LZ4 compression, hasta 100k+ ev/s.
- **Sonda de disponibilidad** en Python: polling a PostgreSQL, mide latencia = `visible_at − produced_at`.
- **Stack completo en Docker Compose**: Kafka, Spark, Flink, Postgres, Prometheus, Grafana, cAdvisor, exporters.
- **Jobs Java** compilados con Gradle wrapper (Spark Batch, Spark SS, Flink Streaming). Ahora con parseo robusto JSON via **Jackson Databind**.
- **Scripts de orquestación**: setup, run, clean, teardown, export de métricas.
- **Análisis estadístico** (`analyze.py`): 12 gráficas de publicación + Kruskal-Wallis + Bonferroni.
- **36+ métricas** capturadas: latencia (p50/p75/p95/p99), throughput, CPU, memoria, red, disco, Kafka lag, Flink checkpoints, errores, PostgreSQL transactions.

---

## Requisitos

- Docker Desktop ≥ 8 GB RAM asignados (12 GB recomendado para `extreme-load`).
- Docker Compose v2.
- Java 17+.
- Python 3.10+.
- Bash (WSL2 o Git Bash en Windows).

---

## Inicio rápido

```bash
# 1. Setup inicial
bash ./scripts/setup.sh

# 2. Limpiar todo antes de empezar (opcional pero recomendado)
bash ./scripts/clean.sh --all
```

El script verifica prerrequisitos, crea `.env`, compila jobs, construye imágenes y levanta la infraestructura.

---

## Flujo de uso

### 1) Validar estado

```bash
bash ./scripts/doctor.sh
```

### 2) Ejecutar una estrategia

```bash
# Batch
bash ./scripts/run_batch.sh low-load run_1

# Micro-batch (trigger configurable)
bash ./scripts/run_microbatch.sh medium-load run_1 "5 seconds"

# Flink streaming
FLINK_DETACHED=true bash ./scripts/run_streaming.sh burst run_1
```

### 3) Experimento completo automatizado (60+ corridas)

```bash
# Estándar: 3 estrategias × 4 escenarios × 5 repeticiones (ventana 20m)
bash ./scripts/run_experiment.sh

# Experimento rápido (Light): 1 repetición, 4 min por corrida (~1h total)
bash ./scripts/run_experiment.sh --reps 1 --duration 240

# Con escenarios extremos incluidos
bash ./scripts/run_experiment.sh \
  --scenarios "low-load medium-load high-load burst extreme-load mixed-payload"

# Override de schema
bash ./scripts/run_experiment.sh --schema financial_tick

# Ventana de export de métricas de 10 minutos
bash ./scripts/run_experiment.sh --window 10m
```

### 4) Generar gráficas y análisis estadístico

```bash
# Windows
analysis\.venv\Scripts\python.exe analysis\analyze.py

# Linux / macOS / WSL2
analysis/.venv/bin/python analysis/analyze.py
```

Genera **12 gráficas** en `results/figures/` incluyendo radar multi-KPI, tests de significancia estadística, y serie temporal de latencia.

### 5) Bajar y limpiar

```bash
bash ./scripts/teardown.sh

# Conservar volúmenes
REMOVE_VOLUMES=false bash ./scripts/teardown.sh
```

---

## Escenarios disponibles

| Escenario | Tasa | Payload | Schema | Duración |
|---|---|---|---|---|
| `low-load` | 2.000 ev/s | 512 B | iot_sensor | 20 min |
| `medium-load` | 10.000 ev/s | 512 B | financial_tick | 20 min |
| `high-load` | 30.000 ev/s | 512 B | health_monitor | 20 min |
| `burst` | 10k / pico 50k ev/s | 512 B | financial_tick | 20 min |
| `extreme-load` | 100.000 ev/s | 512 B | iot_sensor | 30 min |
| `mixed-payload` | 10.000 ev/s | 512B/4KB/64KB rotativo | iot_sensor | 20 min |

---

## URLs útiles

| Servicio | URL |
|---|---|
| Grafana | `http://localhost:3000` (admin/admin) |
| Prometheus | `http://localhost:9090` |
| Spark UI | `http://localhost:8080` |
| Flink UI | `http://localhost:8081` |
| Generator metrics | `http://localhost:8000/metrics` |
| Probe metrics | `http://localhost:8001/metrics` |

---

## Variables de entorno clave (`.env`)

| Variable | Descripción |
|---|---|
| `GENERATOR_SCENARIO` | `low-load\|medium-load\|high-load\|burst\|extreme-load\|mixed-payload` |
| `GENERATOR_EVENT_RATE` | Tasa base de eventos/seg (override del escenario) |
| `GENERATOR_PAYLOAD_BYTES` | Tamaño base de payload en bytes |
| `GENERATOR_EVENT_SCHEMA` | `iot_sensor\|financial_tick\|health_monitor` (override) |
| `GENERATOR_RUN_DURATION_SECONDS` | Duración del run post-warmup (0 = infinito) |
| `KAFKA_NUM_PARTITIONS` | Particiones del tópico `events` (default: 12) |
| `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` | Recursos del worker Spark |
| `FLINK_PARALLELISM` / `FLINK_TASKMANAGER_MEMORY` | Recursos Flink |
| `PROBE_POLL_INTERVAL_MS` | Frecuencia de sondeo del probe (default: 500ms) |

---

## Scripts principales

| Script | Descripción |
|---|---|
| `scripts/setup.sh` | Setup completo de cero a listo |
| `scripts/doctor.sh` | Chequeo rápido de salud (servicios, endpoints, DB) |
| `scripts/clean.sh` | Resetea tópico, tabla y checkpoints. Usa `--all` para borrar `results/`. |
| `scripts/run_batch.sh` | Corrida Spark Batch |
| `scripts/run_microbatch.sh` | Corrida Spark Structured Streaming |
| `scripts/run_streaming.sh` | Corrida Flink (soporta `FLINK_DETACHED=true`) |
| `scripts/run_experiment.sh` | Orquestación completa. Flags: `--reps`, `--duration`, `--strategies`, `--scenarios`. |
| `scripts/export_metrics.py` | Snapshot de 36+ métricas Prometheus a CSV por corrida |
| `scripts/run_stress.sh` | Perfil de carga intensa para validación |
| `scripts/teardown.sh` | Baja stack y limpia recursos Docker |
| `analysis/analyze.py` | 12 gráficas + tests estadísticos (Kruskal-Wallis + Bonferroni) |

---

## Estructura del repositorio

```text
batch/          Spark Batch job (Java)
microbatch/     Spark Structured Streaming job (Java)
streaming/      Flink Streaming job (Java)
common/         Clases compartidas
generator/      Productor Kafka multi-thread + schemas IoT/Financial/Health
probe/          Sonda de disponibilidad + CSV + métricas Prometheus
analysis/       analyze.py + requirements.txt
infrastructure/ SQL, Prometheus, Grafana, Kafka config
scripts/        Orquestación (bash) + export_metrics.py
docs/           Arquitectura + protocolo experimental
results/        CSV de latencias + snapshots Prometheus + figuras
```

---

## Troubleshooting rápido

- **Docker no responde**: inicia Docker Desktop y reintenta `bash ./scripts/setup.sh`.
- **Generator arranca pero 0 eventos**: revisa que Kafka tenga el tópico `events` creado (`bash ./scripts/clean.sh`).
- **Sin gráficas**: ejecuta `bash ./scripts/doctor.sh` y confirma targets en Prometheus.
- **Flink job no aparece en UI**: usa `FLINK_DETACHED=true` o revisa logs con `docker compose logs flink-jobmanager`.
- **Throughput menor al esperado**: el generador escala threads automáticamente; verifica `docker compose logs generator` para ver la configuración de threads activa.

---

## Referencias

- **Arquitectura detallada**: [`docs/architecture.md`](docs/architecture.md)
- **Protocolo experimental completo**: [`docs/experiment_protocol.md`](docs/experiment_protocol.md)
