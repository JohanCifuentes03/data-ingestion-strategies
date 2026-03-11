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

## ¿Qué es un evento?

Un **evento** es un registro JSON que representa un dato del mundo real (IoT, financiero, salud)
que el generador produce y las estrategias de ingestión procesan.

### Ejemplo de evento (IoT Sensor)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "produced_at": 1709500000000,
  "schema": "iot_sensor",
  "device_id": "sensor-0042",
  "temperature_c": 22.4,
  "humidity_pct": 58.1,
  "pressure_hpa": 1013.5,
  "battery_v": 3.72,
  "status": "ok",
  "payload": "a7K9mNpQrS2tUvWxYz..."
}
```

### Tamaño de los eventos

| Campo | Tamaño aproximado |
|-------|------------------|
| event_id (UUID) | 36 bytes |
| produced_at (timestamp) | 13 bytes |
| schema | 10-14 bytes |
| Campos específicos | 80-200 bytes |
| payload (random) | ~200-300 bytes |
| **Total** | **~350-500 bytes** |

El tamaño **real** del evento es dinámico. El generador añade un campo `payload` con caracteres
aleatorios hasta alcanzar el tamaño objetivo del escenario (512 bytes, 4KB, o 64KB).
Esto simula payloads reales de telemetría IoT/Financiera que pueden variar en tamaño.

**Nota:** El payload de 512B es un objetivo, no exacto. El tamaño real JSON típico es ~350-400 bytes
para el schema base, más el padding aleatorio.

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

### Opción 1: Windows (más fácil)

```powershell
# Doble clic en quick-start.bat o ejecutar en PowerShell:
.\quick-start.bat
```

### Opción 2: Linux/macOS/WSL2

```bash
# Gestión del entorno
./scripts/manage.sh up        # Levantar infraestructura
./scripts/manage.sh status    # Ver estado
./scripts/manage.sh clean     # Limpiar
./scripts/manage.sh down      # Bajar contenedores
```

### Opción 3: Setup completo (requiere Java + Gradle)

```bash
# 1. Compilar jobs y levantar infraestructura
./scripts/manage.sh build
./scripts/manage.sh up

# 2. Limpiar todo antes de empezar (opcional pero recomendado)
./scripts/manage.sh clean
```

El script verifica prerrequisitos, crea `.env`, compila jobs, construye imágenes y levanta la infraestructura.

---

## Experimentos: Rápido vs Completo

| Comando | Duración | Uso |
|---------|----------|-----|
| `bash ./scripts/experiment.sh --smoke` | ~5 min | Validar que todo funciona |
| `bash ./scripts/experiment.sh --quick` | ~30 min | Prueba rápida de 3 estrategias |
| `bash ./scripts/experiment.sh` | ~1-2 horas | Experimento estándar (5 min/run) |

---

## Flujo de uso

### 1) Validar estado

```bash
bash ./scripts/doctor.sh
```

### 2) Ejecutar una estrategia

```bash
# Batch
./scripts/run.sh batch low-load run_1

# Microbatch (trigger configurable)
./scripts/run.sh microbatch medium-load run_1 "5 seconds"

# Flink streaming
FLINK_DETACHED=true ./scripts/run.sh streaming burst run_1
```

### 3) Experimento completo automatizado (60+ corridas)

```bash
# Estándar: 3 estrategias × 4 escenarios × 5 repeticiones (ventana 5m)
./scripts/experiment.sh

# Experimento rápido: 3 estrategias × 2 escenarios × 1 repetición (~30 min)
./scripts/experiment.sh --quick

# Con escenarios extremos incluidos
./scripts/experiment.sh \
  --scenarios "low-load medium-load high-load burst extreme-load mixed-payload"

# Override de schema
./scripts/experiment.sh --schema financial_tick

# Ventana de export de métricas de 10 minutos
./scripts/experiment.sh --window 10m
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
./scripts/manage.sh down

# Reset completo (borra todo)
./scripts/manage.sh reset
```

---

## Escenarios disponibles

| Escenario | Tasa | Payload | Schema | Duración default |
|---|---|---|---|---|
| `low-load` | 2.000 ev/s | ~350-500 B | iot_sensor | 5 min |
| `medium-load` | 10.000 ev/s | ~350-500 B | financial_tick | 5 min |
| `high-load` | 30.000 ev/s | ~350-500 B | health_monitor | 5 min |
| `burst` | 10k / pico 50k ev/s | ~350-500 B | financial_tick | 5 min |
| `extreme-load` | 100.000 ev/s | ~350-500 B | iot_sensor | 5 min |
| `mixed-payload` | 10.000 ev/s | 512B/4KB/64KB rotativo | iot_sensor | 5 min |

**Nota:** Duración default es 5 minutos (reducida de 20 min). Usa `--duration` para cambiar.

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
|--------|-------------|
| `scripts/manage.sh` | Gestión del entorno: `up`, `build`, `status`, `clean`, `down`, `reset` |
| `scripts/run.sh` | Ejecuta una estrategia: `batch`, `microbatch`, `streaming` |
| `scripts/experiment.sh` | Experimento automatizado: `--smoke`, `--quick`, `--standard`, `--full` |
| `scripts/stress.sh` | Perfil de carga intensa para validación |
| `scripts/export_metrics.py` | Snapshot de 36+ métricas Prometheus a CSV por corrida |
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

- **Docker no responde**: inicia Docker Desktop y reintenta `./scripts/manage.sh up`.
- **Generator arranca pero 0 eventos**: revisa que Kafka tenga el tópico `events` creado (`./scripts/manage.sh clean`).
- **Sin gráficas**: ejecuta `./scripts/manage.sh status` y confirma targets en Prometheus.
- **Flink job no aparece en UI**: usa `FLINK_DETACHED=true` o revisa logs con `docker compose logs flink-jobmanager`.
- **Throughput menor al esperado**: el generador escala threads automáticamente; verifica `docker compose logs generator` para ver la configuración de threads activa.

---

## Referencias

- **Guía de instalación detallada**: [`INSTALL.md`](INSTALL.md) — Para nuevos equipos
- **Arquitectura detallada**: [`docs/architecture.md`](docs/architecture.md)
- **Protocolo experimental completo**: [`docs/experiment_protocol.md`](docs/experiment_protocol.md)
