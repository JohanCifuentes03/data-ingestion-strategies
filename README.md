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

> **Aclaración conceptual:** En algunas disciplinas (como la electrónica o el control automático), un "evento" suele referirse a un cambio de estado físico, un flanco de voltaje o un trigger instantáneo. 
> 
> Sin embargo, en la arquitectura de datos y procesamiento de flujos (Stream Processing), **un "evento" es equivalente a un "registro de datos" (data record) invariable**. Es una estructura de datos digital (en este caso, un documento JSON) que contiene la **fotografía de un estado** o una medición en un instante de tiempo específico.

Para este benchmark, cada evento generado sintéticamente representa el registro digital de un dato del mundo real (como una lectura de temperatura IoT, un tick financiero o un monitor de salud) que viaja por la red y requiere ser procesado y almacenado.

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

### Volumen Decimal y Huella en Disco (El impacto real)

| Campo | Tamaño aproximado crudo |
|-------|------------------|
| event_id (UUID) | 36 bytes |
| produced_at (timestamp) | 13 bytes |
| schema | 10-14 bytes |
| Campos específicos | 80-200 bytes |
| payload (random) | ~200-300 bytes |
| **Total** | **~350-500 bytes** |

Aunque el significado semántico del evento es "sintético" (valores aleatorios fingiendo ser sensores reales para no usar datos privados), **su impacto computacional y peso físico es 100% real**. 

El tamaño de un evento es dinámico. El generador rellena el campo `payload` con caracteres aleatorios hasta alcanzar el tamaño objetivo de bytes (512 bytes, 4 KB, o 64 KB). Este impacto de red y almacenamiento se materializa durante cada simulación:

*   **Escenario Base (`low-load`):** 2,000 ev/s por 5 min = 600,000 eventos. Genera un tráfico real en Kafka y una base de datos PostgreSQL de **~100 MB** por corrida.
*   **Escenario de alta exigencia (`extreme-load`):** 100,000 ev/s en 5 minutos = 30,000,000 de eventos. En este escenario, la base de datos de PostgreSQL termina ingiriendo y pesando **entre 10 GB y 12 GB físicos en disco** tras una sola ejecución de 5 minutos (incluyendo índices obligatorios).
*   **Escenario pesado mixto (`mixed-payload`):** Inyecta payloads gigantes de hasta 64 KB, llegando a forzar volcados a disco y bases de datos transitorias de entre **15 GB a 25 GB** por simulación.

> **Importante Methodology:** Debido a estos volúmenes físicos masivos de I/O, el pipeline del experimento ejecuta un `TRUNCATE TABLE` borrando los GBs de datos antes de cada corrida. Esto previene que el disco duro colapse y asegura una línea base neutral y justa para todos los motores analíticos evaluados.

---

## Qué incluye

- **Generador multi-thread** en Python: schemas realistas IoT/Financiero/Salud, LZ4 compression, hasta 100k+ ev/s.
- **Sonda de disponibilidad** en Python: polling a PostgreSQL, mide latencia = `visible_at − produced_at`.
- **Stack completo en Docker Compose**: Kafka, Spark, Flink, Postgres, Prometheus, Grafana, cAdvisor, exporters.
- **Jobs Java** compilados con Gradle wrapper (Spark Batch, Spark SS, Flink Streaming). Parseo robusto JSON via **Jackson Databind**.
- **Scripts de orquestación**: setup, run, clean, teardown, export de métricas.
- **Análisis estadístico** (`analyze.py`): **14 gráficas** de calidad de publicación + filtro de warmup + Kruskal-Wallis + Bonferroni.
- **36+ métricas** capturadas: latencia (p50/p75/p95/p99), IQR, CV%, throughput E2E y de escritura, CPU, memoria, red, disco, Kafka lag, Flink checkpoints, errores, PostgreSQL transactions.

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
./scripts/manage.sh clean     # Limpiar Kafka, PostgreSQL y checkpoints
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

---

## Experimentos: Rápido vs Completo

| Comando | Duración | Uso |
|---------|----------|-----|
| `bash ./scripts/experiment.sh --smoke` | ~5 min | Validar que todo funciona |
| `bash ./scripts/experiment.sh --quick` | ~30 min | Prueba rápida de 3 estrategias × 2 escenarios |
| `bash ./scripts/experiment.sh` | ~2-3 horas | Experimento estándar (5 min/run, 30 s warmup) |

---

## Flujo de uso

### 1) Validar estado

```bash
./scripts/manage.sh status
```

### 2) Ejecutar una estrategia individual

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
# Estándar: 3 estrategias × 4 escenarios × 5 repeticiones
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

Debes crear un entorno virtual de Python, instalar las dependencias y correr el script de análisis.

```bash
# 1. Crear entorno virtual
python -m venv analysis/.venv

# 2. Instalar dependencias
# En Windows:
analysis\.venv\Scripts\pip install -r analysis\requirements.txt
# En Linux/macOS:
analysis/.venv/bin/pip install -r analysis/requirements.txt

# 3. Ejecutar análisis
# En Windows:
analysis\.venv\Scripts\python.exe analysis\analyze.py
# En Linux/macOS:
analysis/.venv/bin/python analysis/analyze.py
```

Genera **14 gráficas** en `results/figures/` incluyendo:
- Violin + boxplot (distribución real con densidad)
- CDF en escala logarítmica (3 estrategias visibles simultáneamente)
- Throughput E2E vs escritura al sink diferenciados
- Tabla resumen con IQR, CV%, Min, Max
- Heatmap de escalabilidad por escenario
- Ranking table objetivo (sin normalización arbitraria)
- Tests de significancia estadística (Kruskal-Wallis + Bonferroni)

> **Nota:** El filtro de warmup (`--warmup-ms`, default 30 000 ms) excluye automáticamente
> los primeros 30 s de cada run en estrategias streaming y microbatch, permitiendo
> que la JVM (JIT) se estabilice antes de medir.

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

**Nota:** Usa `--duration <segundos>` para cambiar la duración por corrida.

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
| `scripts/export_metrics.py` | Snapshot de 36+ métricas Prometheus a CSV por corrida |
| `analysis/analyze.py` | **14 gráficas** + filtro warmup + tests estadísticos (Kruskal-Wallis + Bonferroni) |

---

## Estructura del repositorio

```text
batch/          Spark Batch job (Java)
microbatch/     Spark Structured Streaming job (Java)
streaming/      Flink Streaming job (Java)
common/         Clases compartidas
generator/      Productor Kafka multi-thread + schemas IoT/Financial/Health
probe/          Sonda de disponibilidad + CSV + métricas Prometheus
analysis/       analyze.py + requirements.txt + .venv/ (auto-creado)
infrastructure/ SQL, Prometheus, Grafana, Kafka config
scripts/        Orquestación (bash) + export_metrics.py
docs/           Arquitectura + protocolo experimental
results/        CSV de latencias + snapshots Prometheus + figuras
```

---

## Troubleshooting rápido

- **Docker no responde**: inicia Docker Desktop y reintenta `./scripts/manage.sh up`.
- **Generator arranca pero 0 eventos**: revisa que Kafka tenga el tópico `events` creado (`./scripts/manage.sh clean`).
- **Flink job no aparece en UI**: usa `FLINK_DETACHED=true` o revisa logs con `docker compose logs flink-jobmanager`.
- **Throughput menor al esperado**: el generador escala threads automáticamente; verifica `docker compose logs generator` para ver la configuración de threads activa.
- **p99 anormalmente alto en primer run**: es normal — el filtro de warmup en `analyze.py` excluye los primeros 30 s automáticamente.

---

## Referencias

- **Arquitectura detallada**: [`docs/architecture.md`](docs/architecture.md)
- **Protocolo experimental completo**: [`docs/experiment_protocol.md`](docs/experiment_protocol.md)
