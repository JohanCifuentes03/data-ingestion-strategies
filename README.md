# Data Ingestion Strategies Benchmark

Banco de pruebas reproducible para comparar tres estrategias de ingestion sobre la misma infraestructura:

- `batch` (Spark one-shot)
- `microbatch` (Spark Structured Streaming)
- `streaming` (Flink continuo)

Todas consumen de Kafka y escriben en PostgreSQL, con metricas en Prometheus y paneles en Grafana.

## Que incluye

- Stack completo en Docker Compose (Kafka, Spark, Flink, Postgres, Prometheus, Grafana, generator, probe).
- Jobs Java compilados con Gradle wrapper.
- Scripts de operacion para setup, limpieza, ejecucion y validacion.
- Resultados en CSV dentro de `results/`.

## Requisitos

- Docker Desktop + Docker Compose v2.
- Java 21 instalado localmente (compilacion via Gradle wrapper).
- Bash (WSL o Git Bash en Windows).

## Levantar todo en un comando

Desde la raiz del repo:

```bash
bash ./scripts/setup.sh
```

El script hace esto automaticamente:

1. Verifica prerequisitos (`docker`, `java`, `bash`, daemon Docker).
2. Crea `.env` desde `.env.example` si no existe.
3. Compila jobs (`buildJobs`).
4. Construye imagenes `generator` y `probe`.
5. Levanta infraestructura.
6. Ejecuta limpieza inicial (`scripts/clean.sh`).

## Flujo rapido de uso

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

# Flink streaming (recomendado detached)
FLINK_DETACHED=true bash ./scripts/run_streaming.sh burst run_1
```

### 3) Prueba intensa (stress)

```bash
# default: microbatch + burst + rate alta
bash ./scripts/run_stress.sh

# variante: streaming
bash ./scripts/run_stress.sh streaming burst run_stress_1
```

## URLs utiles

- Grafana: `http://localhost:3000` (admin/admin)
- Prometheus: `http://localhost:9090`
- Spark UI: `http://localhost:8080`
- Flink UI: `http://localhost:8081`
- Generator metrics: `http://localhost:8000/metrics`
- Probe metrics: `http://localhost:8001/metrics`

## Variables de entorno clave

Configuralas en `.env`:

- `GENERATOR_SCENARIO`: `low-load|medium-load|high-load|burst`
- `GENERATOR_EVENT_RATE`: tasa base de eventos/seg.
- `GENERATOR_PAYLOAD_BYTES`: tamano payload.
- `PROBE_POLL_INTERVAL_MS`: frecuencia de sondeo del probe.
- `FLINK_PARALLELISM`: paralelismo default Flink.
- `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`: capacidad Spark.

## Scripts principales

- `scripts/setup.sh`: setup completo de cero a listo.
- `scripts/doctor.sh`: chequeo rapido de salud (servicios, endpoints, targets, DB).
- `scripts/clean.sh`: resetea topic `events`, tabla `events` y checkpoints.
- `scripts/run_batch.sh`: corrida batch.
- `scripts/run_microbatch.sh`: corrida micro-batch.
- `scripts/run_streaming.sh`: corrida Flink (soporta `FLINK_DETACHED=true`).
- `scripts/run_stress.sh`: perfil de carga intensa para validacion visual y de estabilidad.
- `scripts/run_experiment.sh`: orquestacion por estrategias/escenarios/repeticiones.
- `scripts/export_metrics.py`: snapshot de metricas Prometheus a CSV.

## Estructura del repositorio

```text
batch/        Spark Batch job (Java)
microbatch/   Spark Structured Streaming job (Java)
streaming/    Flink Streaming job (Java)
common/       Clases compartidas
generator/    Productor Kafka + metricas
probe/        Muestreo de latencia + CSV + metricas
infrastructure/
scripts/
results/
```

## Notas operativas importantes

- Este repo compila jobs para Java 17 en runtime (compatible con imagenes Spark/Flink usadas).
- `visible_at` lo asigna PostgreSQL por `DEFAULT`, igual para todas las estrategias.
- Si corres desde Git Bash en Windows, los scripts ya manejan conversion de paths para `docker compose exec`.

## Troubleshooting rapido

- Si Docker no responde: inicia Docker Desktop y reintenta `bash ./scripts/setup.sh`.
- Si no ves graficas: revisa `bash ./scripts/doctor.sh` y confirma targets `generator` y `probe` en Prometheus.
- Si `clean.sh` se bloquea: el script ya pausa `generator` y `probe` durante el reset para evitar locks.

## Referencias

- Arquitectura: `docs/architecture.md`
- Protocolo experimental: `docs/experiment_protocol.md`
