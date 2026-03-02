# Data Ingestion Strategies Benchmark

Repositorio de tesis para comparar el desempeño de estrategias de ingestión **Batch**, **Micro-batch** y **Streaming** usando un banco de pruebas reproducible basado en Docker Compose.

> **Tesis:** _Evaluación de desempeño en estrategias de ingestión y disponibilización de datos a gran escala_  
> Universidad Católica de Colombia — Ingeniería de Sistemas y Computación

---

## Objetivo

Medir la **latencia de disponibilidad** (`visible_at - produced_at`) y el **throughput de ingesta** para tres pipelines sobre la misma infraestructura:

1. **Spark Batch** — ejecución one-shot sobre eventos acumulados en Kafka.
2. **Spark Structured Streaming** — micro-batch con trigger configurable (1/5/10 s).
3. **Flink Streaming** — flujo continuo con exactly-once checkpointing.

Todo el ecosistema comparte una única fuente (Kafka), un único sink (PostgreSQL) y observabilidad centralizada (Prometheus + Grafana + cAdvisor).

## Arquitectura

El diseño completo con diagramas Mermaid se documenta en [`docs/architecture.md`](docs/architecture.md).  
El protocolo experimental detallado se describe en [`docs/experiment_protocol.md`](docs/experiment_protocol.md).

### Servicios principales (Docker Compose)

| Categoría | Servicios |
|-----------|----------|
| **Broker** | Kafka + ZooKeeper |
| **Compute** | Spark Master + Worker, Flink JobManager + TaskManager |
| **Sink** | PostgreSQL 15 |
| **Workload** | Generator (Python), Availability Probe (Python) |
| **Observabilidad** | Prometheus, Grafana, cAdvisor, kafka-exporter, postgres-exporter |

### Uniformidad de medición

> `visible_at` se registra **exclusivamente** como `DEFAULT` en PostgreSQL (`EXTRACT(EPOCH FROM NOW()) * 1000`), eliminando cualquier bias entre estrategias. Ningún job establece este campo manualmente.

## Estructura

```
data-ingestion-strategies/
├── batch/                 # Spark batch job (Java)
├── microbatch/            # Spark Structured Streaming (Java)
├── streaming/             # Flink streaming job (Java)
├── common/                # Clases compartidas (Event, ConfigLoader, JdbcEventWriter)
├── generator/             # Productor de eventos + métricas Prometheus
├── probe/                 # Sonda de latencia, exportación CSV + Prometheus
├── infrastructure/
│   ├── kafka-config/      # Scripts de tópicos
│   ├── prometheus/        # Scrape config (Kafka, PG, Flink, cAdvisor, Generator, Probe)
│   ├── grafana/           # Datasource + dashboards auto-provisionados
│   └── sql/               # Migración initial (CREATE TABLE events)
├── scripts/
│   ├── clean.sh           # Reset Kafka + PostgreSQL + checkpoints
│   ├── run_batch.sh       # Ejecutar estrategia Batch
│   ├── run_microbatch.sh  # Ejecutar estrategia Micro-batch
│   ├── run_streaming.sh   # Ejecutar estrategia Streaming
│   ├── run_experiment.sh  # Runner automatizado (estrategias × escenarios × repeticiones)
│   └── export_metrics.py  # Exportar métricas Prometheus → CSV
├── results/               # Salidas de experimentos (gitignored)
├── docs/
│   ├── architecture.md    # Arquitectura detallada + diagramas Mermaid
│   └── experiment_protocol.md  # Protocolo experimental paso a paso
├── docker-compose.yml
├── build.gradle           # Multi-proyecto Gradle (batch, microbatch, streaming, common)
└── .env.example           # Variables de entorno configurables
```

## Requisitos previos

- Docker Desktop (WSL2 en Windows) con **≥ 6 GB** de RAM asignados.
- Docker Compose v2.
- Java 21 (el Gradle wrapper está incluido).
- Bash (para los scripts). En Windows usar WSL2.

## Puesta en marcha

```bash
# 1. Clonar y configurar
git clone https://github.com/JohanCifuentes03/data-ingestion-strategies.git
cd data-ingestion-strategies
cp .env.example .env          # Ajustar credenciales, tasas y duración

### 2. Compilar los JARs

**Linux / macOS / WSL2:**
```bash
./gradlew buildJobs
```

**Windows (PowerShell):**
```powershell
.\gradlew.bat buildJobs
```

### 3. Construir imágenes Python
```bash
docker compose build generator probe
```

### 4. Levantar infraestructura
```bash
docker compose up -d --no-build
docker compose ps
```

### 5. Ejecutar una estrategia individual

**Linux / macOS / WSL2:**
```bash
./scripts/run_batch.sh low-load run_1
./scripts/run_microbatch.sh medium-load run_1 "5 seconds"
./scripts/run_streaming.sh burst run_1
```

**Windows (PowerShell/CMD):**
*Nota: Se requiere una terminal compatible con Bash (Git Bash, WSL2 o similar).*
```bash
bash ./scripts/run_batch.sh low-load run_1
bash ./scripts/run_microbatch.sh medium-load run_1 "5 seconds"
bash ./scripts/run_streaming.sh burst run_1
```

## Ejecución de experimentos

### Corrida individual

```bash
# Batch con offsets controlados
./scripts/run_batch.sh low-load run_1 earliest latest

# Structured Streaming con trigger de 5 s
./scripts/run_microbatch.sh medium-load run_1 "5 seconds"

# Flink Streaming
./scripts/run_streaming.sh high-load run_1
```

### Experimento completo automatizado

```bash
# Todas las estrategias × todos los escenarios × 5 repeticiones = 60 corridas
./scripts/run_experiment.sh

# Solo streaming, solo high-load, 3 repeticiones
./scripts/run_experiment.sh --strategies streaming --scenarios high-load --reps 3

# Con trigger personalizado para micro-batch
./scripts/run_experiment.sh --trigger "10 seconds"
```

### Protocolo por corrida

Cada corrida sigue 5 fases: **clean → warmup → run → cooldown → export**.

- **Clean**: borra tópico Kafka, trunca tabla PostgreSQL, limpia checkpoints.
- **Warmup** (configurable, default 30 s): estabiliza JVMs, caches y conexiones.
- **Run**: producción sostenida a la tasa del escenario.
- **Cooldown** (10 s): drena buffers pendientes.
- **Export**: copia CSV y snapshot de Prometheus al directorio de resultados.

## Métricas

| Métrica | Prometheus Key | Fuente |
|---------|---------------|--------|
| Latencia p50/p95/p99 | `probe_latency` | Probe |
| Throughput observado | `probe_throughput_events_per_sec` | Probe |
| Eventos visibles | `probe_visible_events_total` | Probe |
| Eventos emitidos | `generator_events_total` | Generator |
| Errores de generación | `generator_errors_total` | Generator |
| CPU/Memoria/Red contenedor | `container_*` | cAdvisor |
| Consumer lag Kafka | `kafka_consumergroup_*` | kafka-exporter |
| Conexiones PostgreSQL | `pg_stat_*` | postgres-exporter |

Cada corrida genera:
- `results/<strategy>/<scenario>/run_<n>/latency_samples.csv`
- `results/<strategy>/<scenario>/run_<n>/prometheus_snapshot.csv`

## Escenarios oficiales

| Escenario | Rate base | Payload | Pico | Duración |
|-----------|-----------|---------|------|----------|
| `low-load` | 2.000/s | 512 B | — | 20 min |
| `medium-load` | 10.000/s | 512 B | — | 20 min |
| `high-load` | 30.000/s | 512 B | — | 20 min |
| `burst` | 10.000/s | 512 B | 50.000/s por 60s cada 5 min | 20 min |

Cada escenario se repite **5 veces** para obtener distribución robusta.

## Utilidades

| Script | Descripción |
|--------|-------------|
| `scripts/clean.sh` | Borra tópico `events`, trunca tabla, limpia checkpoints |
| `scripts/run_experiment.sh` | Runner completo con loop de repeticiones |
| `scripts/export_metrics.py` | Exporta métricas Prometheus a CSV |
| `infrastructure/kafka-config/create-topics.sh` | Crear tópicos adicionales |

## Buenas prácticas

- Mantener RAM disponible **≥ 6 GB** (8 GB para `high-load` y `burst`).
- Usar `docker compose logs -f <servicio>` para depurar.
- El generador y la sonda exponen Prometheus en `:8000` y `:8001`.
- **Monitoreo en vivo**: Grafana http://localhost:3000 (admin/admin), Prometheus http://localhost:9090.
- Los resultados se organizan automáticamente en `results/<strategy>/<scenario>/run_<n>/`.

## Próximos pasos sugeridos

1. Crear notebook Jupyter para análisis estadístico de los CSV consolidados.
2. Añadir más workers Spark/Flink para estudiar escalamiento horizontal.
3. Incluir workloads sintéticos adicionales (payload variable, transformaciones neutrales).
4. Automatizar la generación de tablas y gráficas para la tesis.

---

Con esta base puedes ejecutar el banco de pruebas completo, versionar las configuraciones y respaldar los resultados de la tesis con rigurosidad.
