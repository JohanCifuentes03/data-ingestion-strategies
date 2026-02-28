# Data Ingestion Strategies Benchmark

Repositorio de tesis para comparar estrategias de ingestión Batch, Micro-batch y Streaming usando un banco de pruebas reproducible basado en Docker Compose.

## 🎯 Objetivo

Medir la latencia de disponibilidad (`visible_at - produced_at`), throughput, tasa de error y uso de recursos para tres pipelines sobre la misma infraestructura:

1. **Spark Batch** (ejecución cada 60 s)
2. **Spark Structured Streaming** (micro-batch de 1/5/10 s)
3. **Flink Streaming** (flujo continuo exactly-once)

Todo el ecosistema comparte una única fuente (Kafka), un único sink (PostgreSQL) y observabilidad centralizada (Prometheus + Grafana).

## 🧱 Arquitectura

El diseño completo se documenta en `docs/architecture.md`, incluyendo los diagramas Mermaid reutilizables para papers o presentaciones.

Servicios principales (todos dentro de `docker-compose.yml`):

- Kafka + ZooKeeper
- Spark Master + Worker (1 ejecutor por defecto)
- Flink JobManager + TaskManager
- PostgreSQL (sink) + Exporter
- Workload generator (Python)
- Availability probe (Python)
- Prometheus + Grafana (dashboards auto-provisionados)

## 📁 Estructura

```
tesis-ingestion-benchmark/
├── batch/                 # Spark batch job (Java)
├── microbatch/            # Spark Structured Streaming (Java)
├── streaming/             # Flink streaming job (Java)
├── common/                # Clases compartidas (Event, JDBC utils)
├── generator/             # Productor de eventos + métricas
├── probe/                 # Sonda de latencia y exportación CSV
├── infrastructure/
│   ├── kafka-config/      # Scripts y overrides
│   ├── prometheus/        # Scrape config
│   ├── grafana/           # Datasource + dashboards
│   └── sql/               # Migraciones iniciales
├── scripts/               # Orquestación (batch/micro/streaming/clean)
├── results/               # Salidas de experimentos (gitignored)
└── docs/                  # Arquitectura y notas
```

## ⚙️ Requisitos previos

- Docker Desktop (WSL2 en Windows) con **≥4 GB** asignados.
- Docker Compose v2.
- Java 21 (el wrapper Gradle ya está incluido, no necesitas Maven instalado).
- Bash (para los scripts en `scripts/`). En Windows usar WSL2.

## 🚀 Puesta en marcha

Sigue estos pasos en orden para dejar todo operativo:

1. **Clona y configura**
   ```bash
   git clone https://github.com/JohanCifuentes03/data-ingestion-strategies.git
   cd data-ingestion-strategies
   cp .env.example .env  # Ajusta credenciales, tasas y memoria
   ```
2. **Prepara Java/Maven**: verifica que `java -version` muestre 21 y que Maven use ese runtime.
3. **Compila los jobs** (Gradle wrapper genera los JAR sombreados montados en los contenedores):
   ```bash
   ./gradlew clean buildJobs
   ```
4. **Construye imágenes auxiliares y levanta la infraestructura**:
   ```bash
   docker compose up -d --build
   ```
   Espera a que Kafka, Spark, Flink, PostgreSQL, Prometheus y Grafana estén `healthy` (`docker compose ps`).
5. **Inicializa entorno experimental** (topics limpios + tabla truncada):
   ```bash
   ./scripts/clean.sh
   ```
6. **Corre la estrategia deseada** usando los scripts:
   ```bash
   ./scripts/run_batch.sh low-load
   ./scripts/run_microbatch.sh medium-load "5 seconds"
   ./scripts/run_streaming.sh burst
   ```
   Ajusta escenario, offsets, triggers o paralelismo según tus pruebas.
7. **Monitorea y valida**:
   - Prometheus: http://localhost:9090
   - Grafana (admin/admin): http://localhost:3000 (dashboard "Latency Overview").
   - Logs: `docker compose logs -f <servicio>`.
8. **Repite escenarios** siguiendo el protocolo (`clean → warmup → run → cooldown`) y exporta resultados desde `results/`.

Los jars sombreados quedan en `batch/build/libs/batch-job.jar`, `microbatch/build/libs/microbatch-job.jar` y `streaming/build/libs/streaming-job.jar`, montados automáticamente en los contenedores correspondientes.

## 🔁 Ejecución de escenarios

Cada estrategia expone un script dedicado que ejecuta el protocolo `clean → warmup → run`.

```bash
# Spark Batch con offsets controlados
./scripts/run_batch.sh low-load earliest latest

# Spark Structured Streaming con trigger de 5 s
./scripts/run_microbatch.sh medium-load "5 seconds"

# Flink Streaming con paralelo configurable (via .env)
./scripts/run_streaming.sh burst
```

- **Generator**: parametriza tasa (`EVENT_RATE`), payload y escenarios (`SCENARIO`).
- **Probe**: mide latencia real consultando PostgreSQL y persiste muestras en `results/latency_samples.csv`.
- **Prometheus/Grafana**: exponen métricas históricas y un dashboard `Latency Overview` (http://localhost:3000, admin/admin).

## 📊 Métricas + Resultados

1. **Latencia p50/p95/p99** desde la sonda (`probe_latency`).
2. **Throughput observado** (`probe_visible_events_total`).
3. **Eventos emitidos/errores** (`generator_events_total`, `generator_errors_total`).
4. **Uso de recursos** vía exporters (Kafka, PostgreSQL, Spark/Flink endpoints).

Cada corrida agrega registros CSV a `results/latency_samples.csv`, que sirven para análisis posteriores (por ejemplo en un cuaderno Jupyter o pandas).

## 🧪 Escenarios oficiales

| Escenario   | Rate base | Payload | Pico | Duración |
|-------------|-----------|---------|------|----------|
| low-load    | 2.000/s   | 512 B   | —    | 20 min   |
| medium-load | 10.000/s  | 512 B   | —    | 20 min   |
| high-load   | 30.000/s  | 512 B   | —    | 20 min   |
| burst       | 10.000/s  | 512 B   | 50.000/s por 60s cada 5 min | 20 min |

Repetir cada escenario 5 veces para obtener distribución robusta.

## 🧹 Utilidades

- `scripts/clean.sh`: borra el tópico `events` y trunca la tabla `events`.
- `infrastructure/kafka-config/create-topics.sh`: script ejemplar para crear tópicos adicionales desde dentro del contenedor Kafka.
- `docs/architecture.md`: descripción detallada + diagramas Mermaid reutilizables.

## 🛡️ Buenas prácticas

- Mantener la RAM disponible ≥4 GB; si se sube la carga, aumentar memoria en Docker Desktop.
- Utilizar `docker compose logs -f <servicio>` para depurar.
- El generador y la sonda exponen métricas Prometheus en `:8000` y `:8001` respectivamente.
- Para reproducibilidad, guardar resúmenes por corrida dentro de `results/<estrategia>/<escenario>/run_<n>/` (scripts base pueden ampliarse para automatizarlo).

## 📦 Próximos pasos sugeridos

1. Automatizar la recolección (Prometheus API → CSV) y generar tablas finales.
2. Añadir más workers Spark/Flink para estudiar escalamiento horizontal.
3. Incluir workloads sintéticos adicionales (payload variable, transformaciones neutrales).

---

Con esta base puedes ejecutar el banco de pruebas completo, versionar las configuraciones y respaldar los resultados de la tesis con rigurosidad.
