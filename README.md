# Data Ingestion Strategies Benchmark

Banco de pruebas reproducible para comparar tres estrategias de ingestión de datos
a gran escala sobre la misma infraestructura:

| Estrategia | Motor | Modo |
|---|---|---|
| `batch` | Apache Spark 3.5 | Lectura completa de Kafka → append a PostgreSQL |
| `microbatch` | Spark Structured Streaming + Kafka | Trigger periódico (1/5/10 s) |
| `streaming` | Apache Flink 1.18 + Kafka | Flujo continuo exactly-once |

Todas consumen de **Apache Kafka** (12 particiones) y escriben en **PostgreSQL**.
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
- **Stack completo en Docker Compose**: Kafka, Spark, Flink, Postgres.
- **Jobs Java** compilados con Gradle wrapper (Spark Batch, Spark SS, Flink Streaming). Parseo robusto JSON via **Jackson Databind**.
- **Scripts de orquestación**: setup, run, clean, teardown, export de métricas.
- **Análisis estadístico y Dashboards** (`analyze.py`): Centralizado nativamente en Python, el motor extrae directamente los snapshots de Prometheus sin depender de Grafana, exportando **9 gráficas estáticas** de calidad de publicación + filtro de warmup + Kruskal-Wallis + Bonferroni.
- **Observabilidad Centralizada**: Prometheus almacena CPU, memoria pasiva (cAdvisor) y lag (kafka-exporter).
- **Métricas** capturadas: latencia E2E, throughput absoluto, throughput de escritura a base de datos, fault recovery, eficiencia de escalado horizontal, consumo de RAM (MB) absoluto y consumer lag.

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
| `bash ./scripts/experiment.sh --fault-inject` | +~15 min | Incluye medición de fault recovery |
| `bash ./scripts/experiment.sh --scaling-test` | +~20 min | Incluye medición de eficiencia de escalado |

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

# Con fault injection (mide tiempo de recuperación ante fallos)
./scripts/experiment.sh --fault-inject

# Con scaling test (mide eficiencia de escalado 1→2→3 workers)
./scripts/experiment.sh --scaling-test

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

Genera **9 gráficas** en `results/figures/` incluyendo:
- Boxplot anotado con p50/p95/p99/IQR/CV% por estrategia × escenario
- Throughput E2E vs escritura al sink (barras duales)
- Tiempo de recuperación ante fallos (barras horizontales)
- Eficiencia de escalado horizontal 1→2→3 workers
- Scatter de recursos: CPU cores vs MB/evento
- Kafka Consumer Lag con umbral crítico de 10.000 mensajes
- Tabla resumen completa (p50/p95/p99/IQR/CV%/Min/Max)
- Heatmap de escalabilidad p95 por escenario
- Ranking objetivo multi-criterio (p95=35%, throughput=30%, recovery=20%, CV=15%)

> **Nota:** El filtro de warmup (default 30 s) excluye automáticamente los primeros
> 30 s de cada run en estrategias streaming y microbatch, permitiendo que la JVM (JIT)
> se estabilice antes de medir. Desactivar con `--no-warmup-filter`.

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
| Spark UI | `http://localhost:8080` |
| Flink UI | `http://localhost:8081` |
| Prometheus | `http://localhost:9090` |
| cAdvisor | `http://localhost:8083` |
| Kafka Metrics | `http://localhost:9308/metrics` |

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
| `scripts/experiment.sh` | Experimento automatizado: `--smoke`, `--quick`, `--fault-inject`, `--scaling-test` |
| `scripts/fault_inject.sh` | Inyección de fallos y medición de fault recovery time |
| `analysis/analyze.py` | **9 gráficas** + filtro warmup + tests estadísticos (Kruskal-Wallis + Bonferroni) |

---

## Estructura del repositorio

```text
batch/          Spark Batch job (Java)
microbatch/     Spark Structured Streaming job (Java)
streaming/      Flink Streaming job (Java)
common/         Clases compartidas
generator/      Productor Kafka multi-thread + schemas IoT/Financial/Health
probe/          Sonda de disponibilidad + CSV
analysis/       analyze.py + requirements.txt + .venv/ (auto-creado)
infrastructure/ SQL, Kafka config, prometheus.yml
scripts/        Orquestación (bash): manage, run, experiment, fault_inject
docs/           Arquitectura + protocolo experimental
results/        CSV de latencias + fault_recovery.csv + figuras/
```

---

## Troubleshooting rápido

- **Docker no responde**: inicia Docker Desktop y reintenta `./scripts/manage.sh up`.
- **Generator arranca pero 0 eventos**: revisa que Kafka tenga el tópico `events` creado (`./scripts/manage.sh clean`).
- **Flink job no aparece en UI**: usa `FLINK_DETACHED=true` o revisa logs con `docker compose logs flink-jobmanager`.
- **Throughput menor al esperado**: el generador escala threads automáticamente; verifica `docker compose logs generator` para ver la configuración de threads activa.
- **p99 anormalmente alto en primer run**: es normal — el filtro de warmup en `analyze.py` excluye los primeros 30 s automáticamente.
- **Prometheus no disponible**: verifica `./scripts/manage.sh status` — debe mostrar `✅ Prometheus:9090`. Si falla, revisa `docker compose logs prometheus`.
- **Chart 03 (fault recovery) vacío**: ejecuta primero `./scripts/experiment.sh --fault-inject` para generar `results/fault_recovery.csv`.
- **Chart 04 (scaling) con datos sintéticos**: ejecuta `./scripts/experiment.sh --scaling-test` para reemplazar el placeholder con datos reales.

---

## Modo distribuido (AWS)

El benchmark soporta un modo distribuido donde cada capa del stack se despliega en
una instancia EC2 dedicada en **Amazon Web Services (AWS)**, replicando condiciones de
producción reales. Este modo es el que se utiliza en los experimentos de validación
de la tesis.

### Topología de red

```
  ┌─────────────────────────────────────── VPC: 10.0.0.0/16 ───────────────────────────────────────┐
  │                               subnet pública: 10.0.1.0/24                                       │
  │                                                                                                  │
  │   ┌──────────────────┐    produce JSON (lz4)    ┌─────────────────────┐                        │
  │   │  VM-1            │ ─────────────────────── ▶ │  VM-2               │                        │
  │   │  node-producers  │                           │  node-broker        │                        │
  │   │  10.0.1.10       │                           │  10.0.1.20          │                        │
  │   │  t3.medium       │                           │  t3.large           │                        │
  │   │  · generator     │     probe ─── SELECT ──── │  · zookeeper:2181   │                        │
  │   │  · probe         │         ▼                 │  · kafka:9092       │                        │
  │   └──────────────────┘         │                 │  · kafka-exporter   │                        │
  │          │ /metrics            │                 └─────────────────────┘                        │
  │          │                     │                         │ consume                               │
  │          ▼                     ▼                         ▼                                      │
  │   ┌──────────────────────────────────┐     ┌──────────────────────────┐                        │
  │   │  VM-4                            │     │  VM-3                    │                        │
  │   │  node-sink                       │     │  node-compute            │                        │
  │   │  10.0.1.40                       │ ◀── │  10.0.1.30              │                        │
  │   │  t3.medium                       │JDBC │  t3.xlarge               │                        │
  │   │  · postgres:5432  ◀ INSERT       │     │  · spark-master:7077     │                        │
  │   │  · prometheus:9090               │     │  · spark-worker (4 cores)│                        │
  │   │  · cadvisor:8083                 │     │  · flink-jobmanager:8081 │                        │
  │   └──────────────────────────────────┘     │  · flink-taskmanager     │                        │
  │                                             └──────────────────────────┘                        │
  └──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### VMs y recursos AWS (x86_64)

| VM | Rol | IP Privada | Shape EC2 | vCPUs | RAM | Servicios |
|---|---|---|---|---|---|---|
| VM-1 | node-producers | 10.0.1.10 | t3.medium | 2 | 4 GB | generator, probe |
| VM-2 | node-broker | 10.0.1.20 | t3.large | 2 | 8 GB | zookeeper, kafka, kafka-exporter |
| VM-3 | node-compute | 10.0.1.30 | t3.xlarge | 4 | 16 GB | spark-master, spark-worker, flink |
| VM-4 | node-sink | 10.0.1.40 | t3.medium | 2 | 4 GB | postgres, prometheus, cadvisor |

> **Región elegida: `us-east-1` (N. Virginia).**
> Justificación: mayor disponibilidad de instancias t3 y servicios.

> ⚠️ **Costo estimado:** El despliegue de las 4 instancias cuesta aproximadamente **~$0.37 USD por hora** bajo el modelo On-Demand. Es ideal aprovechar los créditos de Free Trial (ej. $300 USD de bienvenida o GitHub Student Pack). **Recuerda destruir las VMs al terminar.**

### Prerrequisitos

```bash
# Herramientas requeridas en la máquina local
terraform --version  # >= 1.5.0
ansible --version    # >= 2.14.0
aws --version        # AWS CLI (opcional, Terraform usa las access keys)

# Asegúrate de tener un par de claves SSH en ~/.ssh/id_rsa
# Si no, genera uno: ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
```

### Flujo completo de uso (8 pasos)

```bash
# 1. Configurar credenciales AWS en Terraform
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
# Edita terraform.tfvars colocando tue aws_access_key y aws_secret_key

# 2. Crear infraestructura en AWS (~3 min)
cd infra/terraform
terraform init
terraform apply
# Genera automáticamente:
#   infra/ansible/inventory.ini
#   infra/terraform/outputs.env

# 3. Provisionar las 4 VMs: Docker, NTP, repo, compilación (~10 min)
cd ../ansible
ansible-playbook -i inventory.ini site.yml

# 4. Verificar que todo está en pie (pre-flight check)
bash scripts/up.sh

# 5. Ejecutar experimento en modo distribuido
MODE=distributed bash scripts/experiment.sh --smoke   # validación rápida (~5 min)
MODE=distributed bash scripts/experiment.sh --quick   # prueba rápida (~30 min)
MODE=distributed bash scripts/experiment.sh           # experimento completo (~2-3 h)

# 6. Recolectar resultados desde VM-4 (node-sink)
bash scripts/collect-results.sh

# 7. Analizar con el mismo pipeline estadístico de modo local
analysis/.venv/bin/python analysis/analyze.py \
    --results-dir results-distributed/

# 8. IMPORTANTE: destruir VMs para no gastar saldo AWS
cd infra/terraform
terraform destroy
```

### Sincronización de relojes (validez experimental)

En modo distribuido, `produced_at` se genera en VM-1 y `visible_at` se asigna
en VM-4. Si sus relojes difieren, la latencia medida puede ser incorrecta.

**Solución implementada:** chrony apunta al NTP interno de AWS (`169.254.169.123`,
latencia ~0.1 ms). El offset residual típico es < 1 ms, despreciable frente a
latencias de batch (segundos) y streaming (decenas de ms).

El script `check-clock-sync.sh` bloquea automáticamente el experimento si
cualquier nodo supera el umbral de **5 ms**:

```bash
# Verificar manualmente
bash scripts/check-clock-sync.sh

# Omitir en el up.sh (no recomendado en producción)
bash scripts/up.sh --skip-clock
```

---

## Referencias

- **Arquitectura detallada**: [`docs/architecture.md`](docs/architecture.md)
- **Protocolo experimental completo**: [`docs/experiment_protocol.md`](docs/experiment_protocol.md)
