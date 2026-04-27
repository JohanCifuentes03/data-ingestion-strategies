# Data Ingestion Strategies: A Comparative Benchmark

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Java 17](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.org/)

A comprehensive benchmark comparing three data ingestion architectures: **Batch Processing** (Apache Spark), **Micro-batch Processing** (Spark Structured Streaming), and **Stream Processing** (Apache Flink). The official thesis scope is limited to the three official scenarios `low-load`, `medium-load`, and `high-load`.

## Research Questions

1. **RQ1**: How does ingestion strategy affect end-to-end latency under varying workloads?
2. **RQ2**: What is the throughput-latency trade-off for each strategy?
3. **RQ3**: How resource-efficient is each strategy in terms of CPU, memory, and cost?
4. **RQ4**: How do strategies handle failures and recovery?

## Key Findings

| Strategy | Latency (p50) | Throughput | Recovery Time | Best Use Case |
|----------|---------------|------------|---------------|---------------|
| **Batch** | ~2 minutes | Very High | N/A (scheduled) | Historical analytics, ETL |
| **Micro-batch** | ~5 seconds | High | < 30s | Near real-time dashboards |
| **Stream** | ~200ms | Medium-High | < 10s | Real-time monitoring, IoT |

*Results from distributed deployment (4-VM AWS topology, medium load)*

## Official Workflow

### Prerequisites
- **Docker** 20.10+ and **Docker Compose** 2.x
- 16GB RAM, 4 CPU cores (for local mode)
- *(Optional)* AWS account for distributed deployment

The official entrypoint is:

```bash
bash scripts/thesis.sh <subcommand> [options]
```

The workflow is intentionally split into three phases:

1. Infraestructura
2. Ejecución
3. Resultados y análisis

The same entrypoint is used for both local debug runs and the official distributed execution:

```bash
bash scripts/thesis.sh <subcommand> [options]
```

## Local Workflow

Use local mode only for debugging and quick verification.

### Local Setup

```bash
git clone <repository-url>
cd data-ingestion-strategies
cp .env.example .env
```

The first local run will automatically execute local setup, image build, and container startup unless you pass `--skip-setup`.

### Local Execution

Quick local debug run:

```bash
bash scripts/thesis.sh run --mode local --reps 1 --duration 60 --warmup 5 --cooldown 5
```

Longer local debug run:

```bash
bash scripts/thesis.sh run --mode local --reps 1 --duration 120 --warmup 10 --cooldown 10
```

### Local Results and Analysis

```bash
bash scripts/thesis.sh analyze --mode local
bash scripts/thesis.sh validate --mode local
```

### Local Teardown

```bash
bash scripts/thesis.sh destroy --mode local
```

Local outputs are written to `results/`.

## Distributed Workflow

Use distributed mode for the official thesis execution.

### 1. Infrastructure

Provision and deploy:

```bash
bash scripts/thesis.sh provision --mode distributed
bash scripts/thesis.sh deploy --mode distributed
```

### 2. Execution

Official full run:

```bash
bash scripts/thesis.sh run --mode distributed
```

Short distributed verification run:

```bash
bash scripts/thesis.sh run --mode distributed --reps 1 --duration 60 --warmup 5 --cooldown 5
```

### 3. Results and Analysis

Distributed results pipeline:

```bash
bash scripts/thesis.sh collect --mode distributed
bash scripts/thesis.sh analyze --mode distributed
bash scripts/thesis.sh validate --mode distributed
```

### 4. Distributed Teardown

```bash
bash scripts/thesis.sh destroy --mode distributed
```

Distributed outputs are written to `results-distributed/`.

## Convenience Mode

If you already understand the three-phase workflow, you can still use:

```bash
bash scripts/thesis.sh full --mode distributed --deploy
```

## Project Structure

```
data-ingestion-strategies/
├── src/
│   ├── jobs/                      # Processing implementations (Java)
│   │   ├── batch/                 # Spark Batch Job
│   │   ├── microbatch/            # Spark Structured Streaming Job
│   │   ├── streaming/             # Flink Streaming Job
│   │   └── common/                # Shared utilities
│   └── python/                    # Benchmark tools
│       └── benchmark/
│           ├── probe/             # Latency measurement
│           ├── analysis/          # Statistical analysis & visualization
│           ├── validation/        # Result validation
│           └── generator/         # Event generator
├── infra/                         # Infrastructure as Code
│   ├── docker/
│   │   ├── compose/               # Docker Compose configurations
│   │   └── images/                # Multi-stage Dockerfiles
│   ├── terraform/                 # AWS provisioning
│   ├── ansible/                   # Configuration management
│   └── config/                    # Service configurations
├── scripts/                       # Official workflow + execution scripts
├── docs/                          # Technical architecture documentation
├── results/                       # Experiment outputs
```

## Architecture Overview

### Data Flow

```mermaid
flowchart LR
    G[Event Generator\nPython Producer] -->|JSON events| K[(Kafka\n12 partitions)]
    K --> B[Batch Job\nSpark SQL]
    K --> M[Micro-batch Job\nStructured Streaming]
    K --> S[Stream Job\nApache Flink]

    B --> P[(PostgreSQL Sink)]
    M --> P
    S --> P

    P --> PR[Availability Probe\nLatency sampler]

    K -.metrics.-> PM[(Prometheus)]
    B -.metrics.-> PM
    M -.metrics.-> PM
    S -.metrics.-> PM
    P -.metrics.-> PM
    PR -.metrics.-> PM
```

### Components at a glance

- **Producer (`benchmark.generator`)**: generates synthetic events with `event_id`, `produced_at`, payload size, and scenario-specific rate; writes to Kafka topic `events`.
- **Kafka (broker + topic partitions)**: decouples producers from consumers, buffers bursts, and enables parallel consumption (12 partitions by default).
- **Processing strategies**:
  - **Batch (Spark Batch)**: reads accumulated offsets and writes in larger JDBC batches.
  - **Micro-batch (Spark Structured Streaming)**: processes periodic mini-batches with trigger/checkpoint semantics.
  - **Streaming (Flink)**: continuous event-at-a-time pipeline with low-latency sink writes.
- **Sink (PostgreSQL)**: persistence layer where each row gets `visible_at` (insert visibility timestamp), enabling a common latency reference.
- **Observability (Prometheus + exporters + cAdvisor)**: scrapes throughput, lag, and resource metrics across all services.
- **Probe (`benchmark.probe.availability_probe`)**: continuously polls sink for newly visible rows and computes end-to-end latency as:

```text
latency_ms = visible_at - produced_at
```

Probe outputs are written to `latency_samples.csv` and are the primary source for statistical analysis and validation.

### Topology & Data Budget (Thesis Defense)

- **Distributed network topology (4 VMs)**:
  - `node-producers (10.0.1.10)`: Generator + Probe
  - `node-broker (10.0.1.20)`: Kafka + ZooKeeper + kafka-exporter
  - `node-compute (10.0.1.30)`: Spark + Flink
  - `node-sink (10.0.1.40)`: PostgreSQL + Prometheus
- **Main data-plane flow**: `Generator -> Kafka:9092 -> Spark/Flink -> PostgreSQL:5432 -> Probe(read-only)`
- **Observability flow**: `Prometheus(node-sink) -> scrape targets (probe, exporter, cadvisor, engines)`
- **Event payload model**: JSON UTF-8 with `event_id`, `produced_at`, `schema`, domain fields, and `payload`.

Official thesis scenario profile used by this repository (`high-load = 30,000 ev/s`):

| Escenario | Tasa objetivo | Payload base | Schema | Eventos estimados por run (300s) |
|----------|---------------:|-------------:|--------|----------------------------------:|
| low-load | 2,000 ev/s | 1,500 B | `iot_sensor` | 600,000 |
| medium-load | 10,000 ev/s | 1,500 B | `financial_tick` | 3,000,000 |
| high-load | 30,000 ev/s | 1,500 B | `health_monitor` | 9,000,000 |

Approximate generated volume (JSON over Kafka) for a 300s run:

- **low-load**: ~0.95-1.1 GB
- **medium-load**: ~4.8-5.6 GB
- **high-load**: ~14.4-16.8 GB

For full defense-oriented details (protocols, ports, data types, formulas, and per-experiment totals), see:
- `docs/architecture.md`

```mermaid
flowchart TB
    subgraph LAT[Latency/Throughput Envelope]
        B1[Batch\nHigh throughput\nMinute-level latency]
        M1[Micro-batch\nBalanced throughput\nSecond-level latency]
        S1[Streaming\nLowest latency\nMillisecond-level]
    end

    B1 --> M1 --> S1
```

### Processing Strategies

#### 1. Batch Processing (Apache Spark)
- **Model**: MapReduce paradigm
- **Latency**: Minutes to hours (depends on schedule)
- **Throughput**: Very high (optimized for large batches)
- **Use Case**: Historical analysis, overnight ETL

**Implementation**: `src/jobs/batch/SparkBatchJob.java`

#### 2. Micro-batch Processing (Spark Structured Streaming)
- **Model**: Mini-batch streaming with configurable triggers
- **Latency**: Seconds (configurable trigger interval)
- **Throughput**: High
- **Use Case**: Near real-time dashboards, windowed aggregations

**Implementation**: `src/jobs/microbatch/SparkStructuredJob.java`

#### 3. Stream Processing (Apache Flink)
- **Model**: True event-at-a-time processing with event-time semantics
- **Latency**: Milliseconds
- **Throughput**: Medium to high
- **Use Case**: Real-time alerting, fraud detection, IoT

**Implementation**: `src/jobs/streaming/FlinkStreamingJob.java`

## Documentation

- **[Architecture](docs/architecture.md)**: system design, deployment topology, data model, network flows, and official benchmark baseline

## Reproducibility

This benchmark follows a simple reproducibility model:

- **Containerized**: Docker-based local and distributed runtime
- **Declarative**: Terraform + Ansible for distributed provisioning
- **Versioned**: code, configs, and analysis tracked in git
- **Validated**: result validation via `benchmark.validation.validator`

## Official Commands

```bash
# Local debug flow
bash scripts/thesis.sh run --mode local --reps 1 --duration 60 --warmup 5 --cooldown 5
bash scripts/thesis.sh analyze --mode local
bash scripts/thesis.sh validate --mode local
bash scripts/thesis.sh destroy --mode local

# Distributed official flow
bash scripts/thesis.sh provision --mode distributed
bash scripts/thesis.sh deploy --mode distributed
bash scripts/thesis.sh run --mode distributed
bash scripts/thesis.sh collect --mode distributed
bash scripts/thesis.sh analyze --mode distributed
bash scripts/thesis.sh validate --mode distributed
bash scripts/thesis.sh destroy --mode distributed
```

## Results & Metrics

### Primary Metrics
- **End-to-end Latency**: Time from event production to visibility in sink (p50, p95, p99)
- **Throughput**:
  - generated real (`generated_events / generation_duration_seconds`)
  - visible equivalente normalizado a ventana oficial (`visible_events / official_duration_seconds`)
- **Resource Usage**: average CPU cores and memory RSS by strategy/scenario

### Secondary Metrics
- **Consumer Lag**: diagnostic-only, only when real exporter coverage exists

Figures are exported in publication-ready formats:
- **PNG**: Quick preview for notebooks/slides
- **PDF**: Thesis and paper inclusion (vector quality)

Current compact figure set (thesis-focused):
- `latency_distribution_boxplot.*`: latency distribution with p50/p95/p99 annotations
- `generated_vs_sink_throughput.*`: generated-real throughput vs sink-visible throughput (official window)
- `delivery_capacity_by_scenario.*`: target vs generated-real vs visible-equivalent (window-normalized) throughput
- `delivery_ratio_by_scenario.*`: visible/generated delivery ratio (%) after drain
- `latency_slo_compliance.*`: latency SLO compliance (<=500ms, <=2s, <=10s)
- `time_to_drain_by_scenario.*`: time from generation end to processing drain completion
- `resource_usage_compute_node.*`: average CPU cores and memory RSS by scenario
- `latency_summary_table.*`: summary table (CSV + figure)
- `statistical_tests.csv`: Kruskal + pairwise Mann-Whitney (Bonferroni-adjusted)

Diagnostic figures (not part of the primary comparison conclusions):
- `kafka_consumer_lag.*`: only where real lag metrics are available
- `backlog_estimado.*`: diagnostic estimate, not a primary thesis metric
- `cpu_cost_per_million_visible_events.*`: diagnostic compute-cost view

## Notes

1. **Local Mode**: Requires 16GB+ RAM for concurrent strategies
2. **Distributed Mode**: A full 45-run distributed experiment can take around 5 hours and should be executed in sessions
3. **Official Scope**: the official thesis benchmark uses only `low-load`, `medium-load`, and `high-load`
4. **Cost**: AWS distributed tests cost roughly a few USD per full benchmark session, depending on instance uptime

## License

MIT License - see [LICENSE](LICENSE) for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/username/data-ingestion-strategies/issues)
- **Documentation**: [docs/](docs/)
- **Thesis**: [Link to published thesis]

---

**Status**: Production-ready for thesis defense  
**Last Updated**: 2026-04-06
