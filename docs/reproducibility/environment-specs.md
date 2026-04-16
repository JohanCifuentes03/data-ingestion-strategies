# Environment Specifications for Reproducibility

This document provides the exact environment specifications used in the experiments,
enabling full reproducibility of results for thesis defense and paper review.

## 1. Hardware Specifications

### Local Mode (Single Machine)

| Component | Specification |
|-----------|---------------|
| CPU | Minimum 4 cores (8 recommended) |
| RAM | Minimum 16 GB (32 GB recommended) |
| Storage | 50 GB SSD available |
| Network | Localhost (no network latency) |

### Distributed Mode (AWS EC2)

| Node | Instance Type | vCPUs | RAM | Role |
|------|---------------|-------|-----|------|
| node-producers | t3.medium | 2 | 4 GB | Kafka producer (generator) |
| node-broker | t3.large | 2 | 8 GB | Kafka + Zookeeper |
| node-compute | t3.xlarge | 4 | 16 GB | Spark + Flink clusters |
| node-sink | t3.medium | 2 | 4 GB | PostgreSQL + Prometheus |

**AWS Region**: us-east-1 (N. Virginia)

**VPC Configuration**:
- CIDR: 10.0.0.0/16
- Subnet: 10.0.1.0/24
- All nodes in same availability zone to minimize network variance

## 2. Software Versions

### Container Runtime

| Software | Version | Notes |
|----------|---------|-------|
| Docker | 24.0+ | Docker Engine |
| Docker Compose | 2.20+ | V2 syntax |

### Message Broker

| Software | Version | Image |
|----------|---------|-------|
| Apache Kafka | 7.5.3 | confluentinc/cp-kafka:7.5.3 |
| Apache Zookeeper | 7.5.3 | confluentinc/cp-zookeeper:7.5.3 |

### Compute Engines

| Software | Version | Image |
|----------|---------|-------|
| Apache Spark | 3.5.8 | apache/spark:3.5.8-java17 |
| Apache Flink | 1.18.1 | flink:1.18.1-scala_2.12-java17 |
| OpenJDK | 17 | Included in images |

### Data Sink

| Software | Version | Image |
|----------|---------|-------|
| PostgreSQL | 15 | postgres:15 |

### Observability

| Software | Version | Image |
|----------|---------|-------|
| Prometheus | 2.51.0 | prom/prometheus:v2.51.0 |
| cAdvisor | 0.49.1 | gcr.io/cadvisor/cadvisor:v0.49.1 |
| Kafka Exporter | 1.7.0 | danielqsj/kafka-exporter:v1.7.0 |

### Build Tools

| Software | Version | Notes |
|----------|---------|-------|
| Gradle | 8.5+ | Wrapper included |
| Python | 3.10+ | For analysis scripts |
| Make | 4.0+ | For automation |

## 3. Network Configuration

### Kafka Configuration

```properties
# Topic configuration
num.partitions=4
replication.factor=1
auto.create.topics.enable=false

# Performance tuning
batch.size=16384
linger.ms=5
buffer.memory=33554432
```

### Spark Configuration

```properties
spark.master=spark://spark-master:7077
spark.executor.memory=2g
spark.executor.cores=2
spark.streaming.backpressure.enabled=true
```

### Flink Configuration

```yaml
parallelism.default: 2
taskmanager.numberOfTaskSlots: 2
taskmanager.memory.process.size: 2048m
```

## 4. Experiment Parameters

### Default Configuration (.env)

```bash
# Kafka
KAFKA_NUM_PARTITIONS=4
KAFKA_REPLICATION_FACTOR=1

# Spark
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g

# Flink
FLINK_PARALLELISM=2
FLINK_TASKMANAGER_MEMORY=2048m

# Generator
GENERATOR_EVENT_RATE=1000        # events/second
GENERATOR_PAYLOAD_BYTES=1500     # minimum bytes per event
GENERATOR_WARMUP_SECONDS=30      # warmup period
GENERATOR_RUN_DURATION_SECONDS=0 # 0 = indefinite

# Probe
PROBE_POLL_INTERVAL_MS=100
```

### Load Scenarios

| Scenario | Event Rate | Payload Size | Duration |
|----------|------------|--------------|----------|
| low-load | 100 ev/s | 1,500 bytes | 5 min |
| medium-load | 1,000 ev/s | 1,500 bytes | 5 min |
| high-load | 5,000 ev/s | 1,500 bytes | 5 min |

## 5. Resource Limits

### Docker Resource Constraints

```yaml
# Applied per container
deploy:
  resources:
    limits:
      cpus: '2.0'
      memory: 4G
    reservations:
      cpus: '0.5'
      memory: 512M
```

## 6. Time Synchronization

For accurate latency measurements across distributed nodes:

```bash
# NTP synchronization (all nodes)
sudo timedatectl set-ntp true

# Verify clock offset < 10ms
chronyc tracking | grep "System time"
```

Clock offsets are recorded in `results/clock_offsets_*.csv` for each experiment run.

## 7. Verification Commands

Before running experiments, verify the environment:

```bash
# Check Docker version
docker --version
docker compose version

# Check available resources
docker info | grep -E "(CPUs|Memory)"

# Verify network connectivity (distributed mode)
ping -c 3 node-broker

# Check Kafka health
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check PostgreSQL health
docker compose exec postgres pg_isready -U benchmark
```

## 8. Known Environment Variations

The following factors may affect reproducibility:

1. **Host OS**: Linux kernel version differences may affect cAdvisor metrics
2. **Hypervisor**: VM overhead vs bare-metal performance
3. **Network**: Cloud provider network jitter
4. **Disk I/O**: SSD vs HDD significantly impacts batch processing
5. **Memory pressure**: Other processes competing for resources

**Mitigation**: Run experiments on a dedicated machine with minimal background processes.

## 9. Container Image Digests

For exact reproducibility, use specific image digests:

```bash
# Get current digests
docker images --digests | grep -E "(kafka|spark|flink|postgres|prometheus)"

# Pin to specific digest in docker-compose.yml
image: confluentinc/cp-kafka@sha256:abc123...
```

## 10. Reproducibility Checklist

Before each experiment run:

- [ ] Verify all containers are using specified versions
- [ ] Confirm resource limits are applied
- [ ] Check clock synchronization across nodes
- [ ] Clear previous results if starting fresh
- [ ] Verify network connectivity
- [ ] Check Kafka topic configuration
- [ ] Ensure PostgreSQL schema is initialized

See [experiment-checklist.md](experiment-checklist.md) for the complete pre-flight checklist.
