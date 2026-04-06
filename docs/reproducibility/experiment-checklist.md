# Experiment Checklist for Reproducibility

This checklist ensures consistent, reproducible experiment execution.
Follow these steps for every experiment run to guarantee valid results.

## Pre-Experiment Checklist

### 1. Environment Verification

#### 1.1 Local Mode
```bash
# Verify Docker is running
docker info > /dev/null 2>&1 && echo "OK: Docker running" || echo "FAIL: Docker not running"

# Check Docker Compose version (must be v2+)
docker compose version

# Verify available resources
echo "Available CPUs: $(nproc)"
echo "Available RAM: $(free -h | awk '/^Mem:/ {print $2}')"
echo "Available Disk: $(df -h . | awk 'NR==2 {print $4}')"

# Check no conflicting containers running
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(kafka|spark|flink|postgres)"
```

#### 1.2 Distributed Mode (AWS)
```bash
# Verify all nodes are reachable
for ip in $NODE_PRODUCERS_IP $NODE_BROKER_IP $NODE_COMPUTE_IP $NODE_SINK_IP; do
    ssh -o ConnectTimeout=5 ubuntu@$ip "echo 'OK: $ip reachable'" || echo "FAIL: $ip unreachable"
done

# Check clock synchronization (must be < 10ms offset)
for ip in $NODE_PRODUCERS_IP $NODE_BROKER_IP $NODE_COMPUTE_IP $NODE_SINK_IP; do
    ssh ubuntu@$ip "chronyc tracking | grep 'System time'"
done
```

### 2. Configuration Check

```bash
# Verify .env file exists and has required variables
[ -f .env ] && echo "OK: .env exists" || echo "FAIL: .env missing"

# Check critical variables are set
source .env
echo "Kafka Partitions: $KAFKA_NUM_PARTITIONS"
echo "Spark Workers: $SPARK_WORKER_CORES cores, $SPARK_WORKER_MEMORY"
echo "Flink Parallelism: $FLINK_PARALLELISM"
echo "Generator Rate: $GENERATOR_EVENT_RATE events/s"
echo "Payload Size: $GENERATOR_PAYLOAD_BYTES bytes"
```

### 3. Clean State

```bash
# Stop any running containers
docker compose down -v 2>/dev/null || true

# Clean previous results (if starting fresh experiment)
# WARNING: This deletes all previous results!
# rm -rf results/*

# Clean Docker system (optional, for consistent resource state)
docker system prune -f
```

### 4. Build Verification

```bash
# Compile Java jobs (must succeed without errors)
./gradlew clean shadowJar

# Verify JARs were created
ls -la batch/build/libs/*.jar
ls -la microbatch/build/libs/*.jar
ls -la streaming/build/libs/*.jar

# Build Python services
docker compose build generator probe
```

---

## During Experiment Checklist

### 5. Infrastructure Startup

```bash
# Start infrastructure and wait for health
docker compose up -d zookeeper kafka postgres prometheus cadvisor

# Wait for Kafka to be healthy (up to 60s)
echo "Waiting for Kafka..."
timeout 60 bash -c 'until docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null; do sleep 2; done'
echo "Kafka ready"

# Verify PostgreSQL is ready
docker compose exec postgres pg_isready -U benchmark
```

### 6. Topic Setup

```bash
# Create events topic with correct partitions
docker compose exec kafka kafka-topics --create \
    --topic events \
    --bootstrap-server localhost:9092 \
    --partitions $KAFKA_NUM_PARTITIONS \
    --replication-factor 1 \
    --if-not-exists

# Verify topic configuration
docker compose exec kafka kafka-topics --describe \
    --topic events \
    --bootstrap-server localhost:9092
```

### 7. Compute Cluster Startup

```bash
# Start Spark cluster
docker compose up -d spark-master spark-worker

# Verify Spark master is ready
timeout 30 bash -c 'until curl -s localhost:8080 | grep -q "Spark Master"; do sleep 2; done'
echo "Spark Master ready"

# Start Flink cluster
docker compose up -d flink-jobmanager flink-taskmanager

# Verify Flink is ready
timeout 30 bash -c 'until curl -s localhost:8081/overview | grep -q "flink-version"; do sleep 2; done'
echo "Flink JobManager ready"
```

### 8. Pre-Run Validation

```bash
# Check all services are healthy
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Health}}"

# Verify Prometheus is scraping targets
curl -s localhost:9090/api/v1/targets | jq '.data.activeTargets | length'

# Clear any existing data in events topic
docker compose exec kafka kafka-topics --delete --topic events --bootstrap-server localhost:9092 2>/dev/null || true
docker compose exec kafka kafka-topics --create --topic events --bootstrap-server localhost:9092 --partitions $KAFKA_NUM_PARTITIONS --replication-factor 1

# Clear PostgreSQL events table
docker compose exec postgres psql -U benchmark -d benchmark -c "TRUNCATE TABLE events;"
```

---

## Experiment Execution

### 9. Run Experiment

```bash
# Option A: Use Makefile (recommended)
make experiment STRATEGY=batch SCENARIO=medium-load DURATION=300

# Option B: Use run.sh directly
./scripts/run.sh --mode local --strategy batch --scenario medium-load --duration 300

# Option C: Manual execution
# 1. Start generator
docker compose up -d generator

# 2. Wait for warmup
sleep $GENERATOR_WARMUP_SECONDS

# 3. Start probe
docker compose up -d probe

# 4. Start processing job (example: batch)
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --class com.tesis.batch.BatchJob \
    /opt/spark/jobs/batch/batch-1.0-SNAPSHOT-all.jar

# 5. Wait for completion or duration
sleep $DURATION

# 6. Stop generator
docker compose stop generator

# 7. Wait for processing to complete
sleep 60

# 8. Stop probe and collect results
docker compose stop probe
```

### 10. During Run Monitoring

```bash
# Monitor Kafka consumer lag
watch -n 5 'docker compose exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe --all-groups 2>/dev/null | grep events'

# Monitor container resources
docker stats --no-stream

# Check Prometheus metrics
curl -s 'localhost:9090/api/v1/query?query=container_cpu_usage_seconds_total' | jq '.data.result | length'
```

---

## Post-Experiment Checklist

### 11. Data Collection

```bash
# Verify results were written
ls -la results/

# Check latency samples exist
wc -l results/$STRATEGY/$SCENARIO/*/latency_samples.csv

# Verify Prometheus snapshot was taken
ls -la results/$STRATEGY/$SCENARIO/*/prometheus_snapshot.csv

# Record clock offsets
./scripts/record_clock_offsets.sh > results/clock_offsets_$(date +%Y%m%d_%H%M%S).csv
```

### 12. Data Validation

```bash
# Validate latency samples
python3 -c "
import pandas as pd
df = pd.read_csv('results/$STRATEGY/$SCENARIO/run_1/latency_samples.csv')
print(f'Records: {len(df):,}')
print(f'Latency p50: {df.latency_ms.quantile(0.50):.1f} ms')
print(f'Latency p95: {df.latency_ms.quantile(0.95):.1f} ms')
print(f'Latency p99: {df.latency_ms.quantile(0.99):.1f} ms')
print(f'Min: {df.latency_ms.min():.1f} ms, Max: {df.latency_ms.max():.1f} ms')
"

# Check for anomalies
python3 -c "
import pandas as pd
df = pd.read_csv('results/$STRATEGY/$SCENARIO/run_1/latency_samples.csv')
negative = (df.latency_ms < 0).sum()
extreme = (df.latency_ms > 300000).sum()  # > 5 minutes
print(f'Negative latencies: {negative}')
print(f'Extreme latencies (>5min): {extreme}')
if negative > 0 or extreme > len(df) * 0.01:
    print('WARNING: Data quality issues detected!')
"
```

### 13. Cleanup

```bash
# Stop all containers
docker compose down

# Optionally remove volumes (for completely fresh next run)
# docker compose down -v

# Archive results (recommended)
tar -czvf results_$(date +%Y%m%d_%H%M%S).tar.gz results/
```

---

## Analysis Checklist

### 14. Generate Figures

```bash
# Run analysis script
python -m benchmark.analysis.analyzer --results-dir results --output results/figures

# Verify all figures were generated
ls -la results/figures/*.png
ls -la results/figures/*.csv

# Check figure quality
# Open figures and verify:
# - [ ] Axes labels are readable
# - [ ] Legend is clear
# - [ ] Colors distinguish strategies
# - [ ] No data truncation
# - [ ] Statistical annotations are correct
```

### 15. Statistical Validation

```bash
# Verify statistical tests
cat results/figures/statistical_tests.csv

# Check for significance
python3 -c "
import pandas as pd
df = pd.read_csv('results/figures/statistical_tests.csv')
significant = df[df['significant'] == 'si']
print(f'Significant differences in {len(significant)}/{len(df)} scenarios')
"
```

---

## Quick Reference: Common Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Kafka not starting | Container restarts | Check Zookeeper is healthy first |
| No latency data | Empty CSV | Verify probe can reach PostgreSQL |
| Negative latency | latency_ms < 0 | Clock synchronization issue |
| High variance | CV% > 100 | System under memory pressure |
| Missing metrics | Empty Prometheus snapshot | Check cAdvisor is running |

---

## Experiment Log Template

For each experiment run, record:

```
Date: YYYY-MM-DD HH:MM
Strategy: [batch|microbatch|streaming]
Scenario: [low-load|medium-load|high-load|burst|extreme-load|mixed-payload]
Mode: [local|distributed]
Duration: XXX seconds
Events Generated: XXX,XXX
Events Processed: XXX,XXX
Notes: [Any anomalies or observations]
Result Location: results/<strategy>/<scenario>/run_N/
```
