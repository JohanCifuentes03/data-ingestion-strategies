"""
export_metrics.py — Comprehensive Prometheus metric snapshot per experiment run.

Queries Prometheus HTTP API at the end of each run and exports a flat CSV with
all key performance indicators: latency, throughput, per-container CPU/mem/net,
Kafka consumer lag, Flink checkpoints, PostgreSQL, error rates, and JVM GC.

Usage:
    python scripts/export_metrics.py \\
        --strategy streaming \\
        --scenario high-load \\
        --run-id run_3 \\
        --output results/streaming/high-load/run_3/prometheus_snapshot.csv \\
        --window 20m
"""

import argparse
import csv
import json
import sys
import time
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

PROMETHEUS_URL = "http://localhost:9090"

# ---------------------------------------------------------------------------
# Query catalogue
# ---------------------------------------------------------------------------
# Each entry: (metric_name, promql, category)
#
# Window placeholder {W} is replaced by --window argument (default "5m").
# ---------------------------------------------------------------------------
METRIC_QUERIES = [
    # ── Latency (probe histogram) ──────────────────────────────────────────
    ("probe_latency_p50_ms",
     "histogram_quantile(0.50, rate(probe_latency_bucket[{W}]))",
     "latency"),
    ("probe_latency_p75_ms",
     "histogram_quantile(0.75, rate(probe_latency_bucket[{W}]))",
     "latency"),
    ("probe_latency_p95_ms",
     "histogram_quantile(0.95, rate(probe_latency_bucket[{W}]))",
     "latency"),
    ("probe_latency_p99_ms",
     "histogram_quantile(0.99, rate(probe_latency_bucket[{W}]))",
     "latency"),
    ("probe_latency_mean_ms",
     "rate(probe_latency_sum[{W}]) / rate(probe_latency_count[{W}])",
     "latency"),

    # ── Throughput ─────────────────────────────────────────────────────────
    ("probe_visible_events_total",   "probe_visible_events_total",          "throughput"),
    ("probe_throughput_eps",         "probe_throughput_events_per_sec",     "throughput"),
    ("generator_events_total",       "generator_events_total",              "throughput"),
    ("generator_bytes_total",        "generator_bytes_total",               "throughput"),
    ("generator_throughput_eps",
     "rate(generator_events_total[{W}])",
     "throughput"),

    # ── Error rates ────────────────────────────────────────────────────────
    ("generator_errors_total",       "generator_errors_total",              "errors"),
    ("generator_error_rate_eps",
     "rate(generator_errors_total[{W}])",
     "errors"),
    ("probe_errors_total",           "probe_errors_total",                  "errors"),
    ("probe_error_rate_eps",
     "rate(probe_errors_total[{W}])",
     "errors"),

    # ── Producer latency ───────────────────────────────────────────────────
    ("generator_produce_latency_p99_ms",
     "histogram_quantile(0.99, rate(generator_produce_latency_ms_bucket[{W}]))",
     "producer"),
    ("generator_produce_latency_p50_ms",
     "histogram_quantile(0.50, rate(generator_produce_latency_ms_bucket[{W}]))",
     "producer"),

    # ── Kafka consumer lag ─────────────────────────────────────────────────
    ("kafka_consumer_lag_sum",
     "sum(kafka_consumergroup_lag) by (consumergroup)",
     "kafka"),
    ("kafka_consumer_lag_max",
     "max(kafka_consumergroup_lag) by (consumergroup)",
     "kafka"),
    ("kafka_topic_partitions",
     "kafka_topic_partitions",
     "kafka"),
    ("kafka_offset_lag_rate",
     "rate(kafka_consumergroup_lag[{W}])",
     "kafka"),

    # ── CPU per container ──────────────────────────────────────────────────
    ("cpu_usage_rate",
     'sum(rate(container_cpu_usage_seconds_total{name=~".+"}[{W}])) by (name)',
     "cpu"),
    ("cpu_throttled_fraction",
     'sum(rate(container_cpu_cfs_throttled_seconds_total{name=~".+"}[{W}])) by (name) '
     '/ sum(rate(container_cpu_cfs_periods_total{name=~".+"}[{W}])) by (name)',
     "cpu"),

    # ── Memory per container ───────────────────────────────────────────────
    ("memory_usage_bytes",
     'container_memory_usage_bytes{name=~".+"}',
     "memory"),
    ("memory_rss_bytes",
     'container_memory_rss{name=~".+"}',
     "memory"),
    ("memory_cache_bytes",
     'container_memory_cache{name=~".+"}',
     "memory"),

    # ── Network I/O per container ──────────────────────────────────────────
    ("network_rx_bytes_rate",
     'sum(rate(container_network_receive_bytes_total{name=~".+"}[{W}])) by (name)',
     "network"),
    ("network_tx_bytes_rate",
     'sum(rate(container_network_transmit_bytes_total{name=~".+"}[{W}])) by (name)',
     "network"),
    ("network_rx_errors_rate",
     'sum(rate(container_network_receive_errors_total{name=~".+"}[{W}])) by (name)',
     "network"),
    ("network_tx_errors_rate",
     'sum(rate(container_network_transmit_errors_total{name=~".+"}[{W}])) by (name)',
     "network"),

    # ── Disk I/O per container ─────────────────────────────────────────────
    ("disk_read_bytes_rate",
     'sum(rate(container_fs_reads_bytes_total{name=~".+"}[{W}])) by (name)',
     "disk"),
    ("disk_write_bytes_rate",
     'sum(rate(container_fs_writes_bytes_total{name=~".+"}[{W}])) by (name)',
     "disk"),

    # ── Flink metrics ──────────────────────────────────────────────────────
    ("flink_checkpoint_duration_ms",
     "flink_jobmanager_job_lastCheckpointDuration",
     "flink"),
    ("flink_checkpoint_size_bytes",
     "flink_jobmanager_job_lastCheckpointSize",
     "flink"),
    ("flink_num_checkpoints_completed",
     "flink_jobmanager_job_numberOfCompletedCheckpoints",
     "flink"),
    ("flink_num_checkpoints_failed",
     "flink_jobmanager_job_numberOfFailedCheckpoints",
     "flink"),
    ("flink_records_in_rate",
     'rate(flink_taskmanager_job_task_operator_numRecordsIn[{W}])',
     "flink"),
    ("flink_records_out_rate",
     'rate(flink_taskmanager_job_task_operator_numRecordsOut[{W}])',
     "flink"),
    ("flink_backpressure",
     "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
     "flink"),
    ("flink_heap_used_bytes",
     'flink_taskmanager_Status_JVM_Memory_Heap_Used',
     "flink"),
    ("flink_gc_time_ms",
     'flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time',
     "flink"),

    # ── PostgreSQL metrics ─────────────────────────────────────────────────
    ("pg_active_connections",
     "pg_stat_activity_count",
     "postgres"),
    ("pg_xact_commit_rate",
     "rate(pg_stat_database_xact_commit{datname='benchmark'}[{W}])",
     "postgres"),
    ("pg_xact_rollback_rate",
     "rate(pg_stat_database_xact_rollback{datname='benchmark'}[{W}])",
     "postgres"),
    ("pg_blocks_hit_rate",
     "rate(pg_stat_database_blks_hit{datname='benchmark'}[{W}])",
     "postgres"),
    ("pg_tup_inserted_rate",
     "rate(pg_stat_database_tup_inserted{datname='benchmark'}[{W}])",
     "postgres"),
    ("pg_db_size_bytes",
     "pg_database_size_bytes{datname='benchmark'}",
     "postgres"),
]


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------
def query_prometheus(query: str, base_url: str) -> list:
    """Execute a PromQL instant query; return list of result dicts."""
    url = f"{base_url}/api/v1/query"
    encoded = quote(query, safe="")
    full_url = f"{url}?query={encoded}&time={time.time()}"
    try:
        req = Request(full_url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
            print(f"[export] PromQL non-success for: {query[:80]}", file=sys.stderr)
    except (URLError, json.JSONDecodeError, TimeoutError) as exc:
        print(f"[export] Warning: query failed ({exc}): {query[:80]}", file=sys.stderr)
    return []


def query_range_prometheus(query: str, base_url: str, duration_s: int = 300) -> dict:
    """Execute a PromQL range query; return avg over duration."""
    end = time.time()
    start = end - duration_s
    url = f"{base_url}/api/v1/query_range"
    encoded = quote(query, safe="")
    full_url = f"{url}?query={encoded}&start={start}&end={end}&step=15"
    try:
        req = Request(full_url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
    except Exception as exc:
        print(f"[export] range query failed ({exc}): {query[:80]}", file=sys.stderr)
    return []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Export Prometheus metrics snapshot to CSV")
    parser.add_argument("--strategy",       required=True, help="Strategy name (batch|microbatch|streaming)")
    parser.add_argument("--scenario",       required=True, help="Scenario name (low-load|high-load|...)")
    parser.add_argument("--run-id",         required=True, help="Run identifier (run_1, run_2, ...)")
    parser.add_argument("--output",         required=True, help="Output CSV path")
    parser.add_argument("--prometheus-url", default=PROMETHEUS_URL, help="Prometheus base URL")
    parser.add_argument("--window",         default="5m",  help="PromQL lookback window (e.g. 5m, 10m, 20m)")
    args = parser.parse_args()

    base_url = args.prometheus_url.rstrip("/")
    window   = args.window
    ts       = time.strftime("%Y-%m-%dT%H:%M:%S")

    rows = []
    ok = 0
    missing = 0

    print(f"[export] Querying Prometheus at {base_url} (window={window})…")

    for metric_name, query_template, category in METRIC_QUERIES:
        query = query_template.replace("{W}", window)
        results = query_prometheus(query, base_url)

        if results:
            for r in results:
                labels = r.get("metric", {})
                value  = r.get("value", [None, "NaN"])[1]
                label_str = "|".join(
                    f"{k}={v}" for k, v in sorted(labels.items()) if k != "__name__"
                )
                rows.append({
                    "strategy":  args.strategy,
                    "scenario":  args.scenario,
                    "run_id":    args.run_id,
                    "category":  category,
                    "metric":    metric_name,
                    "labels":    label_str,
                    "value":     value,
                    "timestamp": ts,
                })
            ok += 1
        else:
            rows.append({
                "strategy":  args.strategy,
                "scenario":  args.scenario,
                "run_id":    args.run_id,
                "category":  category,
                "metric":    metric_name,
                "labels":    "",
                "value":     "NaN",
                "timestamp": ts,
            })
            missing += 1

    fieldnames = ["strategy", "scenario", "run_id", "category",
                  "metric", "labels", "value", "timestamp"]

    with open(args.output, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    total = ok + missing
    print(
        f"[export] Done — {total} metrics ({ok} with data, {missing} NaN) → {args.output}"
    )


if __name__ == "__main__":
    main()
