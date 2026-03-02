"""
export_metrics.py — Queries Prometheus HTTP API at the end of a run
and exports key metrics to a CSV file for offline analysis.

Usage:
    python scripts/export_metrics.py \
        --strategy streaming \
        --scenario high-load \
        --run-id run_3 \
        --output results/streaming/high-load/run_3/prometheus_snapshot.csv
"""

import argparse
import csv
import sys
import time
from urllib.request import urlopen, Request
from urllib.error import URLError
import json


PROMETHEUS_URL = "http://localhost:9090"

# Metrics to export with their PromQL queries
METRIC_QUERIES = [
    ("generator_events_total",          'generator_events_total'),
    ("generator_errors_total",          'generator_errors_total'),
    ("generator_bytes_total",           'generator_bytes_total'),
    ("probe_visible_events_total",      'probe_visible_events_total'),
    ("probe_errors_total",              'probe_errors_total'),
    ("probe_latency_p50",              'histogram_quantile(0.50, rate(probe_latency_bucket[5m]))'),
    ("probe_latency_p95",              'histogram_quantile(0.95, rate(probe_latency_bucket[5m]))'),
    ("probe_latency_p99",              'histogram_quantile(0.99, rate(probe_latency_bucket[5m]))'),
    ("probe_throughput",               'probe_throughput_events_per_sec'),
    ("cadvisor_cpu_usage_total",       'sum(rate(container_cpu_usage_seconds_total{name=~".+"}[5m])) by (name)'),
    ("cadvisor_memory_usage",          'sum(container_memory_usage_bytes{name=~".+"}) by (name)'),
    ("cadvisor_network_rx_bytes",      'sum(rate(container_network_receive_bytes_total{name=~".+"}[5m])) by (name)'),
    ("cadvisor_network_tx_bytes",      'sum(rate(container_network_transmit_bytes_total{name=~".+"}[5m])) by (name)'),
]


def query_prometheus(query: str) -> list:
    """Execute an instant query against Prometheus."""
    url = f"{PROMETHEUS_URL}/api/v1/query"
    params = f"query={query}&time={time.time()}"
    full_url = f"{url}?{params}"
    try:
        req = Request(full_url)
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
    except (URLError, json.JSONDecodeError) as exc:
        print(f"[export] Warning: Failed to query '{query}': {exc}", file=sys.stderr)
    return []


def main():
    parser = argparse.ArgumentParser(description="Export Prometheus metrics to CSV")
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--prometheus-url", default=PROMETHEUS_URL)
    args = parser.parse_args()

    global PROMETHEUS_URL
    PROMETHEUS_URL = args.prometheus_url

    rows = []
    for metric_name, query in METRIC_QUERIES:
        results = query_prometheus(query)
        if results:
            for r in results:
                labels = r.get("metric", {})
                value = r.get("value", [None, "NaN"])[1]
                label_str = ",".join(f"{k}={v}" for k, v in labels.items()
                                    if k != "__name__")
                rows.append({
                    "strategy": args.strategy,
                    "scenario": args.scenario,
                    "run_id": args.run_id,
                    "metric": metric_name,
                    "labels": label_str,
                    "value": value,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                })
        else:
            rows.append({
                "strategy": args.strategy,
                "scenario": args.scenario,
                "run_id": args.run_id,
                "metric": metric_name,
                "labels": "",
                "value": "NaN",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            })

    fieldnames = ["strategy", "scenario", "run_id", "metric", "labels", "value", "timestamp"]
    with open(args.output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[export] Exported {len(rows)} metric rows to {args.output}")


if __name__ == "__main__":
    main()
