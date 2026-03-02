"""
Workload generator — produces synthetic events to a Kafka topic at a
configurable rate, with support for burst patterns, warmup phases, and
finite run durations for reproducible experiments.
"""

import json
import logging
import os
import random
import string
import time
import uuid
from pathlib import Path

import yaml
from confluent_kafka import Producer, KafkaException
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [generator] %(levelname)s %(message)s",
)
log = logging.getLogger("generator")

# ── Scenario defaults ──────────────────────────────────────────────
DEFAULT_SCENARIOS = {
    "low-load":    {"event_rate": 2000,  "payload": 512},
    "medium-load": {"event_rate": 10000, "payload": 512},
    "high-load":   {"event_rate": 30000, "payload": 512},
    "burst": {
        "event_rate": 10000,
        "payload": 512,
        "burst_rate": 50000,
        "burst_duration": 60,
        "burst_period": 300,
    },
}

# ── Prometheus metrics ─────────────────────────────────────────────
EVENT_COUNTER = Counter(
    "generator_events_total", "Events produced", ["scenario"]
)
ERROR_COUNTER = Counter(
    "generator_errors_total", "Errors while producing", ["scenario"]
)
BYTE_COUNTER = Counter(
    "generator_bytes_total", "Payload bytes produced", ["scenario"]
)
CURRENT_RATE = Gauge(
    "generator_current_rate", "Configured event rate per second", ["scenario"]
)
WARMUP_GAUGE = Gauge(
    "generator_warmup_active", "1 during warmup phase, 0 after"
)
SEND_LATENCY = Histogram(
    "generator_produce_latency_ms",
    "Time spent sending a single event to Kafka (ms)",
    buckets=(1, 5, 10, 20, 50, 100, 250, 500, 1000),
)


# ── Helpers ────────────────────────────────────────────────────────
def load_scenarios() -> dict:
    scenario_file = os.getenv("SCENARIO_FILE")
    if scenario_file and Path(scenario_file).is_file():
        with open(scenario_file, "r", encoding="utf-8") as handle:
            user_def = yaml.safe_load(handle) or {}
        return {**DEFAULT_SCENARIOS, **user_def}
    return DEFAULT_SCENARIOS


def random_payload(size: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choices(alphabet, k=size))


def build_event(payload_size: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "produced_at": int(time.time() * 1000),
        "payload": random_payload(payload_size),
    }


def configure_producer(max_retries: int = 30, retry_interval: float = 2.0) -> Producer:
    """Create a Kafka producer with retry logic for startup resilience."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    for attempt in range(1, max_retries + 1):
        try:
            producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 5,
                "batch.num.messages": 10000,
                "queue.buffering.max.messages": 500000,
                "queue.buffering.max.kbytes": 1048576,
            })
            producer.list_topics(timeout=5)
            log.info("Connected to Kafka at %s (attempt %d)", bootstrap_servers, attempt)
            return producer
        except KafkaException as exc:
            log.warning("Kafka not ready (attempt %d/%d): %s", attempt, max_retries, exc)
            if attempt == max_retries:
                raise
            time.sleep(retry_interval)
    raise RuntimeError("Unreachable")


def resolve_scenario(configs: dict):
    scenario_key = os.getenv("SCENARIO", "low-load")
    scenario = configs.get(scenario_key, DEFAULT_SCENARIOS["low-load"])
    event_rate = int(os.getenv("EVENT_RATE", scenario.get("event_rate", 2000)))
    payload_size = int(os.getenv("PAYLOAD_SIZE", scenario.get("payload", 512)))
    return scenario_key, {**scenario, "event_rate": event_rate, "payload": payload_size}


# ── Main loop ──────────────────────────────────────────────────────
def main():
    prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8000"))
    start_http_server(prometheus_port)

    run_duration = int(os.getenv("RUN_DURATION_SECONDS", "0"))   # 0 = infinite
    warmup_seconds = int(os.getenv("WARMUP_SECONDS", "30"))

    scenarios = load_scenarios()
    scenario_name, scenario = resolve_scenario(scenarios)

    topic = os.getenv("TOPIC_NAME", "events")
    producer = configure_producer()

    burst_rate = scenario.get("burst_rate", scenario["event_rate"])
    burst_duration = scenario.get("burst_duration", 60)
    burst_period = scenario.get("burst_period", 300)

    CURRENT_RATE.labels(scenario_name).set(scenario["event_rate"])
    WARMUP_GAUGE.set(1)
    warmup_complete = False

    interval = 1.0
    last_burst = time.time()
    start_time = time.time()

    log.info(
        "Starting generation: scenario=%s rate=%d payload=%dB duration=%s warmup=%ds",
        scenario_name,
        scenario["event_rate"],
        scenario["payload"],
        f"{run_duration}s" if run_duration > 0 else "infinite",
        warmup_seconds,
    )

    while True:
        loop_start = time.time()
        elapsed_total = loop_start - start_time

        # ── Warmup transition ──────────────────────────────────────
        if not warmup_complete and elapsed_total >= warmup_seconds:
            warmup_complete = True
            WARMUP_GAUGE.set(0)
            log.info("Warmup complete after %ds — measurements are now valid", warmup_seconds)

        # ── Duration check (post-warmup) ───────────────────────────
        if run_duration > 0 and elapsed_total >= (warmup_seconds + run_duration):
            log.info("Run duration reached (%ds post-warmup), shutting down", run_duration)
            break

        # ── Burst logic ────────────────────────────────────────────
        now = time.time()
        in_burst = False
        if scenario_name == "burst":
            if now - last_burst >= burst_period:
                last_burst = now
            in_burst = (now - last_burst) <= burst_duration

        rate = burst_rate if in_burst else scenario["event_rate"]
        events_to_send = max(1, int(rate * interval))
        CURRENT_RATE.labels(scenario_name).set(rate)

        # ── Produce events ─────────────────────────────────────────
        for _ in range(events_to_send):
            event = build_event(scenario["payload"])
            payload = json.dumps(event).encode("utf-8")
            start = time.time()
            try:
                producer.produce(topic, value=payload, key=event["event_id"])
                producer.poll(0)
                duration_ms = (time.time() - start) * 1000
                SEND_LATENCY.observe(duration_ms)
                EVENT_COUNTER.labels(scenario_name).inc()
                BYTE_COUNTER.labels(scenario_name).inc(len(payload))
            except Exception as exc:  # pylint: disable=broad-except
                ERROR_COUNTER.labels(scenario_name).inc()
                log.error("Failed to send event: %s", exc)

        producer.flush()
        elapsed = time.time() - loop_start
        time.sleep(max(0.0, interval - elapsed))

    producer.flush()
    log.info("Generator finished.")


if __name__ == "__main__":
    main()
