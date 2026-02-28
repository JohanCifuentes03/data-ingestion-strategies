import json
import os
import random
import string
import time
import uuid
from pathlib import Path

import yaml
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

DEFAULT_SCENARIOS = {
    "low-load": {"event_rate": 2000, "payload": 512},
    "medium-load": {"event_rate": 10000, "payload": 512},
    "high-load": {"event_rate": 30000, "payload": 512},
    "burst": {
        "event_rate": 10000,
        "payload": 512,
        "burst_rate": 50000,
        "burst_duration": 60,
        "burst_period": 300,
    },
}

EVENT_COUNTER = Counter("generator_events_total", "Events produced", ["scenario"])
ERROR_COUNTER = Counter("generator_errors_total", "Errors while producing", ["scenario"])
BYTE_COUNTER = Counter("generator_bytes_total", "Payload bytes produced", ["scenario"])
CURRENT_RATE = Gauge("generator_current_rate", "Configured event rate per second", ["scenario"])
SEND_LATENCY = Histogram(
    "generator_produce_latency_ms",
    "Time spent sending to Kafka",
    buckets=(1, 5, 10, 20, 50, 100, 250, 500, 1000),
)


def load_scenarios() -> dict:
    scenario_file = os.getenv("SCENARIO_FILE")
    if scenario_file and Path(scenario_file).is_file():
        with open(scenario_file, "r", encoding="utf-8") as handle:
            user_def = yaml.safe_load(handle) or {}
        return {**DEFAULT_SCENARIOS, **user_def}
    return DEFAULT_SCENARIOS


def random_payload(size: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(size))


def build_event(payload_size: int) -> dict:
    produced_at = int(time.time() * 1000)
    return {
        "event_id": str(uuid.uuid4()),
        "produced_at": produced_at,
        "payload": random_payload(payload_size),
    }


def configure_producer() -> Producer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return Producer({"bootstrap.servers": bootstrap_servers})


def resolve_scenario(configs: dict):
    scenario_key = os.getenv("SCENARIO", "low-load")
    scenario = configs.get(scenario_key, DEFAULT_SCENARIOS["low-load"])
    event_rate = int(os.getenv("EVENT_RATE", scenario.get("event_rate", 2000)))
    payload_size = int(os.getenv("PAYLOAD_SIZE", scenario.get("payload", 512)))
    return scenario_key, {**scenario, "event_rate": event_rate, "payload": payload_size}


def main():
    prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8000"))
    start_http_server(prometheus_port)

    scenarios = load_scenarios()
    scenario_name, scenario = resolve_scenario(scenarios)

    topic = os.getenv("TOPIC_NAME", "events")
    producer = configure_producer()

    burst_rate = scenario.get("burst_rate", scenario["event_rate"])
    burst_duration = scenario.get("burst_duration", 60)
    burst_period = scenario.get("burst_period", 300)

    CURRENT_RATE.labels(scenario_name).set(scenario["event_rate"])

    interval = 1.0
    last_burst = time.time()

    while True:
        loop_start = time.time()
        now = time.time()
        in_burst = False
        if scenario_name == "burst":
            if now - last_burst >= burst_period:
                last_burst = now
            in_burst = (now - last_burst) <= burst_duration

        rate = burst_rate if in_burst else scenario["event_rate"]
        events_to_send = max(1, int(rate * interval))
        CURRENT_RATE.labels(scenario_name).set(rate)

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
                print(f"[generator] Failed to send event: {exc}")

        producer.flush()
        elapsed = time.time() - loop_start
        time.sleep(max(0.0, interval - elapsed))


if __name__ == "__main__":
    main()
