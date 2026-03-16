"""
Workload generator — high-throughput multi-threaded Kafka producer.

Supports realistic event schemas (IoT sensor, financial tick, health monitor),
variable payload sizes, burst patterns, warmup phases, and finite run durations
for fully reproducible experiments at large scale.
"""

import json
import logging
import os
import random
import string
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import yaml
from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [generator] %(levelname)s %(message)s",
)
log = logging.getLogger("generator")

# ── Scenario defaults ───────────────────────────────────────────────
DEFAULT_SCENARIOS: dict[str, Any] = {
    "low-load": {
        "event_rate": 2_000,
        "payload": 512,
        "schema": "iot_sensor",
    },
    "medium-load": {
        "event_rate": 10_000,
        "payload": 512,
        "schema": "financial_tick",
    },
    "high-load": {
        "event_rate": 30_000,
        "payload": 512,
        "schema": "health_monitor",
    },
    "extreme-load": {
        "event_rate": 100_000,
        "payload": 512,
        "schema": "iot_sensor",
    },
    "burst": {
        "event_rate": 10_000,
        "payload": 512,
        "burst_rate": 50_000,
        "burst_duration": 60,
        "burst_period": 300,
        "schema": "financial_tick",
    },
    "mixed-payload": {
        "event_rate": 10_000,
        "payload": 512,          # base; rotates across payload_sizes
        "payload_sizes": [512, 4096, 65536],
        "schema": "iot_sensor",
    },
}




# ── Event schema builders ───────────────────────────────────────────
def _rand_str(n: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def _iot_sensor_event(payload_size: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "produced_at": int(time.time() * 1000),
        "schema": "iot_sensor",
        "device_id": f"sensor-{random.randint(1, 9999):04d}",
        "temperature_c": round(random.uniform(-10.0, 60.0), 2),
        "humidity_pct": round(random.uniform(10.0, 99.9), 2),
        "pressure_hpa": round(random.uniform(950.0, 1050.0), 2),
        "battery_v": round(random.uniform(2.5, 4.2), 3),
        "status": random.choice(["ok", "ok", "ok", "warn", "error"]),
        "payload": _rand_str(max(0, payload_size - 120)),
    }


def _financial_tick_event(payload_size: int) -> dict:
    symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "AAPL", "MSFT", "GOOG", "AMZN"]
    exchanges = ["binance", "coinbase", "kraken", "nasdaq", "nyse"]
    price = round(random.uniform(1.0, 80000.0), 4)
    return {
        "event_id": str(uuid.uuid4()),
        "produced_at": int(time.time() * 1000),
        "schema": "financial_tick",
        "symbol": random.choice(symbols),
        "exchange": random.choice(exchanges),
        "price": price,
        "bid": round(price * random.uniform(0.999, 1.0), 4),
        "ask": round(price * random.uniform(1.0, 1.001), 4),
        "volume": round(random.uniform(0.001, 100.0), 6),
        "trade_id": str(uuid.uuid4()),
        "payload": _rand_str(max(0, payload_size - 200)),
    }


def _health_monitor_event(payload_size: int) -> dict:
    hr = random.randint(40, 180)
    return {
        "event_id": str(uuid.uuid4()),
        "produced_at": int(time.time() * 1000),
        "schema": "health_monitor",
        "patient_id": f"P-{random.randint(1000, 9999)}",
        "device_model": random.choice(["AppleWatch9", "GarminVenu3", "FitbitSense2"]),
        "heart_rate_bpm": hr,
        "spo2_pct": round(random.uniform(92.0, 100.0), 1),
        "steps_delta": random.randint(0, 20),
        "alert": hr > 150 or hr < 45,
        "location": random.choice(["home", "hospital", "clinic", "ambulatory"]),
        "payload": _rand_str(max(0, payload_size - 200)),
    }


SCHEMA_BUILDERS = {
    "iot_sensor": _iot_sensor_event,
    "financial_tick": _financial_tick_event,
    "health_monitor": _health_monitor_event,
}


def build_event(schema: str, payload_size: int) -> bytes:
    builder = SCHEMA_BUILDERS.get(schema, _iot_sensor_event)
    return json.dumps(builder(payload_size)).encode("utf-8")


# ── Kafka helpers ───────────────────────────────────────────────────
def configure_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        # Throughput-oriented settings
        "linger.ms": 5,
        "batch.num.messages": 10_000,
        "batch.size": 1_048_576,          # 1 MB batch
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes": 2_097_152,
        "compression.type": "lz4",
        "acks": "1",                      # Leader ack — balance speed/durability
        "retries": 3,
        "retry.backoff.ms": 100,
    })


def wait_for_kafka(bootstrap_servers: str, max_retries: int = 30) -> Producer:
    for attempt in range(1, max_retries + 1):
        try:
            p = configure_producer(bootstrap_servers)
            p.list_topics(timeout=5)
            log.info("Kafka ready at %s (attempt %d)", bootstrap_servers, attempt)
            return p
        except KafkaException as exc:
            log.warning("Kafka not ready (%d/%d): %s", attempt, max_retries, exc)
            if attempt == max_retries:
                raise
            time.sleep(2.0)
    raise RuntimeError("Unreachable")


# ── Shared state for multi-threaded production ──────────────────────
class SharedState:
    def __init__(self):
        self.running = True
        self.in_burst = False
        self.current_rate = 0
        self.schema = "iot_sensor"
        self.payload_sizes: list[int] = [512]
        self._payload_idx = 0
        self._lock = threading.Lock()

    def next_payload_size(self) -> int:
        with self._lock:
            size = self.payload_sizes[self._payload_idx % len(self.payload_sizes)]
            self._payload_idx += 1
            return size


# ── Producer thread ─────────────────────────────────────────────────
def producer_thread(
    thread_id: int,
    bootstrap_servers: str,
    topic: str,
    scenario_name: str,
    state: SharedState,
    target_rate_fn,          # callable() → int (events/s for this thread)
    interval: float = 0.1,   # send window in seconds (100ms → fine-grained pacing)
):
    """Each thread manages its own Kafka Producer and sends events independently."""
    try:
        producer = configure_producer(bootstrap_servers)
    except Exception as exc:
        log.error("Thread %d: producer init failed: %s", thread_id, exc)
        return

    log.info("Producer thread %d started", thread_id)

    def on_delivery(err, msg):
        if err:
            pass

    while state.running:
        try:
            thread_rate = target_rate_fn() // max(1, state._n_threads)  # type: ignore[attr-defined]
            events_per_window = max(1, int(thread_rate * interval))
            t_start = time.perf_counter()

            for _ in range(events_per_window):
                schema = state.schema
                payload_size = state.next_payload_size()
                # Build dict first so we can extract the key cleanly,
                # then serialize — avoids brittle byte-offset slicing.
                builder = SCHEMA_BUILDERS.get(schema, _iot_sensor_event)
                event_dict = builder(payload_size)
                key = event_dict["event_id"].encode("utf-8")
                raw = json.dumps(event_dict).encode("utf-8")

                t0 = time.perf_counter()
                try:
                    producer.produce(topic, value=raw, key=key, on_delivery=on_delivery)
                    producer.poll(0)
                except BufferError:
                    producer.poll(0.01)
                except Exception as exc:
                    log.error("Thread %d: produce error: %s", thread_id, exc)

            producer.flush(0)  # non-blocking flush

            elapsed = time.perf_counter() - t_start
            sleep_for = max(0.0, interval - elapsed)
            time.sleep(sleep_for)

        except Exception as exc:
            log.error("Thread %d: unexpected error: %s", thread_id, exc)
            time.sleep(1.0)

    producer.flush(2.0)
    log.info("Producer thread %d finished", thread_id)


# ── Scenario / config loading ───────────────────────────────────────
def load_scenarios() -> dict:
    scenario_file = os.getenv("SCENARIO_FILE")
    if scenario_file and Path(scenario_file).is_file():
        with open(scenario_file, "r", encoding="utf-8") as f:
            user_def = yaml.safe_load(f) or {}
        return {**DEFAULT_SCENARIOS, **user_def}
    return DEFAULT_SCENARIOS


def resolve_scenario(configs: dict):
    key = os.getenv("SCENARIO", "low-load")
    base = configs.get(key, DEFAULT_SCENARIOS["low-load"])
    rate = int(os.getenv("EVENT_RATE", base.get("event_rate", 2000)))
    payload = int(os.getenv("PAYLOAD_SIZE", base.get("payload", 512)))
    schema = os.getenv("EVENT_SCHEMA", base.get("schema", "iot_sensor"))
    return key, {**base, "event_rate": rate, "payload": payload, "schema": schema}


def decide_n_threads(target_rate: int) -> int:
    """Heuristic: 1 thread per 20k ev/s, min 2, max 16."""
    return max(2, min(16, (target_rate + 19_999) // 20_000))


# ── Main ────────────────────────────────────────────────────────────
def main():
    run_duration = int(os.getenv("RUN_DURATION_SECONDS", "0"))  # 0 = infinite
    warmup_seconds = int(os.getenv("WARMUP_SECONDS", "30"))

    scenarios = load_scenarios()
    scenario_name, scenario = resolve_scenario(scenarios)

    topic = os.getenv("TOPIC_NAME", "events")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    base_rate = scenario["event_rate"]
    burst_rate = scenario.get("burst_rate", base_rate)
    burst_duration = scenario.get("burst_duration", 60)
    burst_period = scenario.get("burst_period", 300)
    payload_sizes = scenario.get("payload_sizes", [scenario["payload"]])

    n_threads = decide_n_threads(max(base_rate, burst_rate))
    log.info(
        "Config: scenario=%s rate=%d burst=%d schema=%s payload=%s threads=%d duration=%s warmup=%ds",
        scenario_name, base_rate, burst_rate, scenario["schema"],
        payload_sizes, n_threads,
        f"{run_duration}s" if run_duration > 0 else "infinite",
        warmup_seconds,
    )

    # Wait for Kafka
    wait_for_kafka(bootstrap_servers)

    # Shared state
    state = SharedState()
    state.schema = scenario["schema"]
    state.payload_sizes = payload_sizes
    state._n_threads = n_threads  # type: ignore[attr-defined]
    state.current_rate = base_rate

    # Rate accessor (reads live state for burst support)
    def current_rate_fn() -> int:
        return state.current_rate

    # Launch producer threads
    threads = []
    for i in range(n_threads):
        t = threading.Thread(
            target=producer_thread,
            args=(i, bootstrap_servers, topic, scenario_name, state, current_rate_fn),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # ── Control loop (burst logic + rate gauge + shutdown) ──────────
    start_time = time.time()
    last_burst = start_time
    warmup_complete = False

    try:
        while True:
            now = time.time()
            elapsed = now - start_time

            # Warmup transition
            if not warmup_complete and elapsed >= warmup_seconds:
                warmup_complete = True
                log.info("Warmup complete (%ds) — measurements now valid", warmup_seconds)

            # Duration check
            if run_duration > 0 and elapsed >= (warmup_seconds + run_duration):
                log.info("Run duration reached (%ds post-warmup), stopping", run_duration)
                break

            # Burst logic
            if scenario_name == "burst":
                if now - last_burst >= burst_period:
                    last_burst = now
                in_burst = (now - last_burst) <= burst_duration
            else:
                in_burst = False

            new_rate = burst_rate if in_burst else base_rate
            state.current_rate = new_rate

            time.sleep(0.5)  # control loop ticks every 500ms

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    log.info("Signalling threads to stop…")
    state.running = False
    for t in threads:
        t.join(timeout=5.0)

    log.info("Generator finished.")


if __name__ == "__main__":
    main()
