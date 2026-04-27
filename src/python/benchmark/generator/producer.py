"""
Workload generator for the official thesis benchmark.

Supports the three official scenarios only:
- low-load
- medium-load
- high-load

Each scenario maps to a fixed event schema, payload size, warmup, and finite run duration.
"""

import json
import logging
import os
import random
import signal
import string
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import yaml
from confluent_kafka import Producer, KafkaException
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [generator] %(levelname)s %(message)s",
)
log = logging.getLogger("generator")

# ── Prometheus metrics ───────────────────────────────────────────────
MESSAGES_TOTAL = Counter(
    "kafka_produced_messages_total",
    "Total events enviados a Kafka",
    labelnames=["scenario", "run_id", "strategy"],
)
BYTES_TOTAL = Counter(
    "kafka_produced_bytes_total",
    "Bytes producidos hacia Kafka",
    labelnames=["scenario", "run_id", "strategy"],
)
ERROR_TOTAL = Counter(
    "kafka_produce_errors_total",
    "Errores al enviar eventos",
    labelnames=["scenario", "run_id", "strategy"],
)
CURRENT_RATE = Gauge(
    "generator_current_rate",
    "Tasa objetivo actual (eventos/s)",
    labelnames=["scenario", "run_id", "strategy"],
)
PRODUCE_LAT_MS = Histogram(
    "kafka_produce_latency_ms",
    "Latencia de produce() a Kafka en ms",
    labelnames=["scenario", "run_id", "strategy"],
    buckets=(0.5, 1, 2, 5, 10, 20, 50, 100, 250, 500, 1000),
)

# ── Scenario defaults ───────────────────────────────────────────────
DEFAULT_SCENARIOS: dict[str, Any] = {
    "low-load": {
        "event_rate": 2_000,
        "payload": 1500,
        "schema": "iot_sensor",
    },
    "medium-load": {
        "event_rate": 10_000,
        "payload": 1500,
        "schema": "financial_tick",
    },
    "high-load": {
        "event_rate": 30_000,
        "payload": 1500,
        "schema": "health_monitor",
    },
}

MIN_PAYLOAD_BYTES = 1500


# ── Event schema builders ───────────────────────────────────────────
def _rand_str(n: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def _iot_sensor_event(payload_size: int, event_id: str, payload_base: str) -> dict:
    return {
        "event_id": event_id,
        "produced_at": int(time.time() * 1000),
        "schema": "iot_sensor",
        "device_id": f"sensor-{random.randint(1, 9999):04d}",
        "temperature_c": round(random.uniform(-10.0, 60.0), 2),
        "humidity_pct": round(random.uniform(10.0, 99.9), 2),
        "pressure_hpa": round(random.uniform(950.0, 1050.0), 2),
        "battery_v": round(random.uniform(2.5, 4.2), 3),
        "status": random.choice(["ok", "ok", "ok", "warn", "error"]),
        "payload": payload_base,
    }


def _financial_tick_event(payload_size: int, event_id: str, payload_base: str) -> dict:
    symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "AAPL", "MSFT", "GOOG", "AMZN"]
    exchanges = ["binance", "coinbase", "kraken", "nasdaq", "nyse"]
    price = round(random.uniform(1.0, 80000.0), 4)
    return {
        "event_id": event_id,
        "produced_at": int(time.time() * 1000),
        "schema": "financial_tick",
        "symbol": random.choice(symbols),
        "exchange": random.choice(exchanges),
        "price": price,
        "bid": round(price * random.uniform(0.999, 1.0), 4),
        "ask": round(price * random.uniform(1.0, 1.001), 4),
        "volume": round(random.uniform(0.001, 100.0), 6),
        "trade_id": event_id,
        "payload": payload_base,
    }


def _health_monitor_event(payload_size: int, event_id: str, payload_base: str) -> dict:
    hr = random.randint(40, 180)
    return {
        "event_id": event_id,
        "produced_at": int(time.time() * 1000),
        "schema": "health_monitor",
        "patient_id": f"P-{random.randint(1000, 9999)}",
        "device_model": random.choice(["AppleWatch9", "GarminVenu3", "FitbitSense2"]),
        "heart_rate_bpm": hr,
        "spo2_pct": round(random.uniform(92.0, 100.0), 1),
        "steps_delta": random.randint(0, 20),
        "alert": hr > 150 or hr < 45,
        "location": random.choice(["home", "hospital", "clinic", "ambulatory"]),
        "payload": payload_base,
    }


SCHEMA_BUILDERS = {
    "iot_sensor": _iot_sensor_event,
    "financial_tick": _financial_tick_event,
    "health_monitor": _health_monitor_event,
}


def build_event(schema: str, payload_size: int) -> bytes:
    builder = SCHEMA_BUILDERS.get(schema, _iot_sensor_event)
    return json.dumps(builder(payload_size, str(uuid.uuid4()), _rand_str(max(0, payload_size - 200)))).encode("utf-8")


# ── Kafka helpers ───────────────────────────────────────────────────
def configure_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            # Throughput-oriented settings
            "linger.ms": 5,
            "batch.num.messages": 10_000,
            "batch.size": 1_048_576,  # 1 MB batch
            "queue.buffering.max.messages": 1_000_000,
            "queue.buffering.max.kbytes": 2_097_152,
            "compression.type": "lz4",
            "acks": "1",  # Leader ack — balance speed/durability
            "retries": 3,
            "retry.backoff.ms": 100,
        }
    )


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
        self.current_rate = 0
        self.schema = "iot_sensor"
        self.payload_sizes: list[int] = [MIN_PAYLOAD_BYTES]
        self._payload_idx = 0
        self._lock = threading.Lock()
        self.generated_events = 0
        self.generated_bytes = 0
        self.produce_errors = 0

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
    run_id: str,
    strategy: str,
    state: SharedState,
    target_rate_fn,
    interval: float = 0.1,
):
    try:
        producer = configure_producer(bootstrap_servers)
    except Exception as exc:
        log.error("Thread %d: producer init failed: %s", thread_id, exc)
        return

    log.info("Producer thread %d started", thread_id)

    def on_delivery(err, msg):
        if err:
            pass

    seq = 0
    payload_size = state.next_payload_size()
    payload_base = _rand_str(max(0, payload_size - 200))

    while state.running:
        try:
            thread_rate = target_rate_fn() // max(1, state._n_threads)  # type: ignore[attr-defined]
            events_per_window = max(1, int(thread_rate * interval))
            t_start = time.perf_counter()

            local_sent = 0
            local_bytes = 0
            local_errors = 0
            produced_at_ms = int(time.time() * 1000)

            for _ in range(events_per_window):
                seq += 1
                event_id = f"{run_id}-{thread_id}-{seq}"
                builder = SCHEMA_BUILDERS.get(state.schema, _iot_sensor_event)
                event_dict = builder(payload_size, event_id, payload_base)
                event_dict["strategy"] = strategy
                event_dict["scenario"] = scenario_name
                event_dict["run_id"] = run_id
                event_dict["produced_at"] = produced_at_ms
                raw = json.dumps(event_dict).encode("utf-8")
                key = event_id.encode("utf-8")

                try:
                    producer.produce(topic, value=raw, key=key, on_delivery=on_delivery)
                    local_sent += 1
                    local_bytes += len(raw)
                except BufferError:
                    producer.poll(0.01)
                except Exception:
                    local_errors += 1

            producer.poll(0)

            if local_sent:
                MESSAGES_TOTAL.labels(scenario_name, run_id, strategy).inc(local_sent)
                BYTES_TOTAL.labels(scenario_name, run_id, strategy).inc(local_bytes)
            if local_errors:
                ERROR_TOTAL.labels(scenario_name, run_id, strategy).inc(local_errors)
                log.error("Thread %d: %d produce errors in window", thread_id, local_errors)

            with state._lock:
                state.generated_events += local_sent
                state.generated_bytes += local_bytes
                state.produce_errors += local_errors

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
    payload = int(os.getenv("PAYLOAD_SIZE", base.get("payload", MIN_PAYLOAD_BYTES)))
    if payload < MIN_PAYLOAD_BYTES:
        log.warning(
            "PAYLOAD_SIZE=%d es menor al minimo requerido (%d); usando %d",
            payload,
            MIN_PAYLOAD_BYTES,
            MIN_PAYLOAD_BYTES,
        )
        payload = MIN_PAYLOAD_BYTES
    schema = os.getenv("EVENT_SCHEMA", base.get("schema", "iot_sensor"))
    return key, {**base, "event_rate": rate, "payload": payload, "schema": schema}


def decide_n_threads(target_rate: int) -> int:
    override = os.getenv("GENERATOR_THREADS")
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            log.warning("GENERATOR_THREADS=%s invalido; usando heuristica", override)
    # More aggressive default so medium/high official scenarios are not
    # artificially capped by too few producer threads.
    return max(2, min(32, (target_rate + 4_999) // 5_000))


# ── Main ────────────────────────────────────────────────────────────
def main():
    run_duration = int(os.getenv("RUN_DURATION_SECONDS", "0"))  # 0 = infinite
    warmup_seconds = int(os.getenv("WARMUP_SECONDS", "30"))

    scenarios = load_scenarios()
    scenario_name, scenario = resolve_scenario(scenarios)
    run_id = os.getenv("RUN_ID", "run_1")
    strategy = os.getenv("STRATEGY", "unknown")

    topic = os.getenv("TOPIC_NAME", "events")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    summary_path = os.getenv("GENERATOR_SUMMARY_PATH", "/results/generator_summary.json")
    load_profile = os.getenv("GENERATOR_LOAD_PROFILE", "constant")

    base_rate = scenario["event_rate"]
    payload_sizes = scenario.get("payload_sizes", [scenario["payload"]])
    payload_sizes = [max(MIN_PAYLOAD_BYTES, int(sz)) for sz in payload_sizes]

    n_threads = decide_n_threads(base_rate)
    log.info(
        "Config: scenario=%s rate=%d schema=%s payload=%s threads=%d duration=%s warmup=%ds",
        scenario_name,
        base_rate,
        scenario["schema"],
        payload_sizes,
        n_threads,
        f"{run_duration}s" if run_duration > 0 else "infinite",
        warmup_seconds,
    )
    log.info("Labels: run_id=%s strategy=%s", run_id, strategy)

    prom_port = int(os.getenv("PROMETHEUS_PORT", "8000"))
    start_http_server(prom_port)
    log.info("Prometheus metrics en :%d", prom_port)

    # Wait for Kafka
    wait_for_kafka(bootstrap_servers)

    # Shared state
    state = SharedState()
    state.schema = scenario["schema"]
    state.payload_sizes = payload_sizes
    state._n_threads = n_threads  # type: ignore[attr-defined]
    state.current_rate = base_rate

    # Rate accessor for producer threads
    def current_rate_fn() -> int:
        return state.current_rate

    def handle_shutdown(signum, _frame):
        log.info("Signal %s received, stopping generator", signum)
        state.running = False

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # Launch producer threads
    threads = []
    for i in range(n_threads):
        t = threading.Thread(
            target=producer_thread,
            args=(i, bootstrap_servers, topic, scenario_name, run_id, strategy, state, current_rate_fn),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # ── Control loop (rate gauge + shutdown) ────────────────────────
    start_time = time.time()
    generation_start_epoch_ms = int(start_time * 1000)
    warmup_complete = False

    try:
        while state.running:
            now = time.time()
            elapsed = now - start_time

            # Warmup transition
            if not warmup_complete and elapsed >= warmup_seconds:
                warmup_complete = True
                log.info(
                    "Warmup complete (%ds) — measurements now valid", warmup_seconds
                )

            # Duration check
            if run_duration > 0 and elapsed >= (warmup_seconds + run_duration):
                log.info(
                    "Run duration reached (%ds post-warmup), stopping", run_duration
                )
                break

            state.current_rate = base_rate
            CURRENT_RATE.labels(scenario_name, run_id, strategy).set(base_rate)

            time.sleep(0.5)  # control loop ticks every 500ms

    except KeyboardInterrupt:
        log.info("Interrupted by user")
        state.running = False

    log.info("Signalling threads to stop…")
    state.running = False
    for t in threads:
        t.join(timeout=5.0)

    generation_end_epoch_ms = int(time.time() * 1000)
    generation_duration_seconds = max(
        0.001, (generation_end_epoch_ms - generation_start_epoch_ms) / 1000.0
    )
    generated_eps_real = state.generated_events / generation_duration_seconds

    summary = {
        "strategy": strategy,
        "scenario": scenario_name,
        "run_id": run_id,
        "target_eps": int(base_rate),
        "generated_events": int(state.generated_events),
        "generated_bytes": int(state.generated_bytes),
        "produce_errors": int(state.produce_errors),
        "generation_start_epoch_ms": int(generation_start_epoch_ms),
        "generation_end_epoch_ms": int(generation_end_epoch_ms),
        "generation_duration_seconds": round(generation_duration_seconds, 3),
        "generated_eps_real": round(generated_eps_real, 3),
        "payload_bytes": int(payload_sizes[0] if payload_sizes else MIN_PAYLOAD_BYTES),
        "load_profile": load_profile,
    }

    log.info("Writing generator summary to %s", summary_path)
    try:
        summary_file = Path(summary_path)
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        with open(summary_file, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        log.info("Generator summary saved: %s", summary_file)
    except Exception as exc:
        log.error("Failed to write generator summary (%s): %s", summary_path, exc)

    log.info("Generator finished.")


if __name__ == "__main__":
    main()
