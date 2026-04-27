"""
Availability probe — polls PostgreSQL for newly visible events, records
latency samples to CSV and exposes Prometheus metrics with per-strategy
and per-scenario labels.
"""

import csv
import logging
import os
import signal
import sys
import time
from pathlib import Path

import psycopg2
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [probe] %(levelname)s %(message)s",
)
log = logging.getLogger("probe")

# ── Prometheus metrics ───────────────────────────────────────────────
SINK_ROWS_TOTAL = Counter(
    "sink_rows_written_total",
    "Filas visibles detectadas en el sink",
    labelnames=["strategy", "scenario"],
)
PROBE_LATENCY = Histogram(
    "probe_latency_ms",
    "Latencia visible_at - produced_at en ms",
    labelnames=["strategy", "scenario"],
    buckets=(1, 2, 5, 10, 20, 50, 100, 250, 500, 1000, 2000, 5000, 10000),
)
LAST_VISIBLE = Gauge(
    "probe_last_visible_at_ms",
    "Último visible_at observado",
    labelnames=["strategy", "scenario"],
)
PROBE_ERRORS = Counter(
    "probe_errors_total",
    "Errores al consultar el sink",
)


# ── Helpers ────────────────────────────────────────────────────────
def pg_connection(max_retries: int = 60, retry_interval: float = 2.0):
    """Create a PostgreSQL connection with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "benchmark"),
                password=os.getenv("POSTGRES_PASSWORD", "benchmark"),
                dbname=os.getenv("POSTGRES_DB", "benchmark"),
            )
            log.info("Connected to PostgreSQL (attempt %d)", attempt)
            return conn
        except psycopg2.OperationalError as exc:
            log.warning(
                "PostgreSQL not ready (attempt %d/%d): %s", attempt, max_retries, exc
            )
            if attempt == max_retries:
                raise
            time.sleep(retry_interval)
    raise RuntimeError("Unreachable")


def ensure_results_file(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "event_id",
                    "strategy",
                    "scenario",
                    "run_id",
                    "produced_at",
                    "visible_at",
                    "latency_ms",
                ]
            )
    return path


RUNNING = True


def handle_signal(signum, frame):  # pylint: disable=unused-argument
    global RUNNING  # noqa: PLW0603
    RUNNING = False


# ── Main loop ──────────────────────────────────────────────────────
def main():
    poll_interval = int(os.getenv("PROBE_POLL_INTERVAL_MS", "500")) / 1000.0
    results_path = Path(os.getenv("RESULTS_PATH", "/results")) / "latency_samples.csv"
    probe_run_id = os.getenv("RUN_ID", "run_1")
    probe_strategy = os.getenv("STRATEGY", "unknown")
    probe_scenario = os.getenv("SCENARIO", "low-load")
    ensure_results_file(results_path)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    last_visible_at = 0
    last_event_id = "00000000-0000-0000-0000-000000000000"

    prom_port = int(os.getenv("PROMETHEUS_PORT", "8001"))
    start_http_server(prom_port)
    log.info("Prometheus metrics en :%d", prom_port)

    conn = pg_connection()
    conn.autocommit = True

    log.info("Probe started — polling every %dms", int(poll_interval * 1000))
    log.info(
        "Probe filter labels: strategy=%s scenario=%s run_id=%s",
        probe_strategy,
        probe_scenario,
        probe_run_id,
    )

    while RUNNING:
        try:
            drained_any = False
            while RUNNING:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT event_id, produced_at, visible_at,
                               strategy, scenario, run_id
                        FROM events
                        WHERE run_id = %s
                          AND strategy = %s
                          AND scenario = %s
                          AND (
                              visible_at > %s
                              OR (visible_at = %s AND event_id::text > %s)
                          )
                        ORDER BY visible_at ASC, event_id ASC
                        LIMIT 5000
                        """,
                        (
                            probe_run_id,
                            probe_strategy,
                            probe_scenario,
                            last_visible_at,
                            last_visible_at,
                            last_event_id,
                        ),
                    )
                    rows = cur.fetchall()

                if not rows:
                    break

                drained_any = True
                with open(results_path, "a", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle)
                    for (
                        event_id,
                        produced_at,
                        visible_at,
                        strategy,
                        scenario,
                        run_id,
                    ) in rows:
                        latency = visible_at - produced_at
                        strategy = strategy or "unknown"
                        scenario = scenario or "unknown"
                        run_id = run_id or "unset"
                        writer.writerow(
                            [
                                event_id,
                                strategy,
                                scenario,
                                run_id,
                                produced_at,
                                visible_at,
                                latency,
                            ]
                        )

                        last_visible_at = visible_at
                        last_event_id = str(event_id)
                        SINK_ROWS_TOTAL.labels(strategy, scenario).inc()
                        PROBE_LATENCY.labels(strategy, scenario).observe(latency)
                        LAST_VISIBLE.labels(strategy, scenario).set(visible_at)

                if len(rows) < 5000:
                    break

            time.sleep(poll_interval)

        except psycopg2.OperationalError as exc:
            log.error("Connection lost, reconnecting: %s", exc)
            PROBE_ERRORS.inc()
            try:
                conn.close()
            except Exception:
                pass
            conn = pg_connection()
            conn.autocommit = True
        except Exception as exc:  # pylint: disable=broad-except
            log.error("Error while sampling: %s", exc)
            PROBE_ERRORS.inc()
            time.sleep(2)

    log.info("Shutting down")
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:  # pylint: disable=broad-except
        log.critical("Probe failed: %s", err)
        sys.exit(1)
