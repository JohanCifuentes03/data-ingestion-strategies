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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [probe] %(levelname)s %(message)s",
)
log = logging.getLogger("probe")

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
            log.warning("PostgreSQL not ready (attempt %d/%d): %s", attempt, max_retries, exc)
            if attempt == max_retries:
                raise
            time.sleep(retry_interval)
    raise RuntimeError("Unreachable")


def ensure_results_file(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "event_id", "produced_at", "visible_at", "latency_ms",
                "strategy", "scenario", "run_id",
            ])
    return path


RUNNING = True


def handle_signal(signum, frame):  # pylint: disable=unused-argument
    global RUNNING  # noqa: PLW0603
    RUNNING = False


# ── Main loop ──────────────────────────────────────────────────────
def main():
    poll_interval = int(os.getenv("PROBE_POLL_INTERVAL_MS", "500")) / 1000.0
    results_path = Path(os.getenv("RESULTS_PATH", "/results")) / "latency_samples.csv"
    ensure_results_file(results_path)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    last_visible_at = 0
    throughput_window_start = time.time()
    throughput_window_count = 0
    last_strategy = "unknown"
    last_scenario = "unknown"

    conn = pg_connection()
    conn.autocommit = True

    log.info("Probe started — polling every %dms", int(poll_interval * 1000))

    while RUNNING:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT event_id, produced_at, visible_at,
                           strategy, scenario, run_id
                    FROM events
                    WHERE visible_at > %s
                    ORDER BY visible_at ASC
                    LIMIT 1000
                    """,
                    (last_visible_at,),
                )
                rows = cur.fetchall()

            if rows:
                with open(results_path, "a", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle)
                    for event_id, produced_at, visible_at, strategy, scenario, run_id in rows:
                        latency = visible_at - produced_at
                        strategy = strategy or "unknown"
                        scenario = scenario or "unknown"
                        run_id = run_id or "unset"
                        writer.writerow([
                            event_id, produced_at, visible_at, latency,
                            strategy, scenario, run_id,
                        ])

                        last_visible_at = max(last_visible_at, visible_at)
                        throughput_window_count += 1
                        last_strategy = strategy
                        last_scenario = scenario

            time.sleep(poll_interval)

        except psycopg2.OperationalError as exc:
            log.error("Connection lost, reconnecting: %s", exc)
            try:
                conn.close()
            except Exception:
                pass
            conn = pg_connection()
            conn.autocommit = True
        except Exception as exc:  # pylint: disable=broad-except
            log.error("Error while sampling: %s", exc)
            time.sleep(2)

    log.info("Shutting down")
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:  # pylint: disable=broad-except
        log.critical("Probe failed: %s", err)
        sys.exit(1)
