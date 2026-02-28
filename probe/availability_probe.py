import csv
import os
import signal
import sys
import time
from pathlib import Path

import psycopg2
from prometheus_client import Counter, Histogram, Gauge, start_http_server

LATENCY_HISTOGRAM = Histogram(
    "probe_latency",
    "Latency between production and visibility",
    buckets=(5, 10, 25, 50, 75, 100, 250, 500, 1000, 2000, 5000, 10000),
)
VISIBLE_EVENTS = Counter("probe_visible_events_total", "Events observed in sink")
ERROR_COUNTER = Counter("probe_errors_total", "Errors while probing")
LAST_VISIBLE_GAUGE = Gauge("probe_last_visible_at_ms", "Latest visible timestamp seen")


def pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "benchmark"),
        password=os.getenv("POSTGRES_PASSWORD", "benchmark"),
        dbname=os.getenv("POSTGRES_DB", "benchmark"),
    )


def ensure_results_file(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["event_id", "produced_at", "visible_at", "latency_ms"])
    return path


RUNNING = True


def handle_signal(signum, frame):  # pylint: disable=unused-argument
    global RUNNING
    RUNNING = False


def main():
    prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8001"))
    start_http_server(prometheus_port)

    poll_interval = int(os.getenv("PROBE_POLL_INTERVAL_MS", "500")) / 1000.0
    results_path = Path(os.getenv("RESULTS_PATH", "/results")) / "latency_samples.csv"
    ensure_results_file(results_path)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    last_visible_at = 0

    with pg_connection() as conn:
        conn.autocommit = True
        while RUNNING:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT event_id, produced_at, visible_at
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
                        for event_id, produced_at, visible_at in rows:
                            latency = visible_at - produced_at
                            LATENCY_HISTOGRAM.observe(latency)
                            VISIBLE_EVENTS.inc()
                            LAST_VISIBLE_GAUGE.set(visible_at)
                            writer.writerow([event_id, produced_at, visible_at, latency])
                            last_visible_at = max(last_visible_at, visible_at)
                time.sleep(poll_interval)
            except Exception as exc:  # pylint: disable=broad-except
                ERROR_COUNTER.inc()
                print(f"[probe] Error while sampling: {exc}")
                time.sleep(2)

    print("[probe] Shutting down")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:  # pylint: disable=broad-except
        print(f"Probe failed: {err}")
        sys.exit(1)
