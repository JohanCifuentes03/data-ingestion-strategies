#!/usr/bin/env python3
"""Official benchmark analyzer for thesis figures and validation."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import kruskal, mannwhitneyu

STRATEGY_ORDER = ["batch", "microbatch", "streaming"]
SCENARIO_ORDER = ["low-load", "medium-load", "high-load"]
SCENARIO_TARGET_EPS = {
    "low-load": 2000,
    "medium-load": 10000,
    "high-load": 30000,
}
SAVE_FORMATS = ["png", "pdf"]
STRATEGY_LABELS = {
    "batch": "Batch",
    "microbatch": "Micro-batch",
    "streaming": "Streaming",
}
SCENARIO_LABELS = {
    "low-load": "Carga baja",
    "medium-load": "Carga media",
    "high-load": "Carga alta",
}
STRATEGY_COLORS = {
    "batch": "#2f2f2f",
    "microbatch": "#7a7a7a",
    "streaming": "#c7c7c7",
}
SERIES_COLORS = {
    "primary_light": "#bdbdbd",
    "primary_dark": "#4a4a4a",
    "secondary_light": "#d9d9d9",
    "secondary_dark": "#595959",
}


@dataclass
class ValidationState:
    ok: int = 0
    warn: int = 0
    err: int = 0

    def report_ok(self, msg: str):
        self.ok += 1
        print(f"[OK] {msg}")

    def report_warn(self, msg: str):
        self.warn += 1
        print(f"[WARN] {msg}")

    def report_err(self, msg: str):
        self.err += 1
        print(f"[ERROR] {msg}")


def _sort_by_known(values: list[str], known: list[str]) -> list[str]:
    present = set(values)
    ordered = [v for v in known if v in present]
    tail = sorted(v for v in values if v not in known)
    return ordered + tail


def _save_figure(fig, out_dir: Path, basename: str):
    for ext in SAVE_FORMATS:
        fig.savefig(out_dir / f"{basename}.{ext}", bbox_inches="tight", dpi=300 if ext == "png" else None)
    plt.close(fig)
    print(f"[OK] {basename}.png")


def _load_json_records(results_dir: Path, filename: str) -> pd.DataFrame:
    rows = []
    for path in results_dir.rglob(filename):
        rel = path.relative_to(results_dir).parts
        if len(rel) < 4:
            continue
        strategy, scenario, run_id = rel[0], rel[1], rel[2]
        try:
            with open(path, encoding="utf-8") as handle:
                row = json.load(handle)
            if "strategy" not in row:
                row["strategy"] = strategy
            if "scenario" not in row:
                row["scenario"] = scenario
            if "run_id" not in row:
                row["run_id"] = run_id
            rows.append(row)
        except Exception:
            continue
    return pd.DataFrame(rows)


def load_latency(results_dir: Path) -> pd.DataFrame:
    frames = []
    for csv_path in results_dir.rglob("latency_samples.csv"):
        rel = csv_path.relative_to(results_dir).parts
        if len(rel) < 4:
            continue
        strategy, scenario, run_id = rel[0], rel[1], rel[2]
        df = pd.read_csv(csv_path, on_bad_lines="skip")
        if df.empty:
            continue
        for key, val in [("strategy", strategy), ("scenario", scenario), ("run_id", run_id)]:
            if key not in df.columns:
                df[key] = val
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "latency_ms" in df.columns:
        df["latency_ms"] = pd.to_numeric(df["latency_ms"], errors="coerce")
        df = df.dropna(subset=["latency_ms"])
    for col in ["produced_at", "visible_at"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_prometheus_snapshot(results_dir: Path) -> pd.DataFrame:
    frames = []
    for csv_path in results_dir.rglob("prometheus_snapshot.csv"):
        rel = csv_path.relative_to(results_dir).parts
        if len(rel) < 4:
            continue
        strategy, scenario, run_id = rel[0], rel[1], rel[2]
        df = pd.read_csv(csv_path, on_bad_lines="skip")
        if df.empty:
            continue
        if "strategy" not in df.columns:
            df["strategy"] = strategy
        if "scenario" not in df.columns:
            df["scenario"] = scenario
        if "run_id" not in df.columns:
            df["run_id"] = run_id
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_run_metadata(results_dir: Path) -> pd.DataFrame:
    return _load_json_records(results_dir, "run_metadata.json")


def load_run_summaries(results_dir: Path) -> pd.DataFrame:
    return _load_json_records(results_dir, "run_summary.json")


def load_generator_summaries(results_dir: Path) -> pd.DataFrame:
    return _load_json_records(results_dir, "generator_summary.json")


def load_kafka_lag_timeseries(results_dir: Path) -> pd.DataFrame:
    frames = []
    for csv_path in results_dir.rglob("kafka_lag_timeseries.csv"):
        rel = csv_path.relative_to(results_dir).parts
        if len(rel) < 4:
            continue
        strategy, scenario, run_id = rel[0], rel[1], rel[2]
        df = pd.read_csv(csv_path, on_bad_lines="skip")
        if df.empty:
            continue
        for key, val in [("strategy", strategy), ("scenario", scenario), ("run_id", run_id)]:
            if key not in df.columns:
                df[key] = val
        if "lag" in df.columns:
            df["lag"] = pd.to_numeric(df["lag"], errors="coerce")
        if "lag_source" not in df.columns:
            df["lag_source"] = "unknown"
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _lag_coverage_by_strategy(lag_ts: pd.DataFrame) -> dict[str, bool]:
    coverage = {strategy: False for strategy in STRATEGY_ORDER}
    if lag_ts.empty or "lag_source" not in lag_ts.columns:
        return coverage
    real = lag_ts[lag_ts["lag_source"] == "real_prometheus"]
    for strategy in STRATEGY_ORDER:
        coverage[strategy] = not real[real["strategy"] == strategy].empty
    return coverage


def load_resources_timeseries(results_dir: Path) -> pd.DataFrame:
    frames = []
    for csv_path in results_dir.rglob("resources_timeseries.csv"):
        rel = csv_path.relative_to(results_dir).parts
        if len(rel) < 4:
            continue
        strategy, scenario, run_id = rel[0], rel[1], rel[2]
        df = pd.read_csv(csv_path, on_bad_lines="skip")
        if df.empty:
            continue
        for key, val in [("strategy", strategy), ("scenario", scenario), ("run_id", run_id)]:
            if key not in df.columns:
                df[key] = val
        for col in ["cpu_cores", "mem_rss_bytes"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def apply_scope_filter(df: pd.DataFrame, scope: str) -> pd.DataFrame:
    if df.empty or scope != "official" or "scenario" not in df.columns:
        return df
    return df[df["scenario"].isin(SCENARIO_ORDER)].copy()


def build_run_inventory(results_dir: Path, scope: str) -> pd.DataFrame:
    rows = []
    for lat_path in results_dir.rglob("latency_samples.csv"):
        rel = lat_path.relative_to(results_dir).parts
        if len(rel) < 4:
            continue
        strategy, scenario, run_id = rel[0], rel[1], rel[2]
        if scope == "official" and scenario not in SCENARIO_ORDER:
            continue
        rows.append({"strategy": strategy, "scenario": scenario, "run_id": run_id, "run_dir": str(lat_path.parent)})
    return pd.DataFrame(rows).drop_duplicates() if rows else pd.DataFrame(columns=["strategy", "scenario", "run_id", "run_dir"])


def run_validations(
    results_dir: Path,
    scope: str,
    latency: pd.DataFrame,
    run_metadata: pd.DataFrame,
    run_summary: pd.DataFrame,
    generator_summary: pd.DataFrame,
    lag_ts: pd.DataFrame,
) -> ValidationState:
    state = ValidationState()
    runs = build_run_inventory(results_dir, scope)
    required = ["latency_samples.csv", "run_metadata.json", "run_summary.json", "generator_summary.json"]
    missing = 0
    for row in runs.itertuples(index=False):
        run_dir = Path(row.run_dir)
        for file in required:
            if not (run_dir / file).exists():
                missing += 1
                state.report_warn(f"Missing {file} in {row.strategy}/{row.scenario}/{row.run_id}")
    if missing == 0:
        state.report_ok(f"{len(runs)}/{len(runs)} corridas oficiales validas")

    if not run_summary.empty:
        vis = pd.to_numeric(run_summary.get("visible_events", 0), errors="coerce").fillna(0)
        gen = pd.to_numeric(run_summary.get("generated_events", 0), errors="coerce").fillna(0)
        if bool((vis > gen).any()):
            state.report_err("Hay corridas con visible_events > generated_events")
        else:
            state.report_ok("No hay eventos visibles > generados")

        dr = pd.to_numeric(run_summary.get("delivery_ratio_pct", np.nan), errors="coerce")
        invalid_dr = dr[(dr < 0) | (dr > 100)]
        if invalid_dr.empty:
            state.report_ok("Delivery ratio dentro de [0,100]")
        else:
            state.report_err("Delivery ratio fuera de rango [0,100]")

        if not latency.empty:
            lat_counts = (
                latency.groupby(["strategy", "scenario", "run_id"], observed=True)
                .size()
                .reset_index(name="latency_rows")
            )
            merged = run_summary.merge(lat_counts, on=["strategy", "scenario", "run_id"], how="left")
            vis_vals = pd.to_numeric(merged.get("visible_events", 0), errors="coerce").fillna(0)
            lat_vals = pd.to_numeric(merged.get("latency_rows", 0), errors="coerce").fillna(0)
            if bool((vis_vals != lat_vals).any()):
                state.report_err("visible_events no coincide con latency_samples.csv exportado")
            else:
                state.report_ok("visible_events coincide con latency_samples.csv exportado")

    if not run_metadata.empty and "official_duration_seconds" in run_metadata.columns:
        dvals = pd.to_numeric(run_metadata["official_duration_seconds"], errors="coerce").dropna().unique()
        if len(dvals) == 1 and int(dvals[0]) == 300:
            state.report_ok("Todos los escenarios tienen duracion oficial = 300 s")
        elif len(dvals) == 1:
            state.report_warn(f"Duracion oficial uniforme pero distinta de 300s: {int(dvals[0])}")
        else:
            state.report_err("Duracion oficial no uniforme entre corridas")

    if not run_summary.empty and "generation_duration_seconds" in run_summary.columns:
        gen_dur = pd.to_numeric(run_summary["generation_duration_seconds"], errors="coerce")
        off_dur = pd.to_numeric(run_summary.get("official_duration_seconds", np.nan), errors="coerce")
        mismatch = (gen_dur - off_dur).abs() > 1.0
        if bool(mismatch.fillna(False).any()):
            state.report_err("generation_duration_seconds no coincide con official_duration_seconds")
        else:
            state.report_ok("generation_duration_seconds coincide con la duracion oficial")

    if not run_metadata.empty and "target_eps" in run_metadata.columns:
        ok = True
        for scenario, expected in SCENARIO_TARGET_EPS.items():
            sub = run_metadata[run_metadata["scenario"] == scenario]
            if sub.empty:
                continue
            vals = pd.to_numeric(sub["target_eps"], errors="coerce").dropna()
            if vals.empty or int(round(vals.mode().iloc[0])) != expected:
                ok = False
                break
        if ok:
            state.report_ok("Tasa objetivo correcta por escenario oficial")
        else:
            state.report_warn("Tasa objetivo no coincide para algun escenario")

    if not run_summary.empty:
        target = pd.to_numeric(run_summary.get("target_eps", np.nan), errors="coerce")
        generated = pd.to_numeric(run_summary.get("generated_eps_real", np.nan), errors="coerce")
        low_gen = run_summary[(target > 0) & (generated / target < 0.70)]
        if low_gen.empty:
            state.report_ok("La generacion real no cae por debajo del 70% del objetivo")
        else:
            state.report_warn("Hay corridas donde generated_eps_real < 70% de target_eps")

    lag_coverage = _lag_coverage_by_strategy(lag_ts)
    missing_real = [strategy for strategy, present in lag_coverage.items() if not present]
    if not missing_real:
        state.report_ok("Kafka lag real disponible para todas las estrategias")
    else:
        state.report_warn(
            "Kafka lag real incompleto; sin cobertura para: " + ", ".join(missing_real)
        )

    if not latency.empty and "latency_ms" in latency.columns:
        if bool((latency["latency_ms"] <= 0).any()):
            state.report_err("Se detectaron latencias no positivas")
        else:
            state.report_ok("latency_ms positivo en todas las muestras")

    run_id_ok = True
    for name, df in [
        ("latency", latency),
        ("run_metadata", run_metadata),
        ("run_summary", run_summary),
        ("generator_summary", generator_summary),
    ]:
        if df.empty or "run_id" not in df.columns:
            run_id_ok = False
            state.report_err(f"run_id ausente en {name}")
    if run_id_ok:
        state.report_ok("Todos los run_id estan presentes")

    if not run_metadata.empty and "git_commit" in run_metadata.columns:
        commits = run_metadata["git_commit"].dropna().astype(str).unique().tolist()
        if len(commits) > 1:
            state.report_warn("Se detectaron commits distintos en el mismo analisis")
        else:
            state.report_ok("Mismo git_commit en corridas analizadas")

    return state


def _ordered_scenarios_from_df(df: pd.DataFrame) -> list[str]:
    if df.empty or "scenario" not in df.columns:
        return []
    return _sort_by_known(df["scenario"].dropna().astype(str).unique().tolist(), SCENARIO_ORDER)


def _ordered_strategies_from_df(df: pd.DataFrame) -> list[str]:
    if df.empty or "strategy" not in df.columns:
        return []
    return [strategy for strategy in STRATEGY_ORDER if strategy in set(df["strategy"].dropna().astype(str))]


def _format_strategy(value: str) -> str:
    return STRATEGY_LABELS.get(value, value)


def _format_scenario(value: str) -> str:
    return SCENARIO_LABELS.get(value, value)


def _light_axis_style(ax):
    ax.grid(axis="y", color="#d9d9d9", linewidth=0.8)
    ax.grid(axis="x", visible=False)
    ax.set_axisbelow(True)


def _add_box_stats(ax, x_pos: float, values: np.ndarray):
    if len(values) == 0:
        return
    vals = pd.Series(values).dropna()
    if vals.empty:
        return
    stats = [
        ("Min", vals.min()),
        ("Q1", vals.quantile(0.25)),
        ("Q2", vals.quantile(0.50)),
        ("Q3", vals.quantile(0.75)),
        ("Max", vals.max()),
    ]
    text = "\n".join(f"{label} {value:,.0f}" for label, value in stats)
    ax.text(
        x_pos + 0.34,
        vals.quantile(0.50),
        text,
        ha="left",
        va="center",
        fontsize=6.5,
        linespacing=0.95,
        bbox={"boxstyle": "round,pad=0.18", "facecolor": "white", "edgecolor": "#bdbdbd", "alpha": 0.82},
    )


def _add_box_min_q2_max(ax, x_pos: float, values: np.ndarray):
    vals = pd.Series(values).dropna()
    if vals.empty:
        return
    stats = [
        ("Min", vals.min()),
        ("Q2", vals.quantile(0.50)),
        ("Max", vals.max()),
    ]
    text = "\n".join(f"{label} {value:,.0f}" for label, value in stats)
    ax.text(
        x_pos + 0.34,
        vals.quantile(0.50),
        text,
        ha="left",
        va="center",
        fontsize=6.5,
        linespacing=0.95,
        bbox={"boxstyle": "round,pad=0.18", "facecolor": "white", "edgecolor": "#bdbdbd", "alpha": 0.82},
    )


def _label_bars(ax, bars, values, formatter, offset_ratio: float = 0.01):
    ymax = ax.get_ylim()[1]
    offset = ymax * offset_ratio if ymax > 0 else 0.1
    for bar, value in zip(bars, values):
        if pd.isna(value):
            continue
        y = max(float(bar.get_height()), 0.0) + offset
        ax.text(bar.get_x() + bar.get_width() / 2, y, formatter(value), ha="center", va="bottom", fontsize=8)


def build_official_metrics(
    latency: pd.DataFrame,
    run_metadata: pd.DataFrame,
    run_summary: pd.DataFrame,
    generator_summary: pd.DataFrame,
    lag_ts: pd.DataFrame,
    resources_ts: pd.DataFrame,
    out_dir: Path,
) -> pd.DataFrame:
    key_cols = ["strategy", "scenario", "run_id"]

    frames = [df[key_cols] for df in [run_metadata, run_summary, generator_summary, latency, lag_ts, resources_ts] if not df.empty]
    if not frames:
        metrics = pd.DataFrame(columns=[
            "strategy",
            "scenario",
            "run_id",
            "target_eps",
            "generated_events",
            "visible_events_cutoff",
            "visible_events_final",
            "official_duration_s",
            "generated_eps_real",
            "visible_eps_cutoff",
            "delivery_ratio_cutoff_pct",
            "delivery_ratio_final_pct",
            "pending_visibility_events",
            "time_to_drain_s",
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_p99_ms",
            "cpu_avg_cores",
            "memory_rss_avg_mb",
            "kafka_lag_real_available",
            "kafka_lag_max",
        ])
        metrics.to_csv(out_dir / "official_metrics_summary.csv", index=False)
        print("[OK] official_metrics_summary.csv")
        return metrics

    metrics = pd.concat(frames, ignore_index=True).drop_duplicates()

    if not run_metadata.empty:
        meta = run_metadata[key_cols].copy()
        if "target_eps" in run_metadata.columns:
            meta["target_eps"] = pd.to_numeric(run_metadata["target_eps"], errors="coerce")
        else:
            meta["target_eps"] = np.nan
        if "official_duration_seconds" in run_metadata.columns:
            meta["official_duration_s"] = pd.to_numeric(run_metadata["official_duration_seconds"], errors="coerce")
        else:
            meta["official_duration_s"] = np.nan
        if "generation_end_ts" in run_metadata.columns:
            meta["generation_end_ts"] = pd.to_numeric(run_metadata["generation_end_ts"], errors="coerce")
        else:
            meta["generation_end_ts"] = np.nan
        metrics = metrics.merge(meta, on=key_cols, how="left")
    else:
        metrics["target_eps"] = np.nan
        metrics["official_duration_s"] = np.nan
        metrics["generation_end_ts"] = np.nan

    if not run_summary.empty:
        summary = run_summary[key_cols].copy()
        summary["generated_events_rs"] = pd.to_numeric(run_summary.get("generated_events", np.nan), errors="coerce")
        summary["visible_events_final"] = pd.to_numeric(run_summary.get("visible_events", np.nan), errors="coerce")
        summary["cpu_avg_cores_rs"] = pd.to_numeric(
            run_summary.get("cpu_cores_avg", run_summary.get("cpu_avg_cores", np.nan)),
            errors="coerce",
        )
        summary["memory_rss_avg_mb_rs"] = pd.to_numeric(
            run_summary.get("mem_rss_mb_avg", run_summary.get("memory_rss_avg_mb", np.nan)),
            errors="coerce",
        )
        metrics = metrics.merge(summary, on=key_cols, how="left")
    else:
        metrics["generated_events_rs"] = np.nan
        metrics["visible_events_final"] = np.nan
        metrics["cpu_avg_cores_rs"] = np.nan
        metrics["memory_rss_avg_mb_rs"] = np.nan

    if not generator_summary.empty:
        gen = generator_summary[key_cols].copy()
        gen["generated_events_gs"] = pd.to_numeric(
            generator_summary.get("total_events_generated", generator_summary.get("generated_events", np.nan)),
            errors="coerce",
        )
        metrics = metrics.merge(gen, on=key_cols, how="left")
    else:
        metrics["generated_events_gs"] = np.nan

    metrics["generated_events"] = metrics["generated_events_rs"].combine_first(metrics["generated_events_gs"])

    if not latency.empty:
        lat = latency[key_cols + [col for col in ["visible_at", "latency_ms"] if col in latency.columns]].copy()
        if "visible_at" in lat.columns:
            lat["visible_at"] = pd.to_numeric(lat["visible_at"], errors="coerce")
        if "latency_ms" in lat.columns:
            lat["latency_ms"] = pd.to_numeric(lat["latency_ms"], errors="coerce")

        cutoff_base = metrics[key_cols + ["generation_end_ts"]].copy()
        cutoff_base["generation_end_ms"] = pd.to_numeric(cutoff_base["generation_end_ts"], errors="coerce") * 1000.0

        cutoff_lat = lat.merge(cutoff_base[key_cols + ["generation_end_ms"]], on=key_cols, how="inner")
        cutoff_lat = cutoff_lat.dropna(subset=["visible_at", "generation_end_ms"])
        cutoff = cutoff_lat[cutoff_lat["visible_at"] <= cutoff_lat["generation_end_ms"]]
        visible_cutoff = cutoff.groupby(key_cols, observed=True).size().reset_index(name="visible_events_cutoff")

        visible_last = (
            lat.dropna(subset=["visible_at"])
            .groupby(key_cols, observed=True)
            .agg(visible_events_final_latency=("visible_at", "size"), last_visible_at_ms=("visible_at", "max"))
            .reset_index()
        )
        latency_stats = (
            lat.dropna(subset=["latency_ms"])
            .groupby(key_cols, observed=True)["latency_ms"]
            .agg(
                latency_p50_ms=lambda s: float(s.quantile(0.50)),
                latency_p95_ms=lambda s: float(s.quantile(0.95)),
                latency_p99_ms=lambda s: float(s.quantile(0.99)),
            )
            .reset_index()
        )

        metrics = metrics.merge(visible_cutoff, on=key_cols, how="left")
        metrics = metrics.merge(visible_last, on=key_cols, how="left")
        metrics = metrics.merge(latency_stats, on=key_cols, how="left")
    else:
        metrics["visible_events_cutoff"] = np.nan
        metrics["visible_events_final_latency"] = np.nan
        metrics["last_visible_at_ms"] = np.nan
        metrics["latency_p50_ms"] = np.nan
        metrics["latency_p95_ms"] = np.nan
        metrics["latency_p99_ms"] = np.nan

    metrics["visible_events_cutoff"] = pd.to_numeric(metrics.get("visible_events_cutoff", np.nan), errors="coerce").fillna(0)
    metrics["visible_events_final"] = pd.to_numeric(metrics.get("visible_events_final", np.nan), errors="coerce")
    metrics["visible_events_final"] = metrics["visible_events_final"].combine_first(
        pd.to_numeric(metrics.get("visible_events_final_latency", np.nan), errors="coerce")
    )

    if not resources_ts.empty:
        res = resources_ts[key_cols].copy()
        if "cpu_cores" in resources_ts.columns:
            res["cpu_avg_cores_ts"] = pd.to_numeric(resources_ts["cpu_cores"], errors="coerce")
        else:
            res["cpu_avg_cores_ts"] = np.nan
        if "mem_rss_bytes" in resources_ts.columns:
            res["memory_rss_avg_mb_ts"] = pd.to_numeric(resources_ts["mem_rss_bytes"], errors="coerce") / 1_048_576.0
        else:
            res["memory_rss_avg_mb_ts"] = np.nan
        res = (
            res.groupby(key_cols, observed=True)
            .agg(cpu_avg_cores_ts=("cpu_avg_cores_ts", "mean"), memory_rss_avg_mb_ts=("memory_rss_avg_mb_ts", "mean"))
            .reset_index()
        )
        metrics = metrics.merge(res, on=key_cols, how="left")
    else:
        metrics["cpu_avg_cores_ts"] = np.nan
        metrics["memory_rss_avg_mb_ts"] = np.nan

    metrics["cpu_avg_cores"] = pd.to_numeric(metrics.get("cpu_avg_cores_rs", np.nan), errors="coerce").combine_first(
        pd.to_numeric(metrics.get("cpu_avg_cores_ts", np.nan), errors="coerce")
    )
    metrics["memory_rss_avg_mb"] = pd.to_numeric(
        metrics.get("memory_rss_avg_mb_rs", np.nan), errors="coerce"
    ).combine_first(pd.to_numeric(metrics.get("memory_rss_avg_mb_ts", np.nan), errors="coerce"))

    if not lag_ts.empty:
        lag = lag_ts[key_cols].copy()
        lag["lag"] = pd.to_numeric(lag_ts.get("lag", np.nan), errors="coerce")
        lag["lag_source"] = lag_ts.get("lag_source", "unknown")
        lag_real = lag[lag["lag_source"] == "real_prometheus"]
        lag_agg = (
            lag_real.groupby(key_cols, observed=True)
            .agg(kafka_lag_max=("lag", "max"))
            .reset_index()
        )
        lag_agg["kafka_lag_real_available"] = True
        metrics = metrics.merge(lag_agg, on=key_cols, how="left")
    else:
        metrics["kafka_lag_real_available"] = np.nan
        metrics["kafka_lag_max"] = np.nan

    metrics["kafka_lag_real_available"] = metrics.get("kafka_lag_real_available", False).fillna(False).astype(bool)
    metrics["generated_events"] = pd.to_numeric(metrics["generated_events"], errors="coerce")
    metrics["official_duration_s"] = pd.to_numeric(metrics["official_duration_s"], errors="coerce")
    metrics["visible_events_final"] = pd.to_numeric(metrics["visible_events_final"], errors="coerce")
    metrics["last_visible_at_ms"] = pd.to_numeric(metrics.get("last_visible_at_ms", np.nan), errors="coerce")
    metrics["generation_end_ts"] = pd.to_numeric(metrics["generation_end_ts"], errors="coerce")

    valid_duration = metrics["official_duration_s"] > 0
    valid_generated = metrics["generated_events"] > 0

    metrics["generated_eps_real"] = np.where(
        valid_duration,
        metrics["generated_events"] / metrics["official_duration_s"],
        np.nan,
    )
    metrics["visible_eps_cutoff"] = np.where(
        valid_duration,
        metrics["visible_events_cutoff"] / metrics["official_duration_s"],
        np.nan,
    )
    metrics["delivery_ratio_cutoff_pct"] = np.where(
        valid_generated,
        metrics["visible_events_cutoff"] / metrics["generated_events"] * 100.0,
        np.nan,
    )
    metrics["delivery_ratio_final_pct"] = np.where(
        valid_generated,
        metrics["visible_events_final"] / metrics["generated_events"] * 100.0,
        np.nan,
    )
    metrics["pending_visibility_events"] = (metrics["generated_events"] - metrics["visible_events_cutoff"]).clip(lower=0)

    generation_end_ms = metrics["generation_end_ts"] * 1000.0
    metrics["time_to_drain_s"] = ((metrics["last_visible_at_ms"] - generation_end_ms) / 1000.0).clip(lower=0)

    metrics = metrics[[
        "strategy",
        "scenario",
        "run_id",
        "target_eps",
        "generated_events",
        "visible_events_cutoff",
        "visible_events_final",
        "official_duration_s",
        "generated_eps_real",
        "visible_eps_cutoff",
        "delivery_ratio_cutoff_pct",
        "delivery_ratio_final_pct",
        "pending_visibility_events",
        "time_to_drain_s",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "cpu_avg_cores",
        "memory_rss_avg_mb",
        "kafka_lag_real_available",
        "kafka_lag_max",
    ]].copy()

    metrics["strategy"] = pd.Categorical(metrics["strategy"], categories=STRATEGY_ORDER, ordered=True)
    metrics["scenario"] = metrics["scenario"].astype(str)
    scenario_order = _sort_by_known(metrics["scenario"].dropna().unique().tolist(), SCENARIO_ORDER)
    metrics["scenario_sort"] = pd.Categorical(metrics["scenario"], categories=scenario_order, ordered=True)
    metrics = metrics.sort_values(["scenario_sort", "strategy", "run_id"]).drop(columns=["scenario_sort"]).reset_index(drop=True)
    metrics.to_csv(out_dir / "official_metrics_summary.csv", index=False)
    print("[OK] official_metrics_summary.csv")
    return metrics


def export_latency_summary_table(latency: pd.DataFrame, out_dir: Path):
    if latency.empty or "latency_ms" not in latency.columns:
        print("[WARN] Sin datos de latencia para latency_summary_table.csv")
        return
    rows = []
    for (strategy, scenario), grp in latency.groupby(["strategy", "scenario"], observed=True):
        lat_vals = pd.to_numeric(grp["latency_ms"], errors="coerce").dropna()
        if lat_vals.empty:
            continue
        rows.append(
            {
                "Estrategia": _format_strategy(strategy),
                "Escenario": _format_scenario(scenario),
                "Eventos visibles": int(len(lat_vals)),
                "p50 (ms)": round(float(lat_vals.quantile(0.50)), 2),
                "p95 (ms)": round(float(lat_vals.quantile(0.95)), 2),
                "p99 (ms)": round(float(lat_vals.quantile(0.99)), 2),
                "Mín. (ms)": round(float(lat_vals.min()), 2),
                "Máx. (ms)": round(float(lat_vals.max()), 2),
                "_scenario": scenario,
                "_strategy": strategy,
            }
        )
    if not rows:
        print("[WARN] Sin filas para latency_summary_table.csv")
        return
    table = pd.DataFrame(rows).sort_values(["_scenario", "_strategy"]).drop(columns=["_scenario", "_strategy"])
    table.to_csv(out_dir / "latency_summary_table.csv", index=False)
    print("[OK] latency_summary_table.csv")


def fig_11_1_latency_distribution(latency: pd.DataFrame, out_dir: Path):
    if latency.empty or "latency_ms" not in latency.columns:
        print("[WARN] Sin datos para fig_11_1_latency_distribution")
        return
    latency = latency[latency["latency_ms"] > 0].copy()
    scenarios = _ordered_scenarios_from_df(latency)
    if not scenarios:
        print("[WARN] Sin escenarios para fig_11_1_latency_distribution")
        return
    with plt.style.context("seaborn-v0_8-whitegrid"):
        fig, axes = plt.subplots(1, len(scenarios), figsize=(max(7.5, 4.8 * len(scenarios)), 5.2), sharey=True, squeeze=False)
        axes = axes[0]
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            sub = latency[latency["scenario"] == scenario]
            order = _ordered_strategies_from_df(sub)
            data = [sub[sub["strategy"] == strategy]["latency_ms"].to_numpy() for strategy in order]
            bp = ax.boxplot(data, patch_artist=True, showfliers=False, widths=0.55)
            for patch, strategy in zip(bp["boxes"], order):
                patch.set_facecolor(STRATEGY_COLORS[strategy])
                patch.set_alpha(0.9)
                patch.set_edgecolor("#333333")
            for median in bp["medians"]:
                median.set_color("#111111")
                median.set_linewidth(1.5)
            for whisker in bp["whiskers"]:
                whisker.set_color("#555555")
            for cap in bp["caps"]:
                cap.set_color("#555555")
            for pos, values in enumerate(data, start=1):
                _add_box_stats(ax, float(pos), values)
            ax.set_title(_format_scenario(scenario))
            ax.set_xticks(range(1, len(order) + 1))
            ax.set_xticklabels([_format_strategy(strategy) for strategy in order])
            ax.set_yscale("log")
            _light_axis_style(ax)
            if idx == 0:
                ax.set_ylabel("Latencia de disponibilidad (ms, escala logarítmica)")
        fig.suptitle("Distribución de la latencia de disponibilidad por estrategia y escenario")
        fig.tight_layout(rect=[0, 0, 1, 0.94])
        _save_figure(fig, out_dir, "fig_11_1_latency_distribution")


def _aggregate_metrics(metrics: pd.DataFrame, aggregations: dict[str, str]) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame()
    agg = metrics.groupby(["strategy", "scenario"], observed=True).agg(aggregations).reset_index()
    agg["strategy"] = pd.Categorical(agg["strategy"], categories=STRATEGY_ORDER, ordered=True)
    agg["scenario"] = pd.Categorical(agg["scenario"], categories=SCENARIO_ORDER, ordered=True)
    return agg.sort_values(["scenario", "strategy"]).reset_index(drop=True)


def fig_11_2_official_window_throughput(metrics: pd.DataFrame, out_dir: Path):
    agg = _aggregate_metrics(
        metrics,
        {"generated_eps_real": "mean", "visible_eps_cutoff": "mean", "delivery_ratio_cutoff_pct": "mean"},
    )
    if agg.empty:
        print("[WARN] Sin datos para fig_11_2_official_window_throughput")
        return
    scenarios = _ordered_scenarios_from_df(agg)
    with plt.style.context("seaborn-v0_8-whitegrid"):
        fig, axes = plt.subplots(1, len(scenarios), figsize=(max(8.5, 5.2 * len(scenarios)), 5.4), sharey=True, squeeze=False)
        axes = axes[0]
        all_rates = []
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            sub = agg[agg["scenario"] == scenario].set_index("strategy")
            order = [strategy for strategy in STRATEGY_ORDER if strategy in sub.index]
            x = np.arange(len(order))
            width = 0.34
            produced = [float(sub.at[strategy, "generated_eps_real"]) for strategy in order]
            visible = [float(sub.at[strategy, "visible_eps_cutoff"]) for strategy in order]
            all_rates.extend(produced)
            all_rates.extend(visible)
            ratios = [float(sub.at[strategy, "delivery_ratio_cutoff_pct"]) for strategy in order]
            ax.bar(x - width / 2, produced, width=width, color=SERIES_COLORS["primary_light"], edgecolor="#333333", linewidth=0.4, label="Producido real" if idx == 0 else None)
            visible_bars = ax.bar(x + width / 2, visible, width=width, color=SERIES_COLORS["primary_dark"], edgecolor="#333333", linewidth=0.4, label="Visible en PostgreSQL al corte oficial" if idx == 0 else None)
            ax.set_title(_format_scenario(scenario))
            ax.set_xticks(x)
            ax.set_xticklabels([_format_strategy(strategy) for strategy in order])
            _light_axis_style(ax)
            if idx == 0:
                ax.set_ylabel("Eventos por segundo")
            _label_bars(ax, visible_bars, ratios, lambda v: f"V/G {v:.1f}%", offset_ratio=0.018)
        y_max = max(all_rates) * 1.18 if all_rates else 1
        for ax in axes:
            ax.set_ylim(0, y_max)
        handles, labels = axes[0].get_legend_handles_labels()
        fig.suptitle("Tasa producida y tasa visible en PostgreSQL durante la ventana oficial", y=0.985)
        fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.90), fontsize=10)
        fig.tight_layout(rect=[0, 0, 1, 0.80])
        _save_figure(fig, out_dir, "fig_11_2_official_window_throughput")


def fig_11_3_delivery_ratio_cutoff_vs_drain(metrics: pd.DataFrame, out_dir: Path):
    agg = _aggregate_metrics(
        metrics,
        {"delivery_ratio_cutoff_pct": "mean", "delivery_ratio_final_pct": "mean"},
    )
    if agg.empty:
        print("[WARN] Sin datos para fig_11_3_delivery_ratio_cutoff_vs_drain")
        return
    scenarios = _ordered_scenarios_from_df(agg)
    with plt.style.context("seaborn-v0_8-whitegrid"):
        fig, axes = plt.subplots(1, len(scenarios), figsize=(max(7.5, 4.8 * len(scenarios)), 5.2), sharey=True, squeeze=False)
        axes = axes[0]
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            sub = agg[agg["scenario"] == scenario].set_index("strategy")
            order = [strategy for strategy in STRATEGY_ORDER if strategy in sub.index]
            x = np.arange(len(order))
            width = 0.34
            cutoff_vals = [float(sub.at[strategy, "delivery_ratio_cutoff_pct"]) for strategy in order]
            drain_vals = [float(sub.at[strategy, "delivery_ratio_final_pct"]) for strategy in order]
            bars_cutoff = ax.bar(x - width / 2, cutoff_vals, width=width, color=SERIES_COLORS["secondary_light"], edgecolor="#333333", linewidth=0.4, label="Al corte oficial" if idx == 0 else None)
            bars_drain = ax.bar(x + width / 2, drain_vals, width=width, color=SERIES_COLORS["secondary_dark"], edgecolor="#333333", linewidth=0.4, label="Después del drenaje" if idx == 0 else None)
            ax.set_title(_format_scenario(scenario))
            ax.set_xticks(x)
            ax.set_xticklabels([_format_strategy(strategy) for strategy in order])
            ax.set_ylim(0, 105)
            _light_axis_style(ax)
            if idx == 0:
                ax.set_ylabel("Eventos visibles / eventos producidos (%)")
            _label_bars(ax, bars_cutoff, cutoff_vals, lambda v: f"{v:.1f}%")
            _label_bars(ax, bars_drain, drain_vals, lambda v: f"{v:.1f}%")
        handles, labels = axes[0].get_legend_handles_labels()
        fig.suptitle("Proporción de eventos visibles al corte oficial y después del drenaje", y=0.985)
        fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 0.90), fontsize=10)
        fig.tight_layout(rect=[0, 0, 1, 0.80])
        _save_figure(fig, out_dir, "fig_11_3_delivery_ratio_cutoff_vs_drain")


def fig_11_4_pending_visibility_backlog(metrics: pd.DataFrame, out_dir: Path):
    agg = _aggregate_metrics(metrics, {"pending_visibility_events": "mean"})
    if agg.empty:
        print("[WARN] Sin datos para fig_11_4_pending_visibility_backlog")
        return
    scenarios = _ordered_scenarios_from_df(agg)
    with plt.style.context("seaborn-v0_8-whitegrid"):
        fig, axes = plt.subplots(1, len(scenarios), figsize=(max(7.5, 4.8 * len(scenarios)), 5.1), sharey=False, squeeze=False)
        axes = axes[0]
        all_vals = []
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            sub = agg[agg["scenario"] == scenario].set_index("strategy")
            order = [strategy for strategy in STRATEGY_ORDER if strategy in sub.index]
            x = np.arange(len(order))
            vals = [float(sub.at[strategy, "pending_visibility_events"]) for strategy in order]
            all_vals.extend(vals)
            bars = ax.bar(x, vals, width=0.58, color=[STRATEGY_COLORS[strategy] for strategy in order])
            ax.set_title(_format_scenario(scenario))
            ax.set_xticks(x)
            ax.set_xticklabels([_format_strategy(strategy) for strategy in order])
            _light_axis_style(ax)
            if idx == 0:
                ax.set_ylabel("Eventos pendientes de visibilidad")
            _label_bars(ax, bars, vals, lambda v: f"{v:,.0f}")
        y_max = max(all_vals) * 1.15 if all_vals else 1
        for ax in axes:
            ax.set_ylim(0, y_max)
        fig.suptitle("Acumulación estimada de eventos pendientes de visibilidad al corte oficial")
        fig.tight_layout(rect=[0, 0, 1, 0.94])
        _save_figure(fig, out_dir, "fig_11_4_pending_visibility_backlog")


def fig_11_4b_kafka_consumer_lag_real(metrics: pd.DataFrame, out_dir: Path):
    if metrics.empty:
        return
    lag_data = metrics.dropna(subset=["kafka_lag_max"]).copy()
    expected_pairs = {(strategy, scenario) for strategy in STRATEGY_ORDER for scenario in SCENARIO_ORDER}
    available_pairs = set(
        lag_data[lag_data["kafka_lag_real_available"]][["strategy", "scenario"]].drop_duplicates().itertuples(index=False, name=None)
    )
    if expected_pairs - available_pairs:
        return
    agg = _aggregate_metrics(lag_data, {"kafka_lag_max": "mean"})
    if agg.empty:
        return
    scenarios = _ordered_scenarios_from_df(agg)
    with plt.style.context("seaborn-v0_8-whitegrid"):
        fig, axes = plt.subplots(1, len(scenarios), figsize=(max(7.5, 4.8 * len(scenarios)), 5.1), sharey=True, squeeze=False)
        axes = axes[0]
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            sub = agg[agg["scenario"] == scenario].set_index("strategy")
            order = [strategy for strategy in STRATEGY_ORDER if strategy in sub.index]
            x = np.arange(len(order))
            vals = [float(sub.at[strategy, "kafka_lag_max"]) for strategy in order]
            bars = ax.bar(x, vals, width=0.58, color=[STRATEGY_COLORS[strategy] for strategy in order])
            ax.set_title(_format_scenario(scenario))
            ax.set_xticks(x)
            ax.set_xticklabels([_format_strategy(strategy) for strategy in order])
            ax.set_ylim(bottom=0)
            _light_axis_style(ax)
            if idx == 0:
                ax.set_ylabel("Consumer lag (mensajes)")
            _label_bars(ax, bars, vals, lambda v: f"{v:,.0f}")
        fig.suptitle("Rezago real de consumidores Kafka por estrategia y escenario", y=0.985)
        fig.tight_layout(rect=[0, 0, 1, 0.88])
        _save_figure(fig, out_dir, "fig_11_4b_kafka_consumer_lag_real")


def fig_11_5_drain_time(metrics: pd.DataFrame, out_dir: Path):
    agg = _aggregate_metrics(metrics, {"time_to_drain_s": "mean"})
    if agg.empty:
        print("[WARN] Sin datos para fig_11_5_drain_time")
        return
    scenarios = _ordered_scenarios_from_df(agg)
    with plt.style.context("seaborn-v0_8-whitegrid"):
        fig, axes = plt.subplots(1, len(scenarios), figsize=(max(7.5, 4.8 * len(scenarios)), 5.1), sharey=False, squeeze=False)
        axes = axes[0]
        all_vals = []
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            sub = agg[agg["scenario"] == scenario].set_index("strategy")
            order = [strategy for strategy in STRATEGY_ORDER if strategy in sub.index]
            x = np.arange(len(order))
            vals = [float(sub.at[strategy, "time_to_drain_s"]) for strategy in order]
            all_vals.extend(vals)
            bars = ax.bar(x, vals, width=0.58, color=[STRATEGY_COLORS[strategy] for strategy in order])
            ax.set_title(_format_scenario(scenario))
            ax.set_xticks(x)
            ax.set_xticklabels([_format_strategy(strategy) for strategy in order])
            _light_axis_style(ax)
            if idx == 0:
                ax.set_ylabel("Tiempo de drenaje (s)")
            _label_bars(ax, bars, vals, lambda v: f"{v:.1f}")
        y_max = max(all_vals) * 1.15 if all_vals else 1
        for ax in axes:
            ax.set_ylim(0, y_max)
        fig.suptitle("Tiempo de drenaje posterior a la finalización de la generación")
        fig.tight_layout(rect=[0, 0, 1, 0.94])
        _save_figure(fig, out_dir, "fig_11_5_drain_time")


def fig_11_6_compute_resource_usage(metrics: pd.DataFrame, out_dir: Path):
    agg = _aggregate_metrics(metrics, {"cpu_avg_cores": "mean", "memory_rss_avg_mb": "mean"})
    if agg.empty:
        print("[WARN] Sin datos para fig_11_6_compute_resource_usage")
        return
    scenarios = _ordered_scenarios_from_df(agg)
    x = np.arange(len(scenarios))
    width = 0.22
    with plt.style.context("seaborn-v0_8-whitegrid"):
        fig, axes = plt.subplots(1, 2, figsize=(11.5, 5.1), squeeze=False)
        axes = axes[0]
        for idx, strategy in enumerate(STRATEGY_ORDER):
            sub = agg[agg["strategy"] == strategy].set_index("scenario")
            cpu_vals = [float(sub.at[scenario, "cpu_avg_cores"]) if scenario in sub.index else 0.0 for scenario in scenarios]
            mem_vals = [float(sub.at[scenario, "memory_rss_avg_mb"]) if scenario in sub.index else 0.0 for scenario in scenarios]
            positions = x + (idx - 1) * width
            axes[0].bar(positions, cpu_vals, width=width, color=STRATEGY_COLORS[strategy], label=_format_strategy(strategy))
            axes[1].bar(positions, mem_vals, width=width, color=STRATEGY_COLORS[strategy], label=_format_strategy(strategy))
        axes[0].set_title("CPU promedio (cores)")
        axes[1].set_title("Memoria RSS promedio (MB)")
        for ax in axes:
            ax.set_xticks(x)
            ax.set_xticklabels([_format_scenario(scenario) for scenario in scenarios])
            ax.set_ylim(bottom=0)
            _light_axis_style(ax)
        axes[0].set_ylabel("CPU promedio (cores)")
        axes[1].set_ylabel("Memoria RSS promedio (MB)")
        handles, labels = axes[0].get_legend_handles_labels()
        fig.suptitle("Uso promedio de CPU y memoria en el nodo de cómputo", y=0.985)
        fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.90), fontsize=10)
        fig.tight_layout(rect=[0, 0, 1, 0.80])
        _save_figure(fig, out_dir, "fig_11_6_compute_resource_usage")


def export_statistical_tests(latency: pd.DataFrame, out_dir: Path):
    if latency.empty or "latency_ms" not in latency.columns:
        print("[WARN] Sin datos de latencia para pruebas estadisticas")
        return
    rows = []
    pair_names = [
        ("batch", "microbatch", "batch_vs_microbatch_p_adj"),
        ("batch", "streaming", "batch_vs_streaming_p_adj"),
        ("microbatch", "streaming", "microbatch_vs_streaming_p_adj"),
    ]
    for scenario in _sort_by_known(latency["scenario"].unique().tolist(), SCENARIO_ORDER):
        sub = latency[latency["scenario"] == scenario]
        groups = {}
        for strategy in STRATEGY_ORDER:
            vals = pd.to_numeric(sub[sub["strategy"] == strategy]["latency_ms"], errors="coerce").dropna().to_numpy()
            if len(vals) > 0:
                groups[strategy] = vals
        if len(groups) < 2:
            continue
        k_stat, k_p = (np.nan, np.nan)
        if len(groups) >= 3:
            k_stat, k_p = kruskal(*(groups[s] for s in STRATEGY_ORDER if s in groups))
        row = {
            "escenario": scenario,
            "kruskal_H": float(k_stat) if pd.notna(k_stat) else np.nan,
            "kruskal_p": float(k_p) if pd.notna(k_p) else np.nan,
            "significant": "si" if pd.notna(k_p) and k_p < 0.05 else "no",
        }
        for left, right, col in pair_names:
            if left in groups and right in groups:
                _, p = mannwhitneyu(groups[left], groups[right], alternative="two-sided")
                row[col] = float(min(1.0, p * 3.0))
            else:
                row[col] = np.nan
        rows.append(row)
    if not rows:
        print("[WARN] Sin filas para statistical_tests.csv")
        return
    pd.DataFrame(rows).to_csv(out_dir / "statistical_tests.csv", index=False)
    print("[OK] statistical_tests.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Official thesis analyzer")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--output", default=None)
    parser.add_argument("--scope", choices=["official", "all"], default="official")
    parser.add_argument("--validate", action="store_true")
    return parser.parse_args()



CYCLIC_RATES = [1000, 5000, 10000, 500, 3000]
CYCLIC_SEGMENT_SECONDS = 30
CYCLIC_CYCLE_SECONDS = CYCLIC_SEGMENT_SECONDS * len(CYCLIC_RATES)


def _cyclic_target_rate(elapsed_s: float) -> int:
    idx = int((elapsed_s % CYCLIC_CYCLE_SECONDS) // CYCLIC_SEGMENT_SECONDS)
    return CYCLIC_RATES[idx]


def _advanced_keys(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame(columns=["strategy", "scenario", "run_id"])
    mask = metrics["scenario"].astype(str).str.contains("cyclic|bursty", regex=True, na=False)
    return metrics.loc[mask, ["strategy", "scenario", "run_id"]].drop_duplicates()


def build_advanced_timeseries(
    latency: pd.DataFrame,
    metrics: pd.DataFrame,
    generator_summary: pd.DataFrame,
    out_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    keys = _advanced_keys(metrics)
    if keys.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty

    produced_rows: list[pd.DataFrame] = []
    visible_rows: list[pd.DataFrame] = []
    latency_rows: list[pd.DataFrame] = []
    backlog_rows: list[pd.DataFrame] = []

    for _, key in keys.iterrows():
        strategy = str(key["strategy"])
        scenario = str(key["scenario"])
        run_id = str(key["run_id"])
        gen = generator_summary[
            (generator_summary["strategy"].astype(str) == strategy)
            & (generator_summary["scenario"].astype(str) == scenario)
            & (generator_summary["run_id"].astype(str) == run_id)
        ]
        run_lat = latency[
            (latency["strategy"].astype(str) == strategy)
            & (latency["scenario"].astype(str) == scenario)
            & (latency["run_id"].astype(str) == run_id)
        ].copy()
        if gen.empty or run_lat.empty:
            continue

        gen_row = gen.iloc[0]
        duration_s = int(round(float(gen_row.get("generation_duration_seconds", 0))))
        if duration_s <= 0:
            duration_s = int(round(float(metrics.loc[
                (metrics["strategy"].astype(str) == strategy)
                & (metrics["scenario"].astype(str) == scenario)
                & (metrics["run_id"].astype(str) == run_id),
                "official_duration_s",
            ].iloc[0])))
        generated_events = int(float(gen_row.get("generated_events", 0)))
        elapsed = np.arange(0, duration_s, dtype=int)
        target_rates = np.array([_cyclic_target_rate(float(t)) for t in elapsed], dtype=float)
        target_total = float(target_rates.sum()) if len(target_rates) else 0.0
        scale = generated_events / target_total if target_total > 0 else 0.0
        produced_counts = target_rates * scale
        produced = pd.DataFrame({
            "elapsed_s": elapsed,
            "target_rate": target_rates,
            "produced_count_window": produced_counts,
            "produced_eps": produced_counts,
            "cumulative_produced": np.cumsum(produced_counts),
            "strategy": strategy,
            "scenario": scenario,
            "run_id": run_id,
        })
        produced_rows.append(produced)

        start_ms = float(pd.to_numeric(run_lat["produced_at"], errors="coerce").min())
        run_lat["visible_elapsed_s"] = np.floor((pd.to_numeric(run_lat["visible_at"], errors="coerce") - start_ms) / 1000.0).astype("Int64")
        run_lat["produced_elapsed_s"] = np.floor((pd.to_numeric(run_lat["produced_at"], errors="coerce") - start_ms) / 1000.0).astype("Int64")

        visible_counts = (
            run_lat.dropna(subset=["visible_elapsed_s"])
            .groupby("visible_elapsed_s", observed=True)
            .size()
            .rename("visible_count_window")
            .reset_index()
            .rename(columns={"visible_elapsed_s": "elapsed_s"})
        )
        max_visible_s = int(max(duration_s, int(visible_counts["elapsed_s"].max()) if not visible_counts.empty else duration_s))
        visible_axis = pd.DataFrame({"elapsed_s": np.arange(0, max_visible_s + 1, dtype=int)})
        visible = visible_axis.merge(visible_counts, on="elapsed_s", how="left").fillna({"visible_count_window": 0})
        visible["visible_eps"] = visible["visible_count_window"]
        visible["cumulative_visible"] = visible["visible_count_window"].cumsum()
        visible["strategy"] = strategy
        visible["scenario"] = scenario
        visible["run_id"] = run_id
        visible_rows.append(visible)

        lat_bucket = run_lat.dropna(subset=["produced_elapsed_s", "latency_ms"]).copy()
        lat_bucket["bucket_start_s"] = (lat_bucket["produced_elapsed_s"].astype(int) // 10) * 10
        lat_ts = (
            lat_bucket.groupby("bucket_start_s", observed=True)["latency_ms"]
            .quantile([0.5, 0.95, 0.99])
            .unstack()
            .reset_index()
            .rename(columns={0.5: "p50_ms", 0.95: "p95_ms", 0.99: "p99_ms"})
        )
        lat_ts["bucket_end_s"] = lat_ts["bucket_start_s"] + 10
        lat_ts["strategy"] = strategy
        lat_ts["scenario"] = scenario
        lat_ts["run_id"] = run_id
        latency_rows.append(lat_ts[["bucket_start_s", "bucket_end_s", "p50_ms", "p95_ms", "p99_ms", "strategy", "scenario", "run_id"]])

        backlog = visible[["elapsed_s", "cumulative_visible"]].merge(
            produced[["elapsed_s", "cumulative_produced"]], on="elapsed_s", how="left"
        )
        backlog["cumulative_produced"] = backlog["cumulative_produced"].ffill().fillna(0).clip(upper=generated_events)
        backlog["observable_backlog"] = (backlog["cumulative_produced"] - backlog["cumulative_visible"]).clip(lower=0)
        backlog["strategy"] = strategy
        backlog["scenario"] = scenario
        backlog["run_id"] = run_id
        backlog_rows.append(backlog[["elapsed_s", "cumulative_produced", "cumulative_visible", "observable_backlog", "strategy", "scenario", "run_id"]])

    produced_df = pd.concat(produced_rows, ignore_index=True) if produced_rows else pd.DataFrame()
    visible_df = pd.concat(visible_rows, ignore_index=True) if visible_rows else pd.DataFrame()
    latency_df = pd.concat(latency_rows, ignore_index=True) if latency_rows else pd.DataFrame()
    backlog_df = pd.concat(backlog_rows, ignore_index=True) if backlog_rows else pd.DataFrame()

    if not produced_df.empty:
        produced_df.to_csv(out_dir / "generator_rate_timeline.csv", index=False)
    if not visible_df.empty:
        visible_df.to_csv(out_dir / "sink_visibility_timeline.csv", index=False)
    if not latency_df.empty:
        latency_df.to_csv(out_dir / "latency_timeseries.csv", index=False)
    if not backlog_df.empty:
        backlog_df.to_csv(out_dir / "backlog_timeseries.csv", index=False)
    return produced_df, visible_df, latency_df, backlog_df


def _aggregate_rate_window(df: pd.DataFrame, value_cols: list[str], window_s: int = 5) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["window_start_s"] = (pd.to_numeric(tmp["elapsed_s"], errors="coerce") // window_s * window_s).astype(int)
    agg = tmp.groupby(["strategy", "scenario", "run_id", "window_start_s"], observed=True)[value_cols].mean().reset_index()
    return agg.rename(columns={"window_start_s": "elapsed_s"})


def fig_a1_cyclic_response_timeseries(produced: pd.DataFrame, visible: pd.DataFrame, out_dir: Path):
    if produced.empty or visible.empty:
        print("[WARN] Sin series temporales para fig_a1_cyclic_response_timeseries")
        return
    produced_plot = _aggregate_rate_window(produced, ["target_rate", "produced_eps"], window_s=5)
    visible_plot = _aggregate_rate_window(visible, ["visible_eps"], window_s=5)
    strategies = [s for s in STRATEGY_ORDER if s in set(produced_plot["strategy"].astype(str))]
    fig, axes = plt.subplots(len(strategies), 1, figsize=(10.5, max(3.4, 2.9 * len(strategies))), sharex=True, squeeze=False)
    for idx, strategy in enumerate(strategies):
        ax = axes[idx][0]
        p = produced_plot[produced_plot["strategy"] == strategy]
        v = visible_plot[visible_plot["strategy"] == strategy]
        ax.step(p["elapsed_s"], p["target_rate"], where="post", color="#111111", linewidth=1.35, label="Tasa objetivo")
        ax.plot(p["elapsed_s"], p["produced_eps"], color="#555555", linewidth=1.2, linestyle="--", label="Tasa producida")
        ax.plot(v["elapsed_s"], v["visible_eps"], color="#8f8f8f", linewidth=1.25, label="Tasa visible")
        ax.axvline(300, color="#d0d0d0", linewidth=0.8, linestyle=":")
        ax.set_title(STRATEGY_LABELS.get(strategy, strategy.capitalize()), loc="left", fontsize=10)
        ax.set_ylabel("Eventos/s")
        ax.grid(axis="y", alpha=0.22)
        if idx == 0:
            ax.legend(ncol=3, fontsize=8, bbox_to_anchor=(1.0, 1.35), loc="upper right")
    axes[-1][0].set_xlabel("Tiempo transcurrido (s)")
    fig.suptitle("Respuesta temporal bajo carga cíclica interregional", fontsize=13, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    _save_figure(fig, out_dir, "fig_a1_cyclic_response_timeseries")


def fig_a2_observable_backlog_timeseries(backlog: pd.DataFrame, out_dir: Path):
    if backlog.empty:
        print("[WARN] Sin backlog temporal para fig_a2_observable_backlog_timeseries")
        return
    fig, axes = plt.subplots(2, 1, figsize=(10, 7.2), sharex=True, squeeze=False)
    ax_all, ax_zoom = axes[0][0], axes[1][0]
    strategies = [s for s in STRATEGY_ORDER if s in set(backlog["strategy"].astype(str))]
    for strategy in strategies:
        sub = backlog[backlog["strategy"] == strategy]
        label = STRATEGY_LABELS.get(strategy, strategy)
        color = STRATEGY_COLORS.get(strategy, "#999")
        ax_all.plot(sub["elapsed_s"], sub["observable_backlog"], label=label, color=color, linewidth=1.25)
        if strategy != "batch":
            ax_zoom.plot(sub["elapsed_s"], sub["observable_backlog"], label=label, color=color, linewidth=1.35)
    ax_all.set_title("Todas las estrategias", loc="left", fontsize=10)
    ax_zoom.set_title("Detalle Micro-batch vs Streaming", loc="left", fontsize=10)
    for ax in (ax_all, ax_zoom):
        ax.axvline(300, color="#d0d0d0", linewidth=0.8, linestyle=":")
        ax.set_ylabel("Eventos pendientes")
        ax.grid(axis="y", alpha=0.22)
        ax.legend(fontsize=8)
    ax_zoom.set_xlabel("Tiempo transcurrido (s)")
    fig.suptitle("Evolución del backlog observable bajo carga cíclica interregional", fontsize=13, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    _save_figure(fig, out_dir, "fig_a2_observable_backlog_timeseries")

def fig_a3_latency_percentiles_timeseries(latency_ts: pd.DataFrame, out_dir: Path):
    if latency_ts.empty:
        print("[WARN] Sin latencia temporal para fig_a3_latency_percentiles_timeseries")
        return
    strategies = [s for s in STRATEGY_ORDER if s in set(latency_ts["strategy"].astype(str))]
    fig, axes = plt.subplots(len(strategies), 1, figsize=(10, max(3.2, 2.8 * len(strategies))), sharex=True, squeeze=False)
    for idx, strategy in enumerate(strategies):
        ax = axes[idx][0]
        sub = latency_ts[latency_ts["strategy"] == strategy]
        ax.plot(sub["bucket_start_s"], sub["p50_ms"], label="p50", color="#4a4a4a", linewidth=1.2)
        ax.plot(sub["bucket_start_s"], sub["p95_ms"], label="p95", color="#9a9a9a", linewidth=1.2)
        ax.set_yscale("log")
        ax.set_title(STRATEGY_LABELS.get(strategy, strategy.capitalize()), loc="left", fontsize=10)
        ax.set_ylabel("Latencia (ms)")
        ax.grid(axis="y", alpha=0.2)
        if idx == 0:
            ax.legend(ncol=2, fontsize=8, bbox_to_anchor=(1.0, 1.35), loc="upper right")
    axes[-1][0].set_xlabel("Tiempo de producción del evento (s)")
    fig.suptitle("Evolución temporal de la latencia bajo carga cíclica interregional", fontsize=13, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    _save_figure(fig, out_dir, "fig_a3_latency_percentiles_timeseries")


def fig_a4_latency_distribution_cyclic(latency: pd.DataFrame, metrics: pd.DataFrame, out_dir: Path):
    adv_scenarios = metrics["scenario"].astype(str).unique()
    adv_latency = latency[latency["scenario"].astype(str).isin(adv_scenarios)]
    if adv_latency.empty:
        return

    groups = []
    labels = []
    order = []
    for strategy in STRATEGY_ORDER:
        grp = adv_latency[adv_latency["strategy"].astype(str) == strategy]
        if grp.empty:
            continue
        values = pd.to_numeric(grp["latency_ms"], errors="coerce").dropna()
        if values.empty:
            continue
        groups.append(values.values)
        labels.append(STRATEGY_LABELS.get(strategy, strategy.capitalize()))
        order.append(strategy)

    if not groups:
        return

    with plt.style.context("seaborn-v0_8-whitegrid"):
        fig, ax = plt.subplots(figsize=(7.5, 5.2))
        bp = ax.boxplot(groups, patch_artist=True, showfliers=False, widths=0.55)
        for patch, strategy in zip(bp["boxes"], order):
            patch.set_facecolor(STRATEGY_COLORS[strategy])
            patch.set_alpha(0.9)
            patch.set_edgecolor("#333333")
        for median in bp["medians"]:
            median.set_color("#111111")
            median.set_linewidth(1.5)
        for whisker in bp["whiskers"]:
            whisker.set_color("#555555")
        for cap in bp["caps"]:
            cap.set_color("#555555")
        for pos, values in enumerate(groups, start=1):
            _add_box_min_q2_max(ax, float(pos), values)
        ax.set_xticks(range(1, len(labels) + 1))
        ax.set_xticklabels(labels)
        ax.set_yscale("log")
        ax.set_ylabel("Latencia de disponibilidad (ms, escala logarítmica)")
        ax.set_title("Distribución global de latencia bajo carga cíclica interregional")
        _light_axis_style(ax)
        fig.tight_layout(rect=[0, 0, 1, 0.94])
        _save_figure(fig, out_dir, "fig_a4_latency_distribution_cyclic")

def main():
    args = parse_args()
    results_dir = Path(args.results_dir).resolve()
    out_dir = Path(args.output).resolve() if args.output else results_dir / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)

    latency = apply_scope_filter(load_latency(results_dir), args.scope)
    _ = apply_scope_filter(load_prometheus_snapshot(results_dir), args.scope)
    run_metadata = apply_scope_filter(load_run_metadata(results_dir), args.scope)
    run_summary = apply_scope_filter(load_run_summaries(results_dir), args.scope)
    generator_summary = apply_scope_filter(load_generator_summaries(results_dir), args.scope)
    lag_ts = apply_scope_filter(load_kafka_lag_timeseries(results_dir), args.scope)
    resources_ts = apply_scope_filter(load_resources_timeseries(results_dir), args.scope)

    if latency.empty and run_summary.empty:
        print("[ERROR] No benchmark data found")
        sys.exit(1)

    val_state = run_validations(results_dir, args.scope, latency, run_metadata, run_summary, generator_summary, lag_ts)
    if args.validate and val_state.err > 0:
        sys.exit(1)

    metrics = build_official_metrics(latency, run_metadata, run_summary, generator_summary, lag_ts, resources_ts, out_dir)

    export_latency_summary_table(latency, out_dir)

    official_metrics = metrics[metrics["scenario"].isin(SCENARIO_ORDER)].copy()
    official_latency = latency[latency["scenario"].isin(SCENARIO_ORDER)].copy()
    if not official_metrics.empty:
        fig_11_1_latency_distribution(official_latency, out_dir)
        fig_11_2_official_window_throughput(official_metrics, out_dir)
        fig_11_3_delivery_ratio_cutoff_vs_drain(official_metrics, out_dir)
        fig_11_4_pending_visibility_backlog(official_metrics, out_dir)
        fig_11_4b_kafka_consumer_lag_real(official_metrics, out_dir)
        fig_11_5_drain_time(official_metrics, out_dir)
        fig_11_6_compute_resource_usage(official_metrics, out_dir)
        export_statistical_tests(official_latency, out_dir)
    else:
        print("[INFO] Sin escenarios oficiales; omitiendo figuras oficiales fig_11_*")

    advanced_scenarios = [s for s in metrics["scenario"].dropna().astype(str).unique() if "cyclic" in s or "bursty" in s]
    if advanced_scenarios:
        adv_metrics = metrics[metrics["scenario"].isin(advanced_scenarios)]
        produced_ts, visible_ts, latency_ts, backlog_ts = build_advanced_timeseries(
            latency, adv_metrics, generator_summary, out_dir
        )
        fig_a1_cyclic_response_timeseries(produced_ts, visible_ts, out_dir)
        fig_a2_observable_backlog_timeseries(backlog_ts, out_dir)
        fig_a4_latency_distribution_cyclic(latency, adv_metrics, out_dir)

    print(f"\n[INFO] Done. Output: {out_dir}")


if __name__ == "__main__":
    main()
