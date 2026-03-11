#!/usr/bin/env python3
"""
analyze.py  —  Análisis estadístico y generación de gráficas para la tesis
===========================================================================
Lee latency_samples.csv y prometheus_snapshot.csv de results/ y genera
15 gráficas de calidad de publicación con tests estadísticos rigurosos.

Mejoras sobre la versión anterior:
  • Filtro de warmup (30 s) por run en estrategias streaming/microbatch
  • Violin + Box combinado (Chart 01)  →  revela densidad real
  • CDF en escala logarítmica (Chart 02) →  3 estrategias visibles
  • Throughput E2E vs Escritura al Sink (Chart 04b)
  • Tabla resumen: añade IQR, CV%, Min, Max (Chart 06)
  • Serie temporal facetada × escenario × estrategia (Chart 07)
  • Heatmap de escalabilidad p95 (Chart 13)
  • Ranking table objetivo (Chart 14)

Uso:
    python analysis/analyze.py
    python analysis/analyze.py --results-dir ../results --output ../results/figures
"""

import argparse
import sys
from pathlib import Path

# Force UTF-8 output on Windows to avoid codec errors with ✓, etc.
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import scipy.stats as stats
import seaborn as sns
from matplotlib.patches import Patch

# ── Style ────────────────────────────────────────────────────────────
sns.set_theme(
    style="whitegrid",
    font_scale=1.15,
    rc={
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "font.family": "sans-serif",
        "axes.titleweight": "bold",
        "axes.spines.top": False,
        "axes.spines.right": False,
    },
)

PALETTE = {
    "batch":      "#2196F3",
    "microbatch": "#FF9800",
    "streaming":  "#4CAF50",
}
STRATEGY_ORDER = ["batch", "microbatch", "streaming"]
STRATEGY_LABELS = {
    "batch":      "Batch\n(Spark)",
    "microbatch": "Micro-batch\n(Spark SS)",
    "streaming":  "Streaming\n(Flink)",
}
SCENARIO_ORDER = ["low-load", "medium-load", "high-load", "burst",
                  "extreme-load", "mixed-payload"]

FIG_W = 5          # width per scenario subplot
WARMUP_MS = 30_000  # 30 s warmup exclusion per run (non-batch)


# ════════════════════════════════════════════════════════════════════
# DATA LOADING
# ════════════════════════════════════════════════════════════════════
def load_latency(results_dir: Path) -> pd.DataFrame:
    """Walk results/<strategy>/<scenario>/<run_N>/latency_samples.csv"""
    frames = []
    for csv_path in results_dir.rglob("latency_samples.csv"):
        parts = csv_path.relative_to(results_dir).parts
        if len(parts) < 4:
            continue
        strategy, scenario, run_dir = parts[0], parts[1], parts[2]
        df = pd.read_csv(csv_path, on_bad_lines="skip")
        if "latency_ms" not in df.columns or df.empty:
            continue
        df["strategy"] = strategy
        df["scenario"] = scenario
        df["run_id"]   = run_dir
        frames.append(df)

    if not frames:
        print("[ERROR] Sin archivos latency_samples.csv con datos.")
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    combined["latency_ms"] = pd.to_numeric(combined["latency_ms"], errors="coerce")
    combined = combined.dropna(subset=["latency_ms"])
    combined = combined[combined["latency_ms"] > 0]

    _print_summary("latency", combined)
    return combined


def load_resources(results_dir: Path) -> pd.DataFrame:
    """Walk results/<strategy>/<scenario>/<run>/prometheus_snapshot.csv"""
    frames = []
    for csv_path in results_dir.rglob("prometheus_snapshot.csv"):
        parts = csv_path.relative_to(results_dir).parts
        if len(parts) < 4:
            continue
        df = pd.read_csv(csv_path, on_bad_lines="skip")
        if df.empty:
            continue
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["value"] = pd.to_numeric(combined["value"], errors="coerce")
    return combined


def _print_summary(kind: str, df: pd.DataFrame):
    strats = sorted(df["strategy"].unique())
    scens  = sorted(df["scenario"].unique())
    print(f"[INFO] {kind}: {len(df):,} registros | estrategias={strats} | escenarios={scens}")


def _sort_scenarios(scenarios):
    known   = [s for s in SCENARIO_ORDER if s in scenarios]
    unknown = sorted(s for s in scenarios if s not in SCENARIO_ORDER)
    return known + unknown


def filter_warmup(df: pd.DataFrame, warmup_ms: int = WARMUP_MS) -> pd.DataFrame:
    """
    Exclude latency samples produced during the warmup period of each run.

    For streaming and microbatch, removes events with produced_at within the
    first `warmup_ms` milliseconds of each run start.  Batch is exempt because
    its events are all produced *before* the Spark job runs — the warmup is
    structurally incorporated in the accumulation phase.
    """
    if df.empty:
        return df
    parts = []
    for (strategy, scenario, run_id), grp in df.groupby(
        ["strategy", "scenario", "run_id"], observed=True
    ):
        if strategy == "batch":
            parts.append(grp)
        else:
            t0 = grp["produced_at"].min()
            parts.append(grp[grp["produced_at"] >= t0 + warmup_ms])

    combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=df.columns)
    removed  = len(df) - len(combined)
    if removed > 0:
        print(
            f"[INFO] Warmup filter: removed {removed:,} samples "
            f"(first {warmup_ms/1000:.0f} s per run, non-batch only)"
        )
    return combined


# ════════════════════════════════════════════════════════════════════
# STATISTICAL TESTS
# ════════════════════════════════════════════════════════════════════
def run_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per scenario:
      - Kruskal-Wallis H test across all 3 strategies
      - Dunn pairwise post-hoc with Bonferroni correction
    """
    from itertools import combinations

    records = []
    for scenario in _sort_scenarios(df["scenario"].unique()):
        sub = df[df["scenario"] == scenario]
        groups = {
            s: sub[sub["strategy"] == s]["latency_ms"].values
            for s in STRATEGY_ORDER
            if s in sub["strategy"].unique()
        }
        if len(groups) < 2:
            continue

        kw_stat, kw_p = stats.kruskal(*groups.values())

        pairs = list(combinations(groups.keys(), 2))
        bonf_factor = len(pairs)
        pairwise = {}
        for a, b in pairs:
            _, p = stats.mannwhitneyu(groups[a], groups[b], alternative="two-sided")
            p_adj = min(p * bonf_factor, 1.0)
            pairwise[f"{a}_vs_{b}_p_adj"] = round(p_adj, 6)

        rec = {
            "scenario":    scenario,
            "kruskal_H":   round(kw_stat, 4),
            "kruskal_p":   round(kw_p, 6),
            "significant": "✓" if kw_p < 0.05 else "✗",
        }
        rec.update(pairwise)
        records.append(rec)

    return pd.DataFrame(records)


# ════════════════════════════════════════════════════════════════════
# CHART 01 — Violin + Boxplot combinado
# ════════════════════════════════════════════════════════════════════
def chart_boxplot(df: pd.DataFrame, out: Path):
    """Violin + narrow boxplot overlay — shows full distribution shape."""
    scenarios = _sort_scenarios(df["scenario"].unique())
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(FIG_W * n, 6), sharey=True, squeeze=False)
    axes = axes[0]

    for i, scenario in enumerate(scenarios):
        sub = df[df["scenario"] == scenario]
        order = [s for s in STRATEGY_ORDER if s in sub["strategy"].unique()]

        # Violin — shows density
        sns.violinplot(
            data=sub, x="strategy", y="latency_ms", order=order,
            palette=PALETTE, ax=axes[i], inner=None, cut=0,
            hue="strategy", legend=False, linewidth=0.8, alpha=0.65,
        )
        # Narrow boxplot overlay
        sns.boxplot(
            data=sub, x="strategy", y="latency_ms", order=order,
            ax=axes[i], showfliers=False, width=0.12, linewidth=1.4,
            boxprops=dict(facecolor="white", zorder=3),
            medianprops=dict(color="#E53935", linewidth=2.5, zorder=4),
            whiskerprops=dict(linewidth=1.2, color="#555"),
            capprops=dict(linewidth=1.2, color="#555"),
            hue="strategy", legend=False,
        )
        axes[i].set_title(scenario, fontsize=11, pad=8)
        axes[i].set_xlabel("")
        axes[i].set_xticks(range(len(order)))
        axes[i].set_xticklabels(
            [STRATEGY_LABELS.get(s, s) for s in order], fontsize=9
        )
        if i == 0:
            axes[i].set_ylabel("Latencia de disponibilidad (ms)")
        else:
            axes[i].set_ylabel("")
        axes[i].yaxis.set_major_formatter(ticker.FuncFormatter(
            lambda x, _: f"{x:,.0f}"
        ))

    fig.suptitle(
        "Distribución de Latencia por Estrategia y Escenario\n"
        "(violín = densidad de probabilidad;  línea roja = mediana)",
        fontsize=13,
    )
    fig.tight_layout()
    fig.savefig(out / "01_violin_boxplot_latencia.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 01_violin_boxplot_latencia.png")


# ════════════════════════════════════════════════════════════════════
# CHART 02 — CDF en escala logarítmica
# ════════════════════════════════════════════════════════════════════
def chart_cdf(df: pd.DataFrame, out: Path):
    scenarios = _sort_scenarios(df["scenario"].unique())
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(FIG_W * n, 5), sharey=True, squeeze=False)
    axes = axes[0]

    for i, scenario in enumerate(scenarios):
        sub = df[df["scenario"] == scenario]
        for strat in STRATEGY_ORDER:
            s = sub[sub["strategy"] == strat]["latency_ms"].sort_values()
            if len(s) == 0:
                continue
            cdf = s.rank(method="max") / len(s)
            axes[i].plot(s, cdf, label=STRATEGY_LABELS.get(strat, strat),
                         color=PALETTE.get(strat), linewidth=1.8)
        axes[i].axhline(0.95, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        axes[i].axhline(0.99, color="gray", linestyle=":",  linewidth=0.8, alpha=0.7)
        axes[i].set_title(scenario, fontsize=11)
        axes[i].set_xlabel("Latencia (ms) — escala log")
        axes[i].set_xscale("log")
        axes[i].xaxis.set_major_formatter(
            ticker.FuncFormatter(lambda x, _: f"{x:,.0f}")
        )
        if i == 0:
            axes[i].set_ylabel("CDF")
        axes[i].legend(loc="lower right", fontsize=8)
        axes[i].set_ylim(0, 1.05)

    fig.suptitle(
        "Función de Distribución Acumulada (CDF) de Latencia — Escala Logarítmica\n"
        "Las líneas punteadas marcan p95 y p99",
        fontsize=13,
    )
    fig.tight_layout()
    fig.savefig(out / "02_cdf_latencia.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 02_cdf_latencia.png")


# ════════════════════════════════════════════════════════════════════
# CHART 03 — Barras de percentiles (p50 / p95 / p99)
# ════════════════════════════════════════════════════════════════════
def chart_percentile_bars(df: pd.DataFrame, out: Path):
    records = []
    for (strategy, scenario), grp in df.groupby(["strategy", "scenario"]):
        lat = grp["latency_ms"]
        records.append({
            "strategy": strategy,
            "scenario": scenario,
            "p50":  lat.quantile(0.50),
            "p95":  lat.quantile(0.95),
            "p99":  lat.quantile(0.99),
        })
    pct_df = pd.DataFrame(records)

    scenarios = _sort_scenarios(pct_df["scenario"].unique())
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(FIG_W * n, 5), sharey=True, squeeze=False)
    axes = axes[0]

    for i, scenario in enumerate(scenarios):
        sub = pct_df[pct_df["scenario"] == scenario]
        order = [s for s in STRATEGY_ORDER if s in sub["strategy"].values]
        sub = sub.set_index("strategy").loc[order]
        x = np.arange(len(order))
        w = 0.22

        axes[i].bar(x - w, sub["p50"], w, label="p50", color="#66BB6A", zorder=3)
        axes[i].bar(x,      sub["p95"], w, label="p95", color="#FFA726", zorder=3)
        axes[i].bar(x + w,  sub["p99"], w, label="p99", color="#EF5350", zorder=3)

        axes[i].set_xticks(x)
        axes[i].set_xticklabels(
            [STRATEGY_LABELS.get(s, s) for s in order], fontsize=9
        )
        axes[i].set_title(scenario, fontsize=11)
        if i == 0:
            axes[i].set_ylabel("Latencia (ms)")
        axes[i].legend(fontsize=9, framealpha=0.8)
        axes[i].yaxis.set_major_formatter(
            ticker.FuncFormatter(lambda x, _: f"{x:,.0f}")
        )

    fig.suptitle("Percentiles de Latencia (p50, p95, p99)", fontsize=14)
    fig.tight_layout()
    fig.savefig(out / "03_percentiles_barras.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 03_percentiles_barras.png")


# ════════════════════════════════════════════════════════════════════
# CHART 04 — Throughput End-to-End
# ════════════════════════════════════════════════════════════════════
def chart_throughput(df: pd.DataFrame, out: Path):
    records = []
    for (strategy, scenario, run_id), grp in df.groupby(["strategy", "scenario", "run_id"]):
        dur = grp["visible_at"].max() - grp["produced_at"].min()
        if dur <= 0:
            continue
        records.append({
            "strategy":        strategy,
            "scenario":        scenario,
            "run_id":          run_id,
            "throughput_eps":  len(grp) / (dur / 1000.0),
        })
    tp_df = pd.DataFrame(records)
    if tp_df.empty:
        return

    scenarios = _sort_scenarios(tp_df["scenario"].unique())
    order = [s for s in STRATEGY_ORDER if s in tp_df["strategy"].unique()]

    fig, ax = plt.subplots(figsize=(max(9, len(scenarios) * 3), 6))
    sns.barplot(
        data=tp_df,
        x="scenario", y="throughput_eps", hue="strategy",
        order=scenarios, hue_order=order,
        palette=PALETTE, ax=ax,
        errorbar="sd", capsize=0.08, err_kws={"linewidth": 1.5},
    )
    ax.set_ylabel("Throughput End-to-End (eventos/segundo)")
    ax.set_xlabel("Escenario")
    ax.set_title(
        "Throughput E2E por Estrategia y Escenario\n"
        "(N eventos / Δt desde produced_at.min hasta visible_at.max)",
        fontsize=13,
    )
    ax.legend(title="Estrategia", title_fontsize=10)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    fig.tight_layout()
    fig.savefig(out / "04_throughput.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 04_throughput.png")


# ════════════════════════════════════════════════════════════════════
# CHART 04b — Throughput dual: E2E vs Escritura al Sink
# ════════════════════════════════════════════════════════════════════
def chart_throughput_dual(df: pd.DataFrame, out: Path):
    """
    Compares two throughput definitions:
    - E2E:   N / (visible_at.max - produced_at.min)   → penalises batch accumulation
    - Write: N / (visible_at.max - visible_at.min)    → measures active sink write rate
    """
    records = []
    for (strategy, scenario, run_id), grp in df.groupby(["strategy", "scenario", "run_id"]):
        e2e_dur   = grp["visible_at"].max() - grp["produced_at"].min()
        write_dur = grp["visible_at"].max() - grp["visible_at"].min()
        e2e_tput   = len(grp) / (e2e_dur   / 1000.0) if e2e_dur   > 0    else 0
        write_tput = len(grp) / (write_dur / 1000.0) if write_dur > 500  else np.nan
        records.append({
            "strategy": strategy, "scenario": scenario, "run_id": run_id,
            "e2e_tput": e2e_tput, "write_tput": write_tput,
        })
    tp_df = pd.DataFrame(records)
    if tp_df.empty:
        return

    scenarios = _sort_scenarios(tp_df["scenario"].unique())
    order = [s for s in STRATEGY_ORDER if s in tp_df["strategy"].unique()]

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    meta = [
        ("e2e_tput",   "Throughput End-to-End (E2E)",
         "N / (visible_at.max − produced_at.min)"),
        ("write_tput", "Throughput de Escritura al Sink",
         "N / (visible_at.max − visible_at.min)  [NaN si Δt ≤ 500 ms]"),
    ]
    for ax, (col, title, note) in zip(axes, meta):
        data = tp_df[tp_df[col].notna()].copy()
        if data.empty:
            ax.set_visible(False)
            continue
        sns.barplot(
            data=data, x="scenario", y=col, hue="strategy",
            order=scenarios, hue_order=order,
            palette=PALETTE, ax=ax,
            errorbar="sd", capsize=0.08,
        )
        ax.set_title(f"{title}\n({note})", fontsize=10)
        ax.set_ylabel("Eventos / segundo")
        ax.set_xlabel("Escenario")
        ax.legend(title="Estrategia", fontsize=9)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    fig.suptitle(
        "Comparación de Throughput: End-to-End vs Escritura al Sink",
        fontsize=13, fontweight="bold",
    )
    fig.tight_layout()
    fig.savefig(out / "04b_throughput_dual.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 04b_throughput_dual.png")


# ════════════════════════════════════════════════════════════════════
# CHART 05 — Estabilidad entre repeticiones
# ════════════════════════════════════════════════════════════════════
def chart_stability(df: pd.DataFrame, out: Path):
    scenarios  = _sort_scenarios(df["scenario"].unique())
    strategies = [s for s in STRATEGY_ORDER if s in df["strategy"].unique()]

    fig, axes = plt.subplots(
        len(strategies), len(scenarios),
        figsize=(4 * len(scenarios), 4 * len(strategies)),
        squeeze=False, sharey="row",
    )

    for r, strategy in enumerate(strategies):
        for c, scenario in enumerate(scenarios):
            ax  = axes[r][c]
            sub = df[(df["strategy"] == strategy) & (df["scenario"] == scenario)]
            if sub.empty:
                ax.set_visible(False)
                continue
            runs = sorted(sub["run_id"].unique())
            sns.boxplot(
                data=sub, x="run_id", y="latency_ms", order=runs,
                color=PALETTE.get(strategy, "#999"),
                ax=ax, showfliers=False, width=0.5, linewidth=1.0,
            )
            if r == 0:
                ax.set_title(scenario, fontsize=10)
            if c == 0:
                ax.set_ylabel(f"{strategy}\nLatencia (ms)", fontsize=9)
            else:
                ax.set_ylabel("")
            ax.set_xlabel("")
            ax.tick_params(axis="x", rotation=45, labelsize=8)

    fig.suptitle("Estabilidad entre Repeticiones por Estrategia y Escenario",
                 fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out / "05_estabilidad_runs.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 05_estabilidad_runs.png")


# ════════════════════════════════════════════════════════════════════
# CHART 06 — Tabla resumen enriquecida (CSV + PNG)
# ════════════════════════════════════════════════════════════════════
def chart_summary_table(df: pd.DataFrame, stats_df: pd.DataFrame, out: Path):
    records = []
    for (strategy, scenario), grp in df.groupby(["strategy", "scenario"]):
        lat  = grp["latency_ms"]
        dur  = grp["visible_at"].max() - grp["produced_at"].min()
        tput = len(grp) / (dur / 1000.0) if dur > 0 else 0

        kw_p = "N/A"
        if not stats_df.empty and scenario in stats_df["scenario"].values:
            row  = stats_df[stats_df["scenario"] == scenario].iloc[0]
            kw_p = f"{row['kruskal_p']:.4f} {row['significant']}"

        cv = (lat.std() / lat.mean() * 100) if lat.mean() > 0 else float("nan")

        records.append({
            "Estrategia":        strategy,
            "Escenario":         scenario,
            "N eventos":         f"{len(grp):,}",
            "Runs":              grp["run_id"].nunique(),
            "Min (ms)":          f"{lat.min():,.0f}",
            "p50 (ms)":          f"{lat.quantile(0.50):,.1f}",
            "p95 (ms)":          f"{lat.quantile(0.95):,.1f}",
            "p99 (ms)":          f"{lat.quantile(0.99):,.1f}",
            "Max (ms)":          f"{lat.max():,.0f}",
            "Media (ms)":        f"{lat.mean():,.1f}",
            "IQR (ms)":          f"{lat.quantile(0.75) - lat.quantile(0.25):,.1f}",
            "CV (%)":            f"{cv:,.1f}" if not np.isnan(cv) else "N/A",
            "Throughput (ev/s)": f"{tput:,.0f}",
            "p-valor KW":        kw_p,
        })

    summary = pd.DataFrame(records)
    csv_path = out / "06_tabla_resumen.csv"
    summary.to_csv(csv_path, index=False)

    fig, ax = plt.subplots(figsize=(22, max(2, 0.55 * len(summary) + 1.8)))
    ax.axis("off")
    table = ax.table(
        cellText=summary.values,
        colLabels=summary.columns,
        cellLoc="center", loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(6.5)
    table.scale(1, 1.5)

    for j in range(len(summary.columns)):
        table[0, j].set_facecolor("#1A237E")
        table[0, j].set_text_props(color="white", fontweight="bold")
    for i in range(len(summary)):
        bg = "#E8EAF6" if i % 2 == 0 else "white"
        for j in range(len(summary.columns)):
            table[i + 1, j].set_facecolor(bg)

    fig.suptitle("Tabla Resumen de Resultados del Experimento (post-warmup filter)",
                 fontsize=13, fontweight="bold", y=0.98)
    fig.tight_layout()
    fig.savefig(out / "06_tabla_resumen.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 06_tabla_resumen.csv + png")


# ════════════════════════════════════════════════════════════════════
# CHART 07 — Latencia temporal facetada (todos los escenarios)
# ════════════════════════════════════════════════════════════════════
def chart_latency_over_time(df: pd.DataFrame, out: Path):
    """Rolling p50+band[p95] for every available (scenario × strategy)."""
    if "produced_at" not in df.columns:
        return

    scenarios  = _sort_scenarios(df["scenario"].unique())
    strategies = [s for s in STRATEGY_ORDER if s in df["strategy"].unique()]
    n_sc = len(scenarios)

    fig, axes = plt.subplots(
        1, n_sc,
        figsize=(max(7, FIG_W * n_sc), 5),
        squeeze=False,
    )
    axes = axes[0]

    for col_i, scenario in enumerate(scenarios):
        ax = axes[col_i]
        for strategy in strategies:
            s = df[
                (df["scenario"] == scenario) & (df["strategy"] == strategy)
            ].copy()
            if s.empty:
                continue
            best_run = s.groupby("run_id").size().idxmax()
            s = s[s["run_id"] == best_run].sort_values("produced_at")

            t0 = s["produced_at"].min()
            s["elapsed_s"]  = (s["produced_at"] - t0) / 1000.0
            s["bucket_s"]   = (s["elapsed_s"] // 30) * 30

            roll = s.groupby("bucket_s")["latency_ms"].agg(
                median="median", p95=lambda x: x.quantile(0.95)
            ).reset_index()

            ax.plot(
                roll["bucket_s"], roll["median"],
                label=STRATEGY_LABELS.get(strategy, strategy),
                color=PALETTE[strategy], linewidth=2,
            )
            ax.fill_between(
                roll["bucket_s"], roll["median"], roll["p95"],
                alpha=0.15, color=PALETTE[strategy],
            )

        ax.set_title(scenario, fontsize=10)
        ax.set_xlabel("Tiempo transcurrido (s)")
        if col_i == 0:
            ax.set_ylabel("Latencia (ms)")
        ax.legend(fontsize=8)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    fig.suptitle(
        "Evolución Temporal de Latencia por Escenario\n"
        "(banda = p50 a p95; ventana 30 s; run con más eventos)",
        fontsize=13,
    )
    fig.tight_layout()
    fig.savefig(out / "07_latencia_temporal.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 07_latencia_temporal.png")


# ════════════════════════════════════════════════════════════════════
# CHART 08 — Eficiencia de recursos (latencia vs. CPU)
# ════════════════════════════════════════════════════════════════════
def chart_resource_efficiency(df: pd.DataFrame, res_df: pd.DataFrame, out: Path):
    if res_df.empty:
        _missing_chart("08_eficiencia_recursos.png", "No prometheus_snapshot.csv encontrado")
        return

    cpu_df = res_df[
        (res_df["metric"] == "cpu_usage_rate") &
        (res_df["labels"].str.contains("spark|flink", case=False, na=False))
    ].copy()

    if cpu_df.empty:
        _missing_chart("08_eficiencia_recursos.png", "Sin métricas CPU en snapshots")
        return

    cpu_agg = (
        cpu_df.groupby(["strategy", "scenario", "run_id"])["value"]
        .mean()
        .reset_index()
        .rename(columns={"value": "avg_cpu"})
    )

    lat_agg = (
        df.groupby(["strategy", "scenario", "run_id"])["latency_ms"]
        .quantile(0.95)
        .reset_index()
        .rename(columns={"latency_ms": "p95_ms"})
    )

    merged = pd.merge(lat_agg, cpu_agg, on=["strategy", "scenario", "run_id"], how="inner")
    if merged.empty:
        _missing_chart("08_eficiencia_recursos.png", "Sin datos cruzados latencia/CPU")
        return

    scenarios = _sort_scenarios(merged["scenario"].unique())
    markers   = ["o", "s", "D", "^", "v", "P"]

    fig, ax = plt.subplots(figsize=(9, 6))
    for i, scenario in enumerate(scenarios):
        s = merged[merged["scenario"] == scenario]
        for strategy in STRATEGY_ORDER:
            pts = s[s["strategy"] == strategy]
            ax.scatter(pts["avg_cpu"], pts["p95_ms"],
                       color=PALETTE.get(strategy, "#999"),
                       marker=markers[i % len(markers)],
                       s=90, alpha=0.85, zorder=3)

    legend_handles = [
        Patch(color=PALETTE[s], label=STRATEGY_LABELS.get(s, s))
        for s in STRATEGY_ORDER if s in merged["strategy"].unique()
    ]
    ax.legend(handles=legend_handles, title="Estrategia", fontsize=9)
    ax.set_xlabel("CPU promedio (cores equivalentes)")
    ax.set_ylabel("Latencia p95 (ms)")
    ax.set_title("Eficiencia de Recursos: Latencia p95 vs. CPU Promedio", fontsize=13)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    fig.tight_layout()
    fig.savefig(out / "08_eficiencia_recursos.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 08_eficiencia_recursos.png")


# ════════════════════════════════════════════════════════════════════
# CHART 09 — Kafka consumer lag
# ════════════════════════════════════════════════════════════════════
def chart_kafka_lag(res_df: pd.DataFrame, out: Path):
    if res_df.empty:
        _missing_chart("09_kafka_lag.png", "Sin prometheus_snapshot.csv")
        return

    lag_df = res_df[res_df["metric"] == "kafka_consumer_lag_max"].copy()
    if lag_df.empty:
        _missing_chart("09_kafka_lag.png", "Métrica kafka_consumer_lag_max no encontrada")
        return

    lag_df = lag_df[lag_df["value"].notna() & (lag_df["value"] > 0)]
    if lag_df.empty:
        _missing_chart("09_kafka_lag.png", "kafka_consumer_lag_max = 0 en todos los runs")
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    scenarios = _sort_scenarios(lag_df["scenario"].unique())
    order     = [s for s in STRATEGY_ORDER if s in lag_df["strategy"].unique()]

    sns.barplot(
        data=lag_df,
        x="scenario", y="value", hue="strategy",
        order=scenarios, hue_order=order,
        palette=PALETTE, ax=ax, errorbar="sd", capsize=0.08,
    )
    ax.set_ylabel("Consumer Lag máximo (mensajes)")
    ax.set_xlabel("Escenario")
    ax.set_title("Kafka Consumer Lag Máximo por Estrategia y Escenario", fontsize=13)
    ax.legend(title="Estrategia", fontsize=9)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    fig.tight_layout()
    fig.savefig(out / "09_kafka_lag.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 09_kafka_lag.png")


# ════════════════════════════════════════════════════════════════════
# CHART 10 — Tasa de errores
# ════════════════════════════════════════════════════════════════════
def chart_error_rate(res_df: pd.DataFrame, out: Path):
    if res_df.empty:
        _missing_chart("10_tasa_errores.png", "Sin prometheus_snapshot.csv")
        return

    err_df = res_df[res_df["metric"].isin(
        ["generator_error_rate_eps", "probe_error_rate_eps"]
    )].copy()
    if err_df.empty:
        _missing_chart("10_tasa_errores.png", "Sin métricas de errores")
        return

    err_df["source"] = err_df["metric"].map({
        "generator_error_rate_eps": "Generador",
        "probe_error_rate_eps":     "Probe",
    })

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=False)
    scenarios = _sort_scenarios(err_df["scenario"].unique())
    order     = [s for s in STRATEGY_ORDER if s in err_df["strategy"].unique()]

    for ax, source in zip(axes, ["Generador", "Probe"]):
        sub = err_df[err_df["source"] == source]
        if sub.empty:
            ax.set_visible(False)
            continue
        sns.barplot(
            data=sub, x="scenario", y="value", hue="strategy",
            order=scenarios, hue_order=order,
            palette=PALETTE, ax=ax, errorbar="sd", capsize=0.08,
        )
        ax.set_title(f"Tasa de Errores — {source}", fontsize=11)
        ax.set_ylabel("Errores / segundo")
        ax.set_xlabel("Escenario")
        ax.legend(title="Estrategia", fontsize=8)

    fig.suptitle("Tasa de Errores por Estrategia y Fuente", fontsize=13)
    fig.tight_layout()
    fig.savefig(out / "10_tasa_errores.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 10_tasa_errores.png")


# ════════════════════════════════════════════════════════════════════
# CHART 11 — Significancia estadística (tabla visual)
# ════════════════════════════════════════════════════════════════════
def chart_significance_table(stats_df: pd.DataFrame, out: Path):
    if stats_df.empty:
        _missing_chart("11_significancia.png", "Sin datos suficientes para pruebas estadísticas")
        return

    cols = ["scenario", "kruskal_H", "kruskal_p", "significant"]
    pair_cols = [c for c in stats_df.columns if "_vs_" in c]
    display_df = stats_df[cols + pair_cols].copy()
    display_df.columns = [
        "Escenario", "H (KW)", "p-valor", "¿Sig?"
    ] + [c.replace("_p_adj", " (Bonf.)") for c in pair_cols]

    fig, ax = plt.subplots(figsize=(max(12, len(display_df.columns) * 2),
                                    max(3, 0.6 * len(display_df) + 2)))
    ax.axis("off")
    table = ax.table(
        cellText=display_df.values,
        colLabels=display_df.columns,
        cellLoc="center", loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.6)

    for j in range(len(display_df.columns)):
        table[0, j].set_facecolor("#1A237E")
        table[0, j].set_text_props(color="white", fontweight="bold")
    for i, row in enumerate(display_df.itertuples()):
        bg = "#E8F5E9" if row[4] == "✓" else "#FFEBEE"
        for j in range(len(display_df.columns)):
            table[i + 1, j].set_facecolor(bg)

    fig.suptitle(
        "Pruebas de Significancia Estadística\n"
        "(Kruskal-Wallis + Mann-Whitney U con corrección Bonferroni)",
        fontsize=12, fontweight="bold",
    )
    fig.tight_layout()
    fig.savefig(out / "11_significancia.png", bbox_inches="tight")
    plt.close(fig)

    stats_df.to_csv(out / "11_significancia.csv", index=False)
    print("  [OK] 11_significancia.png + csv")


# ════════════════════════════════════════════════════════════════════
# CHART 12 — Radar chart (multi-KPI holístico)
# ════════════════════════════════════════════════════════════════════
def chart_radar(df: pd.DataFrame, res_df: pd.DataFrame, out: Path):
    records = {}
    for strategy in STRATEGY_ORDER:
        sub = df[df["strategy"] == strategy]
        if sub.empty:
            continue
        dur  = sub["visible_at"].max() - sub["produced_at"].min()
        tput = len(sub) / (dur / 1000.0) if dur > 0 else 0
        records[strategy] = {
            "p50_ms":   sub["latency_ms"].quantile(0.50),
            "p99_ms":   sub["latency_ms"].quantile(0.99),
            "tput_eps": tput,
            "cpu":      np.nan,
            "err_rate": np.nan,
            "lag":      np.nan,
        }

    if not res_df.empty:
        for strategy in records:
            cpu_sub = res_df[
                (res_df["strategy"] == strategy) &
                (res_df["metric"] == "cpu_usage_rate") &
                (res_df["labels"].str.contains("spark|flink", case=False, na=False))
            ]
            if not cpu_sub.empty:
                records[strategy]["cpu"] = cpu_sub["value"].mean()

            err_sub = res_df[
                (res_df["strategy"] == strategy) &
                (res_df["metric"] == "generator_error_rate_eps")
            ]
            if not err_sub.empty:
                records[strategy]["err_rate"] = err_sub["value"].mean()

            lag_sub = res_df[
                (res_df["strategy"] == strategy) &
                (res_df["metric"] == "kafka_consumer_lag_max")
            ]
            if not lag_sub.empty:
                records[strategy]["lag"] = lag_sub["value"].mean()

    if not records:
        return

    kpis = ["p50_ms", "p99_ms", "tput_eps", "cpu", "err_rate", "lag"]
    kpi_labels = [
        "Latencia p50\n(↓ mejor)", "Latencia p99\n(↓ mejor)",
        "Throughput\n(↑ mejor)", "CPU uso\n(↓ mejor)",
        "Tasa error\n(↓ mejor)", "Kafka lag\n(↓ mejor)",
    ]
    inverted = [True, True, False, True, True, True]

    mat  = pd.DataFrame(records).T[kpis].fillna(pd.DataFrame(records).T[kpis].mean())
    norm = pd.DataFrame(index=mat.index, columns=kpis, dtype=float)
    for j, kpi in enumerate(kpis):
        col = mat[kpi]
        mn, mx = col.min(), col.max()
        if mx == mn:
            norm[kpi] = 0.5
        else:
            scaled = (col - mn) / (mx - mn)
            norm[kpi] = 1 - scaled if inverted[j] else scaled

    num_vars = len(kpis)
    angles   = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist() + [0]

    fig, ax = plt.subplots(figsize=(7, 7), subplot_kw={"polar": True})
    for strategy in norm.index:
        vals = norm.loc[strategy].tolist() + [norm.loc[strategy, kpis[0]]]
        ax.plot(angles, vals, color=PALETTE.get(strategy, "#999"),
                linewidth=2, label=STRATEGY_LABELS.get(strategy, strategy))
        ax.fill(angles, vals, color=PALETTE.get(strategy, "#999"), alpha=0.12)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(kpi_labels, size=9)
    ax.set_yticks([0.25, 0.50, 0.75, 1.0])
    ax.set_yticklabels(["0.25", "0.50", "0.75", "1.0"], size=7)
    ax.set_ylim(0, 1)
    ax.set_title("Comparación Multi-KPI por Estrategia\n(normalizado — mayor=mejor en todos los ejes)",
                 size=12, pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.15), fontsize=9)
    fig.tight_layout()
    fig.savefig(out / "12_radar_multikpi.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 12_radar_multikpi.png")


# ════════════════════════════════════════════════════════════════════
# CHART 13 — Heatmap de escalabilidad (latencia p95)
# ════════════════════════════════════════════════════════════════════
def chart_heatmap(df: pd.DataFrame, out: Path):
    """
    Heatmap: rows = strategies, cols = scenarios, cells = p95 latency.
    Annotated with human-readable values (ms or s).
    """
    records = []
    for (strategy, scenario), grp in df.groupby(["strategy", "scenario"]):
        records.append({
            "strategy": strategy,
            "scenario": scenario,
            "p95_ms":   grp["latency_ms"].quantile(0.95),
        })
    if not records:
        return

    heat_df = pd.DataFrame(records)
    pivot   = heat_df.pivot(index="strategy", columns="scenario", values="p95_ms")

    row_order = [s for s in STRATEGY_ORDER if s in pivot.index]
    col_order = [c for c in _sort_scenarios(list(pivot.columns)) if c in pivot.columns]
    pivot = pivot.loc[row_order, col_order]

    def fmt(x):
        if np.isnan(x):
            return "N/A"
        return f"{x/1000:.1f} s" if x >= 1_000 else f"{x:.0f} ms"

    annot = pivot.map(fmt)  # pandas >= 2.1: applymap → map

    fig, ax = plt.subplots(figsize=(max(8, len(col_order) * 2.2), len(row_order) * 1.6 + 2))
    sns.heatmap(
        pivot, annot=annot, fmt="", ax=ax,
        cmap="YlOrRd", linewidths=0.5, linecolor="#ddd",
        cbar_kws={"label": "Latencia p95 (ms)", "shrink": 0.8},
    )
    ax.set_title(
        "Heatmap de Escalabilidad — Latencia p95 por Estrategia y Escenario\n"
        "(escala de color: rojo = peor latencia)",
        fontsize=12, fontweight="bold",
    )
    ax.set_ylabel("")
    ax.set_xlabel("")
    ax.set_yticklabels(
        [STRATEGY_LABELS.get(t.get_text(), t.get_text()).replace("\n", " ")
         for t in ax.get_yticklabels()],
        rotation=0, fontsize=10,
    )
    fig.tight_layout()
    fig.savefig(out / "13_heatmap_escalabilidad.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 13_heatmap_escalabilidad.png")


# ════════════════════════════════════════════════════════════════════
# CHART 14 — Ranking table objetivo
# ════════════════════════════════════════════════════════════════════
def chart_ranking_table(df: pd.DataFrame, out: Path):
    """
    Objective ranking table: ranks each strategy per KPI (1 = best).
    Score = sum of ranks across p50, p99, throughput E2E.
    Lower score = better overall balance.
    """
    agg = []
    for (strategy, scenario), grp in df.groupby(["strategy", "scenario"]):
        dur  = grp["visible_at"].max() - grp["produced_at"].min()
        tput = len(grp) / (dur / 1000.0) if dur > 0 else 0
        agg.append({
            "strategy": strategy, "scenario": scenario,
            "p50_ms":   grp["latency_ms"].quantile(0.50),
            "p99_ms":   grp["latency_ms"].quantile(0.99),
            "tput_eps": tput,
        })
    if not agg:
        return

    agg_df  = pd.DataFrame(agg)
    records = []
    for scenario in _sort_scenarios(agg_df["scenario"].unique()):
        sub = agg_df[agg_df["scenario"] == scenario].copy()
        sub["rank_p50"]  = sub["p50_ms"].rank(ascending=True).astype(int)
        sub["rank_p99"]  = sub["p99_ms"].rank(ascending=True).astype(int)
        sub["rank_tput"] = sub["tput_eps"].rank(ascending=False).astype(int)
        sub["score"]     = sub["rank_p50"] + sub["rank_p99"] + sub["rank_tput"]
        for _, row in sub.iterrows():
            records.append({
                "Escenario":             scenario,
                "Estrategia":            row["strategy"],
                "Rank p50 ↓":            int(row["rank_p50"]),
                "Rank p99 ↓":            int(row["rank_p99"]),
                "Rank Throughput ↑":     int(row["rank_tput"]),
                "Score (menor=mejor)":   int(row["score"]),
                "p50 (ms)":              f"{row['p50_ms']:,.0f}",
                "p99 (ms)":              f"{row['p99_ms']:,.0f}",
                "Throughput (ev/s)":     f"{row['tput_eps']:,.0f}",
            })

    rank_df = pd.DataFrame(records)
    rank_df.to_csv(out / "14_ranking_table.csv", index=False)

    fig, ax = plt.subplots(figsize=(18, max(3.5, 0.55 * len(rank_df) + 2.5)))
    ax.axis("off")
    table = ax.table(
        cellText=rank_df.values,
        colLabels=rank_df.columns,
        cellLoc="center", loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.7)

    for j in range(len(rank_df.columns)):
        table[0, j].set_facecolor("#1A237E")
        table[0, j].set_text_props(color="white", fontweight="bold")

    score_col_idx = list(rank_df.columns).index("Score (menor=mejor)")
    n_strats = agg_df["strategy"].nunique()
    best_s, worst_s = n_strats, n_strats * 3

    for i, row in enumerate(rank_df.itertuples(index=False)):
        score = row[5]  # "Score (menor=mejor)"
        ratio = (score - best_s) / max(worst_s - best_s, 1)
        g = int(200 - ratio * 140)
        r = int(50  + ratio * 160)
        for j in range(len(rank_df.columns)):
            if j == score_col_idx:
                table[i + 1, j].set_facecolor(f"#{r:02x}{g:02x}50")
                table[i + 1, j].set_text_props(color="white", fontweight="bold")
            else:
                table[i + 1, j].set_facecolor("#E8EAF6" if i % 2 == 0 else "white")

    fig.suptitle(
        "Tabla de Ranking Objetivo por Estrategia y Escenario\n"
        "Rank 1 = mejor en cada KPI  ·  Score = suma de ranks  ·  Menor Score = estrategia más balanceada",
        fontsize=12, fontweight="bold",
    )
    fig.tight_layout()
    fig.savefig(out / "14_ranking_table.png", bbox_inches="tight")
    plt.close(fig)
    print("  [OK] 14_ranking_table.csv + png")


# ════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════
def _missing_chart(filename: str, reason: str):
    print(f"  [SKIP] {filename} — {reason}")


# ════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Generador de gráficas para la tesis")
    parser.add_argument("--results-dir", default=None)
    parser.add_argument("--output",      default=None)
    parser.add_argument(
        "--warmup-ms", type=int, default=WARMUP_MS,
        help="Warmup period to exclude from non-batch runs (ms, default 30000)"
    )
    args = parser.parse_args()

    script_dir  = Path(__file__).resolve().parent
    results_dir = Path(args.results_dir) if args.results_dir else script_dir.parent / "results"
    output_dir  = Path(args.output)      if args.output      else results_dir / "figures"

    results_dir = results_dir.resolve()
    output_dir  = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*65}")
    print(f"  ANÁLISIS DE RESULTADOS — TESIS")
    print(f"{'='*65}")
    print(f"  Fuente   : {results_dir}")
    print(f"  Salida   : {output_dir}")
    print(f"  Warmup   : {args.warmup_ms:,} ms excluidos de runs no-batch")
    print(f"{'='*65}\n")

    df     = load_latency(results_dir)
    df     = filter_warmup(df, warmup_ms=args.warmup_ms)
    res_df = load_resources(results_dir)

    if res_df.empty:
        print("[WARN] No se encontraron prometheus_snapshot.csv. "
              "Los gráficos de recursos y lag se omitirán.")
    else:
        _print_summary("prometheus_snapshot", res_df)

    # Statistical tests
    print("\nEjecutando pruebas estadísticas…")
    stats_df = run_statistics(df)
    if not stats_df.empty:
        print(stats_df[["scenario", "kruskal_H", "kruskal_p", "significant"]].to_string(index=False))

    print("\nGenerando gráficas…\n")

    # ── Latencia ──────────────────────────────────────────────────
    chart_boxplot(df, output_dir)             # 01 — Violin + Box
    chart_cdf(df, output_dir)                 # 02 — CDF log-scale
    chart_percentile_bars(df, output_dir)     # 03 — p50/p95/p99 bars

    # ── Throughput ─────────────────────────────────────────────────
    chart_throughput(df, output_dir)          # 04  — E2E throughput
    chart_throughput_dual(df, output_dir)     # 04b — E2E vs Write throughput

    # ── Repeticiones ────────────────────────────────────────────────
    chart_stability(df, output_dir)           # 05 — Variabilidad entre runs

    # ── Tabla resumen ───────────────────────────────────────────────
    chart_summary_table(df, stats_df, output_dir)  # 06 — con IQR, CV, Min, Max

    # ── Temporales ──────────────────────────────────────────────────
    chart_latency_over_time(df, output_dir)   # 07 — serie temporal facetada

    # ── Recursos ────────────────────────────────────────────────────
    chart_resource_efficiency(df, res_df, output_dir)  # 08
    chart_kafka_lag(res_df, output_dir)                # 09
    chart_error_rate(res_df, output_dir)               # 10

    # ── Estadística ─────────────────────────────────────────────────
    chart_significance_table(stats_df, output_dir)     # 11

    # ── Multi-KPI holístico ─────────────────────────────────────────
    chart_radar(df, res_df, output_dir)       # 12 — Radar chart
    chart_heatmap(df, output_dir)             # 13 — Heatmap escalabilidad
    chart_ranking_table(df, output_dir)       # 14 — Ranking objetivo

    figs = list(output_dir.glob("*.png"))
    print(f"\n{'='*65}")
    print(f"  Completado. {len(figs)} gráficas en: {output_dir}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
