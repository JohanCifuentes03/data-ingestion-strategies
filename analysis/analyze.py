#!/usr/bin/env python3
"""
analyze.py  --  Generador de graficas para la tesis
====================================================
Lee todos los CSV de latency_samples.csv dentro de results/ y genera
graficas de calidad publicacion en results/figures/.

Uso:
    python analyze.py
    python analyze.py --results-dir ../results --output ../results/figures
"""

import argparse
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import seaborn as sns
from scipy import stats

# ── Style ──────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", font_scale=1.15, rc={
    "figure.dpi": 150,
    "savefig.dpi": 300,
    "font.family": "sans-serif",
    "axes.titleweight": "bold",
})
PALETTE = {"batch": "#2196F3", "microbatch": "#FF9800", "streaming": "#4CAF50"}
STRATEGY_ORDER = ["batch", "microbatch", "streaming"]


# ── Data Loading ───────────────────────────────────────────────────
def load_all_results(results_dir: Path) -> pd.DataFrame:
    """Walk results/<strategy>/<scenario>/run_N/latency_samples.csv"""
    frames = []
    for csv_path in results_dir.rglob("latency_samples.csv"):
        # Skip the root-level accumulated CSV
        parts = csv_path.relative_to(results_dir).parts
        if len(parts) < 4:
            continue
        strategy, scenario, run_dir = parts[0], parts[1], parts[2]
        run_id = run_dir  # e.g. "run_1"

        df = pd.read_csv(csv_path, on_bad_lines="skip")
        if "latency_ms" not in df.columns:
            continue
        df["strategy"] = strategy
        df["scenario"] = scenario
        df["run_id"] = run_id
        frames.append(df)

    if not frames:
        print("[ERROR] No se encontraron archivos latency_samples.csv con datos validos.")
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    print(f"[INFO] Cargados {len(combined):,} registros de {len(frames)} archivos CSV")
    print(f"       Estrategias: {sorted(combined['strategy'].unique())}")
    print(f"       Escenarios:  {sorted(combined['scenario'].unique())}")
    return combined


# ── Chart 1: Boxplot de latencia ───────────────────────────────────
def chart_boxplot(df: pd.DataFrame, out: Path):
    """Boxplot de latencia por estrategia, facetado por escenario."""
    scenarios = sorted(df["scenario"].unique())
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(5 * n, 6), sharey=True, squeeze=False)
    axes = axes[0]

    for i, scenario in enumerate(scenarios):
        sub = df[df["scenario"] == scenario]
        order = [s for s in STRATEGY_ORDER if s in sub["strategy"].unique()]
        sns.boxplot(
            data=sub, x="strategy", y="latency_ms", order=order,
            palette=PALETTE, ax=axes[i], showfliers=False, width=0.6,
        )
        axes[i].set_title(scenario)
        axes[i].set_xlabel("")
        if i == 0:
            axes[i].set_ylabel("Latencia (ms)")
        else:
            axes[i].set_ylabel("")

    fig.suptitle("Distribucion de Latencia por Estrategia y Escenario", fontsize=14)
    fig.tight_layout()
    fig.savefig(out / "01_boxplot_latencia.png", bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] 01_boxplot_latencia.png")


# ── Chart 2: CDF de latencia ──────────────────────────────────────
def chart_cdf(df: pd.DataFrame, out: Path):
    """CDF (Cumulative Distribution Function) de latencia por estrategia, facetado por escenario."""
    scenarios = sorted(df["scenario"].unique())
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(5 * n, 5), sharey=True, squeeze=False)
    axes = axes[0]

    for i, scenario in enumerate(scenarios):
        sub = df[df["scenario"] == scenario]
        for strat in STRATEGY_ORDER:
            s = sub[sub["strategy"] == strat]["latency_ms"].sort_values()
            if len(s) == 0:
                continue
            cdf = s.rank(method="max") / len(s)
            axes[i].plot(s, cdf, label=strat, color=PALETTE.get(strat), linewidth=1.5)
        axes[i].set_title(scenario)
        axes[i].set_xlabel("Latencia (ms)")
        if i == 0:
            axes[i].set_ylabel("CDF")
        axes[i].legend(loc="lower right", fontsize=9)
        axes[i].set_ylim(0, 1.05)

    fig.suptitle("Funcion de Distribucion Acumulada (CDF) de Latencia", fontsize=14)
    fig.tight_layout()
    fig.savefig(out / "02_cdf_latencia.png", bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] 02_cdf_latencia.png")


# ── Chart 3: Barras de percentiles ────────────────────────────────
def chart_percentile_bars(df: pd.DataFrame, out: Path):
    """Bar chart comparativo de p50, p95, p99 por estrategia y escenario."""
    records = []
    for (strategy, scenario), grp in df.groupby(["strategy", "scenario"]):
        lat = grp["latency_ms"]
        records.append({
            "strategy": strategy,
            "scenario": scenario,
            "p50": lat.quantile(0.50),
            "p95": lat.quantile(0.95),
            "p99": lat.quantile(0.99),
        })
    pct_df = pd.DataFrame(records)

    scenarios = sorted(pct_df["scenario"].unique())
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(5 * n, 5), sharey=True, squeeze=False)
    axes = axes[0]

    for i, scenario in enumerate(scenarios):
        sub = pct_df[pct_df["scenario"] == scenario]
        order = [s for s in STRATEGY_ORDER if s in sub["strategy"].values]
        sub = sub.set_index("strategy").loc[order]
        x_pos = range(len(order))
        w = 0.25
        axes[i].bar([p - w for p in x_pos], sub["p50"], w, label="p50", color="#66BB6A")
        axes[i].bar(x_pos, sub["p95"], w, label="p95", color="#FFA726")
        axes[i].bar([p + w for p in x_pos], sub["p99"], w, label="p99", color="#EF5350")
        axes[i].set_xticks(list(x_pos))
        axes[i].set_xticklabels(order)
        axes[i].set_title(scenario)
        if i == 0:
            axes[i].set_ylabel("Latencia (ms)")
        axes[i].legend(fontsize=9)

    fig.suptitle("Percentiles de Latencia (p50, p95, p99)", fontsize=14)
    fig.tight_layout()
    fig.savefig(out / "03_percentiles_barras.png", bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] 03_percentiles_barras.png")


# ── Chart 4: Throughput por estrategia ────────────────────────────
def chart_throughput(df: pd.DataFrame, out: Path):
    """Throughput estimado (eventos / duracion efectiva) por estrategia y escenario."""
    records = []
    for (strategy, scenario, run_id), grp in df.groupby(["strategy", "scenario", "run_id"]):
        duration_ms = grp["visible_at"].max() - grp["produced_at"].min()
        if duration_ms <= 0:
            continue
        throughput = len(grp) / (duration_ms / 1000.0)
        records.append({
            "strategy": strategy,
            "scenario": scenario,
            "run_id": run_id,
            "throughput_eps": throughput,
        })
    tp_df = pd.DataFrame(records)
    if tp_df.empty:
        return

    order = [s for s in STRATEGY_ORDER if s in tp_df["strategy"].unique()]
    fig, ax = plt.subplots(figsize=(max(8, len(tp_df["scenario"].unique()) * 3), 6))
    sns.barplot(
        data=tp_df, x="scenario", y="throughput_eps", hue="strategy",
        hue_order=order, palette=PALETTE, ax=ax, errorbar="sd", capsize=0.1,
    )
    ax.set_ylabel("Throughput (eventos / segundo)")
    ax.set_xlabel("Escenario")
    ax.set_title("Throughput Promedio por Estrategia y Escenario", fontsize=14, fontweight="bold")
    ax.legend(title="Estrategia")
    fig.tight_layout()
    fig.savefig(out / "04_throughput.png", bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] 04_throughput.png")


# ── Chart 5: Estabilidad entre repeticiones ───────────────────────
def chart_stability(df: pd.DataFrame, out: Path):
    """Boxplot de latencia por run_id, agrupado por estrategia (un subplot por escenario)."""
    scenarios = sorted(df["scenario"].unique())
    strategies = [s for s in STRATEGY_ORDER if s in df["strategy"].unique()]
    n_scenarios = len(scenarios)
    n_strategies = len(strategies)

    fig, axes = plt.subplots(
        n_strategies, n_scenarios,
        figsize=(4 * n_scenarios, 4 * n_strategies),
        squeeze=False, sharey="row",
    )

    for r, strategy in enumerate(strategies):
        for c, scenario in enumerate(scenarios):
            ax = axes[r][c]
            sub = df[(df["strategy"] == strategy) & (df["scenario"] == scenario)]
            if sub.empty:
                ax.set_visible(False)
                continue
            runs = sorted(sub["run_id"].unique())
            sns.boxplot(
                data=sub, x="run_id", y="latency_ms", order=runs,
                color=PALETTE.get(strategy, "#999"), ax=ax,
                showfliers=False, width=0.5,
            )
            if r == 0:
                ax.set_title(scenario, fontsize=11)
            if c == 0:
                ax.set_ylabel(f"{strategy}\nLatencia (ms)")
            else:
                ax.set_ylabel("")
            ax.set_xlabel("")
            ax.tick_params(axis="x", rotation=45, labelsize=8)

    fig.suptitle("Estabilidad entre Repeticiones", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out / "05_estabilidad_runs.png", bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] 05_estabilidad_runs.png")


# ── Chart 6: Tabla resumen ────────────────────────────────────────
def chart_summary_table(df: pd.DataFrame, out: Path):
    """Genera una tabla resumen como CSV y como imagen PNG."""
    records = []
    for (strategy, scenario), grp in df.groupby(["strategy", "scenario"]):
        lat = grp["latency_ms"]
        duration_ms = grp["visible_at"].max() - grp["produced_at"].min()
        throughput = len(grp) / (duration_ms / 1000.0) if duration_ms > 0 else 0
        n_runs = grp["run_id"].nunique()
        records.append({
            "Estrategia": strategy,
            "Escenario": scenario,
            "N eventos": f"{len(grp):,}",
            "Runs": n_runs,
            "p50 (ms)": f"{lat.quantile(0.50):,.1f}",
            "p95 (ms)": f"{lat.quantile(0.95):,.1f}",
            "p99 (ms)": f"{lat.quantile(0.99):,.1f}",
            "Media (ms)": f"{lat.mean():,.1f}",
            "Desv. Est.": f"{lat.std():,.1f}",
            "Throughput (ev/s)": f"{throughput:,.0f}",
        })

    summary = pd.DataFrame(records)
    csv_path = out / "06_tabla_resumen.csv"
    summary.to_csv(csv_path, index=False)

    # Render table as image
    fig, ax = plt.subplots(figsize=(16, max(2, 0.5 * len(summary) + 1.5)))
    ax.axis("off")
    table = ax.table(
        cellText=summary.values,
        colLabels=summary.columns,
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.4)

    # Style header
    for j, col in enumerate(summary.columns):
        cell = table[0, j]
        cell.set_facecolor("#37474F")
        cell.set_text_props(color="white", fontweight="bold")

    # Alternate row colors
    for i in range(len(summary)):
        color = "#ECEFF1" if i % 2 == 0 else "white"
        for j in range(len(summary.columns)):
            table[i + 1, j].set_facecolor(color)

    fig.suptitle("Tabla Resumen de Resultados", fontsize=13, fontweight="bold", y=0.98)
    fig.tight_layout()
    fig.savefig(out / "06_tabla_resumen.png", bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] 06_tabla_resumen.csv")
    print(f"  [OK] 06_tabla_resumen.png")


# ── Main ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Generador de graficas para la tesis")
    parser.add_argument("--results-dir", type=str, default=None,
                        help="Directorio raiz de resultados (default: ../results)")
    parser.add_argument("--output", type=str, default=None,
                        help="Directorio de salida para las graficas (default: ../results/figures)")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    results_dir = Path(args.results_dir) if args.results_dir else script_dir.parent / "results"
    output_dir = Path(args.output) if args.output else results_dir / "figures"

    results_dir = results_dir.resolve()
    output_dir = output_dir.resolve()

    print(f"\n{'='*60}")
    print(f"  ANALISIS DE RESULTADOS - TESIS")
    print(f"{'='*60}")
    print(f"  Fuente:  {results_dir}")
    print(f"  Salida:  {output_dir}")
    print(f"{'='*60}\n")

    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_all_results(results_dir)

    print(f"\nGenerando graficas...\n")
    chart_boxplot(df, output_dir)
    chart_cdf(df, output_dir)
    chart_percentile_bars(df, output_dir)
    chart_throughput(df, output_dir)
    chart_stability(df, output_dir)
    chart_summary_table(df, output_dir)

    print(f"\n{'='*60}")
    print(f"  Completado. {len(list(output_dir.glob('*.png')))} graficas generadas.")
    print(f"  Directorio: {output_dir}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
