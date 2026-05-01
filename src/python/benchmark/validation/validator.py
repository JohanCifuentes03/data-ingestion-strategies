#!/usr/bin/env python3
"""
validate_results.py - Validación de calidad de datos experimentales

Este script verifica que los resultados del benchmark sean válidos y coherentes
para garantizar la calidad de los datos antes del análisis.

Uso:
    python -m benchmark.validation.validator
    python -m benchmark.validation.validator --results-dir results
    python -m benchmark.validation.validator --strict  # Falla en warnings
"""

import argparse
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Tuple
import json

import pandas as pd
import numpy as np


LATENCY_CHUNK_ROWS = 250_000
LATENCY_SAMPLE_PER_FILE = 20_000


@dataclass
class ValidationResult:
    """Resultado de una validación individual."""
    name: str
    passed: bool
    message: str
    severity: str = "error"  # "error", "warning", "info"


@dataclass
class ValidationReport:
    """Reporte completo de validación."""
    results: List[ValidationResult] = field(default_factory=list)
    
    def add(self, result: ValidationResult):
        """Adds one validation result to the report.
        
        Args:
            result: ValidationResult instance to append.
        """
        self.results.append(result)
    
    @property
    def errors(self) -> List[ValidationResult]:
        """Returns failed validation checks with error severity."""
        return [r for r in self.results if r.severity == "error" and not r.passed]
    
    @property
    def warnings(self) -> List[ValidationResult]:
        """Returns failed validation checks with warning severity."""
        return [r for r in self.results if r.severity == "warning" and not r.passed]
    
    @property
    def passed_count(self) -> int:
        """Returns the number of checks that passed."""
        return sum(1 for r in self.results if r.passed)
    
    @property
    def all_passed(self) -> bool:
        """Returns True when no error-severity checks failed."""
        return len(self.errors) == 0
    
    def print_report(self):
        """Imprime el reporte de validación."""
        print("\n" + "=" * 60)
        print("  VALIDATION REPORT")
        print("=" * 60)
        
        # Agrupar por estado
        passed = [r for r in self.results if r.passed]
        failed = [r for r in self.results if not r.passed]
        
        if passed:
            print(f"\n[PASSED] ({len(passed)} checks)")
            for r in passed:
                print(f"  + {r.name}")
        
        if failed:
            print(f"\n[ISSUES] ({len(failed)} checks)")
            for r in failed:
                icon = "X" if r.severity == "error" else "!"
                print(f"  {icon} [{r.severity.upper()}] {r.name}")
                print(f"    {r.message}")
        
        print("\n" + "-" * 60)
        print(f"  Total: {len(self.results)} checks")
        print(f"  Passed: {self.passed_count}")
        print(f"  Errors: {len(self.errors)}")
        print(f"  Warnings: {len(self.warnings)}")
        print("=" * 60 + "\n")


def load_latency_data(results_dir: Path) -> pd.DataFrame:
    """Carga una muestra acotada y conserva contadores exactos por chunks."""
    frames = []
    stats = {
        "total_rows": 0,
        "negative": 0,
        "extreme": 0,
        "zero": 0,
        "nan_latency": 0,
        "invalid_timestamps": 0,
        "timestamp_out_of_range": 0,
        "strategy_counts": {},
        "has_produced_at": False,
        "has_visible_at": False,
    }
    extreme_threshold = 300_000
    min_ts = 1577836800000
    max_ts = 1893456000000

    def consume_csv(csv_path: Path, strategy: str | None, scenario: str | None, run_id: str | None, seed: int):
        """Streams one latency CSV into exact counters and bounded samples.
        
        Args:
            csv_path: CSV file to read.
            strategy: Optional strategy label inferred from the path.
            scenario: Optional scenario label inferred from the path.
            run_id: Optional run identifier inferred from the path.
            seed: Random seed for deterministic sampling.
        """
        sample_parts = []
        try:
            chunks = pd.read_csv(csv_path, on_bad_lines="skip", chunksize=LATENCY_CHUNK_ROWS)
            for chunk_idx, chunk in enumerate(chunks):
                if chunk.empty or "latency_ms" not in chunk.columns:
                    continue
                chunk = chunk.copy()
                chunk["latency_ms"] = pd.to_numeric(chunk["latency_ms"], errors="coerce")
                stats["nan_latency"] += int(chunk["latency_ms"].isna().sum())
                chunk = chunk.dropna(subset=["latency_ms"])
                if chunk.empty:
                    continue
                if strategy is not None:
                    chunk["strategy"] = strategy
                if scenario is not None:
                    chunk["scenario"] = scenario
                if run_id is not None:
                    chunk["run_id"] = run_id
                chunk["source_file"] = str(csv_path)

                lat = chunk["latency_ms"]
                stats["total_rows"] += int(len(chunk))
                stats["negative"] += int((lat < 0).sum())
                stats["extreme"] += int((lat > extreme_threshold).sum())
                stats["zero"] += int((lat == 0).sum())

                if "strategy" in chunk.columns:
                    for name, count in chunk["strategy"].value_counts().items():
                        stats["strategy_counts"][str(name)] = stats["strategy_counts"].get(str(name), 0) + int(count)

                if "produced_at" in chunk.columns and "visible_at" in chunk.columns:
                    stats["has_produced_at"] = True
                    stats["has_visible_at"] = True
                    produced_at = pd.to_numeric(chunk["produced_at"], errors="coerce")
                    visible_at = pd.to_numeric(chunk["visible_at"], errors="coerce")
                    stats["invalid_timestamps"] += int((visible_at < produced_at).sum())
                    out_of_range = (
                        (produced_at < min_ts) | (produced_at > max_ts) |
                        (visible_at < min_ts) | (visible_at > max_ts)
                    )
                    stats["timestamp_out_of_range"] += int(out_of_range.sum())

                keep_cols = [col for col in ["latency_ms", "strategy", "scenario", "run_id", "produced_at", "visible_at", "source_file"] if col in chunk.columns]
                per_chunk_sample = max(1, LATENCY_SAMPLE_PER_FILE // 8)
                if len(chunk) > per_chunk_sample:
                    sample_parts.append(chunk[keep_cols].sample(n=per_chunk_sample, random_state=seed + chunk_idx))
                else:
                    sample_parts.append(chunk[keep_cols])
        except pd.errors.EmptyDataError:
            return
        if sample_parts:
            sample = pd.concat(sample_parts, ignore_index=True)
            if len(sample) > LATENCY_SAMPLE_PER_FILE:
                sample = sample.sample(n=LATENCY_SAMPLE_PER_FILE, random_state=seed)
            frames.append(sample)
    
    # Buscar en estructura anidada
    for idx, csv_path in enumerate(sorted(results_dir.rglob("latency_samples.csv"))):
        parts = csv_path.relative_to(results_dir).parts
        if len(parts) >= 3:
            strategy, scenario, run_dir = parts[0], parts[1], parts[2]
            consume_csv(csv_path, strategy, scenario, run_dir, seed=idx + 10)
    
    # Buscar en raíz (formato plano)
    root_csv = results_dir / "latency_samples.csv"
    if root_csv.exists():
        consume_csv(root_csv, None, None, None, seed=99_999)
    
    if not frames:
        df = pd.DataFrame()
        df.attrs["stats"] = stats
        return df

    df = pd.concat(frames, ignore_index=True)
    df.attrs["stats"] = stats
    return df


def validate_latency_range(df: pd.DataFrame, report: ValidationReport):
    """Valida que las latencias estén en rangos razonables."""
    stats = df.attrs.get("stats", {})
    total_rows = int(stats.get("total_rows", len(df)))
    if total_rows == 0:
        report.add(ValidationResult(
            "Latency data exists",
            False,
            "No latency data found",
            "error"
        ))
        return
    
    report.add(ValidationResult(
        "Latency data exists",
        True,
        f"Found {total_rows:,} records"
    ))
    
    # Latencias negativas
    if "negative" in stats:
        negative = int(stats["negative"])
    else:
        negative = int((df["latency_ms"] < 0).sum())
    report.add(ValidationResult(
        "No negative latencies",
        negative == 0,
        f"Found {negative:,} negative latency values (clock sync issue?)",
        "error"
    ))
    
    # Latencias extremadamente altas (> 5 minutos)
    extreme_threshold = 300_000  # 5 minutos
    if "extreme" in stats:
        extreme = int(stats["extreme"])
    else:
        extreme = int((df["latency_ms"] > extreme_threshold).sum())
    extreme_pct = extreme / total_rows * 100 if total_rows > 0 else 0
    report.add(ValidationResult(
        "No extreme latencies (> 5 min)",
        extreme_pct < 1.0,  # Tolerar hasta 1%
        f"Found {extreme:,} records ({extreme_pct:.2f}%) with latency > 5 min",
        "warning"
    ))
    
    # Latencias cero
    if "zero" in stats:
        zero = int(stats["zero"])
    else:
        zero = int((df["latency_ms"] == 0).sum())
    zero_pct = zero / total_rows * 100 if total_rows > 0 else 0
    report.add(ValidationResult(
        "Minimal zero latencies",
        zero_pct < 5.0,
        f"Found {zero:,} records ({zero_pct:.2f}%) with zero latency",
        "warning"
    ))


def validate_data_completeness(df: pd.DataFrame, report: ValidationReport):
    """Valida que los datos estén completos."""
    stats = df.attrs.get("stats", {})
    if int(stats.get("total_rows", len(df))) == 0:
        return
    
    # Columnas requeridas
    required_cols = ["latency_ms", "strategy", "scenario"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    report.add(ValidationResult(
        "Required columns present",
        len(missing_cols) == 0,
        f"Missing columns: {missing_cols}",
        "error"
    ))
    
    # NaN en latency_ms
    if "nan_latency" in stats:
        nan_count = int(stats["nan_latency"])
    else:
        nan_count = int(df["latency_ms"].isna().sum())
    report.add(ValidationResult(
        "No NaN latencies",
        nan_count == 0,
        f"Found {nan_count:,} NaN values in latency_ms",
        "error"
    ))
    
    # Mínimo de muestras por estrategia
    min_samples = 100
    strategy_counts = stats.get("strategy_counts", {})
    if not strategy_counts and "strategy" in df.columns:
        strategy_counts = {str(strategy): int((df["strategy"] == strategy).sum()) for strategy in df["strategy"].unique()}
    for strategy, count in strategy_counts.items():
        report.add(ValidationResult(
            f"Sufficient samples for {strategy}",
            count >= min_samples,
            f"Only {count} samples (minimum: {min_samples})",
            "warning"
        ))


def validate_statistical_sanity(df: pd.DataFrame, report: ValidationReport):
    """Valida que las estadísticas sean coherentes."""
    if df.empty:
        return
    
    for strategy in df["strategy"].unique():
        sub = df[df["strategy"] == strategy]["latency_ms"]
        if len(sub) < 10:
            continue
        
        p50 = sub.quantile(0.50)
        p95 = sub.quantile(0.95)
        p99 = sub.quantile(0.99)
        
        # p50 < p95 < p99
        report.add(ValidationResult(
            f"{strategy}: p50 < p95 < p99",
            p50 <= p95 <= p99,
            f"Percentile order violated: p50={p50:.1f}, p95={p95:.1f}, p99={p99:.1f}",
            "error"
        ))
        
        # CV% razonable (< 500%)
        cv = sub.std() / sub.mean() * 100 if sub.mean() > 0 else 0
        report.add(ValidationResult(
            f"{strategy}: CV% reasonable (< 500%)",
            cv < 500,
            f"CV% = {cv:.1f}% (extremely high variance)",
            "warning"
        ))


def validate_strategy_expectations(df: pd.DataFrame, report: ValidationReport):
    """
    Valida expectativas teóricas:
    - Streaming debe tener menor latencia que Batch
    - Streaming debe tener menor varianza que Batch
    """
    if df.empty:
        return
    
    strategies = df["strategy"].unique()
    
    if "batch" in strategies and "streaming" in strategies:
        batch_p50 = df[df["strategy"] == "batch"]["latency_ms"].quantile(0.50)
        streaming_p50 = df[df["strategy"] == "streaming"]["latency_ms"].quantile(0.50)
        
        report.add(ValidationResult(
            "Streaming p50 < Batch p50 (expected)",
            streaming_p50 < batch_p50,
            f"Streaming p50={streaming_p50:.1f}ms, Batch p50={batch_p50:.1f}ms",
            "warning"  # Solo warning porque depende del escenario
        ))
    
    if "microbatch" in strategies and "batch" in strategies:
        batch_p50 = df[df["strategy"] == "batch"]["latency_ms"].quantile(0.50)
        microbatch_p50 = df[df["strategy"] == "microbatch"]["latency_ms"].quantile(0.50)
        
        report.add(ValidationResult(
            "Microbatch p50 < Batch p50 (expected)",
            microbatch_p50 < batch_p50,
            f"Microbatch p50={microbatch_p50:.1f}ms, Batch p50={batch_p50:.1f}ms",
            "warning"
        ))


def validate_timestamps(df: pd.DataFrame, report: ValidationReport):
    """Valida coherencia de timestamps."""
    stats = df.attrs.get("stats", {})
    if int(stats.get("total_rows", len(df))) == 0:
        return
    
    if not stats.get("has_produced_at", "produced_at" in df.columns) or not stats.get("has_visible_at", "visible_at" in df.columns):
        report.add(ValidationResult(
            "Timestamp columns present",
            False,
            "Missing produced_at or visible_at columns",
            "warning"
        ))
        return
    
    report.add(ValidationResult(
        "Timestamp columns present",
        True,
        "Both produced_at and visible_at found"
    ))
    
    # visible_at > produced_at (siempre)
    if "invalid_timestamps" in stats:
        invalid = int(stats["invalid_timestamps"])
    else:
        invalid = int((df["visible_at"] < df["produced_at"]).sum())
    report.add(ValidationResult(
        "visible_at >= produced_at",
        invalid == 0,
        f"Found {invalid:,} records where visible_at < produced_at",
        "error"
    ))
    
    # Timestamps en rango razonable (año 2020-2030)
    min_ts = 1577836800000  # 2020-01-01
    max_ts = 1893456000000  # 2030-01-01
    
    if "timestamp_out_of_range" in stats:
        out_of_range = int(stats["timestamp_out_of_range"])
    else:
        out_of_range = int((
            (df["produced_at"] < min_ts) | (df["produced_at"] > max_ts) |
            (df["visible_at"] < min_ts) | (df["visible_at"] > max_ts)
        ).sum())
    
    report.add(ValidationResult(
        "Timestamps in valid range (2020-2030)",
        out_of_range == 0,
        f"Found {out_of_range:,} records with timestamps outside expected range",
        "warning"
    ))


def validate_prometheus_data(results_dir: Path, report: ValidationReport):
    """Valida datos de Prometheus si existen."""
    prom_files = list(results_dir.rglob("prometheus_snapshot.csv"))
    
    if not prom_files:
        report.add(ValidationResult(
            "Prometheus data exists",
            False,
            "No prometheus_snapshot.csv files found",
            "info"
        ))
        return
    
    report.add(ValidationResult(
        "Prometheus data exists",
        True,
        f"Found {len(prom_files)} Prometheus snapshot files"
    ))
    
    # Verificar que haya métricas de recursos
    for prom_file in prom_files[:3]:  # Solo verificar primeros 3
        df = pd.read_csv(prom_file, on_bad_lines="skip")
        if "metric" in df.columns:
            metrics = df["metric"].unique()
            has_cpu = any("cpu" in m.lower() for m in metrics)
            has_mem = any("mem" in m.lower() for m in metrics)
            
            report.add(ValidationResult(
                f"Resource metrics in {prom_file.name}",
                has_cpu and has_mem,
                f"CPU: {has_cpu}, Memory: {has_mem}",
                "warning"
            ))


def main():
    """Runs the benchmark result validator CLI.
    
    The command loads latency samples, executes all validation checks, prints text or JSON output, and exits non-zero on failed error checks.
    """
    parser = argparse.ArgumentParser(description="Validate benchmark results")
    parser.add_argument(
        "--results-dir",
        default="results",
        help="Results directory (default: results)"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on warnings (not just errors)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON"
    )
    args = parser.parse_args()
    
    results_dir = Path(args.results_dir).resolve()
    
    if not results_dir.exists():
        print(f"[ERROR] Results directory not found: {results_dir}")
        sys.exit(1)
    
    print(f"Validating results in: {results_dir}")
    
    # Cargar datos
    df = load_latency_data(results_dir)
    
    # Crear reporte
    report = ValidationReport()
    
    # Ejecutar validaciones
    validate_latency_range(df, report)
    validate_data_completeness(df, report)
    validate_statistical_sanity(df, report)
    validate_strategy_expectations(df, report)
    validate_timestamps(df, report)
    validate_prometheus_data(results_dir, report)
    
    # Output
    if args.json:
        output = {
            "passed": report.all_passed,
            "total_checks": len(report.results),
            "passed_count": report.passed_count,
            "error_count": len(report.errors),
            "warning_count": len(report.warnings),
            "results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "severity": r.severity,
                    "message": r.message
                }
                for r in report.results
            ]
        }
        print(json.dumps(output, indent=2))
    else:
        report.print_report()
    
    # Exit code
    if not report.all_passed:
        sys.exit(1)
    if args.strict and report.warnings:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
