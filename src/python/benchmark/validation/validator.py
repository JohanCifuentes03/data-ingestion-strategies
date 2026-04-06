#!/usr/bin/env python3
"""
validate_results.py - Validación de calidad de datos experimentales

Este script verifica que los resultados del benchmark sean válidos y coherentes
para garantizar la calidad de los datos antes del análisis.

Uso:
    python tests/validation/validate_results.py
    python tests/validation/validate_results.py --results-dir results
    python tests/validation/validate_results.py --strict  # Falla en warnings
"""

import argparse
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Tuple
import json

import pandas as pd
import numpy as np


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
        self.results.append(result)
    
    @property
    def errors(self) -> List[ValidationResult]:
        return [r for r in self.results if r.severity == "error" and not r.passed]
    
    @property
    def warnings(self) -> List[ValidationResult]:
        return [r for r in self.results if r.severity == "warning" and not r.passed]
    
    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)
    
    @property
    def all_passed(self) -> bool:
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
    """Carga todos los archivos latency_samples.csv."""
    frames = []
    
    # Buscar en estructura anidada
    for csv_path in results_dir.rglob("latency_samples.csv"):
        parts = csv_path.relative_to(results_dir).parts
        if len(parts) >= 3:
            strategy, scenario, run_dir = parts[0], parts[1], parts[2]
            df = pd.read_csv(csv_path, on_bad_lines="skip")
            if not df.empty and "latency_ms" in df.columns:
                df["strategy"] = strategy
                df["scenario"] = scenario
                df["run_id"] = run_dir
                df["source_file"] = str(csv_path)
                frames.append(df)
    
    # Buscar en raíz (formato plano)
    root_csv = results_dir / "latency_samples.csv"
    if root_csv.exists():
        df = pd.read_csv(root_csv, on_bad_lines="skip")
        if not df.empty and "latency_ms" in df.columns:
            df["source_file"] = str(root_csv)
            frames.append(df)
    
    if not frames:
        return pd.DataFrame()
    
    return pd.concat(frames, ignore_index=True)


def validate_latency_range(df: pd.DataFrame, report: ValidationReport):
    """Valida que las latencias estén en rangos razonables."""
    if df.empty:
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
        f"Found {len(df):,} records"
    ))
    
    # Latencias negativas
    negative = (df["latency_ms"] < 0).sum()
    report.add(ValidationResult(
        "No negative latencies",
        negative == 0,
        f"Found {negative:,} negative latency values (clock sync issue?)",
        "error"
    ))
    
    # Latencias extremadamente altas (> 5 minutos)
    extreme_threshold = 300_000  # 5 minutos
    extreme = (df["latency_ms"] > extreme_threshold).sum()
    extreme_pct = extreme / len(df) * 100 if len(df) > 0 else 0
    report.add(ValidationResult(
        "No extreme latencies (> 5 min)",
        extreme_pct < 1.0,  # Tolerar hasta 1%
        f"Found {extreme:,} records ({extreme_pct:.2f}%) with latency > 5 min",
        "warning"
    ))
    
    # Latencias cero
    zero = (df["latency_ms"] == 0).sum()
    zero_pct = zero / len(df) * 100 if len(df) > 0 else 0
    report.add(ValidationResult(
        "Minimal zero latencies",
        zero_pct < 5.0,
        f"Found {zero:,} records ({zero_pct:.2f}%) with zero latency",
        "warning"
    ))


def validate_data_completeness(df: pd.DataFrame, report: ValidationReport):
    """Valida que los datos estén completos."""
    if df.empty:
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
    nan_count = df["latency_ms"].isna().sum()
    report.add(ValidationResult(
        "No NaN latencies",
        nan_count == 0,
        f"Found {nan_count:,} NaN values in latency_ms",
        "error"
    ))
    
    # Mínimo de muestras por estrategia
    min_samples = 100
    for strategy in df["strategy"].unique():
        count = (df["strategy"] == strategy).sum()
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
    if df.empty:
        return
    
    if "produced_at" not in df.columns or "visible_at" not in df.columns:
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
    invalid = (df["visible_at"] < df["produced_at"]).sum()
    report.add(ValidationResult(
        "visible_at >= produced_at",
        invalid == 0,
        f"Found {invalid:,} records where visible_at < produced_at",
        "error"
    ))
    
    # Timestamps en rango razonable (año 2020-2030)
    min_ts = 1577836800000  # 2020-01-01
    max_ts = 1893456000000  # 2030-01-01
    
    out_of_range = (
        (df["produced_at"] < min_ts) | (df["produced_at"] > max_ts) |
        (df["visible_at"] < min_ts) | (df["visible_at"] > max_ts)
    ).sum()
    
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
