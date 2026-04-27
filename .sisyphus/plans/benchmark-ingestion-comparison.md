# Benchmark Ingestion Comparison — Plan quirúrgico (simple y funcional)

## 1) Objetivo

Comparar correctamente **Batch vs Micro-batch vs Streaming** en `low-load`, `medium-load`, `high-load` con un pipeline:

- metodológicamente consistente,
- simple de ejecutar y revisar,
- sin métricas ambiguas ni “fallbacks” que maquillen resultados.

---

## 2) Diagnóstico: qué nos estaba dañando la comparación

### A. Riesgos críticos (deben quedar cerrados)

1. **Contaminación entre corridas**
   - Síntoma histórico: `visible_events > generated_events`, ratios >100%.
   - Causa probable histórica: topic compartido/reuso y etiquetado incompleto.
   - Estado actual: ya mitigado con topic por run + labels (`strategy/scenario/run_id`) dentro del evento.

2. **Fuentes de verdad mezcladas**
   - Riesgo: generated desde Prometheus / visible desde otro lado / latencia desde probe parcial.
   - Estado actual: generated desde `generator_summary.json`; visible y latencia oficial desde PostgreSQL exportado.

3. **Semántica de métricas confusa**
   - Se mezcló “entrega durante ventana oficial” con “entrega tras drenaje”.
   - Esto rompe interpretación de throughput y delivery ratio.

4. **Kafka lag mal interpretado**
   - Para Batch/Micro-batch no siempre hay lag real exportado vía `kafka-exporter` en la misma forma que Flink.
   - Riesgo: mostrar 0 o estimados como si fueran comparables.

### B. Complejidad innecesaria (debemos simplificar)

1. Figuras que no responden directo al objetivo de comparación.
2. Métricas secundarias con semántica débil en vez de métricas canónicas.
3. Flujo de validación disperso (sin checklist único por corrida).

---

## 3) Diseño final simple (fuentes oficiales y métricas canónicas)

## 3.1 Fuentes oficiales (únicas)

- **Generated events:** `results/<strategy>/<scenario>/run_<n>/generator_summary.json`
- **Visible events (oficial):** conteo PostgreSQL por (`strategy`,`scenario`,`run_id`)
- **Latency samples (oficial):** export SQL a `latency_samples.csv`
- **Recursos (CPU/Mem):** `resources_timeseries.csv` (Prometheus snapshot consolidado)
- **Kafka lag:** **solo** si hay métrica real disponible; si no, no se reporta como métrica comparativa principal.

## 3.2 Métricas que sí quedan

1. **Latency**: p50, p95, p99 (desde `latency_samples.csv` oficial)
2. **Generation throughput real**: `generated_events / generation_duration_seconds`
3. **Visible throughput equivalente (window-normalized)**: `visible_events / official_duration_seconds`
4. **Delivery ratio (after drain)**: `visible_events / generated_events`
5. **CPU/Mem promedio** por estrategia y escenario

## 3.3 Métricas que salen o se degradan de prioridad

- **Kafka lag comparativo cross-strategy**: fuera del núcleo (solo anexo diagnóstico donde aplique).
- **Backlog estimado**: no usar como métrica principal de tesis.

---

## 4) Set mínimo de figuras oficiales

Solo 4 salidas principales para mantener claridad:

1. `latency_distribution_boxplot.*`
2. `delivery_capacity_by_scenario.*`
3. `delivery_ratio_by_scenario.*`
4. `resource_usage_compute_node.*`

Soporte tabular:

- `latency_summary_table.csv`

Figuras opcionales (anexo técnico, no conclusiones principales):

- `kafka_consumer_lag.*` (solo donde exista lag real)
- `backlog_estimado.*` (si se conserva, marcar explícitamente “estimado / no comparativo principal”)

---

## 5) Simplificación operativa (pipeline de trabajo)

### Paso 1 — Corrida limpia corta (control)

Objetivo: validar que la instrumentación está sana antes de escalar.

```bash
rm -rf results
bash scripts/thesis.sh run --mode local --reps 1 --duration 60 --warmup 5 --cooldown 5
bash scripts/thesis.sh analyze --mode local
bash scripts/thesis.sh validate --mode local
```

### Paso 2 — Checklist obligatorio por run

Por cada `strategy/scenario/run` validar automáticamente:

1. `visible_events <= generated_events`
2. `delivery_ratio_pct in [0,100]`
3. `rows(latency_samples.csv) == visible_events`
4. unicidad de `run_id` y consistencia de labels
5. timestamps válidos (`generation_duration_seconds` y ventana oficial coherentes)

### Paso 3 — Revisión visual guiada

Para cada figura oficial revisar:

- ejes y unidades correctas,
- leyendas claras,
- no mezclar conceptos (window vs drain),
- que la forma de la figura coincida con las tablas fuente.

### Paso 4 — Escalado

Solo si la corrida corta pasa checklist + revisión visual:

**Escalado local (sanidad adicional):**

```bash
rm -rf results
bash scripts/thesis.sh run --mode local --reps 1 --duration 60 --warmup 5 --cooldown 5
bash scripts/thesis.sh analyze --mode local
bash scripts/thesis.sh validate --mode local
```

**Evidencia final distribuida (flujo oficial actual):**

```bash
bash scripts/thesis.sh run --mode distributed
bash scripts/thesis.sh collect --mode distributed
bash scripts/thesis.sh analyze --mode distributed
bash scripts/thesis.sh validate --mode distributed
```

> Nota explícita: la promesa de 3 comandos (`run/analyze/validate`) aplica al flujo local. En distribuido, hoy se requiere `collect`.

---

## 6) Cambios concretos a ejecutar (sin sobreingeniería)

1. **Analyzer**
   - Mantener núcleo de 4 figuras + tabla.
   - Marcar `kafka_consumer_lag` y `backlog_estimado` como anexos diagnósticos (o deshabilitarlos del set por defecto).
   - Asegurar etiquetas explícitas: `after drain`, `window-normalized`.

2. **Validation**
   - Consolidar reglas en un único reporte final por run (PASS/FAIL + razón).

3. **Scripts**
   - Mantener comando único reproducible por modo:
     - local: `run/analyze/validate`
     - distribuido: `run/collect/analyze/validate`
   - No introducir rutas paralelas no documentadas.

4. **Documentación mínima**
   - Añadir sección breve “Métricas oficiales vs métricas diagnósticas” en README.

### 6.1 QA ejecutable por tarea (obligatorio)

1. **Analyzer (figuras + semántica)**
   - Herramienta: `bash scripts/thesis.sh analyze --mode local`
   - Verificación:
     - existen 4 figuras oficiales + `latency_summary_table.csv`.
     - títulos/ejes distinguen `after drain` vs `window-normalized`.
   - Resultado esperado: PASS con archivos generados y semántica explícita.

2. **Validation (consistencia por run)**
   - Herramienta: `bash scripts/thesis.sh validate --mode local`
   - Verificación:
     - `visible_events <= generated_events`
     - `delivery_ratio_pct` entre 0 y 100
     - filas de `latency_samples.csv` = `visible_events`
   - Resultado esperado: PASS en todas las combinaciones del run corto.

3. **Scripts (reproducibilidad)**
   - Herramienta:
     - local: `run -> analyze -> validate`
     - distribuido: `run -> collect -> analyze -> validate`
   - Verificación:
     - cada comando termina exit code 0.
     - outputs aparecen en `results/` (local) o `results-distributed/` (distribuido).
   - Resultado esperado: PASS sin pasos manuales ocultos.

4. **README (claridad metodológica)**
   - Herramienta: revisión textual + ejecución de comandos documentados.
   - Verificación:
     - define métricas oficiales vs diagnósticas.
     - no promete lag comparable si no hay métrica real homogénea.
   - Resultado esperado: PASS con documentación alineada al comportamiento real.

---

## 7) Criterios de aceptación

Se considera “listo” cuando:

1. Corrida limpia corta termina sin errores operativos.
2. Todas las validaciones automáticas pasan en los 3 escenarios × 3 estrategias.
3. Las 4 figuras oficiales son semánticamente correctas y legibles.
4. No hay métricas con interpretación ambigua en conclusiones principales.
5. El flujo completo se corre con 3 comandos (`run`, `analyze`, `validate`).
6. En modo distribuido, el flujo oficial queda explícito y probado con 4 comandos (`run`, `collect`, `analyze`, `validate`).

---

## 8) Riesgos residuales y control

1. **Local mode limita capacidad real en cargas altas**
   - Mitigación: usar local para sanidad; conclusiones de rendimiento final desde distribuido.

2. **Lag no homogéneo entre engines**
   - Mitigación: tratarlo como diagnóstico por engine, no métrica principal cross-strategy.

3. **Drift de semántica en nuevas iteraciones**
   - Mitigación: mantener definiciones oficiales de métricas en analyzer + README.

---

## 9) Secuencia inmediata (qué sigue ya)

1. Aplicar simplificación de figuras/métricas en analyzer.
2. Ejecutar corrida limpia corta.
3. Revisar automáticamente + visualmente.
4. Ajustar solo si hay fallos reales.
5. Repetir hasta estabilidad.

Este plan prioriza **comparación correcta y simple** sobre complejidad técnica adicional.
