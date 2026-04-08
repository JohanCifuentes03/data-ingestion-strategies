# Informe Experimental: Analisis de Resultados por Grafica

Fecha: 2026-04-08

Este documento presenta una lectura tecnica y comparativa de las cinco graficas generadas para el benchmark de ingesta. El analisis se organiza por figura, con enfoque en comportamiento por estrategia (`batch`, `micro-batch`, `streaming`) y por escenario de carga (baja, media, alta).

## 1) Distribucion de latencia E2E por estrategia y escenario

![Distribucion de latencia E2E por estrategia y escenario](figures/latency_distribution_boxplot.png)

La figura evidencia una separacion estructural entre las tres estrategias en todos los escenarios. `batch` opera en orden de cientos de miles de milisegundos (escala de minutos), `micro-batch` en miles de milisegundos (segundos), y `streaming` en centenas de milisegundos para la mediana. Esta jerarquia se mantiene estable entre baja, media y alta carga, lo que sugiere que el modelo de procesamiento (discreto por lotes vs continuo) explica mas variacion en latencia que el nivel de carga por si solo.

Adicionalmente, la dispersion de `streaming` aumenta hacia cola alta (p95/p99) en escenarios exigentes, pero aun asi su mediana se conserva en un rango claramente inferior a `micro-batch` y, especialmente, a `batch`. En terminos de interpretacion academica, esto indica que `streaming` maximiza capacidad de respuesta central, mientras `micro-batch` representa un compromiso intermedio entre costo de coordinacion y oportunidad temporal.

## 2) Throughput generado vs throughput visible en sink

![Throughput generado vs visible en sink por estrategia y escenario](figures/generated_vs_sink_throughput.png)

La comparacion de barras muestra la capacidad efectiva de entrega (eventos visibles en sink respecto a eventos generados). `streaming` presenta la mejor retencion relativa en los tres escenarios (aprox. 98%, 94% y 92%), manteniendo una brecha moderada incluso con incremento de carga. En contraste, `batch` y `micro-batch` muestran brechas significativamente mayores en media y alta carga, lo cual sugiere acumulacion intermedia y menor velocidad de materializacion en almacenamiento final.

Desde una perspectiva de rendimiento operacional, esta grafica no solo evalua volumen procesado, sino eficiencia de transferencia al destino observable. En consecuencia, la superioridad de `streaming` en tasa visible refuerza su ventaja para casos donde la disponibilidad de dato en sink es criterio primario de exito, mientras que `batch` queda orientado a ventanas temporales no estrictas y `micro-batch` depende mas de la configuracion de disparo y del tamano de micro-lote.

## 3) Eficiencia de recursos por carga (centroides)

![Eficiencia de recursos por carga: CPU vs memoria por evento visible](figures/resource_efficiency_scatter.png)

La representacion por centroides resume el comportamiento promedio de cada estrategia en cada nivel de carga, eliminando ruido de ejecuciones individuales. El patron dominante es que `batch` consume mas memoria por evento visible en los tres escenarios (especialmente en baja carga), mientras `micro-batch` y `streaming` se ubican sistematicamente en una zona de menor costo de memoria por evento.

Tambien se observa que el aumento de carga desplaza los centroides hacia mayores niveles de CPU en todas las estrategias, pero con distinta eficiencia marginal. `streaming` y `micro-batch` mantienen mejor relacion CPU/memoria por evento que `batch`, lo cual es consistente con pipelines continuos y menor costo de acumulacion por unidad efectivamente visible. Este resultado aporta evidencia de que la eleccion de estrategia impacta no solo latencia, sino productividad de recurso.

## 4) Kafka consumer lag y backpressure

![Kafka consumer lag y backpressure por estrategia y escenario](figures/kafka_consumer_lag.png)

El grafico de lag confirma el riesgo de backpressure para estrategias menos continuas. `batch` supera ampliamente el umbral critico de 10,000 mensajes en baja, media y alta carga, con crecimiento pronunciado al escalar demanda. `micro-batch` se mantiene bajo umbral en baja, pero lo supera en media y alta, mostrando sensibilidad al incremento de ritmo de entrada.

`streaming` presenta el mejor comportamiento relativo de control de cola: lag muy bajo en baja y media carga, y cercano al umbral en alta. En terminos de estabilidad de pipeline, este comportamiento sugiere menor probabilidad de acumulacion sostenida y, por tanto, menor riesgo de degradacion por retraso creciente. La evidencia es coherente con su mejor throughput visible y menor latencia central.

## 5) Resumen estadistico de latencia

![Resumen estadistico de latencia por estrategia y escenario](figures/latency_summary_table.png)

La tabla cuantifica formalmente lo observado en las figuras previas. En p50, `streaming` se mantiene en ~107-130 ms, `micro-batch` en ~3,669-6,277 ms y `batch` en ~306,950-325,551 ms; la diferencia entre ordenes de magnitud es consistente y robusta entre escenarios. En p95 y p99, `streaming` y `micro-batch` amplian cola bajo mayor carga, pero sin perder la separacion estructural frente a `batch`.

El coeficiente de variacion (CV%) muestra que `batch` es mas estable en terminos relativos, aunque en un nivel absoluto de latencia muy alto; por su parte, `streaming` exhibe mayor variabilidad relativa en alta carga debido al crecimiento de cola extrema, pero conserva una mediana ampliamente favorable. Academicamente, esto respalda la conclusion de que la estrategia optima depende del objetivo principal: minima latencia y alta disponibilidad visible (`streaming`), compromiso operativo (`micro-batch`) o procesamiento diferido de gran volumen (`batch`).
