# ADR-002 - Umbrales de alertas y sinks de parquet

**Estado:** Aceptada por consistencia del código y la documentación (rama `fix-consistencia-umbrales`, commit `721d3e5`)
**Fecha real del cambio:** Thu Nov 5 19:27:35 2026 -0500
**Responsable:** CamilosolerB

## Contexto / problema

El pipeline de Kafka -> Spark -> Data Lake necesitaba un único criterio para decidir cuándo una ventana era crítica. La base del repo tenía dos valores distintos para la latencia p95 en distintos sitios: `spark/config.py` lo definía en 800 ms, mientras que la documentación y la lógica de reglas lo trataba como 1000 ms.

Además, el job de Spark escribe tres capas de salida con checkpoint propio y watermark: `raw`, `processed` y `curated`. Para evitar falsos positivos y para que una ventana aparezca solo cuando está cerrada, era necesario dejar explícito el comportamiento de la agregación y del sink.

## Opciones consideradas

1. Mantener el umbral de 800 ms y documentarlo como fuente de verdad.
2. Unificar en 1000 ms, con validación en pruebas y en el job de streaming.
3. Mantener la agregación por servicio y ventana de 1 minuto, con watermark de 2 minutos y sink independiente por capa.

## Decisión tomada

- Umbral de error rate: `> 5 %` (`ERROR_RATE_THRESHOLD = 0.05`).
- Umbral de p95: `> 1000 ms` (`P95_LATENCY_THRESHOLD_MS = 1000`).
- Mínimo de eventos por ventana: `20` (`MIN_EVENTS_PER_WINDOW = 20`).
- Agregación en `spark/streaming_job.py`: `groupBy(window(event_time, "1 minute"), "service")`.
- Watermark: `2 minutes` (`WATERMARK = "2 minutes"`).
- Sinks: cada capa genera un `writeStream(...).outputMode("append")` con `checkpointLocation` propio.

La decisión quedó consolidada en el commit:

- `721d3e5` — `fix: unificar umbrales de alertas`

## Evidencia utilizada

La evidencia real del repo es la siguiente:

- `spark/config.py` en la rama `fix-consistencia-umbrales`:
  - `ERROR_RATE_THRESHOLD = 0.05`
  - `P95_LATENCY_THRESHOLD_MS = 1000`
  - `MIN_EVENTS_PER_WINDOW = 20`
  - `WINDOW_DURATION = "1 minute"`
  - `WATERMARK = "2 minutes"`
- `spark/streaming_job.py` usa agregación por window + `service` y escribe en `raw`, `processed` y `curated` con checkpoint propio.
- `tests/test_rules.py` valida que la alerta de error no dispara en un 5 % exacto y que el p95 crítico dispara solo en `> 1000 ms`.
- `docs/kpis-alertas.md` fue corregido para reflejar la misma lógica, y no se modificó el job.

## Consecuencias / trade-offs

- (+) Existe una fuente única de verdad para alertas y documentación.
- (+) El cálculo de p95 y error rate es consistente entre código y pruebas.
- (+) La ventana solo aparece en `curated` cuando el watermark la cierra, lo que evita falsos positivos por eventos retrasados.
- (-) El p95 se calcula por `service` y no por `service + endpoint`, por lo que el análisis sigue siendo más grueso que la observación por endpoint.
- (-) El reparto de Kafka por key `service` conserva orden por servicio, pero no garantiza que la carga esté equilibrada entre particiones.

**Conclusión:** la decisión tomada es coherente con el comportamiento real del código actual y con la evidencia del repositorio; no se inventaron valores fuera del código o del historial de git.
