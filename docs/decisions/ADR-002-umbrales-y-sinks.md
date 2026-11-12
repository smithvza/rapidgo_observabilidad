# ADR-002 - Umbrales de alertas y sinks de parquet

**Estado:** Aceptada e integrada en `main` mediante el PR #36
**Evidencia de integración:** commit `e03b559` (`2026-11-10T19:29:12-05:00`)

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

La alineación del umbral de p95 se integró en el siguiente cambio:

- `e03b559` — `feat: integracion y estabilizacion del pipeline completo (producer y spark en compose, umbral p95 alineado, smoke test, guia local)`, integrado por el PR #36.

## Evidencia utilizada

La evidencia real del repo es la siguiente:

- El commit `e03b559` cambió `P95_LATENCY_THRESHOLD_MS` de 800 a 1000 ms en `spark/config.py`.
- El estado integrado de `spark/config.py` declara:
  - `ERROR_RATE_THRESHOLD = 0.05`
  - `P95_LATENCY_THRESHOLD_MS = 1000`
  - `MIN_EVENTS_PER_WINDOW = 20`
  - `WINDOW_DURATION = "1 minute"`
  - `WATERMARK = "2 minutes"`
- `tests/test_config_consistency.py` comprueba que los umbrales de `spark/config.py` coincidan con `spark/rules.py`.
- `spark/streaming_job.py` usa agregación por window + `service` y escribe en `raw`, `processed` y `curated` con checkpoint propio.
- `tests/test_rules.py` valida que la alerta de error no dispara en un 5 % exacto y que el p95 crítico dispara solo en `> 1000 ms`.
- `docs/evidencia/latest_semana4.md` contiene ejecuciones de alertas por servicio con umbral 0.05 y 1000 ms; `docs/evidencia/smoke_test_semana4.txt` registra la validación del pipeline completo.
- `airflow/dags/rapidgo_checks.py` define los mismos umbrales de error y p95 y el mínimo de 20 eventos; `docs/kpis-alertas.md` refleja la agrupación por servicio.

## Consecuencias / trade-offs

- (+) Existe una fuente única de verdad para alertas y documentación.
- (+) El cálculo de p95 y error rate es consistente entre código y pruebas.
- (+) La ventana solo aparece en `curated` cuando el watermark la cierra, lo que evita falsos positivos por eventos retrasados.
- (-) El p95 se calcula por `service` y no por `service + endpoint`, por lo que el análisis sigue siendo más grueso que la observación por endpoint.
- (-) El reparto de Kafka por key `service` conserva orden por servicio, pero no garantiza que la carga esté equilibrada entre particiones.

**Conclusión:** la decisión tomada es coherente con el comportamiento real del código actual y con la evidencia del repositorio; no se inventaron valores fuera del código o del historial de git.
