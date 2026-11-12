# Checkpoint Semana 2

**Grupo 5 · Variante 9 (Observabilidad tecnológica)** · Release `v0.2.0-streaming`
**Equipo:** Sebastian Valencia, Camilo Soler, Javier Prieto

## 1. Qué quedó funcionando esta semana

- Kafka en modo KRaft con el topic `rapidgo.observability.events` (3 particiones, key = `service`), creado por `kafka-init` en `docker-compose.yml`.
- Generador/producer de eventos que publica los 6 servicios según `docs/contrato.md`.
- Job de Spark Structured Streaming (`spark/streaming_job.py`) que consume Kafka, parsea y valida el JSON, calcula KPIs por ventana de 1 minuto y por servicio, y evalúa las 2 alertas.
- Persistencia en Parquet en tres capas (`raw`, `processed`, `curated`), cada una con su propio checkpoint de Spark.
- CI con GitHub Actions (`.github/workflows/ci.yml`) activo en push, pull_request y workflow_dispatch: `python -m compileall producer spark tests`, `pytest -v` y `docker compose config --quiet`.

## 2. Evidencia real disponible en el repositorio

La evidencia concreta que sí está en el repo es:

- `docs/decisions/ADR-001-key-particiones.md`: la ejecución real del producer indicó 1175 eventos y particiones `p0 = 340`, `p1 = 708`, `p2 = 127` para los offsets por partición.
- `spark/config.py`: `ERROR_RATE_THRESHOLD = 0.05`, `P95_LATENCY_THRESHOLD_MS = 1000`, `MIN_EVENTS_PER_WINDOW = 20`, `WINDOW_DURATION = "1 minute"` y `WATERMARK = "2 minutes"`.
- `tests/test_rules.py`: las pruebas validan que la alerta de error dispara solo por encima del 5 %, y que el p95 crítico dispara solo por encima de 1000 ms.
- `git log` del commit `721d3e5` (`fix: unificar umbrales de alertas`) confirma la unificación del umbral y la fecha real: `Thu Nov 5 19:27:35 2026 -0500`.

### PENDIENTE: datos observados en `curated`/`raw`

Los siguientes puntos no tienen fuente real en este checkout y deben obtenerse ejecutando el pipeline con Docker:

- Ventana observada en `curated`: PENDIENTE: servicio, `window_start`, `total_events`, `error_rate`, `p95_latency_ms`.
- Alerta disparada: PENDIENTE: confirmar el servicio y el valor exacto de `alert_critical_latency` o `alert_high_error_rate`.
- Eventos por partición en `raw` (`kafka_partition`): PENDIENTE: `p0`, `p1`, `p2` en la ejecución local real.

Comando mínimo para leer Parquet localmente después de levantar Docker:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('read-curated').getOrCreate()
df = spark.read.parquet('datalake/curated')
df.show(truncate=False)
# opcional: df.printSchema()
PY
```

## 3. Decisión técnica tomada y por qué

- **Un sink Parquet por capa, cada uno con su checkpoint.** Así `raw` conserva el mensaje original con partición y offset (evidencia para el ADR-001), `processed` guarda los eventos válidos y `curated` los KPIs. Si una capa falla, las otras siguen.
- **Modo append con watermark de 2 minutos** para escribir Parquet. La decisión está en `spark/config.py` y en `spark/streaming_job.py`. La ventana solo aparece en `curated` cuando el watermark la cierra; la evidencia real exacta de “cuántos minutos tardó” queda como PENDIENTE porque no hay logs ni captura del pipeline en este checkout.
- **Umbrales centralizados.** La fuente de verdad es `spark/config.py` y la validación real está en `tests/test_rules.py`. Ver ADR-002: [`ADR-002-umbrales-y-sinks.md`](./../decisions/ADR-002-umbrales-y-sinks.md).
