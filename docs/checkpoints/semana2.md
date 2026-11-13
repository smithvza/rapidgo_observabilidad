# Checkpoint Semana 2

**Grupo 5 · Variante 9 (Observabilidad tecnológica)** · Release `v0.2.0-streaming`
**Equipo:** Sebastian Valencia, Camilo Soler, Javier Prieto

## 1. Qué quedó funcionando esta semana

- Kafka en modo KRaft con el topic `rapidgo.observability.events` (3 particiones, key = `service`), creado por `kafka-init` en `docker-compose.yml`.
- Generador/producer de eventos que publica los 6 servicios según `docs/contrato.md`.
- Job de Spark Structured Streaming (`spark/streaming_job.py`) que consume Kafka, parsea y valida el JSON, calcula KPIs por ventana de 1 minuto y por servicio, y evalúa las 2 alertas.
- Persistencia en Parquet en tres capas (`raw`, `processed`, `curated`), cada una con su propio checkpoint de Spark.
- CI con GitHub Actions (`.github/workflows/ci.yml`): job `test` con Python 3.13/Airflow 3.1.3, `compileall`, `pytest` y Compose; job `dag` con Python 3.12/Airflow 2.10.5 y pruebas de integridad del DAG. Se ejecuta en push, pull request y workflow_dispatch.

## 2. Evidencia real disponible en el repositorio

La evidencia concreta que sí está en el repo es:

- `docs/decisions/ADR-001-key-particiones.md`: la ejecución real del producer indicó 1175 eventos y particiones `p0 = 340`, `p1 = 708`, `p2 = 127` para los offsets por partición.
- `spark/config.py`: `ERROR_RATE_THRESHOLD = 0.05`, `P95_LATENCY_THRESHOLD_MS = 1000`, `MIN_EVENTS_PER_WINDOW = 20`, `WINDOW_DURATION = "1 minute"` y `WATERMARK = "2 minutes"`. El commit `e03b559` (PR #36, 10/11/2026) cambió el umbral p95 del job de 800 a 1000 ms.
- `tests/test_rules.py`: las pruebas validan que la alerta de error dispara solo por encima del 5 %, y que el p95 crítico dispara solo por encima de 1000 ms.
- `docs/decisions/ADR-001-key-particiones.md`: en la corrida del 28/10/2026 se enviaron 1175 eventos; offsets p0=340, p1=708 y p2=127.
- `docs/evidencia/latest_semana4.md` (generado 11/11/2026) registra p0=3318, p1=6780 y p2=1093; son datos de otra ejecución y no se mezclan con la corrida anterior.

### Evidencia observada posteriormente en `curated`

- El reporte de Semana 4 registra `tracking-api` a las 23:53 UTC con `error_rate=0.3158` y `p95_latency_ms=1749`, y `notifications-api` a las 23:55 UTC con `error_rate=0.3158` y `p95_latency_ms=1780`.
- **PENDIENTE:** `total_events` de esas ventanas no aparece en el reporte. Puede leerse de los Parquet con el pipeline levantado:

```powershell
docker compose exec airflow python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); import rapidgo_checks as c; print(c.read_parquet_records('/opt/airflow/datalake/curated', columns=['window_start','service','total_events','error_rate','p95_latency_ms']))"
```

## 3. Decisión técnica tomada y por qué

- **Un sink Parquet por capa, cada uno con su checkpoint.** Así `raw` conserva el mensaje original con partición y offset (evidencia para el ADR-001), `processed` guarda los eventos válidos y `curated` los KPIs. Si una capa falla, las otras siguen.
- **Modo append con watermark de 2 minutos** para escribir Parquet. La ventana aparece en `curated` cuando el watermark la cierra. La guía local recomienda esperar aproximadamente 4 minutos para observar `curated`; el tiempo medido exacto de esa espera queda como **PENDIENTE**.
- **Umbrales alineados.** `spark/config.py` es la fuente de valores del job, y `tests/test_rules.py` valida las condiciones. La alineación de p95 se integró mediante el commit `e03b559` (PR #36). Ver [ADR-002](../decisions/ADR-002-umbrales-y-sinks.md).
