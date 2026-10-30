# Checkpoint Semana 2

**Grupo 5 · Variante 9 (Observabilidad tecnológica)** · Release `v0.2.0-streaming`
**Equipo:** Sebastian Valencia, Camilo Soler, Javier Prieto

## 1. Qué quedó funcionando esta semana

- Kafka en modo KRaft con el topic `rapidgo.observability.events` (3 particiones, key = `service`), creado por `kafka-init` en `docker-compose.yml`.
- Generador/producer de eventos que publica los 6 servicios según `docs/contrato.md`.
- Job de Spark Structured Streaming (`spark/streaming_job.py`) que consume Kafka, parsea y valida el JSON, calcula KPIs por ventana de 1 minuto y por servicio, y evalúa las 2 alertas.
- Persistencia en Parquet en tres capas (`raw`, `processed`, `curated`), cada una con su propio checkpoint de Spark.
- CI con GitHub Actions (`ci.yml`) activo en push, pull_request y workflow_dispatch: [COMPLETAR: compileall, pytest, docker compose config].

## 2. Valor o evidencia real observada en la ejecución

Ejecución del 28/10/2026 en Docker Desktop (WSL2), Spark 3.5.3, Java 11. El job arrancó y registró en consola:

```
Queries activas: ['raw', 'processed', 'curated']
```

- Ventana observada en `curated`: [COMPLETAR: servicio, `window_start`, `total_events`, `error_rate`, `p95_latency_ms`].
- Alerta disparada: [COMPLETAR: cuál, para qué servicio y con qué valor; ej. `alert_critical_latency = true` con p95 = ___ ms].
- Eventos por partición en `raw` (`kafka_partition`): [COMPLETAR: p0 = ___, p1 = ___, p2 = ___].

## 3. Decisión técnica tomada y por qué

- **Un sink Parquet por capa, cada uno con su checkpoint.** Así `raw` conserva el mensaje original con partición y offset (evidencia para el ADR-001), `processed` guarda los eventos válidos y `curated` los KPIs. Si una capa falla, las otras siguen.
- **Modo append con watermark de 2 minutos** para escribir Parquet. Consecuencia observada: una ventana solo aparece en `curated` cuando llega un evento lo bastante nuevo para cerrarla ([COMPLETAR: cuántos minutos tardó en aparecer]).
- Umbrales en `spark/config.py`, para que el job y los tests lean los mismos valores. Ver ADR-002: [COMPLETAR: enlace].

## 4. Problema encontrado y cómo se resolvió

- **Problema:** `spark-submit` falló con `python3: can't open file '/opt/app/spark/streaming_job.py': No such file or directory`, aunque el conector de Kafka se resolvió bien (28/10/2026, 12:55).
- **Causa:** [COMPLETAR: archivo ausente en `spark/` o volumen no montado en el contenedor `spark`].
- **Solución:** [COMPLETAR: qué se cambió; ej. recrear con `docker compose up -d --force-recreate spark`]. Resuelto en el PR [COMPLETAR: #__].
- **Impacto:** el job no arrancaba y no se escribía nada en el Data Lake hasta corregirlo.

## 5. Enlaces

- Issues: [#9](COMPLETAR) producer · [#10](COMPLETAR) Spark · [#11](COMPLETAR) tests y CI
- Pull Requests: [COMPLETAR: un PR por integrante]
- GitHub Actions: [COMPLETAR: ejecución en verde] · [COMPLETAR: ejecución que falló y se recuperó]
- ADR-002: [COMPLETAR]
