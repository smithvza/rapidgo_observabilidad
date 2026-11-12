# Release notes v1.0.0

## Resumen

Este release prepara la entrega final del proyecto RapidGo 360 - Observabilidad tecnológica, con base en la evidencia real del repositorio: producer, Kafka, Spark Structured Streaming, Data Lake y validación de la lógica de negocio con `pytest`.

## Qué incluye

- Contrato del evento y validación: `docs/contrato.md`, `producer/contract.py`, `samples/`
- Producción en Kafka con `key=service`: `producer/rapidgo_producer.py`, `docker-compose.yml`
- Procesamiento en Spark por ventana y servicio: `spark/streaming_job.py`, `spark/config.py`, `spark/rules.py`
- Documentación de decisiones: `docs/decisions/ADR-001-key-particiones.md`, `docs/decisions/ADR-002-umbrales-y-sinks.md`
- Checkpoints de semanas: `docs/checkpoints/semana1.md`, `docs/checkpoints/semana2.md`
- CI básico: `.github/workflows/ci.yml`

## Evidencia real del repo

- `docker-compose.yml` declara Kafka `apache/kafka:3.9.0` con 3 particiones y `retention.ms=86400000`.
- `docs/decisions/ADR-001-key-particiones.md` documenta 1175 eventos y particiones `p0=340`, `p1=708`, `p2=127`.
- `spark/config.py` fija los umbrales reales: error rate `> 5 %`, p95 `> 1000 ms`, `MIN_EVENTS_PER_WINDOW = 20`.
- `tests/test_rules.py` valida exactamente esos comportamiento.

## Cómo ejecutarlo

```bash
docker compose up -d --build
python -m pytest -v
```

PENDIENTE: no existe `scripts/smoke_test.py` en este checkout; cuando se añada, debe ejecutarse con:

```bash
python scripts/smoke_test.py
```

## Limitaciones y trabajo pendiente

- El p95 se calcula por servicio, no por servicio + endpoint.
- El reparto de Kafka con `key=service` está desbalanceado; la evidencia real documentada en ADR-001 muestra que la partición 1 recibe ~60 % del tráfico.
- La carpeta `docs/evidencia` no está presente en este checkout, por lo que la evidencia de la última semana precisa queda como PENDIENTE.
- El video final se deja como placeholder visible: `PENDIENTE: enlace al video`.

## Traza de contribuciones

- Camilo: documentación, KPIs y fases de cierre: PENDIENTE verificar mapeo a issues/PR.
- Javier: Kafka, ADR-001 y coordinación de integración: PENDIENTE verificar mapeo a issues/PR.
- Smith: producer, docker compose, release y quality gate: PENDIENTE verificar mapeo a issues/PR.
- Sebastian: coordinación y cierre: PENDIENTE verificar mapeo a issues/PR.

## Nota final

Este release note evita inventar evidencia, métricas o enlaces que no existan en el repositorio. Todo dato que no se puede verificar queda marcado como `PENDIENTE`.
