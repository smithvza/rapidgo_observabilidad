# Release notes v1.0.0

**Estado:** preparado; el tag `v1.0.0` aún no existe.
**Video:** PENDIENTE: enlace al video final.

## Incluye

- **Fundamentos y contrato:** esquema de evento y producer para RapidGo en [docs/contrato.md](contrato.md), [producer/](../producer/) y [samples/](../samples/).
- **Streaming:** Kafka en KRaft, Spark Structured Streaming, Parquet en `raw`, `processed` y `curated`, key `service`, tres particiones y retención de 24 horas.
- **Orquestación:** DAG `rapidgo_observability_pipeline` con seis tareas, XCom, `retries=2` y retraso de 30 segundos.
- **Reglas:** ventanas de un minuto por servicio, watermark de dos minutos, alertas `error_rate > 5 %` y `p95 > 1000 ms`, mínimo de 20 eventos.
- **Calidad y operación:** pruebas de contrato/reglas/DAG, CI y [scripts/smoke_test.py](../scripts/smoke_test.py).
- **Documentación:** [ADR-001](decisions/ADR-001-key-particiones.md), [ADR-002](decisions/ADR-002-umbrales-y-sinks.md), [ADR-003](decisions/ADR-003-orquestacion-y-recuperacion.md), checkpoints de semanas 1–3 y [guía local](ejecucion-local.md).

## Evidencia incluida en el repositorio

El reporte [latest_semana4.md](evidencia/latest_semana4.md), generado el 11 de noviembre de 2026, registra 11.191 filas procesadas, cero eventos fuera de contrato y cero IDs duplicados. Para el período del reporte aparecen 8 alertas (4 de error y 4 de latencia). La distribución por partición fue p0=3318 (29,6 %), p1=6780 (60,6 %) y p2=1093 (9,8 %).

El [smoke_test_semana4.txt](evidencia/smoke_test_semana4.txt) registra otra ejecución: offsets `{0: 6326, 1: 12937, 2: 2175}` (21.438 total), tres particiones, las seis tareas del DAG en `success` y reporte generado. No mezclar sus offsets con el conteo del reporte anterior.

## Ejecución

Consulta la [guía de ejecución local](ejecucion-local.md). Los comandos principales son:

```text
docker compose up -d --build
python scripts/smoke_test.py
python -m pytest -v
```

La guía explica la espera para `curated`, el acceso a Airflow, la contraseña, y cómo limpiar las capas y checkpoints para empezar de cero.

## Limitaciones conocidas

- El p95 se agrupa por servicio, no por endpoint.
- La partición 1 concentra 60,6 % de los eventos del reporte Semana 4; se puede evaluar una key `service+endpoint` o revisar el número de particiones.
- Airflow se ejecuta en modo `standalone` local; no es una configuración de producción.
- El producer genera datos sintéticos.
- El checkpoint de Spark persiste tras `docker compose down`.
- La latencia de aparición de ventanas en `curated` depende del watermark de dos minutos.

## Trazabilidad de contribuciones

| Integrante | Responsabilidades | Issues | PR principales |
|---|---|---|---|
| Sebastian Valencia (`smithvza`, confirmado por el equipo) | Contrato, producer, diagrama, Docker Compose, CI ampliado, quality gate y release | #6, #12 | #22, #23, #24, #30, #32 |
| Camilo Soler (`CamilosolerB`) | KPIs, documentación, Kanban, tests/CI, fallo controlado y checkpoints | #5, #7, #16, #17 | #26, #27, #28, #29, #34, #35, #37 |
| Javier Prieto (`nagi24118`, confirmado por el equipo) | Kafka/ADR-001, DAG e integración/estabilización | #8, #15 | #25, #31, #33, #36 |

Los números provienen de issues y PR del repositorio. Los issues se relacionan por alcance según la planeación, no por asignación de GitHub. La planeación asigna el producer a Sebastian, aunque el PR #31 figura como abierto por `nagi24118` (Javier); la columna de PR registra la cuenta autora, no reasigna la responsabilidad planeada.
