# RapidGo 360 - Observabilidad tecnológica

## 1. Nombre del proyecto, variante 9 y miembros del equipo

Proyecto final de Big Data de la Universidad de Cundinamarca, variante 9: observabilidad tecnológica.

Equipo:
- Sebastian Valencia
- Camilo Soler
- Javier Prieto

## 2. Problema de negocio y valor de la solución

RapidGo necesita detectar degradaciones de sus servicios antes de que afecten de forma visible la operación del negocio. En la práctica, las APIs pueden elevar tiempos de respuesta y tasas de error sin que la operación central lo vea a tiempo.

La solución propone emitir eventos de observabilidad, publicar los eventos en Kafka, procesarlos con Spark Structured Streaming y materializar indicadores por ventana y servicio en el Data Lake. El valor real del proyecto es convertir eventos técnicos en decisiones operativas: detectar picos de error, p95 de latencia y disponibilidad por servicio en ventanas de 1 minuto.

## 3. Arquitectura

![Arquitectura del pipeline](docs/arquitectura.png)

Flujo textual:

```text
Producer / samples -> Kafka (topic rapidgo.observability.events, key=service) -> Spark Structured Streaming -> Data Lake (raw / processed / curated) -> Airflow -> alertas e indicadores
```

Este repo documenta la arquitectura real de la etapa actual: producer, Kafka, Spark y Data Lake, con Airflow como orquestador del pipeline y la validación de la intención operativa.

## 4. Tecnologías y rol de cada una

| Tecnología | Versión real en repo | Rol |
|---|---|---|
| Kafka | `apache/kafka:3.9.0` en `docker-compose.yml` | Broker de eventos en modo KRaft |
| Spark | `apache/spark:3.5.3` y `pyspark==3.5.3` | Procesamiento en streaming y parquet |
| Airflow | `rapidgo-airflow:2.10.5` en `docker-compose.yml` | Orquestación del DAG y validación de ejecución |
| Python | `requirements.txt` y CI (`actions/setup-python`) | Base del producer, tests y reglas de negocio |
| Pytest | Requerido en `requirements.txt` y ejecutado en CI | Validación del contrato y reglas |
| GitHub Actions | `.github/workflows/ci.yml` | CI con compileall, pytest y `docker compose config --quiet` |

Nota: en este checkout no aparece un Dockerfile de Airflow propio; la versión declarada es la imagen `rapidgo-airflow:2.10.5` en `docker-compose.yml`.

## 5. Prerrequisitos y ejecución local

Se recomienda seguir la guía real en [docs/contrato.md](docs/contrato.md) y la documentación de ejecución local disponible en [docs/checkpoints/semana1.md](docs/checkpoints/semana1.md) y en la rama de documentación de cierre. La ejecución esperada del proyecto es:

```bash
docker compose up -d --build
# esperar ~4 minutos para que la capa curated quede alimentada
python scripts/smoke_test.py
```

PENDIENTE: en este checkout no existe `scripts/smoke_test.py`; cuando esté disponible, se ejecuta desde la raíz del repo y debe validar el pipeline completo.

Acceso a Airflow:
- URL: `http://localhost:8080`
- Usuario: `admin`
- Para leer la contraseña del contenedor, se usa:

```bash
docker compose logs airflow | grep -i 'password\|admin'
```

PENDIENTE: confirmar la contraseña real del entorno si la imagen cambia o se genera un usuario distinto.

Reporte esperado del Data Lake:
- Ruta esperada: `datalake/reports/latest.md`
- PENDIENTE: este archivo no existe en este checkout; la generación real debe hacerse después de la ejecución del pipeline.

Advertencia: “empezar de cero” significa limpiar los checkpoints y el contenido generado antes de volver a ejecutar la demo.

```bash
docker compose down
rm -rf datalake/* _checkpoints 2>/dev/null || true
```

## 6. Topic, key, particiones, eventos, KPIs y reglas

El topic real del proyecto es:
- `rapidgo.observability.events`
- Key: `service`
- Particiones: 3
- Retención: 24h (`retention.ms=86400000`)
- Configuración en `docker-compose.yml` y `docs/decisions/ADR-001-key-particiones.md`

Cada evento sigue el contrato documentado en [docs/contrato.md](docs/contrato.md) y usa campos como:
- `event_id`, `timestamp`, `service`, `endpoint`, `http_method`, `http_status`, `latency_ms`, `region`

KPIs reales y reglas del repo:
- Error rate > 5 %
- p95 > 1000 ms
- mínimo 20 eventos por ventana
- ventana de 1 minuto
- watermark de 2 minutos

Estas reglas están consistentes entre `spark/config.py`, `spark/rules.py` y la documentación actual, con `window(event_time, "1 minute")` y `groupBy(..., "service")` en `spark/streaming_job.py`.

## 7. Cómo ejecutar pruebas y GitHub Actions

Pruebas locales:

```bash
python -m pytest -v
```

La validación de CI está definida en [.github/workflows/ci.yml](.github/workflows/ci.yml) y ejecuta:
- `python -m compileall producer spark tests`
- `pytest -v`
- `docker compose config --quiet`

Para disparar el workflow manualmente:
- GitHub -> `Actions` -> `CI` -> `Run workflow`

## 8. Cómo acceder a Airflow y ejecutar el DAG

La orquestación real del repo se documenta en la historia de git y en la evidencia de integración. En este checkout no hay un DAG presente como archivo activo, por lo que la parte operativa debe validarse en la rama de integración.

Se espera lo siguiente:
- 6 tareas en el DAG
- dependencias por orden de carga
- `retries=2` y `retry_delay=30s`
- uso de `XCom` para pasar resultados entre tareas
- disparo manual desde Airflow UI o CLI

El fallo controlado y la recuperación quedan documentados en [docs/checkpoints/semana1.md](docs/checkpoints/semana1.md) y [docs/checkpoints/semana2.md](docs/checkpoints/semana2.md).

## 9. Resultados y evidencias representativas

La evidencia que sí existe en este repo, sin inventar números de documentos no presentes, es la siguiente:

- `docs/decisions/ADR-001-key-particiones.md` reporta: 1175 eventos enviados, con particiones `p0=340`, `p1=708`, `p2=127`.
- `tests/test_rules.py` confirma que la alerta de error no dispara exactamente en 5 % y que la latencia crítica dispara solo por encima de 1000 ms.
- `git log` confirma commits y tags reales del proyecto: `v0.1.0`, `v.0.2.0`, `v0.3.0-orchestration`.

PENDIENTE: no existe `docs/evidencia/latest_semana4.md` ni `docs/evidencia/smoke_test_semana4.txt` en este checkout; no se puede citar “última evidencia” de esa semana sin esos archivos.

## 10. Decisiones y documentación

- [docs/decisions/ADR-001-key-particiones.md](docs/decisions/ADR-001-key-particiones.md)
- [docs/decisions/ADR-002-umbrales-y-sinks.md](docs/decisions/ADR-002-umbrales-y-sinks.md)
- [docs/checkpoints/semana1.md](docs/checkpoints/semana1.md)
- [docs/checkpoints/semana2.md](docs/checkpoints/semana2.md)
- [docs/contrato.md](docs/contrato.md)
- [docs/kpis-alertas.md](docs/kpis-alertas.md)

## 11. Limitaciones conocidas y trabajo futuro

- p95 calculado por servicio y no por endpoint; la agregación actual no presenta p95 por endpoint.
- El reparto de Kafka con `key=service` no está equilibrado: la partición 1 recibe ~60 % del tráfico según la evidencia real de ADR-001.
- Airflow se usa como orquestador local en Docker, no como entorno de producción.
- El producer genera datos sintéticos.
- El repo actual no incluye la carpeta `docs/evidencia` ni el script `scripts/smoke_test.py` que se mencionan en la guía más completa.
- Los umbrales y reglas están centralizados, pero la lógica de reglas y la lógica del job deben seguir revisándose si se cambia el modelo de agregación.
- Hay latencia de arranque y cierre del watermark: la ventana tarda en cerrarse en la capa `curated`.

## 12. Tabla de contribuciones

| Integrante | Responsabilidades | Issues | PR principales |
|---|---|---|---|
| Sebastian Valencia | Planeación general, cierre y documentación final, validación de entregables | PENDIENTE | PENDIENTE |
| Camilo Soler | Docs, Kanban, KPIs/alertas, tests y CI, README final, cierre de documentación | PENDIENTE | PENDIENTE |
| Javier Prieto | Kafka, ADR-001, DAG de Airflow, integración y estabilización | PENDIENTE | PENDIENTE |
| Smith | Contrato JSON, producer, diagrama, CI ampliado, quality gate y release | PENDIENTE | PENDIENTE |

Cuentas observadas en el historial de git:
- `CamilosolerB`
- `nagi24118`
- `smithvza`

No se asoció un usuario a un PR o issue sin verificarlo en el historial real. La correspondencia debe confirmarse con el propietario del repo antes de dejarla como definitiva.

## 13. Video final

PENDIENTE: enlace al video.

---

Este README se mantiene alineado con la evidencia real del repo: lo verificado está documentado, y lo no verificado queda claramente marcado como `PENDIENTE`.
