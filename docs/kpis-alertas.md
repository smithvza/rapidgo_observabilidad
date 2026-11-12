# KPIs y alertas de observabilidad tecnológica

Los indicadores se calculan sobre eventos de peticiones atendidas por los
servicios de RapidGo. El código actual agrega por `window(event_time, "1 minute")`
y `service`, como se ve en `spark/streaming_job.py`; no se agrupa por endpoint en la
salida final del curated sink.

## KPIs

| KPI | Definición | Fórmula | Ventana de tiempo | Por qué importa |
|---|---|---|---|---|
| Tasa de error | Porcentaje de eventos cuyo `http_status` es `>= 500`, calculado por servicio. | `(eventos con http_status >= 500 / total de eventos) * 100` | 1 minuto, por servicio | Permite detectar aumentos de fallos del lado del servicio sin contar respuestas 4xx como fallos del servidor. |
| Latencia p95 | Percentil aproximado 95 de `latency_ms`, calculado por servicio. | `percentile_approx(latency_ms, 0.95)` | 1 minuto, por servicio | Indica el umbral de latencia por debajo del cual se encuentra aproximadamente el 95 % de las peticiones y ayuda a detectar degradaciones que un promedio podría ocultar. |
| Disponibilidad | Porcentaje de eventos con respuesta HTTP inferior a 500, calculado por servicio. | `(eventos con http_status < 500 / total de eventos) * 100` | 1 minuto, por servicio | Resume la proporción de respuestas que no son errores del servidor; permite seguir el impacto de los errores 5xx. |

## Reglas de alerta

Las alertas solo se evalúan cuando hay al menos 20 eventos válidos por servicio y
ventana (`MIN_EVENTS_PER_WINDOW = 20`), según `spark/config.py`.

| Alerta | Condición | Severidad | Justificación del umbral |
|---|---|---|---|
| Tasa de error alta | `error_rate > 5 %` en una ventana de 1 minuto, por servicio. | Alta | Un porcentaje superior al 5 % de respuestas 5xx en un intervalo corto puede señalar una degradación relevante. |
| Latencia crítica | `p95_latency_ms > 1000 ms` en una ventana de 1 minuto, por servicio. | Crítica | Si el percentil 95 supera 1000 ms, la degradación es visible para una parte importante del tráfico. |

La fuente de verdad del umbral es `spark/config.py`; los tests de `spark/rules.py`
validan el mismo comportamiento y `spark/streaming_job.py` usa esos mismos valores en
la lógica del job.
