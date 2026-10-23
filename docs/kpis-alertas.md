# KPIs y alertas de observabilidad tecnológica

Los indicadores se calculan sobre eventos de peticiones atendidas por los
servicios de RapidGo. El evento debe incluir, como mínimo, `service`,
`endpoint`, `latency_ms`, `http_status` y `timestamp`. Las ventanas usan el
timestamp del evento y tienen una duración inicial de un minuto.

## KPIs

| KPI | Definición | Fórmula | Ventana de tiempo | Por qué importa |
|---|---|---|---|---|
| Tasa de error | Porcentaje de eventos cuya respuesta HTTP indica un error del servidor (`http_status >= 500`), calculado por servicio. | `(eventos con http_status >= 500 / total de eventos) * 100` | 1 minuto, por servicio | Permite detectar aumentos de fallos del lado del servicio sin contar respuestas 4xx como fallos del servidor. |
| Latencia p95 | Percentil 95 de `latency_ms`, agrupado por servicio y endpoint. | `percentil_95(latency_ms)` del grupo | 1 minuto, por servicio y endpoint | Muestra la latencia que no supera el 95 % de las peticiones y ayuda a detectar degradaciones que un promedio podría ocultar. |
| Disponibilidad | Porcentaje de eventos con respuesta HTTP inferior a 500, calculado por servicio. | `(eventos con http_status < 500 / total de eventos) * 100` | 1 minuto, por servicio | Resume la proporción de respuestas que no son errores del servidor; permite seguir el impacto de los errores 5xx. |

## Reglas de alerta

Los umbrales son iniciales; deben revisarse y ajustarse con datos reales de
tráfico y comportamiento de los servicios.

| Alerta | Condición | Severidad | Justificación del umbral |
|---|---|---|---|
| Tasa de error alta | `error_rate > 5 %` en una ventana de 1 minuto, por servicio. | Alta | Un porcentaje superior al 5 % de respuestas 5xx en un intervalo corto puede señalar una degradación relevante. Es un umbral inicial y se ajustará con datos reales. |
| Latencia crítica | `latencia_p95 > 1000 ms` en una ventana de 1 minuto, por servicio y endpoint. | Crítica | Que el 5 % de las peticiones más lentas supere un segundo puede afectar la experiencia de uso. Es un umbral inicial y se ajustará con datos reales. |
