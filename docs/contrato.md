S# Contrato del evento - RapidGo Observabilidad (Variante 9)

**Archivos relacionados:**
- `samples/evento_ejemplo.json` (evento válido con error)
- `samples/evento_sano.json` (evento válido sin error)
- `samples/evento_invalido.json` (evento que debe ser rechazado)
- `samples/evento.schema.json` (JSON Schema para validación automática)

## 1. Descripción

Cada evento representa **una petición atendida por un servicio de RapidGo**.
El producer lo publica en Kafka (topic `rapidgo.observability.events`) y
Spark Structured Streaming lo consume para calcular error rate, p95 de
latencia y disponibilidad por servicio.

## 2. Campos

| Campo | Tipo | Obligatorio | Rango / regla válida | Ejemplo | Uso |
|---|---|---|---|---|---|
| `event_id` | string (UUID v4) | Sí | UUID válido, único por evento | `a3f1c9e2-7b44-4d1a-9c0e-52b1f8d6e001` | Trazabilidad y deduplicación |
| `timestamp` | string (ISO 8601 UTC) | Sí | Formato `YYYY-MM-DDTHH:MM:SS.mmmZ`, termina en `Z` | `2026-10-22T14:35:10.123Z` | Event time para la ventana de 1 min |
| `service` | string | Sí | Uno de: `orders-api`, `payments-api`, `tracking-api`, `auth-api`, `catalog-api`, `notifications-api` | `orders-api` | **Key de Kafka** y agrupación de KPIs |
| `endpoint` | string | Sí | Empieza con `/`, sin espacios | `/v1/orders` | Ubicar el punto exacto de falla |
| `http_method` | string | Sí | `GET`, `POST`, `PUT`, `PATCH`, `DELETE` | `POST` | Contexto de la petición |
| `http_status` | integer | Sí | Entre 100 y 599 | `500` | Error rate y disponibilidad |
| `latency_ms` | integer | Sí | Entre 0 y 60000 | `1240` | p95 de latencia |
| `region` | string | No | Texto con formato `pais-ciudad` en minúsculas | `co-bogota` | Segmentación opcional |

No se permiten campos adicionales (`additionalProperties: false`).

## 3. Reglas de validación

1. Todos los campos obligatorios deben estar presentes y no ser nulos.
2. `service` debe pertenecer a la lista cerrada. Un servicio desconocido hace el evento inválido.
3. `http_status` y `latency_ms` deben ser enteros, no strings ni decimales.
4. `timestamp` debe estar en UTC (sufijo `Z`).
5. Un evento inválido **no se publica** en el topic principal. El producer lo descarta y lo registra en el log.

## 4. Cómo se usan los campos en los KPIs

| KPI | Campos usados | Definición |
|---|---|---|
| Error rate | `service`, `http_status`, `timestamp` | eventos con `http_status >= 500` / total de eventos, por servicio y ventana de 1 min |
| Latencia p95 | `service`, `latency_ms`, `timestamp` | percentil 95 de `latency_ms`, por servicio y ventana de 1 min |
| Disponibilidad | `service`, `http_status`, `timestamp` | 1 − error rate (porcentaje de respuestas no 5xx) |

Los códigos 4xx se consideran errores del cliente y **no** cuentan como fallo
del servicio en el error rate.

## 5. Ejemplos

### 5.1 Evento válido con error (`samples/evento_ejemplo.json`)
```json
{
  "event_id": "a3f1c9e2-7b44-4d1a-9c0e-52b1f8d6e001",
  "timestamp": "2026-10-22T14:35:10.123Z",
  "service": "orders-api",
  "endpoint": "/v1/orders",
  "http_method": "POST",
  "http_status": 500,
  "latency_ms": 1240,
  "region": "co-bogota"
}
```

### 5.2 Evento válido sano (`samples/evento_sano.json`)
```json
{
  "event_id": "7c2d5e10-3a9b-4f8e-b1d4-90a6c3e2f102",
  "timestamp": "2026-10-22T14:35:11.480Z",
  "service": "tracking-api",
  "endpoint": "/v1/tracking/position",
  "http_method": "GET",
  "http_status": 200,
  "latency_ms": 85,
  "region": "co-bogota"
}
```

### 5.3 Evento inválido (`samples/evento_invalido.json`)
```json
{
  "event_id": "no-es-un-uuid",
  "timestamp": "22/10/2026 14:35",
  "service": "servicio-inexistente",
  "endpoint": "v1/orders",
  "http_method": "FETCH",
  "http_status": "500",
  "latency_ms": -20
}
```
Errores esperados: `event_id` sin formato UUID, `timestamp` fuera de ISO 8601,
`service` fuera de la lista, `endpoint` sin `/` inicial, `http_method` no
permitido, `http_status` como string y `latency_ms` negativo.

## 6. JSON Schema

Ver `samples/evento.schema.json`. Los tests de la Semana 2 validan los eventos
del producer y los samples contra este esquema.

## 7. Cambios del contrato

Cualquier cambio en los campos se hace por Pull Request con Issue asociado,
sube la versión de este documento y actualiza el schema y los tests.