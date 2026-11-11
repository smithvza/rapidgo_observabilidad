# Reporte de observabilidad - RapidGo

Generado: 2026-11-11T00:01:22.004230+00:00
Archivos Parquet por capa: {'raw': 54, 'processed': 54, 'curated': 29}

## Calidad (processed)
- Filas: 11191 | fuera de contrato: 0 | event_id repetidos: 0
- Minutos desde el ultimo evento: 0.3

## KPIs por servicio (ultimos 120 min, 36 ventanas)

| Servicio | Eventos | Error rate | Disponibilidad | p95 max (ms) | Ventanas |
|---|---|---|---|---|---|
| auth-api | 670 | 0.90% | 99.10% | 314 | 6 |
| catalog-api | 657 | 0.91% | 99.09% | 284 | 6 |
| notifications-api | 332 | 9.04% | 90.96% | 1780 | 6 |
| orders-api | 2943 | 1.19% | 98.81% | 289 | 6 |
| payments-api | 1350 | 0.96% | 99.04% | 280 | 6 |
| tracking-api | 1110 | 8.20% | 91.80% | 1749 | 6 |

## Alertas: 8
- tasa_error_alta: 4
- latencia_critica: 4
  - 2026-11-10T23:53:00+00:00 tracking-api tasa_error_alta (valor 0.3158, umbral 0.05)
  - 2026-11-10T23:53:00+00:00 tracking-api latencia_critica (valor 1749, umbral 1000)
  - 2026-11-10T23:54:00+00:00 tracking-api tasa_error_alta (valor 0.1386, umbral 0.05)
  - 2026-11-10T23:54:00+00:00 tracking-api latencia_critica (valor 1543, umbral 1000)
  - 2026-11-10T23:55:00+00:00 notifications-api tasa_error_alta (valor 0.3158, umbral 0.05)
  - 2026-11-10T23:55:00+00:00 notifications-api latencia_critica (valor 1780, umbral 1000)
  - 2026-11-10T23:56:00+00:00 notifications-api tasa_error_alta (valor 0.1957, umbral 0.05)
  - 2026-11-10T23:56:00+00:00 notifications-api latencia_critica (valor 1642, umbral 1000)

## Distribucion por particion de Kafka (raw)

| Particion | Eventos | Porcentaje |
|---|---|---|
| 0 | 3318 | 29.6% |
| 1 | 6780 | 60.6% |
| 2 | 1093 | 9.8% |

AVISO: la particion 1 concentra mas del 50% de los eventos.
