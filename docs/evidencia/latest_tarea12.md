# Reporte de observabilidad - RapidGo

Generado: 2026-11-06T01:21:38.385965+00:00
Archivos Parquet por capa: {'raw': 85, 'processed': 85, 'curated': 51}

## Calidad (processed)
- Filas: 15273 | fuera de contrato: 0 | event_id repetidos: 0
- Minutos desde el ultimo evento: 5.6

## KPIs por servicio (ultimos 120 min, 72 ventanas)

| Servicio | Eventos | Error rate | Disponibilidad | p95 max (ms) | Ventanas |
|---|---|---|---|---|---|
| auth-api | 1291 | 5.65% | 94.35% | 2079 | 12 |
| catalog-api | 1246 | 4.57% | 95.43% | 1925 | 12 |
| notifications-api | 625 | 0.96% | 99.04% | 370 | 12 |
| orders-api | 4939 | 0.91% | 99.09% | 305 | 12 |
| payments-api | 2465 | 5.15% | 94.85% | 1954 | 12 |
| tracking-api | 1841 | 4.67% | 95.33% | 2030 | 12 |

## Alertas: 16
- tasa_error_alta: 8
- latencia_critica: 8
  - 2026-11-05T23:56:00+00:00 catalog-api tasa_error_alta (valor 0.2417, umbral 0.05)
  - 2026-11-05T23:56:00+00:00 catalog-api latencia_critica (valor 1925, umbral 1000)
  - 2026-11-05T23:57:00+00:00 catalog-api tasa_error_alta (valor 0.1909, umbral 0.05)
  - 2026-11-05T23:57:00+00:00 catalog-api latencia_critica (valor 1616, umbral 1000)
  - 2026-11-05T23:58:00+00:00 auth-api tasa_error_alta (valor 0.3672, umbral 0.05)
  - 2026-11-05T23:58:00+00:00 auth-api latencia_critica (valor 2079, umbral 1000)
  - 2026-11-05T23:59:00+00:00 auth-api tasa_error_alta (valor 0.1478, umbral 0.05)
  - 2026-11-05T23:59:00+00:00 auth-api latencia_critica (valor 1835, umbral 1000)
  - 2026-11-06T00:00:00+00:00 payments-api tasa_error_alta (valor 0.3205, umbral 0.05)
  - 2026-11-06T00:00:00+00:00 payments-api latencia_critica (valor 1954, umbral 1000)

## Distribucion por particion de Kafka (raw)

| Particion | Eventos | Porcentaje |
|---|---|---|
| 0 | 4581 | 30.0% |
| 1 | 9118 | 59.7% |
| 2 | 1574 | 10.3% |

AVISO: la particion 1 concentra mas del 50% de los eventos.
