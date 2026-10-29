# ADR-001 - Key y particiones del topic de Kafka

**Estado:** Aceptada (Semana 1), evidencia corregida con ejecución real (Semana 2)
**Responsable:** Javier Prieto
**Issues relacionados:** #8, #12

## Contexto / problema

Cada evento del pipeline es una petición atendida por un servicio de RapidGo
(ver `docs/contrato.md`). Spark Structured Streaming calcula tres KPIs por
servicio y ventana de 1 minuto: error rate, latencia p95 y disponibilidad.

Hay que decidir cómo se publican esos eventos en Kafka: qué key usar y cuántas
particiones tendrá el topic, para que el procesamiento por servicio sea
consistente y se pueda paralelizar.

## Opciones consideradas

1. **Key = `service`.** Todos los eventos de un mismo servicio caen siempre en
   la misma partición.
2. **Key = `service` + `endpoint`.** Reparte más, pero un servicio queda
   dividido entre varias particiones.
3. **Sin key (round-robin).** Es el reparto más parejo, pero no hay ningún
   orden ni agrupación por servicio en Kafka.

## Decisión tomada

- Topic: `rapidgo.observability.events`
- Key: `service`
- Particiones: 3
- Replication factor: 1 (un solo broker en Docker local)
- Retención: 24 horas (`retention.ms=86400000`)

Esto está configurado en `docker-compose.yml` (servicio `kafka-init`).

## Evidencia utilizada

Corrida real del producer (`python -m producer.rapidgo_producer --rate 20
--duration 60 --incident-every 0`) contra Kafka en Docker, 28/10/2026:

- 1175 eventos enviados, 0 errores de entrega.
- Offsets por partición (`kafka-get-offsets.sh`): partición 0 = 340,
  partición 1 = 708, partición 2 = 127. Suman 1175 y coinciden con el conteo
  que imprimió el producer.

| Partición | Servicios que cayeron ahí | Eventos | % del total |
|---|---|---|---|
| 0 | tracking-api, catalog-api, notifications-api | 340 | 28.9 % |
| 1 | orders-api, payments-api | 708 | 60.3 % |
| 2 | auth-api | 127 | 10.8 % |

El reparto sale de aplicar CRC32 a la key (el particionador por defecto de
librdkafka, que usa `confluent_kafka`) y calcular módulo 3. Con esa cuenta
orders-api y payments-api caen en la partición 1, tracking-api, catalog-api y
notifications-api en la 0, y auth-api en la 2, que es lo observado.

Nota de corrección: la versión anterior de este ADR calculaba la partición con
el algoritmo del cliente Java (murmur2) y concluía que la partición 1 quedaba
vacía. Ese cálculo no aplica al producer del proyecto, y la ejecución real lo
desmintió.

## Consecuencias / trade-offs

- (+) Los eventos de un servicio quedan ordenados dentro de una partición.
- (+) Las 3 particiones se usan.
- (-) El reparto está desbalanceado: la partición 1 recibe el 60 % de los
  eventos porque orders-api (40 % del tráfico) y payments-api (20 %) comparten
  partición. La partición 2 recibe solo el 10 %.
- (-) El reparto depende del cliente: con otro cliente (por ejemplo uno Java con
  murmur2) las mismas keys caerían en otras particiones.
- (-) Subir el número de particiones después cambia a qué partición va cada key.
- Spark hace su propio shuffle en el `groupBy`, así que el desbalance afecta el
  paralelismo de lectura desde Kafka, no la corrección de los KPIs.

**Pendiente:** evaluar en el ADR-002 si conviene cambiar la key (por ejemplo
`service` + `endpoint`) o el número de particiones, midiendo de nuevo los
offsets por partición.
