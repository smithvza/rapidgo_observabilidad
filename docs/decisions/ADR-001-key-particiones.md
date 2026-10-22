# ADR-001 - Key y particiones del topic de Kafka

**Estado:** Aceptada (Semana 1)
**Responsable:** Javier Prieto
**Issue relacionado:** #5

## Contexto / problema

Cada evento del pipeline es una petición atendida por un servicio de RapidGo
(ver `docs/contrato.md`). Spark Structured Streaming calcula tres KPIs por
servicio y ventana de 1 minuto: error rate, latencia p95 y disponibilidad.

Hay que decidir cómo se publican esos eventos en Kafka: qué key usar y cuántas
particiones tendrá el topic, para que el procesamiento por servicio sea
consistente y se pueda paralelizar.

## Opciones consideradas

1. **Key = `service`.** Todos los eventos de un mismo servicio caen en la misma
   partición.
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

Esto ya está configurado en `docker-compose.yml` (servicio `kafka-init`).

## Evidencia utilizada

- El contrato define una lista cerrada de 6 servicios: `orders-api`,
  `payments-api`, `tracking-api`, `auth-api`, `catalog-api` y
  `notifications-api`.
- Los tres KPIs mínimos de la variante 9 se calculan por servicio, por eso la
  key natural es `service`.
- Calculamos con el algoritmo murmur2 del particionador por defecto de Kafka a
  qué partición cae cada key (cálculo teórico, todavía no es una medición del
  producer real):

| Servicio | Partición con 3 particiones |
|---|---|
| orders-api | 2 |
| payments-api | 2 |
| tracking-api | 0 |
| auth-api | 0 |
| catalog-api | 0 |
| notifications-api | 0 |

Resultado: la partición 0 recibe 4 servicios, la partición 2 recibe 2 y la
partición 1 no recibe ninguno.

## Consecuencias / trade-offs

- (+) Los eventos de un mismo servicio quedan ordenados dentro de una partición.
- (+) La key sale directo del campo `service`, que es obligatorio y está
  validado en el contrato.
- (-) Con solo 6 keys el reparto queda desbalanceado: según el cálculo, la
  partición 1 queda vacía y la 0 concentra 4 de los 6 servicios. En la práctica
  solo se usarían 2 de las 3 particiones.
- (-) Subir el número de particiones después cambia a qué partición va cada key.
  Subir a 6 tampoco arregla el reparto (según el mismo cálculo solo se usan 3
  particiones, con 3/1/2 servicios).
- Spark hace su propio shuffle en el `groupBy`, así que el desbalance afecta el
  paralelismo de lectura desde Kafka, no la corrección de los KPIs.

**Pendiente para la Semana 2:** verificar con el producer corriendo cuántos
eventos caen realmente en cada partición (offsets por partición). Si el
desbalance se confirma, se evalúa en el ADR-002 cambiar la key (por ejemplo
`service` + `endpoint`) o ajustar el número de particiones.
