# ADR-001: key y particiones de Kafka

**Estado: BORRADOR: pendiente de validar con el equipo y con la ejecución real.**

## Contexto/problema

La variante 9 procesa eventos de observabilidad que incluyen, como mínimo,
`service`, `endpoint`, `latency_ms`, `http_status` y `timestamp`. Se requiere
proponer una key para el topic de eventos y un número de particiones apropiado
para el entorno local con Docker. La key debe considerar el orden de los
eventos y el agrupamiento de los KPIs.

## Opciones consideradas

| Opción | Ventajas | Desventajas |
|---|---|---|
| Key = `service` | Todos los eventos de un servicio se asignan a la misma partición con una cantidad fija de particiones. Kafka conserva el orden en que esos eventos se escriben en dicha partición. Coincide con la agrupación principal de los KPIs. | Un servicio con mucho más tráfico puede generar una partición más cargada. No garantiza orden por timestamp de evento ni equilibrio entre particiones. |
| Key = `endpoint` | Mantiene juntos los eventos con el mismo valor de endpoint y puede distribuir los endpoints entre particiones. | Los endpoints de un mismo servicio pueden quedar en particiones distintas; no conserva un orden único por servicio. |
| Sin key | No se vinculan deliberadamente los eventos de un servicio a una partición mediante una key. | No hay garantía de que los eventos de un servicio se escriban en una sola partición ni de conservar su orden conjunto. |

## Decisión tomada

**Recomendación para validar:** usar `service` como key del mensaje y comenzar
con **3 particiones** para el topic `rapidgo.observability.events` en Docker
local. La key favorece la afinidad y el orden de escritura por servicio,
relevantes para inspeccionar y agrupar sus eventos. Tres particiones ofrecen
paralelismo acotado para desarrollo sin añadir muchos recursos a un despliegue
local de un solo broker.

El número es una propuesta inicial, no una conclusión basada en mediciones. La
configuración estática actual de `docker-compose.yml` ya declara 3 particiones
y factor de replicación 1; esta ADR no afirma que el topic se haya creado ni
que el pipeline se haya ejecutado.

## Evidencia utilizada

| Evidencia | Uso y alcance |
|---|---|
| Guía y presentación del proyecto | Definen la variante 9 y requieren decidir la key y el número de particiones. No determinan una configuración numérica óptima. |
| `docs/contrato.md` | Define `service` como campo obligatorio y key de Kafka, y describe los KPIs por servicio. Es evidencia del contrato documentado, no de una ejecución. |
| `docker-compose.yml` | Declara `KAFKA_NUM_PARTITIONS: 3`, el topic con 3 particiones y replicación 1. Es configuración versionada; no confirma que se haya desplegado correctamente. |
| Métricas, offsets o ejecución real | **PENDIENTE: completar con datos reales.** No se dispone aquí de evidencia observada para medir el reparto o validar el comportamiento. |

## Consecuencias/trade-offs

- Los eventos con la misma key `service` se dirigen a la misma partición mientras
  se mantenga fijo el número de particiones. El orden garantizado es el de
  escritura en Kafka, no necesariamente el orden de sus timestamps.
- Tres particiones permiten paralelismo de lectura limitado por partición y
  mantienen sencilla la configuración local de un solo broker, pero no
  aseguran una distribución uniforme de servicios o de carga.
- Con una sola instancia de broker, el factor de replicación 1 no proporciona
  redundancia ante la pérdida de ese broker.
- Cambiar el número de particiones puede cambiar la asignación de una key y
  afectar expectativas de orden. Antes de ajustar la configuración se deben
  revisar el tráfico real, la distribución por partición y las necesidades de
  paralelismo.
- Validar esta recomendación con el equipo y con una ejecución del producer y
  del pipeline; actualizar la decisión si las mediciones justifican otra
  configuración.
