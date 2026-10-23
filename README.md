# RapidGo 360 - Observabilidad tecnológica

Proyecto final de Big Data de la Universidad de Cundinamarca. Esta iniciativa
aborda la **variante 9: Observabilidad tecnológica**.

## Equipo

- Sebastian Valencia
- Camilo Soler
- Javier Prieto

## Problema y valor de la solución

La degradación de las APIs y los servicios de RapidGo puede elevar errores y
tiempos de respuesta sin ofrecer una señal oportuna para detectar el impacto.
La solución propone procesar eventos de observabilidad en streaming, conservar
los datos por etapas y producir indicadores y alertas para identificar fallos
HTTP y latencias críticas.

## Arquitectura

```text
Fuente -> Kafka -> Spark -> Data Lake -> Airflow -> Indicadores/Alertas
```

| Tecnología | Rol en la solución |
|---|---|
| Producer / fuente de eventos | Genera eventos de peticiones con servicio, endpoint, latencia, estado HTTP y timestamp. |
| Kafka | Recibe y distribuye los eventos del flujo. |
| Spark Structured Streaming | Consume y transforma eventos en ventanas para calcular indicadores de observabilidad. |
| Data Lake | Conserva datos organizados en las capas Raw, Processed y Curated. |
| Airflow | Orquesta las tareas del pipeline. |
| Indicadores y alertas | Exponen error rate, latencia p95 y disponibilidad, y señalan condiciones críticas. |

**Pendientes de validar/completar:**

- Diagrama en `docs/arquitectura.png`: **PENDIENTE: completar con datos reales y validar que refleje la arquitectura implementada**.
- Contrato JSON: **PENDIENTE: completar con datos reales y validar el contrato final**. Hay un borrador en `docs/contrato.md` y un schema en `samples/evento.schema.json`.
- Pasos de ejecución local: **PENDIENTE: completar con datos reales y validar los comandos de extremo a extremo**.

## Estado

**Semana 1 en curso** — fase de diseño y organización. La documentación y la
ejecución del pipeline aún deben validarse con datos reales. Release esperado:
`v0.1.0-foundation`.