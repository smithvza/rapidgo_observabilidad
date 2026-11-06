# ADR-003 - Orquestación y recuperación ante fallos

**Estado:** Validada mediante ejecución local con Airflow 2.10.5
**Issues:** [#15 DAG de Airflow](https://github.com/smithvza/rapidgo_observabilidad/issues/15),
[#17 fallo controlado](https://github.com/smithvza/rapidgo_observabilidad/issues/17)

## Contexto/problema

El DAG `rapidgo_observability_pipeline` coordina seis tareas: `check_sources`,
`quality_check`, `partition_distribution`, `aggregate_kpis`, `evaluate_alerts`
y `publish_report`. Si Spark aún no ha escrito raw, processed y curated, no es
seguro continuar con calidad, KPIs ni reporte.

## Opciones

1. Reintentar tareas con límite y retraso para tolerar demoras transitorias de
   la ingestión.
2. Fallar sin reintentos ante cualquier falta de datos.
3. Continuar el DAG y publicar un reporte parcial aunque falle una validación.

## Decisión

- Se conservan `retries=2` y `retry_delay=30 s` del `default_args` del DAG.
- Si una capa no contiene archivos Parquet, `check_sources` lanza
  `DataNotReadyError` con las capas faltantes y la ruta.
- Las dependencias usan el comportamiento por defecto `all_success`; si falla
  la verificación de fuentes, las cinco tareas posteriores quedan
  `upstream_failed` y no se publica un reporte parcial.
- La recuperación consiste en restaurar el productor/Spark o los datos,
  comprobar que estén disponibles las tres capas y ejecutar una nueva corrida
  del DAG. Se conservan los resultados de la corrida fallida para diagnóstico.

## Evidencia

- En Docker Desktop, Airflow 2.10.5 ejecutó la corrida
  `manual__2026-11-05T00:00:00+00:00` con el Data Lake vacío. `check_sources`
  falló en los intentos 1, 2 y 3; la excepción fue:
  `Sin archivos Parquet en: raw, processed, curated (ruta /opt/airflow/datalake). Revisar que el job de Spark este corriendo; curated tarda en aparecer porque cada ventana se cierra con el watermark de 2 minutos.`
- El estado leído con `airflow tasks states-for-dag-run` fue
  `check_sources=failed`; las otras cinco tareas quedaron `upstream_failed`.
  La ejecución terminó con `DagRun failed` y código 1.
- Después se ejecutó el producer por 210 s: `Enviados: 3763 | invalidos
  descartados: 0 | errores de entrega: 0`. Spark registró
  `Queries activas: ['raw', 'processed', 'curated']`. Airflow leyó 18 archivos
  Parquet raw, 18 processed y 11 curated; `quality_check` encontró 3763 filas,
  0 inválidas y 0 duplicadas.
- La nueva corrida
  `manual__2026-11-06T02:06:00+00:00` finalizó `state=success`: las seis
  tareas `success`, reporte generado en `datalake/reports/`, 0 alertas.

## Consecuencias y trade-offs

- Dos reintentos con 30 s ayudan si el job acaba de iniciar, pero añaden hasta
  aproximadamente un minuto antes de declarar un fallo persistente.
- El chequeo valida todas las capas juntas; el log identifica exactamente las
  capas vacías. No se produce un reporte parcial ni se enmascara la excepción.
- La recuperación explícita con una nueva corrida conserva el historial, pero
  requiere que el productor y Spark vuelvan a estar disponibles.
- La evidencia se puede reproducir sin borrar datos usando una ruta temporal
  vacía para la corrida fallida; ver el comando en
  [checkpoint Semana 3](../checkpoints/semana3.md).
