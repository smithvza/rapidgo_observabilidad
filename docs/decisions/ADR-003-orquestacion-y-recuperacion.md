# ADR-003 - Orquestación y recuperación ante fallos

**Estado:** Pendiente — falta el DAG y una ejecución observada  
**Issues relacionados:** [#15 DAG de Airflow](https://github.com/smithvza/rapidgo_observabilidad/issues/15),
[#17 fallo controlado](https://github.com/smithvza/rapidgo_observabilidad/issues/17)

## Contexto/problema

La secuencia esperada del curso es comprobar fuentes, validar raw, agregar,
validar curated y generar un reporte. En el repositorio y las ramas remotas
inspeccionadas solo existe `airflow/dags/.gitkeep`; además,
`docker-compose.yml` no contiene un servicio Airflow. No hay tareas, retries,
retrasos ni comportamiento ante fallos que se puedan confirmar.

## Opciones

1. Reintentar todas las tareas con un delay fijo, incluso validaciones
   deterministas.
2. Reintentar fallos transitorios de fuentes; detener el flujo ante
   validaciones fallidas y bloquear las tareas downstream.
3. Continuar el DAG después de una validación fallida y publicar un reporte
   parcial.

## Decisión

**No se adopta todavía una estrategia ejecutable.** Como propuesta para revisar
con Javier: retries limitados con delay solo en operaciones transitorias;
una validación inválida debe fallar explícitamente y no habilitar agregación,
reporte ni tareas downstream. Tras corregir los datos o la fuente, ejecutar
una nueva corrida identificable. La configuración concreta (número de
retries, delay, política de retry y task IDs) queda **PENDIENTE** hasta revisar
el DAG real y reproducir un fallo.

## Evidencia

- En `airflow/dags/` solo está `.gitkeep` en `main` y en todas las ramas
  remotas obtenidas con `git fetch --all --tags`.
- Los servicios declarados en `docker-compose.yml` son Kafka, `kafka-init` y
  Spark; no hay Airflow.
- En esta sesión `docker info` devolvió: `Docker Desktop is unable to start`.
  No se ejecutó un DAG ni se capturó un log de tarea.
- El checkpoint de Semana 3 mantiene explícitamente pendientes el fallo, su
  impacto y la recuperación; no contiene valores de ejecución inventados.

## Consecuencias y trade-offs

- No se afirma que haya retries o recuperación automática implementados.
- Reintentar errores deterministas puede repetir trabajo sin resolver la causa;
  no reintentar errores transitorios puede requerir intervención innecesaria.
- Detener downstream tras una validación protege `curated` y el reporte de
  datos inválidos, pero retrasa la entrega hasta una nueva ejecución.
- Cerrar esta ADR requiere acordar la política con el autor del DAG y adjuntar
  el log real del fallo y el resultado de una corrida exitosa posterior.
