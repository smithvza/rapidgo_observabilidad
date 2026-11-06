# Checkpoint Semana 3 — fallo controlado y recuperación

**Estado:** Ejecutado en Docker Desktop · **DAG:** `rapidgo_observability_pipeline`
**Issues:** [#15 DAG de Airflow](https://github.com/smithvza/rapidgo_observabilidad/issues/15),
[#17 fallo controlado](https://github.com/smithvza/rapidgo_observabilidad/issues/17)

| Evidencia | Resultado real |
|---|---|
| Fallo | Airflow 2.10.5, corrida `manual__2026-11-05T00:00:00+00:00`: falló `check_sources` al encontrar el Data Lake vacío. Log: `rapidgo_checks.DataNotReadyError: Sin archivos Parquet en: raw, processed, curated (ruta /opt/airflow/datalake). Revisar que el job de Spark este corriendo; curated tarda en aparecer porque cada ventana se cierra con el watermark de 2 minutos.` |
| Reintentos e impacto | 3 intentos (2 retries, 30 s). `check_sources=failed`; `quality_check`, `partition_distribution`, `aggregate_kpis`, `evaluate_alerts` y `publish_report=upstream_failed`. No se publicó reporte. |
| Recuperación | Producer: 3.763 enviados, 0 inválidos, 0 errores de entrega. Spark registró `Queries activas: ['raw', 'processed', 'curated']`; se escribieron Parquet en las tres capas. Nueva corrida `manual__2026-11-06T02:06:00+00:00`: las 6 tareas terminaron `success`. |
| Verificación | `quality_check`: 3.763 filas, 0 inválidas, 0 duplicadas. `publish_report` escribió `datalake/reports/kpi_report_20261106T020748Z.md` y JSON; la corrida fue `success`, 0 alertas. |

El fallo se resolvió al habilitar la ingestión y dejar que Spark materializara
las capas; no fue necesario modificar el DAG. Reparto raw observado:
p0=1193, p1=2205, p2=365. Ver la estrategia y sus trade-offs en
[ADR-003](../decisions/ADR-003-orquestacion-y-recuperacion.md).

**Reproducción segura:** no vaciar ni borrar el Data Lake compartido. Se puede
usar una ruta aislada para el fallo y después correr la recuperación con la
ruta normal (PowerShell):

```powershell
python -m pip install -r requirements.txt
docker compose up -d --build
docker compose exec airflow airflow dags list
$failDate = (Get-Date).ToUniversalTime().AddMinutes(-2).ToString("yyyy-MM-ddTHH:mm:ss'Z'")
docker compose exec -T -e DATALAKE_PATH=/tmp/rapidgo-empty airflow airflow dags test rapidgo_observability_pipeline $failDate
docker compose exec -d spark /bin/bash -c 'timeout 420s /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy /opt/app/spark/streaming_job.py > /tmp/rapidgo-spark-week3.log 2>&1'
python -m producer.rapidgo_producer --rate 20 --duration 210 --incident-every 0
docker compose exec airflow python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); import rapidgo_checks as c; print(c.check_sources())"
$recoveryDate = (Get-Date).ToUniversalTime().AddMinutes(-1).ToString("yyyy-MM-ddTHH:mm:ss'Z'")
docker compose exec airflow airflow dags test rapidgo_observability_pipeline $recoveryDate
```

La corrida fallida devuelve código 1 por diseño. Esperar a que existan Parquet
en las tres capas antes de iniciar la corrida de recuperación. El proceso de
Spark se limita a 420 s para no dejar un stream de demostración indefinido.
