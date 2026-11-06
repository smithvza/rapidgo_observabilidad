# Checkpoint Semana 3 — fallo controlado

**Estado:** BLOQUEADO — ejecución pendiente  
**Issues:** [#15 DAG de Airflow](https://github.com/smithvza/rapidgo_observabilidad/issues/15),
[#17 fallo controlado](https://github.com/smithvza/rapidgo_observabilidad/issues/17)

| Dato requerido | Evidencia |
|---|---|
| Tarea fallida | **PENDIENTE:** no hay DAG Python en `airflow/dags/`; solo existe `.gitkeep`. |
| Mensaje real del log | **PENDIENTE:** no se ejecutó una tarea. No se inventa un log. |
| Tareas afectadas / impacto | **PENDIENTE:** la secuencia real de tareas aún no está versionada. |
| Cambio que resolvió el fallo | **PENDIENTE:** requiere el DAG de Javier y una recuperación exitosa observada. |

`docker-compose.yml` solo define Kafka, `kafka-init` y Spark (no Airflow). En
esta sesión Docker informó `Docker Desktop is unable to start`, por lo que no
se provocó un fallo ni se registró una recuperación. El método de inyección
(Kafka detenido o raw vacío) queda pendiente de revisar contra las tareas
reales; no se modificó el trabajo de Javier ni se escribió un DAG.

Para recolectar evidencia cuando exista el DAG y un entorno Airflow en WSL2,
inicializar Airflow (Python 3.13; versión probada oficialmente por Airflow
3.1.3):

```bash
set -o pipefail
python3.13 -m venv /tmp/rapidgo-airflow-venv
source /tmp/rapidgo-airflow-venv/bin/activate
python -m pip install "apache-airflow==3.1.3" "apache-airflow-providers-standard" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.3/constraints-3.13.txt"
export AIRFLOW_HOME=/tmp/rapidgo-airflow
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags"
airflow db migrate
docker compose up -d
airflow dags list
DAG_ID="REEMPLAZAR_CON_EL_ID_REAL"
airflow dags test "$DAG_ID" "2026-11-06T00:00:00+00:00" 2>&1 | tee semana3-fallo.log
```

Después de corregir la causa, ejecutar con una fecha lógica nueva y conservar
su salida. Reemplazar la fecha por una fecha ISO 8601 distinta:

```bash
set -o pipefail
airflow dags test "$DAG_ID" "2026-11-06T00:01:00+00:00" 2>&1 | tee semana3-recuperacion.log
```

Completar con los task IDs, el log literal, las tareas downstream omitidas y
la salida de ambas ejecuciones. La estrategia propuesta está pendiente en
[ADR-003](../decisions/ADR-003-orquestacion-y-recuperacion.md).
