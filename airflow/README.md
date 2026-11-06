# Airflow - RapidGo Observabilidad

DAG `rapidgo_observability_pipeline`: verifica el Data Lake que escribe Spark,
consolida los KPIs, evalua las alertas y publica un reporte en `datalake/reports/`.

## Tareas

| Tarea | Que hace | Falla si |
|---|---|---|
| `check_sources` | Cuenta archivos Parquet en raw, processed y curated | Alguna capa no tiene archivos |
| `quality_check` | Revisa processed: contrato, duplicados, frescura | Hay eventos fuera del contrato o mas de 1 % de event_id repetidos |
| `partition_distribution` | Cuenta eventos por particion de Kafka (raw) | Nunca falla; avisa si una particion supera el 50 % |
| `aggregate_kpis` | Consolida las ventanas de curated por servicio | No hay ventanas en los ultimos 120 min |
| `evaluate_alerts` | Aplica las 2 reglas de `docs/kpis-alertas.md` | Nunca falla; las alertas son resultado, no error |
| `publish_report` | Escribe `kpi_report_<fecha>.json`, `.md` y `latest.md` | Si falla una tarea anterior no se ejecuta |

Dependencias: `check_sources` alimenta a `quality_check`, `partition_distribution` y
`aggregate_kpis`; `aggregate_kpis` alimenta a `evaluate_alerts`; `publish_report` espera
a todas. Las tareas se pasan datos con XCom (el valor que retorna cada una).
Cada tarea reintenta 2 veces con 30 s de espera.

## Como ejecutarlo

```
docker compose up -d --build airflow
```

La primera vez tarda 1-2 minutos. Interfaz: http://localhost:8080 (usuario `admin`).
Contraseña:

```
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

En la interfaz, activa el DAG (interruptor) y usa **Trigger DAG**. Para que haya datos,
Spark y el producer deben haber corrido antes (ver README principal).

## Pruebas

```
python -m pytest tests/test_dag_logic.py
```

`tests/test_dag_integrity.py` revisa el DAG con Airflow y se salta si no esta instalado.