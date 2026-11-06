"""DAG de control del pipeline RapidGo - Observabilidad (Variante 9).

Arquitectura: calidad -> agregacion -> reporte (README, seccion de Airflow).
Spark escribe raw/processed/curated en el Data Lake; este DAG verifica esos
datos, consolida los KPIs, evalua las alertas y publica un reporte.

Las tareas se pasan datos entre si con XCom (el return de cada tarea).
Toda la logica esta en rapidgo_checks.py, que se prueba con pytest.
"""
from __future__ import annotations

from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

import rapidgo_checks as checks

default_args = {
    "owner": "equipo-rapidgo",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="rapidgo_observability_pipeline",
    description="Control de calidad, KPIs y alertas del Data Lake de observabilidad",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["rapidgo", "observabilidad", "variante-9"],
)
def rapidgo_observability_pipeline():
    @task
    def check_sources() -> dict:
        """Verifica que raw, processed y curated tengan archivos Parquet."""
        return checks.check_sources()

    @task
    def quality_check(sources: dict) -> dict:
        """Control de calidad sobre processed (falla si hay datos fuera del contrato)."""
        records = checks.read_parquet_records(
            f"{checks.DATALAKE_PATH}/processed",
            columns=["event_id", "service", "endpoint", "http_method",
                     "http_status", "latency_ms", "event_time"],
        )
        return checks.check_quality(records)

    @task
    def partition_distribution(sources: dict) -> dict:
        """Cuenta eventos por particion de Kafka (evidencia para ADR-001/002)."""
        records = checks.read_parquet_records(
            f"{checks.DATALAKE_PATH}/raw", columns=["kafka_partition"]
        )
        return checks.partition_distribution(records)

    @task
    def aggregate_kpis(sources: dict) -> dict:
        """Consolida las ventanas de curated por servicio."""
        windows = checks.read_parquet_records(
            f"{checks.DATALAKE_PATH}/curated",
            columns=["window_start", "service", "total_events", "error_events",
                     "p95_latency_ms"],
        )
        return checks.aggregate_windows(windows)

    @task
    def evaluate_alerts(kpis: dict) -> dict:
        """Aplica las reglas de alerta a cada ventana consolidada."""
        return checks.evaluate_alerts(kpis["windows"])

    @task
    def publish_report(sources: dict, quality: dict, partitions: dict,
                       kpis: dict, alerts: dict) -> dict:
        """Escribe el reporte JSON y Markdown en datalake/reports."""
        report = checks.build_report(sources, quality, partitions, kpis, alerts)
        return checks.write_report(report)

    sources = check_sources()
    quality = quality_check(sources)
    partitions = partition_distribution(sources)
    kpis = aggregate_kpis(sources)
    alerts = evaluate_alerts(kpis)
    publish_report(sources, quality, partitions, kpis, alerts)


rapidgo_observability_pipeline()
