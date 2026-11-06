"""Pruebas de integridad del DAG. Se saltan si Airflow no esta instalado;
se activan solo cuando el CI instala apache-airflow."""
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_ID = "rapidgo_observability_pipeline"
sys.path.insert(0, str(DAGS_DIR))


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dags_import_without_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_dag_has_at_least_five_tasks(dag_bag):
    assert len(dag_bag.dags[DAG_ID].tasks) >= 5


def test_every_task_has_retries(dag_bag):
    for task in dag_bag.dags[DAG_ID].tasks:
        assert task.retries >= 1, task.task_id


def test_dependencies(dag_bag):
    dag = dag_bag.dags[DAG_ID]
    assert dag.get_task("check_sources").upstream_task_ids == set()
    for task_id in ("quality_check", "partition_distribution", "aggregate_kpis"):
        assert dag.get_task(task_id).upstream_task_ids == {"check_sources"}
    assert dag.get_task("evaluate_alerts").upstream_task_ids == {"aggregate_kpis"}
    assert dag.get_task("publish_report").upstream_task_ids == {
        "check_sources", "quality_check", "partition_distribution",
        "aggregate_kpis", "evaluate_alerts",
    }


def test_tasks_expose_results_through_xcom(dag_bag):
    dag = dag_bag.dags[DAG_ID]
    assert all(task.do_xcom_push for task in dag.tasks)
