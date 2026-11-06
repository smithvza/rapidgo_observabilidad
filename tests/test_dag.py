from pathlib import Path

import pytest

DAG_DIR = Path(__file__).parent.parent / "airflow" / "dags"
DAG_FILES = tuple(
    path
    for path in DAG_DIR.glob("*.py")
    if path.name != "__init__.py"
)


@pytest.mark.skipif(
    not DAG_FILES,
    reason="Airflow DAGs are not present yet; this test activates when a DAG .py file is added.",
)
def test_airflow_dag_structure() -> None:
    from airflow.models.dagbag import DagBag

    dag_bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
    import_errors = "\n".join(
        f"{path}: {error}" for path, error in dag_bag.import_errors.items()
    )
    assert not dag_bag.import_errors, f"Airflow DAG import errors:\n{import_errors}"

    dags = list(dag_bag.dags.values())
    assert dags, f"No DAGs were loaded from {DAG_DIR}"
    dag = next((candidate for candidate in dags if len(candidate.tasks) >= 5), None)
    assert dag is not None, "No loaded DAG has the required five or more tasks"

    task_groups = (
        ("source check", ("check_sources", "check_source", "check_fuentes", "fuentes")),
        ("raw validation", ("validate_raw", "validar_raw", "raw_validation")),
        ("aggregation", ("aggregate", "agregar", "aggregation", "agregacion")),
        (
            "curated validation",
            ("validate_curated", "validar_curated", "curated_validation"),
        ),
        ("report", ("report", "reporte")),
    )
    tasks_by_phase = []
    for phase, aliases in task_groups:
        matches = [
            task
            for task in dag.tasks
            if any(alias in task.task_id.lower() for alias in aliases)
        ]
        assert len(matches) == 1, (
            f"Expected exactly one task for {phase}; found "
            f"{[task.task_id for task in matches]}"
        )
        tasks_by_phase.append(matches[0])

    for upstream, downstream in zip(tasks_by_phase, tasks_by_phase[1:]):
        assert downstream.task_id in upstream.downstream_task_ids, (
            f"Expected {upstream.task_id} -> {downstream.task_id}"
        )

    assert any(task.retries is not None and task.retries > 0 for task in dag.tasks), (
        "No DAG task has retries configured"
    )
    assert any(getattr(task, "do_xcom_push", False) for task in dag.tasks), (
        "No DAG task has XCom push enabled; verify that the DAG exposes task results"
    )
