#!/usr/bin/env python3
"""Prueba de humo del pipeline completo de RapidGo (Variante 9).

Verifica de punta a punta que lo levantado con `docker compose up -d --build`
funciona: contenedores, topic de Kafka, datos en las tres capas del Data Lake,
Airflow y el reporte final. Termina con codigo 1 si algun paso falla.

Uso (desde la raiz del repo, con el pipeline levantado):
  python scripts/smoke_test.py
  python scripts/smoke_test.py --wait-minutes 10
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "airflow" / "dags"))

from rapidgo_checks import LAYERS, count_parquet_files  # noqa: E402

TOPIC = "rapidgo.observability.events"
EXPECTED_SERVICES = ("kafka", "spark", "airflow", "producer")
EXPECTED_PARTITIONS = 3
DAG_ID = "rapidgo_observability_pipeline"
DATALAKE = ROOT / "datalake"
MAX_DATA_AGE_MINUTES = 5


# --- Funciones puras (se prueban en tests/test_smoke_helpers.py) -----------
def missing_services(running_output: str, expected=EXPECTED_SERVICES) -> list[str]:
    """Servicios esperados que no aparecen en la salida de `compose ps --services`."""
    running = {line.strip() for line in running_output.splitlines() if line.strip()}
    return [service for service in expected if service not in running]


def parse_partition_count(text: str) -> int | None:
    match = re.search(r"PartitionCount:\s*(\d+)", text)
    return int(match.group(1)) if match else None


def parse_offsets(text: str) -> dict[int, int]:
    """Convierte lineas `topic:particion:offset` en {particion: offset}."""
    offsets = {}
    for line in text.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
            offsets[int(parts[1])] = int(parts[2])
    return offsets


def newest_parquet_age_minutes(layer_dir, now: float | None = None) -> float | None:
    """Minutos desde el Parquet mas reciente de una capa (None si no hay)."""
    root = Path(layer_dir)
    newest = None
    for path in root.rglob("*.parquet"):
        if any(part.startswith(("_", ".")) for part in path.relative_to(root).parts):
            continue
        mtime = path.stat().st_mtime
        newest = mtime if newest is None else max(newest, mtime)
    if newest is None:
        return None
    return ((time.time() if now is None else now) - newest) / 60


def layers_ready(snapshot: dict, max_age_minutes: float) -> bool:
    """Cada capa debe tener archivos y el mas reciente no puede ser viejo."""
    return all(
        info["files"] > 0 and info["age"] is not None and info["age"] <= max_age_minutes
        for info in snapshot.values()
    )


# --- Ejecucion de comandos -------------------------------------------------
def run(command: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        command, cwd=ROOT, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=timeout,
    )


def compose_exec(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return run(["docker", "compose", "exec", "-T", *args], timeout=timeout)


class Results:
    def __init__(self) -> None:
        self.failed = 0

    def report(self, name: str, ok: bool, detail: str = "") -> None:
        if not ok:
            self.failed += 1
        print(f"[{'OK' if ok else 'FALLO'}] {name}" + (f" - {detail}" if detail else ""), flush=True)


# --- Pasos -----------------------------------------------------------------
def check_containers(results: Results) -> None:
    proc = run(["docker", "compose", "ps", "--services", "--filter", "status=running"])
    missing = missing_services(proc.stdout) if proc.returncode == 0 else list(EXPECTED_SERVICES)
    results.report("Contenedores en ejecucion", not missing,
                   f"faltan: {', '.join(missing)}" if missing else ", ".join(EXPECTED_SERVICES))


def check_topic(results: Results) -> None:
    proc = compose_exec("kafka", "/opt/kafka/bin/kafka-topics.sh", "--describe",
                        "--topic", TOPIC, "--bootstrap-server", "localhost:9092")
    count = parse_partition_count(proc.stdout)
    results.report("Topic de Kafka", count == EXPECTED_PARTITIONS,
                  f"{TOPIC} con {count} particiones (esperadas {EXPECTED_PARTITIONS})")


def check_offsets(results: Results) -> None:
    proc = compose_exec("kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                        "--bootstrap-server", "localhost:9092", "--topic", TOPIC)
    offsets = parse_offsets(proc.stdout)
    total = sum(offsets.values())
    results.report("Eventos en Kafka", len(offsets) == EXPECTED_PARTITIONS and total > 0,
                   f"offsets por particion {dict(sorted(offsets.items()))}, total {total}")


def layer_snapshot() -> dict:
    return {
        layer: {"files": count_parquet_files(DATALAKE / layer),
                "age": newest_parquet_age_minutes(DATALAKE / layer)}
        for layer in LAYERS
    }


def describe(snapshot: dict) -> str:
    return "; ".join(
        f"{layer}: {info['files']} archivos"
        + (f", ultimo hace {info['age']:.1f} min" if info["age"] is not None else "")
        for layer, info in snapshot.items()
    )


def wait_for_layers(wait_minutes: float, max_age: float, poll_seconds: int = 15) -> dict:
    deadline = time.time() + wait_minutes * 60
    while True:
        snapshot = layer_snapshot()
        if layers_ready(snapshot, max_age) or time.time() >= deadline:
            return snapshot
        print(f"      esperando datos recientes en las tres capas... {describe(snapshot)}", flush=True)
        time.sleep(poll_seconds)


def check_datalake(results: Results, wait_minutes: float, max_age: float) -> None:
    snapshot = wait_for_layers(wait_minutes, max_age)
    ok = layers_ready(snapshot, max_age)
    detail = describe(snapshot)
    if not ok:
        detail += (f" | faltan datos o son viejos (mas de {max_age:g} min): "
                   "revisar que Spark este escribiendo; si quedaron de una corrida "
                   "anterior, ver 'Empezar de cero' en docs/ejecucion-local.md")
    results.report("Data Lake (raw, processed, curated)", ok, detail)


def check_airflow(results: Results) -> None:
    proc = compose_exec("airflow", "curl", "--fail", "-s", "http://localhost:8080/health")
    results.report("Airflow responde", proc.returncode == 0)


def check_dag_and_report(results: Results) -> None:
    started = time.time()
    proc = compose_exec("airflow", "airflow", "dags", "test", DAG_ID, timeout=600)
    results.report(f"DAG {DAG_ID}", proc.returncode == 0,
                   "las 6 tareas terminaron en success" if proc.returncode == 0
                   else "fallo; revisar: docker compose logs airflow")
    latest = DATALAKE / "reports" / "latest.md"
    fresh = latest.exists() and latest.stat().st_mtime >= started - 5
    results.report("Reporte generado", fresh, str(latest.relative_to(ROOT)) if fresh else "no se genero")


def main() -> int:
    parser = argparse.ArgumentParser(description="Prueba de humo del pipeline RapidGo")
    parser.add_argument("--wait-minutes", type=float, default=8,
                        help="maximo a esperar que Spark escriba las 3 capas (curated tarda ~3 min)")
    parser.add_argument("--max-age-minutes", type=float, default=MAX_DATA_AGE_MINUTES,
                        help="antiguedad maxima del Parquet mas reciente de cada capa")
    args = parser.parse_args()

    try:
        run(["docker", "compose", "version"], timeout=30)
    except FileNotFoundError:
        print("[FALLO] No se encontro el comando `docker`. Abre Docker Desktop e intenta de nuevo.")
        return 1

    results = Results()
    check_containers(results)
    check_topic(results)
    check_offsets(results)
    check_datalake(results, args.wait_minutes, args.max_age_minutes)
    check_airflow(results)
    check_dag_and_report(results)

    print("\nRESULTADO:", "pipeline completo funcionando" if not results.failed
          else f"{results.failed} paso(s) fallaron")
    return 1 if results.failed else 0


if __name__ == "__main__":
    sys.exit(main())
