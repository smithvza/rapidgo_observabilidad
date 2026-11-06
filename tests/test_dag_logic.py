import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# airflow/dags no es un paquete: se agrega al path igual que hace Airflow.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "dags"))

import rapidgo_checks as checks  # noqa: E402
from producer.contract import SERVICES as CONTRACT_SERVICES  # noqa: E402
from spark import rules  # noqa: E402

NOW = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)


def event(**overrides) -> dict:
    base = {
        "event_id": "e-1",
        "service": "orders-api",
        "endpoint": "/v1/orders",
        "http_method": "GET",
        "http_status": 200,
        "latency_ms": 120,
        "event_time": NOW - timedelta(minutes=1),
    }
    return {**base, **overrides}


def window(service="orders-api", minutes_ago=5, total=100, errors=0, p95=200) -> dict:
    return {
        "window_start": NOW - timedelta(minutes=minutes_ago),
        "service": service,
        "total_events": total,
        "error_events": errors,
        "p95_latency_ms": p95,
    }


# --- Consistencia con el resto del proyecto --------------------------------
def test_thresholds_match_spark_rules():
    assert checks.ERROR_RATE_THRESHOLD * 100 == rules.ERROR_RATE_ALERT_THRESHOLD
    assert checks.P95_LATENCY_THRESHOLD_MS == rules.CRITICAL_LATENCY_THRESHOLD_MS


def test_services_match_contract():
    assert set(checks.SERVICES) == set(CONTRACT_SERVICES)


# --- Tarea 1: fuentes ------------------------------------------------------
def make_layer(base: Path, layer: str, files=("part-0000.snappy.parquet",)):
    folder = base / layer
    (folder / "_spark_metadata").mkdir(parents=True)
    (folder / "_spark_metadata" / "0.parquet").touch()  # no debe contarse
    for name in files:
        (folder / name).touch()


def test_check_sources_counts_parquet_and_ignores_spark_metadata(tmp_path):
    for layer in checks.LAYERS:
        make_layer(tmp_path, layer, files=("a.parquet", "b.parquet"))
    assert checks.check_sources(tmp_path) == {"raw": 2, "processed": 2, "curated": 2}


def test_check_sources_fails_when_a_layer_is_empty(tmp_path):
    make_layer(tmp_path, "raw")
    make_layer(tmp_path, "processed")
    with pytest.raises(checks.DataNotReadyError, match="curated"):
        checks.check_sources(tmp_path)


# --- Tarea 2: calidad ------------------------------------------------------
def test_quality_passes_with_clean_data():
    metrics = checks.check_quality([event(event_id="a"), event(event_id="b")], now=NOW)
    assert metrics["rows"] == 2
    assert metrics["invalid_rows"] == 0
    assert metrics["duplicate_events"] == 0
    assert metrics["minutes_since_last_event"] == 1.0
    assert metrics["warnings"] == []


@pytest.mark.parametrize(
    "bad",
    [
        {"service": "otro-api"},
        {"http_status": 700},
        {"latency_ms": -5},
        {"endpoint": "sin-slash"},
        {"http_method": "TRACE"},
        {"event_id": None},
    ],
)
def test_quality_fails_on_event_outside_contract(bad):
    with pytest.raises(checks.QualityGateError, match="fuera del contrato"):
        checks.check_quality([event(event_id="a"), event(**{"event_id": "b", **bad})], now=NOW)


def test_quality_fails_when_duplicates_exceed_limit():
    records = [event(event_id="x")] * 3 + [event(event_id=str(i)) for i in range(7)]
    with pytest.raises(checks.QualityGateError, match="repetidos"):
        checks.check_quality(records, now=NOW)


def test_quality_warns_but_passes_when_data_is_stale():
    old = event(event_time=NOW - timedelta(minutes=45))
    metrics = checks.check_quality([old], now=NOW)
    assert metrics["minutes_since_last_event"] == 45.0
    assert len(metrics["warnings"]) == 1


def test_quality_fails_on_empty_layer():
    with pytest.raises(checks.QualityGateError):
        checks.check_quality([], now=NOW)


# --- Tarea 3: particiones --------------------------------------------------
def test_partition_distribution_counts_and_flags_skew():
    records = [{"kafka_partition": 1}] * 6 + [{"kafka_partition": 0}] * 3 + [{"kafka_partition": 2}]
    result = checks.partition_distribution(records)
    assert result["counts"] == {"0": 3, "1": 6, "2": 1}
    assert result["shares"]["1"] == 0.6
    assert result["hottest_partition"] == 1
    assert result["skew_warning"] is True


def test_partition_distribution_balanced_has_no_warning():
    records = [{"kafka_partition": p} for p in (0, 1, 2) for _ in range(10)]
    assert checks.partition_distribution(records)["skew_warning"] is False


# --- Tarea 4: KPIs ---------------------------------------------------------
def test_aggregate_uses_weighted_error_rate_and_max_p95():
    windows = [
        window(total=100, errors=10, p95=300),
        window(total=300, errors=0, p95=900, minutes_ago=3),
    ]
    summary = checks.aggregate_windows(windows, now=NOW, lookback_minutes=60)
    orders = summary["services"]["orders-api"]
    assert orders["total_events"] == 400
    assert orders["error_rate"] == 0.025  # 10 / 400, no el promedio de 10 % y 0 %
    assert orders["availability"] == 0.975
    assert orders["p95_max_ms"] == 900
    assert orders["p95_mean_ms"] == 600
    assert summary["window_count"] == 2


def test_aggregate_ignores_windows_outside_lookback():
    windows = [window(minutes_ago=5), window(service="auth-api", minutes_ago=300)]
    summary = checks.aggregate_windows(windows, now=NOW, lookback_minutes=60)
    assert list(summary["services"]) == ["orders-api"]


def test_aggregate_fails_when_no_recent_windows():
    with pytest.raises(checks.DataNotReadyError, match="ventanas"):
        checks.aggregate_windows([window(minutes_ago=300)], now=NOW, lookback_minutes=60)


def test_aggregate_zero_lookback_uses_all_history():
    summary = checks.aggregate_windows([window(minutes_ago=5000)], now=NOW, lookback_minutes=0)
    assert summary["window_count"] == 1


# --- Tarea 5: alertas ------------------------------------------------------
def alert_windows(**kwargs):
    return checks.aggregate_windows([window(**kwargs)], now=NOW, lookback_minutes=60)["windows"]


def test_high_error_rate_alert_fires_above_threshold_only():
    assert checks.evaluate_alerts(alert_windows(total=100, errors=6))["by_type"] == {
        "tasa_error_alta": 1
    }
    assert checks.evaluate_alerts(alert_windows(total=100, errors=5))["alert_count"] == 0


def test_latency_alert_fires_above_threshold_only():
    assert checks.evaluate_alerts(alert_windows(p95=1001))["by_type"] == {"latencia_critica": 1}
    assert checks.evaluate_alerts(alert_windows(p95=1000))["alert_count"] == 0


def test_alerts_ignore_windows_with_too_few_events():
    result = checks.evaluate_alerts(alert_windows(total=10, errors=10, p95=5000))
    assert result["alert_count"] == 0


def test_both_alerts_can_fire_on_one_window():
    result = checks.evaluate_alerts(alert_windows(total=100, errors=20, p95=1500))
    assert result["alert_count"] == 2
    assert result["by_service"] == {"orders-api": 2}


def test_alert_list_is_truncated_but_count_is_complete():
    many = [window(minutes_ago=i + 1, total=100, errors=50) for i in range(60)]
    windows = checks.aggregate_windows(many, now=NOW, lookback_minutes=120)["windows"]
    result = checks.evaluate_alerts(windows)
    assert result["alert_count"] == 60
    assert len(result["alerts"]) == checks.MAX_ALERTS_LISTED
    assert result["truncated"] is True


# --- Tarea 6: reporte ------------------------------------------------------
def test_report_is_written_as_json_and_markdown(tmp_path):
    sources = {"raw": 1, "processed": 1, "curated": 1}
    quality = checks.check_quality([event()], now=NOW)
    partitions = checks.partition_distribution([{"kafka_partition": 0}] * 4)
    kpis = checks.aggregate_windows(
        [window(total=100, errors=20, p95=1500)], now=NOW, lookback_minutes=60
    )
    alerts = checks.evaluate_alerts(kpis["windows"])

    report = checks.build_report(sources, quality, partitions, kpis, alerts, generated_at=NOW)
    paths = checks.write_report(report, tmp_path, now=NOW)

    saved = json.loads(Path(paths["json"]).read_text(encoding="utf-8"))
    assert saved["alerts"]["alert_count"] == 2
    assert "windows" not in saved["kpis"]  # el detalle por ventana no va al reporte
    markdown = Path(paths["markdown"]).read_text(encoding="utf-8")
    assert "orders-api" in markdown
    assert "latencia_critica" in markdown
    assert (tmp_path / "latest.md").exists()
