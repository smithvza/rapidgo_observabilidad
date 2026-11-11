"""Los umbrales del job de Spark deben coincidir con las reglas probadas
(spark/rules.py) y con docs/kpis-alertas.md."""
from spark import config, rules


def test_spark_job_thresholds_match_rules():
    assert config.ERROR_RATE_THRESHOLD * 100 == rules.ERROR_RATE_ALERT_THRESHOLD
    assert config.P95_LATENCY_THRESHOLD_MS == rules.CRITICAL_LATENCY_THRESHOLD_MS
