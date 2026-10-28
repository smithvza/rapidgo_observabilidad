import pytest

from spark.rules import (
    availability_percent,
    error_rate_percent,
    has_critical_latency,
    has_high_error_rate,
    latency_p95_ms,
)


def make_events(*, statuses: list[int], latencies: list[int]) -> list[dict[str, int]]:
    return [
        {"http_status": status, "latency_ms": latency}
        for status, latency in zip(statuses, latencies, strict=True)
    ]


def test_error_rate_alert_fires_above_five_percent() -> None:
    events = make_events(statuses=[500, 200] + [200] * 18, latencies=[10] * 20)

    assert error_rate_percent(events) == pytest.approx(5.0)
    assert has_high_error_rate(
        make_events(statuses=[500, 200] + [200] * 17, latencies=[10] * 19)
    )


def test_error_rate_alert_does_not_fire_at_or_below_five_percent() -> None:
    exactly_five_percent = make_events(
        statuses=[500] + [200] * 19, latencies=[10] * 20
    )
    below_five_percent = make_events(
        statuses=[500] + [200] * 20, latencies=[10] * 21
    )

    assert error_rate_percent(exactly_five_percent) == pytest.approx(5.0)
    assert not has_high_error_rate(exactly_five_percent)
    assert not has_high_error_rate(below_five_percent)


def test_p95_alert_fires_only_above_one_thousand_milliseconds() -> None:
    critical = make_events(statuses=[200], latencies=[1001])
    at_threshold = make_events(statuses=[200], latencies=[1000])

    assert latency_p95_ms(critical) == 1001
    assert has_critical_latency(critical)
    assert latency_p95_ms(at_threshold) == 1000
    assert not has_critical_latency(at_threshold)


def test_availability_with_mixed_http_statuses() -> None:
    events = make_events(
        statuses=[200, 404, 500, 503],
        latencies=[10, 20, 30, 40],
    )

    assert availability_percent(events) == pytest.approx(50.0)


def test_empty_window_has_no_kpi_value_or_alert() -> None:
    assert error_rate_percent([]) is None
    assert latency_p95_ms([]) is None
    assert availability_percent([]) is None
    assert not has_high_error_rate([])
    assert not has_critical_latency([])


def test_single_event_window() -> None:
    events = make_events(statuses=[500], latencies=[1001])

    assert error_rate_percent(events) == pytest.approx(100.0)
    assert latency_p95_ms(events) == 1001
    assert availability_percent(events) == pytest.approx(0.0)
    assert has_high_error_rate(events)
    assert has_critical_latency(events)
