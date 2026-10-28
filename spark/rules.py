from collections.abc import Iterable, Mapping
from math import ceil

Event = Mapping[str, int]
ERROR_RATE_ALERT_THRESHOLD = 5.0
CRITICAL_LATENCY_THRESHOLD_MS = 1000


def error_rate_percent(events: Iterable[Event]) -> float | None:
    events = tuple(events)
    if not events:
        return None
    errors = sum(event["http_status"] >= 500 for event in events)
    return errors / len(events) * 100


def latency_p95_ms(events: Iterable[Event]) -> int | None:
    latencies = sorted(event["latency_ms"] for event in events)
    if not latencies:
        return None
    return latencies[ceil(0.95 * len(latencies)) - 1]


def availability_percent(events: Iterable[Event]) -> float | None:
    events = tuple(events)
    if not events:
        return None
    available = sum(event["http_status"] < 500 for event in events)
    return available / len(events) * 100


def has_high_error_rate(events: Iterable[Event]) -> bool:
    rate = error_rate_percent(events)
    return rate is not None and rate > ERROR_RATE_ALERT_THRESHOLD


def has_critical_latency(events: Iterable[Event]) -> bool:
    p95 = latency_p95_ms(events)
    return p95 is not None and p95 > CRITICAL_LATENCY_THRESHOLD_MS
