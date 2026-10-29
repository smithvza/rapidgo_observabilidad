import json
import random

import pytest

from producer.contract import SERVICES, validate_event
from producer.rapidgo_producer import (
    Stats,
    build_event,
    corrupt_event,
    make_delivery_callback,
    publish_event,
)


class FakeMessage:
    def __init__(self, partition: int) -> None:
        self._partition = partition

    def partition(self) -> int:
        return self._partition


class FakeProducer:
    """Reemplaza al Producer de Kafka: guarda lo que se intenta publicar."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def produce(self, topic, key=None, value=None, on_delivery=None) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(None, FakeMessage(partition=1))


@pytest.mark.parametrize("service", sorted(SERVICES))
@pytest.mark.parametrize("degraded", [False, True])
def test_generated_events_match_contract(service: str, degraded: bool) -> None:
    rng = random.Random(42)
    for _ in range(50):
        validate_event(build_event(service, degraded=degraded, rng=rng))


def test_event_is_published_with_service_as_key() -> None:
    producer, stats = FakeProducer(), Stats()
    event = build_event("payments-api", rng=random.Random(1))

    assert publish_event(producer, event, stats) is True

    call = producer.calls[0]
    assert call["key"] == b"payments-api"
    assert json.loads(call["value"]) == event
    assert stats.sent == 1
    assert stats.per_service["payments-api"] == 1


@pytest.mark.parametrize("seed", range(20))
def test_corrupted_event_is_invalid_and_not_published(seed: int) -> None:
    rng = random.Random(seed)
    producer, stats = FakeProducer(), Stats()
    broken = corrupt_event(build_event("orders-api", rng=rng), rng)

    with pytest.raises(ValueError):
        validate_event(broken)

    assert publish_event(producer, broken, stats) is False
    assert producer.calls == []
    assert stats.invalid == 1
    assert stats.sent == 0


def test_delivery_callback_counts_partitions_and_errors() -> None:
    stats = Stats()
    callback = make_delivery_callback(stats)

    callback(None, FakeMessage(partition=2))
    callback(None, FakeMessage(partition=2))
    callback("broker caido", None)

    assert stats.per_partition[2] == 2
    assert stats.delivery_errors == 1
