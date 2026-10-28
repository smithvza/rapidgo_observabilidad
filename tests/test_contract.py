import json
from pathlib import Path

import pytest

from producer.contract import validate_event

SAMPLE_PATH = Path(__file__).parent.parent / "samples" / "evento_ejemplo.json"


def load_sample() -> dict[str, object]:
    with SAMPLE_PATH.open(encoding="utf-8") as sample_file:
        return json.load(sample_file)


def test_sample_event_matches_contract() -> None:
    event = load_sample()

    validate_event(event)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("latency_ms", "1240"),
        ("latency_ms", -1),
        ("http_status", "500"),
        ("http_status", 600),
        ("timestamp", "not-a-timestamp"),
        ("service", []),
        ("http_method", []),
    ],
)
def test_event_with_invalid_field_is_rejected(
    field: str, invalid_value: object
) -> None:
    event = load_sample()
    event[field] = invalid_value

    with pytest.raises(ValueError):
        validate_event(event)


def test_event_missing_required_field_is_rejected() -> None:
    event = load_sample()
    del event["service"]

    with pytest.raises(ValueError, match="missing required fields"):
        validate_event(event)
