from collections.abc import Mapping
from datetime import datetime, timedelta
from uuid import UUID

REQUIRED_FIELDS = {
    "event_id",
    "timestamp",
    "service",
    "endpoint",
    "http_method",
    "http_status",
    "latency_ms",
}
ALLOWED_FIELDS = REQUIRED_FIELDS | {"region"}
SERVICES = {
    "orders-api",
    "payments-api",
    "tracking-api",
    "auth-api",
    "catalog-api",
    "notifications-api",
}
HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


def validate_event(event: Mapping[str, object]) -> None:
    """Raise ValueError when an event does not match the documented contract."""
    if not isinstance(event, Mapping):
        raise ValueError("event must be a JSON object")

    missing_fields = REQUIRED_FIELDS - event.keys()
    if missing_fields:
        raise ValueError(f"missing required fields: {sorted(missing_fields)}")

    extra_fields = event.keys() - ALLOWED_FIELDS
    if extra_fields:
        raise ValueError(f"unexpected fields: {sorted(extra_fields)}")

    event_id = event["event_id"]
    if not isinstance(event_id, str):
        raise ValueError("event_id must be a UUID v4 string")
    try:
        parsed_event_id = UUID(event_id)
    except ValueError as error:
        raise ValueError("event_id must be a UUID v4 string") from error
    if parsed_event_id.version != 4 or str(parsed_event_id) != event_id:
        raise ValueError("event_id must be a canonical UUID v4 string")

    timestamp = event["timestamp"]
    if not isinstance(timestamp, str) or not timestamp.endswith("Z"):
        raise ValueError("timestamp must be an ISO 8601 UTC string ending in Z")
    try:
        parsed_timestamp = datetime.fromisoformat(timestamp[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError("timestamp must be a parseable ISO 8601 string") from error
    if parsed_timestamp.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be in UTC")

    service = event["service"]
    if not isinstance(service, str) or service not in SERVICES:
        raise ValueError("service is not supported")
    endpoint = event["endpoint"]
    if not isinstance(endpoint, str) or not endpoint.startswith("/") or " " in endpoint:
        raise ValueError("endpoint must start with '/' and contain no spaces")
    http_method = event["http_method"]
    if not isinstance(http_method, str) or http_method not in HTTP_METHODS:
        raise ValueError("http_method is not supported")

    http_status = event["http_status"]
    if type(http_status) is not int or not 100 <= http_status <= 599:
        raise ValueError("http_status must be an integer between 100 and 599")

    latency_ms = event["latency_ms"]
    if type(latency_ms) is not int or not 0 <= latency_ms <= 60000:
        raise ValueError("latency_ms must be an integer between 0 and 60000")

    if "region" in event and not isinstance(event["region"], str):
        raise ValueError("region must be a string when provided")
