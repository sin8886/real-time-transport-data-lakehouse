"""Validation helpers for canonical realtime transport events."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any


SUPPORTED_EVENT_TYPES = {
    "vehicle_position",
    "trip_update",
    "service_alert",
}

TOPIC_EVENT_TYPES = {
    "transport.vehicle_positions.raw": "vehicle_position",
    "transport.trip_updates.raw": "trip_update",
    "transport.service_alerts.raw": "service_alert",
}

COMMON_REQUIRED_FIELDS = {
    "event_type": str,
    "source_provider": str,
    "event_ts": str,
    "ingest_ts": str,
}

EVENT_REQUIRED_FIELDS = {
    "vehicle_position": {},
    "trip_update": {"trip_id": str},
    "service_alert": {},
}

STRING_FIELDS = {
    "source_entity_id",
    "vehicle_id",
    "trip_id",
    "route_id",
    "stop_id",
    "alert_id",
    "cause",
    "effect",
    "description",
    "affected_route_id",
    "affected_trip_id",
    "affected_stop_id",
    "schedule_relationship",
}

NUMERIC_FIELDS = {
    "latitude",
    "longitude",
    "speed",
    "bearing",
}

INTEGER_FIELDS = {
    "stop_sequence",
    "delay_seconds",
}

BOOLEAN_FIELDS = {
    "is_deleted",
}

TIMESTAMP_FIELDS = {
    "scheduled_arrival_ts",
    "scheduled_departure_ts",
    "predicted_arrival_ts",
    "predicted_departure_ts",
    "active_start_ts",
    "active_end_ts",
}


@dataclass(frozen=True)
class ValidationResult:
    """Result of validating one raw Kafka message."""

    is_valid: bool
    error_type: str | None
    error_message: str | None
    event: dict[str, Any] | None


def _invalid_schema(message: str) -> ValidationResult:
    return ValidationResult(False, "SCHEMA_ERROR", message, None)


def _is_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _is_timezone_aware_iso_timestamp(value: str) -> bool:
    """Return whether a string is a valid ISO-8601 timestamp with timezone."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def validate_event(
    event: Any,
    source_topic: str | None = None,
) -> ValidationResult:
    """Validate a parsed canonical event and optionally its source topic."""
    if not isinstance(event, dict):
        return _invalid_schema("JSON payload must be an object")

    for field, expected_type in COMMON_REQUIRED_FIELDS.items():
        value = event.get(field)
        if not isinstance(value, expected_type) or not value.strip():
            return _invalid_schema(f"{field} must be a non-empty string")

    event_type = event["event_type"]
    if event_type not in SUPPORTED_EVENT_TYPES:
        return _invalid_schema(f"unsupported event_type: {event_type}")

    if source_topic is not None:
        expected_event_type = TOPIC_EVENT_TYPES.get(source_topic)
        if expected_event_type is None:
            return _invalid_schema(f"unsupported source topic: {source_topic}")
        if event_type != expected_event_type:
            return _invalid_schema(
                f"event_type {event_type} does not match source topic "
                f"{source_topic}; expected {expected_event_type}"
            )

    for field, expected_type in EVENT_REQUIRED_FIELDS[event_type].items():
        value = event.get(field)
        if not isinstance(value, expected_type) or not value.strip():
            return _invalid_schema(
                f"{field} must be a non-empty string for {event_type}"
            )

    for field in STRING_FIELDS:
        value = event.get(field)
        if value is not None and not isinstance(value, str):
            return _invalid_schema(f"{field} must be a string or null")

    for field in NUMERIC_FIELDS:
        value = event.get(field)
        if value is not None and not _is_number(value):
            return _invalid_schema(f"{field} must be a finite number or null")

    for field in INTEGER_FIELDS:
        value = event.get(field)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int)
        ):
            return _invalid_schema(f"{field} must be an integer or null")

    for field in BOOLEAN_FIELDS:
        value = event.get(field)
        if value is not None and not isinstance(value, bool):
            return _invalid_schema(f"{field} must be a boolean or null")

    for field in ("event_ts", "ingest_ts"):
        if not _is_timezone_aware_iso_timestamp(event[field]):
            return _invalid_schema(
                f"{field} must be a valid timezone-aware ISO-8601 timestamp"
            )

    for field in TIMESTAMP_FIELDS:
        value = event.get(field)
        if value is not None:
            if not isinstance(value, str) or not _is_timezone_aware_iso_timestamp(
                value
            ):
                return _invalid_schema(
                    f"{field} must be a timezone-aware ISO-8601 string or null"
                )

    if event_type == "vehicle_position":
        latitude = event.get("latitude")
        if latitude is not None and not -90 <= latitude <= 90:
            return _invalid_schema("latitude must be between -90 and 90")

        longitude = event.get("longitude")
        if longitude is not None and not -180 <= longitude <= 180:
            return _invalid_schema("longitude must be between -180 and 180")

    if event_type == "trip_update":
        stop_id = event.get("stop_id")
        stop_sequence = event.get("stop_sequence")
        if (stop_id is None or not stop_id.strip()) and stop_sequence is None:
            return _invalid_schema("trip_update requires stop_id or stop_sequence")

    return ValidationResult(True, None, None, event)


def validate_raw_message(
    raw_message: str | bytes | None,
    source_topic: str | None = None,
) -> ValidationResult:
    """Parse and validate one raw JSON Kafka message."""
    if raw_message is None or raw_message == "" or raw_message == b"":
        return ValidationResult(
            False,
            "BAD_MESSAGE",
            "message is empty",
            None,
        )

    try:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")
        if not isinstance(raw_message, str):
            raise TypeError("message must be bytes or string")
        event = json.loads(raw_message)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        return ValidationResult(
            False,
            "BAD_MESSAGE",
            f"invalid JSON message: {exc}",
            None,
        )

    return validate_event(event, source_topic=source_topic)
