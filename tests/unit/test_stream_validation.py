"""Unit tests for canonical realtime event validation."""

import json

import pytest

from transport_lakehouse.streaming.stream_validation import (
    validate_raw_message,
)


def make_vehicle_event(**overrides):
    event = {
        "event_type": "vehicle_position",
        "source_provider": "AT",
        "source_entity_id": "entity-1",
        "event_ts": "2026-11-02T10:00:00+13:00",
        "ingest_ts": "2026-11-02T10:00:05+13:00",
        "is_deleted": False,
        "vehicle_id": "vehicle-1",
        "trip_id": "trip-1",
        "route_id": "route-1",
        "latitude": -36.8485,
        "longitude": 174.7633,
        "speed": 12.5,
        "bearing": 90.0,
    }
    event.update(overrides)
    return event


def make_trip_event(**overrides):
    event = {
        "event_type": "trip_update",
        "source_provider": "AT",
        "source_entity_id": "entity-2",
        "event_ts": "2026-11-02T10:00:00+13:00",
        "ingest_ts": "2026-11-02T10:00:05+13:00",
        "is_deleted": False,
        "trip_id": "trip-2",
        "route_id": "route-2",
        "stop_id": "stop-1",
        "stop_sequence": 3,
        "predicted_arrival_ts": "2026-11-02T10:05:00+13:00",
        "delay_seconds": 60,
    }
    event.update(overrides)
    return event


def make_alert_event(**overrides):
    event = {
        "event_type": "service_alert",
        "source_provider": "AT",
        "source_entity_id": "entity-3",
        "event_ts": "2026-11-02T10:00:00+13:00",
        "ingest_ts": "2026-11-02T10:00:05+13:00",
        "is_deleted": False,
        "alert_id": "alert-1",
        "cause": "OTHER_CAUSE",
        "effect": "NO_SERVICE",
        "description": "Service disruption",
        "affected_route_id": "route-3",
    }
    event.update(overrides)
    return event


@pytest.mark.parametrize(
    "event_factory",
    [make_vehicle_event, make_trip_event, make_alert_event],
)
def test_valid_event_types_are_accepted(event_factory):
    result = validate_raw_message(json.dumps(event_factory()))

    assert result.is_valid is True
    assert result.error_type is None
    assert result.event is not None


@pytest.mark.parametrize(
    "raw_message",
    [
        None,
        "",
        b"",
        "{invalid-json",
        b"\xff\xfe",
    ],
)
def test_bad_messages_are_rejected(raw_message):
    result = validate_raw_message(raw_message)

    assert result.is_valid is False
    assert result.error_type == "BAD_MESSAGE"
    assert result.event is None


@pytest.mark.parametrize("raw_message", ["[]", '"text"', "123", "true", "null"])
def test_non_object_json_is_a_schema_error(raw_message):
    result = validate_raw_message(raw_message)

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"


@pytest.mark.parametrize(
    "field",
    ["event_type", "source_provider", "event_ts", "ingest_ts"],
)
def test_missing_common_required_field_is_rejected(field):
    event = make_vehicle_event()
    del event[field]

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert field in result.error_message


def test_empty_source_provider_is_rejected():
    result = validate_raw_message(json.dumps(make_vehicle_event(source_provider="")))

    assert result.is_valid is False
    assert "source_provider" in result.error_message


def test_unsupported_event_type_is_rejected():
    result = validate_raw_message(
        json.dumps(make_vehicle_event(event_type="unknown_event"))
    )

    assert result.is_valid is False
    assert "unsupported event_type" in result.error_message


def test_trip_update_requires_trip_id():
    result = validate_raw_message(json.dumps(make_trip_event(trip_id=None)))

    assert result.is_valid is False
    assert "trip_id" in result.error_message


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("vehicle_id", 123),
        ("source_entity_id", 123),
        ("latitude", "not-a-number"),
        ("longitude", True),
        ("stop_sequence", True),
        ("delay_seconds", 1.5),
        ("is_deleted", "false"),
    ],
)
def test_invalid_field_types_are_rejected(field, value):
    result = validate_raw_message(json.dumps(make_vehicle_event(**{field: value})))

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert field in result.error_message


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", 90.1),
        ("latitude", -90.1),
        ("longitude", 180.1),
        ("longitude", -180.1),
    ],
)
def test_out_of_range_coordinates_are_rejected(field, value):
    result = validate_raw_message(json.dumps(make_vehicle_event(**{field: value})))

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"


def test_nullable_optional_fields_are_accepted():
    event = make_vehicle_event(
        source_entity_id=None,
        vehicle_id=None,
        trip_id=None,
        route_id=None,
        latitude=None,
        longitude=None,
        speed=None,
        bearing=None,
        is_deleted=None,
    )

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is True


def test_boundary_coordinates_are_accepted():
    event = make_vehicle_event(latitude=-90, longitude=180)

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is True


@pytest.mark.parametrize(
    "field",
    ["event_ts", "ingest_ts"],
)
@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-11-02T10:00:00",
        "not-a-timestamp",
        "2026-13-02T10:00:00+13:00",
    ],
)
def test_invalid_common_timestamps_are_rejected(field, timestamp):
    event = make_vehicle_event(**{field: timestamp})

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert field in result.error_message


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-11-02T10:05:00",
        "not-a-timestamp",
    ],
)
def test_invalid_optional_timestamp_is_rejected(timestamp):
    event = make_trip_event(predicted_arrival_ts=timestamp)

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert "predicted_arrival_ts" in result.error_message


@pytest.mark.parametrize(
    ("topic", "event_factory"),
    [
        ("transport.vehicle_positions.raw", make_vehicle_event),
        ("transport.trip_updates.raw", make_trip_event),
        ("transport.service_alerts.raw", make_alert_event),
    ],
)
def test_event_type_matches_source_topic(topic, event_factory):
    result = validate_raw_message(
        json.dumps(event_factory()),
        source_topic=topic,
    )

    assert result.is_valid is True


@pytest.mark.parametrize(
    ("topic", "event_factory"),
    [
        ("transport.vehicle_positions.raw", make_trip_event),
        ("transport.trip_updates.raw", make_alert_event),
        ("transport.service_alerts.raw", make_vehicle_event),
    ],
)
def test_event_type_mismatch_with_source_topic_is_rejected(topic, event_factory):
    result = validate_raw_message(
        json.dumps(event_factory()),
        source_topic=topic,
    )

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert "does not match source topic" in result.error_message


def test_unknown_source_topic_is_rejected():
    result = validate_raw_message(
        json.dumps(make_vehicle_event()),
        source_topic="transport.unknown.raw",
    )

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert "unsupported source topic" in result.error_message


def test_trip_update_requires_stop_id_or_stop_sequence():
    event = make_trip_event(stop_id=None, stop_sequence=None)

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert "stop_id or stop_sequence" in result.error_message


def test_cancelled_trip_update_allows_missing_stop_identifiers():
    event = make_trip_event(
        stop_id=None,
        stop_sequence=None,
        trip_schedule_relationship="CANCELED",
    )

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is True


def test_trip_update_without_stop_identifiers_requires_cancellation():
    event = make_trip_event(stop_id=None, stop_sequence=None)

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is False
    assert "stop_id or stop_sequence" in result.error_message


def test_trip_schedule_relationship_must_be_string_or_null():
    result = validate_raw_message(
        json.dumps(make_trip_event(trip_schedule_relationship=3))
    )

    assert result.is_valid is False
    assert "trip_schedule_relationship" in result.error_message


def test_trip_update_accepts_stop_sequence_without_stop_id():
    event = make_trip_event(stop_id=None, stop_sequence=3)

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is True


def test_trip_update_accepts_stop_id_without_stop_sequence():
    event = make_trip_event(stop_id="stop-1", stop_sequence=None)

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is True


def test_service_alert_affected_stop_id_must_be_string_or_null():
    event = make_alert_event(affected_stop_id=123)

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert "affected_stop_id" in result.error_message


@pytest.mark.parametrize("stop_id", ["", "   "])
def test_trip_update_rejects_empty_stop_id_without_stop_sequence(stop_id):
    event = make_trip_event(stop_id=stop_id, stop_sequence=None)

    result = validate_raw_message(json.dumps(event))

    assert result.is_valid is False
    assert result.error_type == "SCHEMA_ERROR"
    assert "stop_id or stop_sequence" in result.error_message
