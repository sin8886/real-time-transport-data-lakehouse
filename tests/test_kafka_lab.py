import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from kafka_consumer import deserialize_event
from kafka_producer import (
    REQUIRED_EVENT_FIELDS,
    build_demo_event,
    serialize_event,
    validate_event_payload,
)


def test_demo_event_contains_required_contract_fields() -> None:
    event = build_demo_event(1)

    assert set(REQUIRED_EVENT_FIELDS).issubset(event)
    assert event["event_type"] == "vehicle_position"
    assert event["source_provider"] == "AT"
    validate_event_payload(event)


def test_event_round_trip_serialization() -> None:
    event = build_demo_event(2)

    decoded = deserialize_event(serialize_event(event))

    assert decoded == event


def test_missing_required_field_is_rejected() -> None:
    event = build_demo_event(3)
    del event["event_ts"]

    with pytest.raises(ValueError, match="event_ts"):
        validate_event_payload(event)


def test_invalid_json_is_rejected() -> None:
    with pytest.raises(json.JSONDecodeError):
        deserialize_event(b"not-json")
