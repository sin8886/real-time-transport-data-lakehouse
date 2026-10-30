"""Unit tests for the Kafka trip-update producer."""

import json

import pytest

from transport_lakehouse.streaming.trip_update_producer import (
    TRIP_UPDATE_TOPIC,
    TripUpdateProducer,
    build_trip_update_key,
    serialize_trip_update,
)


class FakeFuture:
    def __init__(self, error=None):
        self.error = error
        self.timeout = None

    def get(self, timeout=None):
        self.timeout = timeout
        if self.error:
            raise self.error
        return None


class FakeKafkaProducer:
    def __init__(self, error=None):
        self.messages = []
        self.error = error
        self.flush_called = False
        self.futures = []

    def send(self, topic, key=None, value=None):
        self.messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
            }
        )
        future = FakeFuture(self.error)
        self.futures.append(future)
        return future

    def flush(self):
        self.flush_called = True


def make_trip_event(
    source_provider="AT",
    trip_id="trip-123",
    stop_id="stop-456",
    stop_sequence=2,
):
    return {
        "event_type": "trip_update",
        "source_provider": source_provider,
        "source_entity_id": "entity-789",
        "event_ts": "2026-10-30T01:00:00+00:00",
        "ingest_ts": "2026-10-30T01:00:01+00:00",
        "is_deleted": False,
        "trip_id": trip_id,
        "route_id": "route-10",
        "vehicle_id": "vehicle-123",
        "stop_id": stop_id,
        "stop_sequence": stop_sequence,
        "scheduled_arrival_ts": None,
        "scheduled_departure_ts": None,
        "predicted_arrival_ts": "2026-10-30T01:05:00+00:00",
        "predicted_departure_ts": None,
        "delay_seconds": 300,
        "schedule_relationship": "SCHEDULED",
    }


def test_serialize_trip_update_returns_json_bytes():
    event = make_trip_event()

    result = serialize_trip_update(event)

    assert isinstance(result, bytes)
    assert json.loads(result.decode("utf-8")) == event


def test_serialize_rejects_non_trip_update_event():
    event = make_trip_event()
    event["event_type"] = "vehicle_position"

    with pytest.raises(ValueError, match="event_type"):
        serialize_trip_update(event)


@pytest.mark.parametrize(
    ("event", "expected_key"),
    [
        (
            make_trip_event(),
            b"AT:trip-123:seq:2",
        ),
        (
            make_trip_event(stop_id="stop-456", stop_sequence=None),
            b"AT:trip-123:stop:stop-456",
        ),
        (
            make_trip_event(source_provider="Metlink"),
            b"Metlink:trip-123:seq:2",
        ),
    ],
)
def test_build_trip_update_key_uses_business_context(event, expected_key):
    assert build_trip_update_key(event) == expected_key


def test_build_trip_update_key_prefers_stop_sequence():
    event = make_trip_event(
        stop_id="stop-456",
        stop_sequence=2,
    )

    assert build_trip_update_key(event) == b"AT:trip-123:seq:2"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("trip_id", None, "trip_id"),
        ("trip_id", "", "trip_id"),
        ("source_provider", None, "source_provider"),
        ("source_provider", "", "source_provider"),
        ("stop_id", None, "stop_sequence or"),
        ("stop_id", "", "stop_sequence or"),
    ],
)
def test_serialize_rejects_missing_required_identity_fields(
    field, value, message
):
    event = make_trip_event(stop_sequence=None)
    event[field] = value

    with pytest.raises(ValueError, match=message):
        serialize_trip_update(event)


@pytest.mark.parametrize("value", [True, 2.5, "2"])
def test_serialize_rejects_invalid_stop_sequence(value):
    event = make_trip_event(stop_sequence=value)

    with pytest.raises(ValueError, match="stop_sequence"):
        serialize_trip_update(event)


def test_publish_sends_trip_update_with_composite_key():
    fake_producer = FakeKafkaProducer()
    producer = TripUpdateProducer(fake_producer)
    event = make_trip_event()

    published = producer.publish([event])

    assert published == 1
    assert len(fake_producer.messages) == 1

    message = fake_producer.messages[0]
    assert message["topic"] == TRIP_UPDATE_TOPIC
    assert message["key"] == b"AT:trip-123:seq:2"
    assert json.loads(message["value"].decode("utf-8")) == event
    assert fake_producer.flush_called is True
    assert fake_producer.futures[0].timeout == 10


def test_publish_uses_stop_id_when_stop_sequence_is_missing():
    fake_producer = FakeKafkaProducer()
    producer = TripUpdateProducer(fake_producer)
    event = make_trip_event(stop_sequence=None)

    published = producer.publish([event])

    assert published == 1
    assert fake_producer.messages[0]["key"] == b"AT:trip-123:stop:stop-456"


def test_publish_ignores_non_trip_update_events():
    fake_producer = FakeKafkaProducer()
    producer = TripUpdateProducer(fake_producer)

    events = [
        make_trip_event(),
        {
            "event_type": "vehicle_position",
            "vehicle_id": "vehicle-999",
        },
        {
            "event_type": "service_alert",
        },
    ]

    published = producer.publish(events)

    assert published == 1
    assert len(fake_producer.messages) == 1


def test_publish_propagates_kafka_send_failure():
    fake_producer = FakeKafkaProducer(
        error=RuntimeError("broker unavailable")
    )
    producer = TripUpdateProducer(fake_producer)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        producer.publish([make_trip_event()])

    assert fake_producer.flush_called is False


def test_publish_empty_events_returns_zero():
    fake_producer = FakeKafkaProducer()
    producer = TripUpdateProducer(fake_producer)

    published = producer.publish([])

    assert published == 0
    assert fake_producer.messages == []
    assert fake_producer.flush_called is True


def test_ack_timeout_must_be_positive():
    with pytest.raises(ValueError, match="ack_timeout"):
        TripUpdateProducer(FakeKafkaProducer(), ack_timeout=0)
