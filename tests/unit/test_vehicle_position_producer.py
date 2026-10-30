"""Unit tests for the Kafka vehicle-position producer."""

import json

import pytest

from transport_lakehouse.streaming.vehicle_position_producer import (
    VEHICLE_POSITION_TOPIC,
    VehiclePositionProducer,
    serialize_vehicle_position,
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

    def send(self, topic, key=None, value=None):
        self.messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
            }
        )
        return FakeFuture(self.error)

    def flush(self):
        self.flush_called = True


def make_vehicle_event(vehicle_id="vehicle-123"):
    return {
        "event_type": "vehicle_position",
        "source_provider": "AT",
        "source_entity_id": "entity-456",
        "event_ts": "2026-10-30T01:00:00+00:00",
        "ingest_ts": "2026-10-30T01:00:01+00:00",
        "is_deleted": False,
        "vehicle_id": vehicle_id,
        "trip_id": "trip-789",
        "route_id": "route-10",
        "latitude": -36.8485,
        "longitude": 174.7633,
        "speed": 12.5,
        "bearing": 180,
    }


def test_serialize_vehicle_position_returns_json_bytes():
    event = make_vehicle_event()

    result = serialize_vehicle_position(event)

    assert isinstance(result, bytes)
    assert json.loads(result.decode("utf-8")) == event


def test_serialize_rejects_non_vehicle_event():
    event = make_vehicle_event()
    event["event_type"] = "trip_update"

    with pytest.raises(ValueError, match="event_type"):
        serialize_vehicle_position(event)


def test_publish_sends_vehicle_event_with_vehicle_id_as_key():
    fake_producer = FakeKafkaProducer()
    producer = VehiclePositionProducer(fake_producer)
    event = make_vehicle_event()

    published = producer.publish([event])

    assert published == 1
    assert len(fake_producer.messages) == 1

    message = fake_producer.messages[0]
    assert message["topic"] == VEHICLE_POSITION_TOPIC
    assert message["key"] == b"vehicle-123"
    assert json.loads(message["value"].decode("utf-8")) == event
    assert fake_producer.flush_called is True


def test_publish_ignores_non_vehicle_events():
    fake_producer = FakeKafkaProducer()
    producer = VehiclePositionProducer(fake_producer)

    events = [
        make_vehicle_event(),
        {
            "event_type": "trip_update",
            "vehicle_id": "vehicle-999",
        },
        {
            "event_type": "service_alert",
        },
    ]

    published = producer.publish(events)

    assert published == 1
    assert len(fake_producer.messages) == 1


def test_publish_uses_null_key_when_vehicle_id_is_missing():
    fake_producer = FakeKafkaProducer()
    producer = VehiclePositionProducer(fake_producer)
    event = make_vehicle_event(vehicle_id=None)

    published = producer.publish([event])

    assert published == 1
    assert fake_producer.messages[0]["key"] is None


def test_publish_rejects_non_string_vehicle_id():
    fake_producer = FakeKafkaProducer()
    producer = VehiclePositionProducer(fake_producer)
    event = make_vehicle_event(vehicle_id=123)

    with pytest.raises(ValueError, match="vehicle_id"):
        producer.publish([event])


def test_publish_propagates_kafka_send_failure():
    fake_producer = FakeKafkaProducer(error=RuntimeError("broker unavailable"))
    producer = VehiclePositionProducer(fake_producer)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        producer.publish([make_vehicle_event()])

    assert fake_producer.flush_called is False


def test_publish_empty_events_returns_zero():
    fake_producer = FakeKafkaProducer()
    producer = VehiclePositionProducer(fake_producer)

    published = producer.publish([])

    assert published == 0
    assert fake_producer.messages == []
    assert fake_producer.flush_called is True


def test_ack_timeout_must_be_positive():
    with pytest.raises(ValueError, match="ack_timeout"):
        VehiclePositionProducer(FakeKafkaProducer(), ack_timeout=0)
