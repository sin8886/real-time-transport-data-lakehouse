"""Unit tests for the Kafka service-alert producer."""

import json

import pytest

from transport_lakehouse.streaming.service_alert_producer import (
    SERVICE_ALERT_TOPIC,
    ServiceAlertProducer,
    build_service_alert_key,
    serialize_service_alert,
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


def make_alert_event(
    source_provider="AT",
    alert_id="alert-123",
):
    return {
        "event_type": "service_alert",
        "source_provider": source_provider,
        "source_entity_id": alert_id,
        "event_ts": "2026-10-30T01:00:00+00:00",
        "ingest_ts": "2026-10-30T01:00:01+00:00",
        "is_deleted": False,
        "alert_id": alert_id,
        "active_start_ts": "2026-10-30T01:00:00+00:00",
        "active_end_ts": None,
        "cause": "CONSTRUCTION",
        "effect": "DETOUR",
        "description": "Service disruption on route 10",
        "affected_route_id": "route-10",
        "affected_trip_id": None,
        "affected_stop_id": "stop-456",
    }


def test_serialize_service_alert_returns_json_bytes():
    event = make_alert_event()

    result = serialize_service_alert(event)

    assert isinstance(result, bytes)
    assert json.loads(result.decode("utf-8")) == event


def test_serialize_preserves_unicode_description():
    event = make_alert_event()
    event["description"] = "线路施工，服务调整"

    result = serialize_service_alert(event)

    assert "线路施工".encode("utf-8") in result
    assert json.loads(result.decode("utf-8")) == event


def test_serialize_rejects_non_service_alert_event():
    event = make_alert_event()
    event["event_type"] = "vehicle_position"

    with pytest.raises(ValueError, match="event_type"):
        serialize_service_alert(event)


@pytest.mark.parametrize(
    ("event", "expected_key"),
    [
        (
            make_alert_event(),
            b"AT:alert-123",
        ),
        (
            make_alert_event(source_provider="Metlink"),
            b"Metlink:alert-123",
        ),
        (
            make_alert_event(alert_id=None),
            None,
        ),
    ],
)
def test_build_service_alert_key(event, expected_key):
    assert build_service_alert_key(event) == expected_key


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("source_provider", None, "source_provider"),
        ("source_provider", "", "source_provider"),
        ("source_provider", 123, "source_provider"),
        ("alert_id", 123, "alert_id"),
    ],
)
def test_serialize_rejects_invalid_identity_fields(field, value, message):
    event = make_alert_event()
    event[field] = value

    with pytest.raises(ValueError, match=message):
        serialize_service_alert(event)


def test_serialize_accepts_missing_alert_id():
    event = make_alert_event(alert_id=None)

    result = serialize_service_alert(event)

    assert json.loads(result.decode("utf-8")) == event


def test_publish_sends_service_alert_with_composite_key():
    fake_producer = FakeKafkaProducer()
    producer = ServiceAlertProducer(fake_producer)
    event = make_alert_event()

    published = producer.publish([event])

    assert published == 1
    assert len(fake_producer.messages) == 1

    message = fake_producer.messages[0]
    assert message["topic"] == SERVICE_ALERT_TOPIC
    assert message["key"] == b"AT:alert-123"
    assert json.loads(message["value"].decode("utf-8")) == event
    assert fake_producer.flush_called is True
    assert fake_producer.futures[0].timeout == 10


def test_publish_uses_null_key_when_alert_id_is_missing():
    fake_producer = FakeKafkaProducer()
    producer = ServiceAlertProducer(fake_producer)
    event = make_alert_event(alert_id=None)

    published = producer.publish([event])

    assert published == 1
    assert fake_producer.messages[0]["key"] is None


def test_publish_ignores_non_service_alert_events():
    fake_producer = FakeKafkaProducer()
    producer = ServiceAlertProducer(fake_producer)

    events = [
        make_alert_event(),
        {
            "event_type": "vehicle_position",
            "vehicle_id": "vehicle-999",
        },
        {
            "event_type": "trip_update",
            "trip_id": "trip-999",
        },
    ]

    published = producer.publish(events)

    assert published == 1
    assert len(fake_producer.messages) == 1


def test_publish_propagates_kafka_send_failure():
    fake_producer = FakeKafkaProducer(
        error=RuntimeError("broker unavailable")
    )
    producer = ServiceAlertProducer(fake_producer)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        producer.publish([make_alert_event()])

    assert fake_producer.flush_called is False


def test_publish_empty_events_returns_zero():
    fake_producer = FakeKafkaProducer()
    producer = ServiceAlertProducer(fake_producer)

    published = producer.publish([])

    assert published == 0
    assert fake_producer.messages == []
    assert fake_producer.flush_called is True


def test_ack_timeout_must_be_positive():
    with pytest.raises(ValueError, match="ack_timeout"):
        ServiceAlertProducer(FakeKafkaProducer(), ack_timeout=0)
