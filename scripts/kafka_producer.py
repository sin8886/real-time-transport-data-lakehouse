"""Produce canonical demo events to the local Kafka lab topic."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Mapping

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "transport.lab"
REQUIRED_EVENT_FIELDS = (
    "event_type",
    "source_provider",
    "source_entity_id",
    "event_ts",
    "ingest_ts",
    "is_deleted",
)
EventPayload = dict[str, object]


def validate_event_payload(payload: Mapping[str, object]) -> None:
    """Validate the required v1-style fields for a demo realtime event."""
    missing = [field for field in REQUIRED_EVENT_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")
    if payload["event_type"] not in {
        "vehicle_position",
        "trip_update",
        "service_alert",
    }:
        raise ValueError(f"unsupported event_type: {payload['event_type']!r}")
    if not isinstance(payload["source_provider"], str):
        raise ValueError("source_provider must be a string")
    if not isinstance(payload["is_deleted"], bool):
        raise ValueError("is_deleted must be a boolean")


def serialize_event(payload: Mapping[str, object]) -> bytes:
    """Validate and serialize one event as UTF-8 JSON."""
    validate_event_payload(payload)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")


def build_demo_event(index: int) -> EventPayload:
    """Build one synthetic AT vehicle-position event."""
    timestamp = datetime.now(timezone.utc).isoformat()
    return {
        "event_type": "vehicle_position",
        "source_provider": "AT",
        "source_entity_id": f"demo-vehicle-{index:03d}",
        "event_ts": timestamp,
        "ingest_ts": timestamp,
        "is_deleted": False,
    }


def create_producer() -> Any:
    """Create a Kafka producer, importing kafka-python only when needed."""
    try:
        from kafka import KafkaProducer
    except ImportError as error:
        raise RuntimeError(
            "kafka-python is required to run the producer; "
            "install it in .venv with: python -m pip install kafka-python"
        ) from error

    return KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)


def produce_demo_events(count: int = 5) -> None:
    """Send synthetic demo events and print their Kafka partition and offset."""
    if count < 5:
        raise ValueError("count must be at least 5")

    producer = create_producer()
    try:
        for index in range(1, count + 1):
            event = build_demo_event(index)
            future = producer.send(TOPIC, value=serialize_event(event))
            metadata = future.get(timeout=10)
            print(
                f"sent source_entity_id={event['source_entity_id']} "
                f"partition={metadata.partition} offset={metadata.offset}"
            )
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    produce_demo_events()
