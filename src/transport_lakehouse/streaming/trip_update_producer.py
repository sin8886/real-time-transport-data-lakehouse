"""Kafka producer for canonical GTFS-RT trip-update events."""

from __future__ import annotations

import json
import os
from typing import Any, Mapping, Sequence

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
TRIP_UPDATE_TOPIC = "transport.trip_updates.raw"
ACK_TIMEOUT_SECONDS = 10


def serialize_trip_update(event: Mapping[str, Any]) -> bytes:
    """Serialize one canonical trip-update event as UTF-8 JSON."""
    if event.get("event_type") != "trip_update":
        raise ValueError("event_type must be 'trip_update'")

    trip_id = event.get("trip_id")
    if not isinstance(trip_id, str) or not trip_id:
        raise ValueError("trip_id must be a non-empty string")

    source_provider = event.get("source_provider")
    if not isinstance(source_provider, str) or not source_provider:
        raise ValueError("source_provider must be a non-empty string")

    stop_sequence = event.get("stop_sequence")
    stop_id = event.get("stop_id")

    if stop_sequence is not None:
        if isinstance(stop_sequence, bool) or not isinstance(stop_sequence, int):
            raise ValueError("stop_sequence must be an integer or None")
    elif not isinstance(stop_id, str) or not stop_id:
        raise ValueError(
            "trip_update requires stop_sequence or a non-empty stop_id"
        )

    return json.dumps(
        dict(event),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")

def build_trip_update_key(event: Mapping[str, Any]) -> bytes:
    """Build a Kafka key from the canonical Trip Stop business context."""
    # Reuse serialization validation to ensure the event is accepted.
    serialize_trip_update(event)

    source_provider = event["source_provider"]
    trip_id = event["trip_id"]
    stop_sequence = event.get("stop_sequence")

    if stop_sequence is not None:
        stop_context = f"seq:{stop_sequence}"
    else:
        stop_context = f"stop:{event['stop_id']}"

    return f"{source_provider}:{trip_id}:{stop_context}".encode("utf-8")


def create_kafka_producer() -> Any:
    """Create a Kafka producer with broker acknowledgements and retries."""
    try:
        from kafka import KafkaProducer
    except ImportError as error:
        raise RuntimeError(
            "kafka-python is required; install project dependencies first"
        ) from error

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_BOOTSTRAP_SERVERS,
    )

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )


class TripUpdateProducer:
    """Publish canonical trip-update events to the raw Kafka topic."""

    def __init__(
        self,
        producer: Any,
        topic: str = TRIP_UPDATE_TOPIC,
        ack_timeout: int = ACK_TIMEOUT_SECONDS,
    ) -> None:
        if ack_timeout <= 0:
            raise ValueError("ack_timeout must be positive")

        self.producer = producer
        self.topic = topic
        self.ack_timeout = ack_timeout

    def publish(self, events: Sequence[Mapping[str, Any]]) -> int:
        """Publish trip-update events and wait for broker acknowledgements.

        Other event types are ignored.
        """
        published = 0

        for event in events:
            if event.get("event_type") != "trip_update":
                continue

            value = serialize_trip_update(event)
            key = build_trip_update_key(event)

            future = self.producer.send(
                self.topic,
                key=key,
                value=value,
            )
            future.get(timeout=self.ack_timeout)
            published += 1

        self.producer.flush()
        return published
