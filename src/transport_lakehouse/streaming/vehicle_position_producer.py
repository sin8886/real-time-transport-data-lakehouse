"""Kafka producer for canonical GTFS-RT vehicle-position events."""

from __future__ import annotations

import json
import os
from typing import Any, Mapping, Sequence

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
VEHICLE_POSITION_TOPIC = "transport.vehicle_positions.raw"
ACK_TIMEOUT_SECONDS = 10


def serialize_vehicle_position(event: Mapping[str, Any]) -> bytes:
    """Serialize one canonical vehicle-position event as UTF-8 JSON."""
    if event.get("event_type") != "vehicle_position":
        raise ValueError("event_type must be 'vehicle_position'")

    vehicle_id = event.get("vehicle_id")
    if vehicle_id is not None and not isinstance(vehicle_id, str):
        raise ValueError("vehicle_id must be a string or None")

    return json.dumps(
        dict(event),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")


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


class VehiclePositionProducer:
    """Publish canonical vehicle-position events to the raw Kafka topic."""

    def __init__(
        self,
        producer: Any,
        topic: str = VEHICLE_POSITION_TOPIC,
        ack_timeout: int = ACK_TIMEOUT_SECONDS,
    ) -> None:
        if ack_timeout <= 0:
            raise ValueError("ack_timeout must be positive")

        self.producer = producer
        self.topic = topic
        self.ack_timeout = ack_timeout

    def publish(self, events: Sequence[Mapping[str, Any]]) -> int:
        """Publish vehicle-position events and wait for broker acknowledgements.

        Events without a vehicle_id are sent with a null Kafka key.
        Other event types are ignored.
        """
        published = 0

        for event in events:
            if event.get("event_type") != "vehicle_position":
                continue

            value = serialize_vehicle_position(event)
            vehicle_id = event.get("vehicle_id")
            key = vehicle_id.encode("utf-8") if vehicle_id is not None else None

            future = self.producer.send(
                self.topic,
                key=key,
                value=value,
            )
            future.get(timeout=self.ack_timeout)
            published += 1

        self.producer.flush()
        return published
