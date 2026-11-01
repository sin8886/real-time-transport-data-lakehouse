"""Kafka producer for canonical GTFS-RT service-alert events."""

from __future__ import annotations

import json
import os
from typing import Any, Mapping, Sequence

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
SERVICE_ALERT_TOPIC = "transport.service_alerts.raw"
ACK_TIMEOUT_SECONDS = 10


def serialize_service_alert(event: Mapping[str, Any]) -> bytes:
    """Serialize one canonical service-alert event as UTF-8 JSON."""
    if event.get("event_type") != "service_alert":
        raise ValueError("event_type must be 'service_alert'")

    source_provider = event.get("source_provider")
    if not isinstance(source_provider, str) or not source_provider:
        raise ValueError("source_provider must be a non-empty string")

    alert_id = event.get("alert_id")
    if alert_id is not None and not isinstance(alert_id, str):
        raise ValueError("alert_id must be a string or None")

    return json.dumps(
        dict(event),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")


def build_service_alert_key(event: Mapping[str, Any]) -> bytes | None:
    """Build a Kafka key from provider and alert identity.

    Events without an alert_id use a null Kafka key.
    """
    serialize_service_alert(event)

    alert_id = event.get("alert_id")
    if not alert_id:
        return None

    return f"{event['source_provider']}:{alert_id}".encode("utf-8")


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


class ServiceAlertProducer:
    """Publish canonical service-alert events to the raw Kafka topic."""

    def __init__(
        self,
        producer: Any,
        topic: str = SERVICE_ALERT_TOPIC,
        ack_timeout: int = ACK_TIMEOUT_SECONDS,
    ) -> None:
        if ack_timeout <= 0:
            raise ValueError("ack_timeout must be positive")

        self.producer = producer
        self.topic = topic
        self.ack_timeout = ack_timeout

    def publish(self, events: Sequence[Mapping[str, Any]]) -> int:
        """Publish service-alert events and wait for broker acknowledgements.

        Events without an alert_id are published with a null Kafka key.
        Other event types are ignored.
        """
        published = 0

        for event in events:
            if event.get("event_type") != "service_alert":
                continue

            value = serialize_service_alert(event)
            key = build_service_alert_key(event)

            future = self.producer.send(
                self.topic,
                key=key,
                value=value,
            )
            future.get(timeout=self.ack_timeout)
            published += 1

        self.producer.flush()
        return published
