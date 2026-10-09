"""Consume and print canonical demo events from the local Kafka lab topic."""

from __future__ import annotations

import json
from typing import Any

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "transport.lab"
CONSUMER_GROUP = "transport-lab-python"


def deserialize_event(value: bytes | str) -> dict[str, Any]:
    """Decode one Kafka value as a JSON object."""
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise ValueError("Kafka JSON value must be an object")
    return decoded


def decode_key(key: bytes | str | None) -> str | None:
    """Decode an optional Kafka message key for display."""
    if key is None:
        return None
    if isinstance(key, bytes):
        return key.decode("utf-8")
    return key


def create_consumer() -> Any:
    """Create a Kafka consumer, importing kafka-python only when needed."""
    try:
        from kafka import KafkaConsumer
    except ImportError as error:
        raise RuntimeError(
            "kafka-python is required to run the consumer; "
            "install it in .venv with: python -m pip install kafka-python"
        ) from error

    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def consume_messages() -> None:
    """Consume messages until interrupted and print Kafka metadata and JSON."""
    consumer = create_consumer()
    try:
        for message in consumer:
            try:
                value = deserialize_event(message.value)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                print(
                    f"FAIL partition={message.partition} offset={message.offset} "
                    f"invalid JSON: {error}"
                )
                continue
            print(
                f"partition={message.partition} offset={message.offset} "
                f"key={decode_key(message.key)} value={value}"
            )
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_messages()
