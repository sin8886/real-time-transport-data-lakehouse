"""Verify Spark Structured Streaming checkpoint restart and recovery."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
    DEFAULT_BOOTSTRAP_SERVERS,
    create_spark_session,
    read_kafka_stream,
    select_message_columns,
)

TEST_TOPIC = "transport.vehicle_positions.raw"
DEFAULT_CHECKPOINT = "/opt/transport-lakehouse-checkpoints/day31/recovery-test"


def build_test_event(index: int) -> dict[str, Any]:
    """Build a canonical synthetic vehicle-position event."""
    timestamp = datetime.now(timezone.utc).isoformat()
    return {
        "event_type": "vehicle_position",
        "source_provider": "AT",
        "source_entity_id": f"day31-recovery-{index}",
        "event_ts": timestamp,
        "ingest_ts": timestamp,
        "is_deleted": False,
        "vehicle_id": f"day31-vehicle-{index}",
        "trip_id": f"day31-trip-{index}",
        "route_id": "DAY31-TEST",
        "latitude": -36.8485,
        "longitude": 174.7633,
        "speed": 8.5,
        "bearing": 90.0,
    }


def publish_test_event(
    spark: Any,
    bootstrap_servers: str,
    topic: str,
    index: int,
) -> None:
    """Publish one synthetic event to the raw Kafka topic using Spark."""
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
    )

    event = build_test_event(index)
    schema = StructType(
        [
            StructField("topic", StringType(), False),
            StructField("key", StringType(), False),
            StructField("value", StringType(), False),
        ]
    )
    row = [
        (
            topic,
            event["vehicle_id"],
            json.dumps(event, ensure_ascii=False, sort_keys=True),
        )
    ]

    (
        spark.createDataFrame(row, schema=schema)
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .save()
    )

    print(f"Published test event: {event['source_entity_id']}")


def start_recovery_query(
    spark: Any,
    bootstrap_servers: str,
    topic: str,
    checkpoint_location: str,
) -> Any:
    """Start a Kafka streaming query using the persistent checkpoint."""
    kafka_stream = read_kafka_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        topics=(topic,),
        starting_offsets="latest",
    )
    message_stream = select_message_columns(kafka_stream)

    return (
        message_stream.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="2 seconds")
        .start()
    )


def wait_for_input_rows(query: Any, expected_rows: int) -> int:
    """Wait until the query has processed the expected number of rows."""
    deadline = time.monotonic() + 45

    while time.monotonic() < deadline:
        query.processAllAvailable()

        progress = query.recentProgress
        total_rows = sum(int(item.get("numInputRows", 0)) for item in progress)

        if total_rows >= expected_rows:
            return total_rows

        if not query.isActive:
            raise RuntimeError("Streaming query terminated unexpectedly.")

        time.sleep(1)

    raise TimeoutError(
        f"Expected {expected_rows} input rows, but observed {total_rows}."
    )


def checkpoint_has_progress(checkpoint_location: Path) -> bool:
    """Check that Spark created offset and commit metadata."""
    offsets = checkpoint_location / "offsets"
    commits = checkpoint_location / "commits"

    return (
        offsets.is_dir()
        and commits.is_dir()
        and any(offsets.iterdir())
        and any(commits.iterdir())
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=("Test Spark Structured Streaming checkpoint restart and recovery.")
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
    )
    parser.add_argument(
        "--checkpoint-location",
        default=DEFAULT_CHECKPOINT,
    )
    parser.add_argument(
        "--topic",
        default=TEST_TOPIC,
        help="Kafka topic isolated for this recovery test.",
    )
    args = parser.parse_args()

    checkpoint_path = Path(args.checkpoint_location)

    if checkpoint_path.exists() and any(checkpoint_path.iterdir()):
        parser.error(
            "Checkpoint directory is not empty. "
            "Use a fresh checkpoint directory for a new test run."
        )

    checkpoint_path.mkdir(parents=True, exist_ok=True)

    spark = None
    query = None

    try:
        spark = create_spark_session()

        print("\n===== Phase 1: Initial Query =====")
        query = start_recovery_query(
            spark,
            args.bootstrap_servers,
            args.topic,
            args.checkpoint_location,
        )
        print(f"Initial query started: {query.id}")

        publish_test_event(spark, args.bootstrap_servers, args.topic, 1)
        first_rows = wait_for_input_rows(query, expected_rows=1)

        if first_rows != 1:
            raise RuntimeError(f"Expected exactly 1 first-phase row, got {first_rows}.")

        print(f"First phase processed rows: {first_rows}")

        query.stop()
        query = None
        print("Initial query stopped gracefully.")

        if not checkpoint_has_progress(checkpoint_path):
            raise RuntimeError("Checkpoint offset/commit metadata was not created.")

        print("Checkpoint offset and commit metadata verified.")

        print("\n===== Phase 2: Restart and Recovery =====")
        query = start_recovery_query(
            spark,
            args.bootstrap_servers,
            args.topic,
            args.checkpoint_location,
        )
        print(f"Recovered query started: {query.id}")

        publish_test_event(spark, args.bootstrap_servers, args.topic, 2)
        second_rows = wait_for_input_rows(query, expected_rows=1)

        if second_rows != 1:
            raise RuntimeError(
                f"Expected exactly 1 recovery-phase row, got {second_rows}."
            )

        print(f"Recovery phase processed rows: {second_rows}")

        print("\nRECOVERY TEST PASSED")
        print("The query restarted with the same checkpoint.")
        print("The second Kafka event was consumed successfully.")
        print("Checkpoint progress was retained across query restart.")
        return 0

    except Exception as error:
        print(f"\nRECOVERY TEST FAILED: {error}")
        return 1

    finally:
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
