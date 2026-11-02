"""Verify contract-based deduplication in Spark Structured Streaming."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
    DEFAULT_BOOTSTRAP_SERVERS,
    create_spark_session,
    parse_event_time,
    read_kafka_stream,
    select_message_columns,
    split_and_deduplicate_events,
)

TEST_TOPIC = "transport.dedup.test"
DEFAULT_CHECKPOINT = "/opt/transport-lakehouse-checkpoints/day33/dedup-test"
DEFAULT_RESULTS = "/opt/transport-lakehouse-checkpoints/day33/dedup-results.jsonl"
DEFAULT_UNRESOLVED = (
    "/opt/transport-lakehouse-checkpoints/day33/unresolved-results.jsonl"
)
WATERMARK_DELAY = "5 minutes"


def build_event(
    event_type: str,
    event_ts: str,
    **fields: Any,
) -> dict[str, Any]:
    """Build one canonical synthetic event."""
    return {
        "event_type": event_type,
        "source_provider": "AT",
        "event_ts": event_ts,
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "is_deleted": False,
        **fields,
    }


def test_events() -> list[tuple[str, dict[str, Any]]]:
    """Return controlled duplicates, legitimate updates, and unresolved data."""
    vehicle = build_event(
        "vehicle_position",
        "2026-10-29T10:00:00+00:00",
        source_entity_id="vehicle-entity-1",
        vehicle_id="vehicle-1",
        trip_id="trip-vehicle-1",
    )
    vehicle_later = build_event(
        "vehicle_position",
        "2026-10-29T10:01:00+00:00",
        source_entity_id="vehicle-entity-1",
        vehicle_id="vehicle-1",
        trip_id="trip-vehicle-1",
    )

    trip_stop_1 = build_event(
        "trip_update",
        "2026-10-29T10:00:00+00:00",
        source_entity_id="trip-entity-1",
        trip_id="trip-1",
        stop_sequence=1,
        stop_id="STOP-A",
    )
    trip_stop_2 = build_event(
        "trip_update",
        "2026-10-29T10:00:00+00:00",
        source_entity_id="trip-entity-1",
        trip_id="trip-1",
        stop_sequence=2,
        stop_id="STOP-B",
    )

    alert = build_event(
        "service_alert",
        "2026-10-29T10:00:00+00:00",
        source_entity_id="alert-entity-1",
        alert_id="alert-1",
    )
    alert_update = build_event(
        "service_alert",
        "2026-10-29T10:01:00+00:00",
        source_entity_id="alert-entity-1",
        alert_id="alert-1",
    )

    unresolved = build_event(
        "vehicle_position",
        "2026-10-29T10:00:00+00:00",
        vehicle_id="vehicle-without-source-entity",
        trip_id="trip-unresolved",
    )

    return [
        ("vehicle-original", vehicle),
        ("vehicle-duplicate", vehicle),
        ("vehicle-new-event-time", vehicle_later),
        ("trip-stop-1-original", trip_stop_1),
        ("trip-stop-1-duplicate", trip_stop_1),
        ("trip-stop-2", trip_stop_2),
        ("alert-original", alert),
        ("alert-duplicate", alert),
        ("alert-lifecycle-update", alert_update),
        ("unresolved-vehicle", unresolved),
    ]


def publish_event(
    spark: Any,
    bootstrap_servers: str,
    label: str,
    event: dict[str, Any],
) -> None:
    """Publish one event to the isolated Kafka test topic."""
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("topic", StringType(), False),
            StructField("key", StringType(), False),
            StructField("value", StringType(), False),
        ]
    )
    row = [
        (
            TEST_TOPIC,
            label,
            json.dumps(event, ensure_ascii=False, sort_keys=True),
        )
    ]

    (
        spark.createDataFrame(row, schema=schema)
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .save()
    )
    print(f"Published {label}", flush=True)


def save_rows(result_path: Path, batch_df: Any, batch_id: int) -> None:
    """Append one micro-batch of results to a JSONL file."""
    rows = []
    for row in batch_df.select(
        "message_value",
        "dedup_identity",
        "identity_status",
    ).collect():
        event = json.loads(row["message_value"])
        rows.append(
            {
                "batch_id": batch_id,
                "event_type": event.get("event_type"),
                "source_provider": event.get("source_provider"),
                "source_entity_id": event.get("source_entity_id"),
                "event_ts": event.get("event_ts"),
                "trip_id": event.get("trip_id"),
                "stop_id": event.get("stop_id"),
                "stop_sequence": event.get("stop_sequence"),
                "alert_id": event.get("alert_id"),
                "dedup_identity": row["dedup_identity"],
                "identity_status": row["identity_status"],
            }
        )

    if rows:
        with result_path.open("a", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(
            f"Saved {len(rows)} rows from micro-batch {batch_id}.",
            flush=True,
        )


def start_result_query(
    stream: Any,
    checkpoint_location: str,
    result_path: Path,
) -> Any:
    """Start a streaming query that persists its selected branch."""
    return (
        stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: save_rows(
                result_path,
                batch_df,
                batch_id,
            )
        )
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="2 seconds")
        .start()
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test contract-based streaming deduplication."
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
        "--result-path",
        default=DEFAULT_RESULTS,
    )
    parser.add_argument(
        "--unresolved-path",
        default=DEFAULT_UNRESOLVED,
    )
    args = parser.parse_args()

    checkpoint_path = Path(args.checkpoint_location)
    result_path = Path(args.result_path)
    unresolved_path = Path(args.unresolved_path)

    if checkpoint_path.exists() and any(checkpoint_path.iterdir()):
        parser.error("Checkpoint directory is not empty; use a fresh path.")

    if result_path.exists() or unresolved_path.exists():
        parser.error("Result file already exists; use fresh result paths.")

    checkpoint_path.mkdir(parents=True, exist_ok=True)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    unresolved_path.parent.mkdir(parents=True, exist_ok=True)

    spark = None
    dedup_query = None
    unresolved_query = None

    try:
        spark = create_spark_session()

        kafka_stream = read_kafka_stream(
            spark,
            bootstrap_servers=args.bootstrap_servers,
            topics=(TEST_TOPIC,),
            starting_offsets="latest",
        )
        message_stream = select_message_columns(kafka_stream)

        event_stream = parse_event_time(message_stream)
        deduplicated, unresolved = split_and_deduplicate_events(
            event_stream,
            delay_threshold=WATERMARK_DELAY,
        )

        dedup_query = start_result_query(
            deduplicated,
            args.checkpoint_location,
            result_path,
        )

        unresolved_query = start_result_query(
            unresolved,
            f"{args.checkpoint_location}-unresolved",
            unresolved_path,
        )

        print(f"Dedup query started: {dedup_query.id}", flush=True)
        print(
            f"Unresolved query started: {unresolved_query.id}",
            flush=True,
        )

        print("\\n===== Publish Test Events =====", flush=True)
        for label, event in test_events():
            publish_event(spark, args.bootstrap_servers, label, event)

        print("\\n===== Process Test Events =====", flush=True)
        dedup_query.processAllAvailable()
        unresolved_query.processAllAvailable()

        if not dedup_query.isActive or not unresolved_query.isActive:
            raise RuntimeError("A streaming query terminated unexpectedly.")

        dedup_rows = [
            json.loads(line)
            for line in result_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        unresolved_rows = [
            json.loads(line)
            for line in unresolved_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

        print("\n===== Deduplicated Results =====", flush=True)
        for row in dedup_rows:
            print(json.dumps(row, ensure_ascii=False), flush=True)

        print("\n===== Unresolved Results =====", flush=True)
        for row in unresolved_rows:
            print(json.dumps(row, ensure_ascii=False), flush=True)

        expected_counts = {
            "vehicle_position": 2,
            "trip_update": 2,
            "service_alert": 2,
        }
        actual_counts = {
            event_type: sum(
                row["event_type"] == event_type for row in dedup_rows
            )
            for event_type in expected_counts
        }

        if actual_counts != expected_counts:
            raise AssertionError(
                f"Expected event counts {expected_counts}, "
                f"got {actual_counts}."
            )

        if len(dedup_rows) != 6:
            raise AssertionError(
                f"Expected 6 deduplicated rows, got {len(dedup_rows)}."
            )

        if len(unresolved_rows) != 1:
            raise AssertionError(
                f"Expected 1 unresolved row, got {len(unresolved_rows)}."
            )

        if any(
            row["identity_status"] != "resolved"
            for row in dedup_rows
        ):
            raise AssertionError("Unresolved identity entered dedup results.")

        if any(
            row["identity_status"] != "identity_unresolved"
            for row in unresolved_rows
        ):
            raise AssertionError("Unexpected status in unresolved results.")

        print("\nDEDUP TEST PASSED", flush=True)
        print(
            "Duplicate events were removed; legitimate event-time and "
            "stop-context changes were retained; unresolved identity "
            "was kept separate.",
            flush=True,
        )
        return 0

    except Exception as error:
        print(f"\nDEDUP TEST FAILED: {error}", flush=True)
        return 1

    finally:
        if dedup_query is not None and dedup_query.isActive:
            dedup_query.stop()
        if unresolved_query is not None and unresolved_query.isActive:
            unresolved_query.stop()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
