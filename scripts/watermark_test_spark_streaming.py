"""Verify Spark Structured Streaming event-time watermark behavior."""

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
    apply_event_time_watermark,
    create_spark_session,
    parse_event_time,
    read_kafka_stream,
    select_message_columns,
)

TEST_TOPIC = "transport.watermark.test"
DEFAULT_CHECKPOINT = (
    "/opt/transport-lakehouse-checkpoints/day32/watermark-test"
)
DEFAULT_RESULTS = (
    "/opt/transport-lakehouse-checkpoints/day32/watermark-results.jsonl"
)
WATERMARK_DELAY = "5 minutes"
WINDOW_DURATION = "1 minute"


def build_test_event(event_id: str, event_ts: str) -> dict[str, Any]:
    """Build a canonical synthetic event with a controlled event timestamp."""
    return {
        "event_type": "vehicle_position",
        "source_provider": "AT",
        "source_entity_id": event_id,
        "event_ts": event_ts,
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "is_deleted": False,
        "vehicle_id": f"watermark-{event_id}",
        "trip_id": f"watermark-trip-{event_id}",
        "route_id": "DAY32-WATERMARK-TEST",
        "latitude": -36.8485,
        "longitude": 174.7633,
    }


def publish_test_event(
    spark: Any,
    bootstrap_servers: str,
    event_id: str,
    event_ts: str,
) -> None:
    """Publish one synthetic event to the isolated Kafka topic."""
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
    )

    event = build_test_event(event_id, event_ts)
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
            event_id,
            json.dumps(event, ensure_ascii=False, sort_keys=True),
        )
    ]

    (
        spark.createDataFrame(row, schema=schema)
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .save()
    )
    print(f"Published {event_id}: {event_ts}", flush=True)


def start_watermark_query(
    spark: Any,
    bootstrap_servers: str,
    checkpoint_location: str,
    result_path: str,
) -> Any:
    """Start a temporary event-time window aggregation."""
    from pyspark.sql.functions import col, count, window

    kafka_stream = read_kafka_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        topics=(TEST_TOPIC,),
        starting_offsets="latest",
    )
    message_stream = select_message_columns(kafka_stream)

    event_stream = parse_event_time(message_stream).filter(
        col("event_time").isNotNull()
    )
    watermarked_stream = apply_event_time_watermark(
        event_stream,
        WATERMARK_DELAY,
    )

    window_counts = (
        watermarked_stream.groupBy(
            window(col("event_time"), WINDOW_DURATION)
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_count"),
        )
    )

    result_file = Path(result_path)

    def save_batch(batch_df: Any, batch_id: int) -> None:
        """Persist finalized windows emitted by the append-mode query."""
        rows = [
            {
                "batch_id": batch_id,
                "window_start": row["window_start"].strftime("%Y-%m-%dT%H:%M:%S"),
                "window_end": row["window_end"].strftime("%Y-%m-%dT%H:%M:%S"),
                "event_count": row["event_count"],
            }
            for row in batch_df.collect()
        ]

        if rows:
            with result_file.open("a", encoding="utf-8") as file:
                for row in rows:
                    file.write(json.dumps(row) + "\n")
            print(f"Saved {len(rows)} finalized windows.", flush=True)

    return (
        window_counts.writeStream.foreachBatch(save_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="2 seconds")
        .start()
    )


def wait_for_processed_rows(query: Any, phase: str) -> None:
    """Wait until all currently available Kafka data is processed."""
    query.processAllAvailable()

    if not query.isActive:
        raise RuntimeError("Watermark query terminated unexpectedly.")

    progress = query.lastProgress
    input_rows = 0
    if progress is not None:
        input_rows = sum(
            int(item.get("numInputRows", 0))
            for item in progress.get("sources", [])
        )

    print(
        f"{phase}: processing completed; "
        f"latest micro-batch input rows={input_rows}.",
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test Spark Structured Streaming watermark behavior."
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
    args = parser.parse_args()

    checkpoint_path = Path(args.checkpoint_location)
    result_path = Path(args.result_path)

    if checkpoint_path.exists() and any(checkpoint_path.iterdir()):
        parser.error("Checkpoint directory is not empty; use a fresh path.")

    if result_path.exists():
        parser.error("Result file already exists; remove it before rerunning.")

    checkpoint_path.mkdir(parents=True, exist_ok=True)
    result_path.parent.mkdir(parents=True, exist_ok=True)

    spark = None
    query = None

    try:
        spark = create_spark_session()
        query = start_watermark_query(
            spark,
            args.bootstrap_servers,
            args.checkpoint_location,
            args.result_path,
        )
        print(f"Watermark query started: {query.id}", flush=True)

        print("\n===== Phase 1: Initial Events =====", flush=True)
        publish_test_event(
            spark, args.bootstrap_servers,
            "day32-on-time-1", "2026-10-29T10:00:10+00:00",
        )
        publish_test_event(
            spark, args.bootstrap_servers,
            "day32-on-time-2", "2026-10-29T10:00:30+00:00",
        )
        wait_for_processed_rows(query, "Initial events")

        print("\n===== Phase 2: Advance Watermark =====", flush=True)
        publish_test_event(
            spark, args.bootstrap_servers,
            "day32-watermark-advance", "2026-10-29T10:07:00+00:00",
        )
        wait_for_processed_rows(query, "Watermark advance")

        print("\n===== Phase 3: Out-of-order Event =====", flush=True)
        publish_test_event(
            spark, args.bootstrap_servers,
            "day32-out-of-order", "2026-10-29T10:03:00+00:00",
        )
        wait_for_processed_rows(query, "Out-of-order event")

        print("\n===== Phase 4: Too-late Event =====", flush=True)
        publish_test_event(
            spark, args.bootstrap_servers,
            "day32-too-late", "2026-10-29T09:50:00+00:00",
        )
        wait_for_processed_rows(query, "Too-late event")

        print("\n===== Phase 5: Finalize Windows =====", flush=True)
        publish_test_event(
            spark, args.bootstrap_servers,
            "day32-final-advance", "2026-10-29T10:15:00+00:00",
        )
        wait_for_processed_rows(query, "Final event-time advance")

        print("\\n===== Phase 6: Final Watermark Advance =====", flush=True)
        publish_test_event(
            spark,
            args.bootstrap_servers,
            "day32-watermark-finalize",
            "2026-10-29T10:20:00+00:00",
        )
        wait_for_processed_rows(query, "Final watermark advance")

        query.processAllAvailable()
        time.sleep(3)
        query.processAllAvailable()

        print("\n===== Finalized Window Results =====", flush=True)
        results = [
            json.loads(line)
            for line in result_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

        for result in results:
            print(json.dumps(result, ensure_ascii=False), flush=True)

        window_counts = {
            item["window_start"]: item["event_count"]
            for item in results
        }

        expected_counts = {
            "2026-10-29T10:00:00": 2,
            "2026-10-29T10:03:00": 1,
        }

        for window_start, expected_count in expected_counts.items():
            actual_count = window_counts.get(window_start)
            if actual_count != expected_count:
                raise AssertionError(
                    f"Window {window_start}: expected "
                    f"{expected_count}, got {actual_count!r}."
                )

        late_window = "2026-10-29T09:50:00"
        if late_window in window_counts:
            raise AssertionError(
                "The too-late 09:50 event unexpectedly appeared "
                "in finalized window results."
            )

        print("\\nWATERMARK TEST PASSED", flush=True)
        print(
            "The 10:00 and 10:03 windows were finalized correctly; "
            "the too-late 09:50 event was excluded.",
            flush=True,
        )
        return 0

    except Exception as error:
        print(f"\nWATERMARK TEST FAILED: {error}", flush=True)
        return 1

    finally:
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
