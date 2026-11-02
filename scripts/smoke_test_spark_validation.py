"""Run a time-bounded Spark Kafka validation and DLQ smoke test."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
    create_spark_session,
    read_kafka_stream,
    select_message_columns,
    split_validated_and_dlq,
    start_validation_query,
    validate_kafka_stream,
    RAW_TOPICS,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke test Spark Kafka schema validation and DLQ routing."
    )
    parser.add_argument(
        "--run-seconds",
        type=int,
        default=45,
        help="How long to run the streaming query (default: 45 seconds).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:29092",
        help="Kafka bootstrap servers (default: kafka:29092).",
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        default=list(RAW_TOPICS),
        help="Kafka topics to subscribe to.",
    )
    parser.add_argument(
        "--checkpoint-location",
        default="/tmp/transport-day30-checkpoint",
        help="Temporary streaming checkpoint location.",
    )
    args = parser.parse_args()

    if args.run_seconds <= 0:
        parser.error("--run-seconds must be greater than zero.")

    spark = None
    query = None

    try:
        spark = create_spark_session()

        kafka_stream = read_kafka_stream(
            spark,
            bootstrap_servers=args.bootstrap_servers,
            topics=tuple(args.topics),
        )
        message_stream = select_message_columns(kafka_stream)
        checked_stream = validate_kafka_stream(message_stream)

        valid_stream, dlq_stream = split_validated_and_dlq(checked_stream)

        query = start_validation_query(
            valid_stream,
            dlq_stream,
            bootstrap_servers=args.bootstrap_servers,
            checkpoint_location=args.checkpoint_location,
        )

        print(f"Validation query started: {query.id}")
        print(f"Running for up to {args.run_seconds} seconds...")

        terminated = query.awaitTermination(args.run_seconds)

        if terminated:
            print("Streaming query terminated unexpectedly.")
            return 1

        if not query.isActive:
            print("Streaming query is not active.")
            return 1

        progress_records = query.recentProgress
        batch_count = len(progress_records)
        total_input_rows = sum(
            int(progress.get("numInputRows", 0)) for progress in progress_records
        )

        print(f"Completed micro-batches: {batch_count}")
        print(f"Total source rows observed (branch reads): {total_input_rows}")

        if total_input_rows <= 0:
            print("SMOKE TEST FAILED: No Kafka messages were consumed.")
            return 1

        print(
            "SMOKE TEST PASSED: Spark consumed and routed "
            "messages through the validation Kafka sink."
        )
        print(
            "Inspect the validated and DLQ topics separately "
            "to confirm both output paths."
        )
        return 0

    finally:
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
