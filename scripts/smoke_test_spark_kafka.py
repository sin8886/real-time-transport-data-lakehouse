"""Run a time-bounded Spark Structured Streaming Kafka smoke test."""

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
    start_console_query,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke test Spark Structured Streaming with Kafka."
    )
    parser.add_argument(
        "--run-seconds",
        type=int,
        default=30,
        help="How long to run the streaming query (default: 30 seconds).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:29092",
        help="Kafka bootstrap servers (default: kafka:29092).",
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
        )
        message_stream = select_message_columns(kafka_stream)

        query = start_console_query(message_stream)

        print(f"Streaming query started: {query.id}")
        print(f"Running for up to {args.run_seconds} seconds...")

        terminated = query.awaitTermination(args.run_seconds)

        if terminated:
            print("Streaming query terminated before the time limit.")
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
        print(f"Total Kafka messages consumed: {total_input_rows}")

        if total_input_rows <= 0:
            print("SMOKE TEST FAILED: No Kafka messages were consumed.")
            return 1

        print("SMOKE TEST PASSED: Spark consumed Kafka messages successfully.")
        return 0

    finally:
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
