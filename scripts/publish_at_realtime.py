"""Poll Auckland GTFS-Realtime and publish vehicle events to Kafka."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from transport_lakehouse.ingestion.realtime_fetcher import RealtimeFetcher  # noqa: E402
from transport_lakehouse.streaming.vehicle_position_producer import (  # noqa: E402
    VehiclePositionProducer,
    create_kafka_producer,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Continuously publish Auckland vehicle positions to Kafka."
    )
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")

    fetcher = RealtimeFetcher()
    kafka_producer = create_kafka_producer()
    producer = VehiclePositionProducer(kafka_producer)

    try:
        while True:
            events = fetcher.fetch(response_format="json")
            published = producer.publish(events)
            print(f"Published {published} Auckland vehicle events.", flush=True)
            if args.once:
                return
            time.sleep(args.interval)
    finally:
        kafka_producer.close()


if __name__ == "__main__":
    main()
