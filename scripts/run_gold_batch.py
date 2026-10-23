"""Run the daily GTFS Static Gold batch pipeline."""

from __future__ import annotations

from pathlib import Path

from transport_lakehouse.processing.gold_metrics import (
    build_gold_metrics,
    filter_active_trips,
    get_active_service_ids,
)
from transport_lakehouse.processing.gold_writer import write_gold_parquet
from transport_lakehouse.processing.parse_static import parse_static_feed
from transport_lakehouse.processing.spark_batch import (
    build_trip_stop_events,
    create_spark_session,
    load_gtfs_tables,
)


def main() -> None:
    provider = "at"
    service_date = "2026-10-22"
    gtfs_dir = "data/gtfs/at"
    output_dir = "data/gold/at"

    spark = create_spark_session()

    try:
        print(f"Running Gold batch for {provider}: {service_date}")

        # Load GTFS service calendar tables.
        parsed_tables = parse_static_feed(Path(gtfs_dir))

        active_service_ids = get_active_service_ids(
            calendar=parsed_tables["calendar"],
            calendar_dates=parsed_tables["calendar_dates"],
            service_date=service_date,
        )

        print(f"Active service IDs: {len(active_service_ids)}")

        # Load the four core GTFS tables once.
        tables = load_gtfs_tables(spark, gtfs_dir)

        # Build trip-stop events from the loaded DataFrames.
        events = build_trip_stop_events(
            routes=tables["routes"],
            trips=tables["trips"],
            stop_times=tables["stop_times"],
            stops=tables["stops"],
        )

        # Filter trips according to the target service date.
        active_trips = filter_active_trips(
            tables["trips"],
            active_service_ids,
        )

        active_trip_ids = active_trips.select("trip_id")

        active_events = events.join(
            active_trip_ids,
            on="trip_id",
            how="inner",
        )

        print(f"Active trips: {active_trips.count()}")
        print(f"Active trip-stop events: {active_events.count()}")

        # Build daily, route, and stop Gold metrics.
        daily_metrics, route_metrics, stop_metrics = build_gold_metrics(
            active_events,
            service_date,
        )

        # Write all three Gold datasets as local Parquet.
        output_paths = write_gold_parquet(
            daily_metrics=daily_metrics,
            route_metrics=route_metrics,
            stop_metrics=stop_metrics,
            output_dir=output_dir,
            service_date=service_date,
        )

        for dataset_name, dataset_path in output_paths.items():
            print(f"{dataset_name}: {dataset_path}")

        print("Gold batch pipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
