"""Gold metrics transformations for GTFS Static data."""

from __future__ import annotations

from datetime import date
from typing import Any


def get_active_service_ids(
    calendar: list[dict[str, Any]],
    calendar_dates: list[dict[str, Any]],
    service_date: str,
) -> set[str]:
    """Return service IDs active on a specific date."""
    try:
        parsed_date = date.fromisoformat(service_date)
    except (TypeError, ValueError) as exc:
        raise ValueError("service_date must use YYYY-MM-DD format") from exc

    if parsed_date.isoformat() != service_date:
        raise ValueError("service_date must use YYYY-MM-DD format")

    date_value = parsed_date.strftime("%Y%m%d")
    weekday_column = parsed_date.strftime("%A").lower()

    active_services = {
        row["service_id"]
        for row in calendar
        if row[weekday_column] == "1"
        and row["start_date"] <= date_value <= row["end_date"]
    }

    for row in calendar_dates:
        if row["date"] != date_value:
            continue

        if row["exception_type"] == "1":
            active_services.add(row["service_id"])
        elif row["exception_type"] == "2":
            active_services.discard(row["service_id"])

    return active_services


def filter_active_trips(
    trips: Any,
    active_service_ids: set[str],
) -> Any:
    """Keep trips whose service IDs are active on the target date."""
    from pyspark.sql import functions as F

    if not active_service_ids:
        return trips.filter(F.lit(False))

    return trips.filter(F.col("service_id").isin(sorted(active_service_ids)))


def build_gold_metrics(
    events: Any,
    service_date: str,
) -> tuple[Any, Any, Any]:
    """Build daily, route, and stop Gold metrics from trip-stop events."""
    from pyspark.sql import functions as F

    required_columns = {"trip_id", "route_id", "stop_id"}
    missing_columns = required_columns.difference(events.columns)

    if missing_columns:
        raise ValueError(
            f"Events DataFrame is missing required columns: {sorted(missing_columns)}"
        )

    dated_events = events.withColumn(
        "service_date",
        F.lit(service_date),
    )

    daily_metrics = (
        dated_events.agg(
            F.countDistinct("trip_id").alias("scheduled_trip_count"),
            F.countDistinct("route_id").alias("active_route_count"),
            F.countDistinct("stop_id").alias("active_stop_count"),
        )
        .withColumn(
            "service_date",
            F.lit(service_date),
        )
        .select(
            "service_date",
            "scheduled_trip_count",
            "active_route_count",
            "active_stop_count",
        )
    )

    route_metrics = dated_events.groupBy(
        "service_date",
        "route_id",
    ).agg(
        F.countDistinct("trip_id").alias("scheduled_trip_count"),
        F.count("*").alias("stop_event_count"),
        F.countDistinct("stop_id").alias("served_stop_count"),
    )

    stop_metrics = dated_events.groupBy(
        "service_date",
        "stop_id",
    ).agg(
        F.countDistinct("trip_id").alias("scheduled_trip_count"),
        F.count("*").alias("stop_event_count"),
    )

    return daily_metrics, route_metrics, stop_metrics


def build_service_reliability_gold(
    trips: Any,
    trip_updates: Any,
    calendar: list[dict[str, Any]],
    calendar_dates: list[dict[str, Any]],
    service_date: str,
    delay_threshold_seconds: int = 300,
) -> Any:
    """Build route-level service reliability metrics for one service date."""
    from pyspark.sql import functions as F

    if delay_threshold_seconds < 0:
        raise ValueError("delay_threshold_seconds cannot be negative")

    required_trip_columns = {"trip_id", "route_id", "service_id"}
    missing_trip_columns = required_trip_columns.difference(trips.columns)
    if missing_trip_columns:
        raise ValueError(
            "Trips DataFrame is missing required columns: "
            f"{sorted(missing_trip_columns)}"
        )

    required_update_columns = {
        "event_ts",
        "trip_id",
        "route_id",
        "delay_seconds",
    }
    missing_update_columns = required_update_columns.difference(
        trip_updates.columns
    )
    if missing_update_columns:
        raise ValueError(
            "Trip updates DataFrame is missing required columns: "
            f"{sorted(missing_update_columns)}"
        )

    active_service_ids = get_active_service_ids(
        calendar=calendar,
        calendar_dates=calendar_dates,
        service_date=service_date,
    )
    scheduled_trips = (
        filter_active_trips(trips, active_service_ids)
        .filter(
            F.col("trip_id").isNotNull()
            & (F.length(F.trim(F.col("trip_id"))) > 0)
            & F.col("route_id").isNotNull()
            & (F.length(F.trim(F.col("route_id"))) > 0)
        )
        .select("route_id", "trip_id")
        .distinct()
    )
    scheduled_by_route = scheduled_trips.groupBy("route_id").agg(
        F.countDistinct("trip_id").alias("scheduled_trips")
    )

    valid_updates = trip_updates.filter(
        F.col("event_ts").isNotNull()
        & (
            F.to_date(
                F.from_utc_timestamp(
                    F.col("event_ts").cast("timestamp"),
                    "Pacific/Auckland",
                )
            )
            == F.lit(service_date)
        )
        & F.col("trip_id").isNotNull()
        & (F.length(F.trim(F.col("trip_id"))) > 0)
        & F.col("route_id").isNotNull()
        & (F.length(F.trim(F.col("route_id"))) > 0)
    )
    if "trip_schedule_relationship" not in valid_updates.columns:
        valid_updates = valid_updates.withColumn(
            "trip_schedule_relationship",
            F.lit(None).cast("string"),
        )

    valid_updates = (
        scheduled_trips.alias("scheduled")
        .join(
            valid_updates.alias("updates"),
            on=[
                F.col("scheduled.trip_id") == F.col("updates.trip_id"),
                F.col("scheduled.route_id") == F.col("updates.route_id"),
            ],
            how="inner",
        )
        .select(
            F.col("updates.route_id").alias("route_id"),
            F.col("updates.trip_id").alias("trip_id"),
            F.col("updates.delay_seconds").alias("delay_seconds"),
            F.col(
                "updates.trip_schedule_relationship"
            ).alias("trip_schedule_relationship"),
        )
    )

    trip_observations = valid_updates.groupBy(
        "route_id",
        "trip_id",
    ).agg(
        F.max(
            F.when(
                F.col("delay_seconds") > 0,
                F.col("delay_seconds"),
            ).otherwise(F.lit(0))
        ).alias("max_positive_delay"),
        F.max(
            F.when(
                F.col("trip_schedule_relationship") == "CANCELED",
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("is_cancelled"),
    )
    observed_by_route = trip_observations.groupBy("route_id").agg(
        F.countDistinct("trip_id").alias("observed_trips"),
        F.sum(
            F.when(
                (F.col("is_cancelled") == 0)
                & (F.col("max_positive_delay") > delay_threshold_seconds),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("delayed_trips"),
        F.sum(
            F.when(F.col("is_cancelled") == 1, F.lit(1)).otherwise(F.lit(0))
        ).alias("cancelled_trips"),
    )

    routes = scheduled_by_route.select("route_id").union(
        observed_by_route.select("route_id")
    ).distinct()

    return (
        routes.join(scheduled_by_route, on="route_id", how="left")
        .join(observed_by_route, on="route_id", how="left")
        .select(
            F.lit(service_date).alias("date"),
            F.col("route_id"),
            F.coalesce(F.col("scheduled_trips"), F.lit(0)).alias(
                "scheduled_trips"
            ),
            F.coalesce(F.col("observed_trips"), F.lit(0)).alias(
                "observed_trips"
            ),
            F.coalesce(F.col("delayed_trips"), F.lit(0)).alias(
                "delayed_trips"
            ),
            F.coalesce(F.col("cancelled_trips"), F.lit(0)).alias(
                "cancelled_trips"
            ),
        )
        .withColumn(
            "reliability_rate",
            F.when(
                F.col("observed_trips") > 0,
                (
                    (
                        F.col("observed_trips")
                        - F.col("delayed_trips")
                        - F.col("cancelled_trips")
                    )
                    / F.col("observed_trips")
                    * 100
                ).cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .select(
            "date",
            "route_id",
            "scheduled_trips",
            "observed_trips",
            "delayed_trips",
            "cancelled_trips",
            "reliability_rate",
        )
    )
