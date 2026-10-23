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
