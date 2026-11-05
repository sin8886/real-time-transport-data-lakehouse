"""Silver transformations for validated, deduplicated realtime events."""

from __future__ import annotations

from typing import Any


def _parse_canonical_events(event_stream: Any) -> Any:
    """Parse canonical JSON events using the shared data contract."""
    from pyspark.sql.functions import col, from_json

    from transport_lakehouse.contracts import build_realtime_spark_schema

    schema = build_realtime_spark_schema()

    if "message_value" not in event_stream.columns:
        raise ValueError("event_stream must contain a message_value column")

    parsed = event_stream.withColumn(
        "_silver_event",
        from_json(col("message_value"), schema),
    )

    if "identity_status" in parsed.columns:
        parsed = parsed.filter(col("identity_status") == "resolved")

    event = col("_silver_event")
    result = parsed.select(
        *[
            event.getField(field).alias(field)
            for field in schema.fieldNames()
        ],
        *[
            col(field)
            for field in (
                "dedup_identity",
                "identity_status",
                "event_time",
                "topic",
                "partition",
                "offset",
                "timestamp",
            )
            if field in parsed.columns
        ],
        col("message_value").alias("raw_message"),
    )

    return (
        result
        .withColumn("event_ts", col("event_ts").cast("timestamp"))
        .withColumn("ingest_ts", col("ingest_ts").cast("timestamp"))
        .withColumn(
            "scheduled_arrival_ts",
            col("scheduled_arrival_ts").cast("timestamp"),
        )
        .withColumn(
            "scheduled_departure_ts",
            col("scheduled_departure_ts").cast("timestamp"),
        )
        .withColumn(
            "predicted_arrival_ts",
            col("predicted_arrival_ts").cast("timestamp"),
        )
        .withColumn(
            "predicted_departure_ts",
            col("predicted_departure_ts").cast("timestamp"),
        )
        .withColumn(
            "active_start_ts",
            col("active_start_ts").cast("timestamp"),
        )
        .withColumn(
            "active_end_ts",
            col("active_end_ts").cast("timestamp"),
        )
    )


def build_vehicle_position_silver(event_stream: Any) -> Any:
    """Build Silver records for vehicle-position events."""
    from pyspark.sql.functions import col

    parsed = _parse_canonical_events(event_stream)
    return parsed.filter(
        col("event_type") == "vehicle_position"
    ).select(
        "event_type",
        "source_provider",
        "source_entity_id",
        "event_ts",
        "ingest_ts",
        "is_deleted",
        "vehicle_id",
        "trip_id",
        "route_id",
        "latitude",
        "longitude",
        "speed",
        "bearing",
        *[
            field
            for field in (
                "dedup_identity",
                "identity_status",
                "event_time",
                "topic",
                "partition",
                "offset",
                "timestamp",
                "raw_message",
            )
            if field in parsed.columns
        ],
    )


def build_trip_update_silver(event_stream: Any) -> Any:
    """Build Silver records for trip-update events."""
    from pyspark.sql.functions import col

    parsed = _parse_canonical_events(event_stream)
    return parsed.filter(
        col("event_type") == "trip_update"
    ).select(
        "event_type",
        "source_provider",
        "source_entity_id",
        "event_ts",
        "ingest_ts",
        "is_deleted",
        "trip_id",
        "route_id",
        "vehicle_id",
        "stop_id",
        "stop_sequence",
        "scheduled_arrival_ts",
        "scheduled_departure_ts",
        "predicted_arrival_ts",
        "predicted_departure_ts",
        "delay_seconds",
        "schedule_relationship",
        "trip_schedule_relationship",
        *[
            field
            for field in (
                "dedup_identity",
                "identity_status",
                "event_time",
                "topic",
                "partition",
                "offset",
                "timestamp",
                "raw_message",
            )
            if field in parsed.columns
        ],
    )


def build_service_alert_silver(event_stream: Any) -> Any:
    """Build Silver records for service-alert events."""
    from pyspark.sql.functions import col

    parsed = _parse_canonical_events(event_stream)
    return parsed.filter(
        col("event_type") == "service_alert"
    ).select(
        "event_type",
        "source_provider",
        "source_entity_id",
        "event_ts",
        "ingest_ts",
        "is_deleted",
        "alert_id",
        "active_start_ts",
        "active_end_ts",
        "cause",
        "effect",
        "description",
        "affected_route_id",
        "affected_trip_id",
        "affected_stop_id",
        *[
            field
            for field in (
                "dedup_identity",
                "identity_status",
                "event_time",
                "topic",
                "partition",
                "offset",
                "timestamp",
                "raw_message",
            )
            if field in parsed.columns
        ],
    )
