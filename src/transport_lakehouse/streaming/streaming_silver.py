"""Silver transformations for validated, deduplicated realtime events."""

from __future__ import annotations

from typing import Any


def _parse_canonical_events(event_stream: Any) -> Any:
    """Parse canonical JSON events into typed Silver columns."""
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("event_type", StringType(), True),
            StructField("source_provider", StringType(), True),
            StructField("source_entity_id", StringType(), True),
            StructField("event_ts", StringType(), True),
            StructField("ingest_ts", StringType(), True),
            StructField("vehicle_id", StringType(), True),
            StructField("trip_id", StringType(), True),
            StructField("route_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("bearing", DoubleType(), True),
            StructField("stop_id", StringType(), True),
            StructField("stop_sequence", IntegerType(), True),
            StructField("scheduled_arrival_ts", StringType(), True),
            StructField("scheduled_departure_ts", StringType(), True),
            StructField("predicted_arrival_ts", StringType(), True),
            StructField("predicted_departure_ts", StringType(), True),
            StructField("delay_seconds", IntegerType(), True),
            StructField("schedule_relationship", StringType(), True),
            StructField("trip_schedule_relationship", StringType(), True),
            StructField("alert_id", StringType(), True),
            StructField("active_start_ts", StringType(), True),
            StructField("active_end_ts", StringType(), True),
            StructField("cause", StringType(), True),
            StructField("effect", StringType(), True),
            StructField("description", StringType(), True),
            StructField("affected_route_id", StringType(), True),
            StructField("affected_trip_id", StringType(), True),
            StructField("affected_stop_id", StringType(), True),
        ]
    )

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
