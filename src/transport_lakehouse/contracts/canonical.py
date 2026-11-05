"""Canonical data contracts shared by batch and streaming pipelines.

Static GTFS dimensions and realtime GTFS events retain separate
business grains while sharing entity identifiers and naming conventions.
"""

from __future__ import annotations


# Shared entity identifiers across Static and Realtime data.
SHARED_ENTITY_FIELDS = (
    "route_id",
    "trip_id",
    "stop_id",
)

# Static GTFS Silver dimension contracts.
STATIC_ROUTE_FIELDS = (
    "route_id",
    "agency_id",
    "route_short_name",
    "route_long_name",
    "route_type",
)

STATIC_STOP_FIELDS = (
    "stop_id",
    "stop_name",
    "latitude",
    "longitude",
    "zone_id",
    "wheelchair_boarding",
)

STATIC_TRIP_FIELDS = (
    "trip_id",
    "route_id",
    "service_id",
    "trip_headsign",
    "direction_id",
    "shape_id",
)

STATIC_DIMENSION_FIELDS = {
    "routes": STATIC_ROUTE_FIELDS,
    "stops": STATIC_STOP_FIELDS,
    "trips": STATIC_TRIP_FIELDS,
}

# Shared entity identifiers use strings in both Static and Realtime data.
SHARED_ENTITY_FIELD_TYPES = {
    field: str
    for field in SHARED_ENTITY_FIELDS
}

# Python-level types for standardized Static Silver dimensions.
STATIC_DIMENSION_FIELD_TYPES = {
    "routes": {
        "route_id": str,
        "agency_id": str,
        "route_short_name": str,
        "route_long_name": str,
        "route_type": int,
    },
    "stops": {
        "stop_id": str,
        "stop_name": str,
        "latitude": float,
        "longitude": float,
        "zone_id": str,
        "wheelchair_boarding": int,
    },
    "trips": {
        "trip_id": str,
        "route_id": str,
        "service_id": str,
        "trip_headsign": str,
        "direction_id": int,
        "shape_id": str,
    },
}

# Realtime event envelope shared by all event types.
REALTIME_COMMON_FIELDS = (
    "event_type",
    "source_provider",
    "source_entity_id",
    "event_ts",
    "ingest_ts",
    "is_deleted",
)

REALTIME_VEHICLE_POSITION_FIELDS = (
    "vehicle_id",
    "trip_id",
    "route_id",
    "latitude",
    "longitude",
    "speed",
    "bearing",
)

REALTIME_TRIP_UPDATE_FIELDS = (
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
)

REALTIME_SERVICE_ALERT_FIELDS = (
    "alert_id",
    "active_start_ts",
    "active_end_ts",
    "cause",
    "effect",
    "description",
    "affected_route_id",
    "affected_trip_id",
    "affected_stop_id",
)

REALTIME_EVENT_FIELDS = {
    "vehicle_position": (
        *REALTIME_COMMON_FIELDS,
        *REALTIME_VEHICLE_POSITION_FIELDS,
    ),
    "trip_update": (
        *REALTIME_COMMON_FIELDS,
        *REALTIME_TRIP_UPDATE_FIELDS,
    ),
    "service_alert": (
        *REALTIME_COMMON_FIELDS,
        *REALTIME_SERVICE_ALERT_FIELDS,
    ),
}

# Ordered union of all fields in the unified Realtime event model.
CANONICAL_REALTIME_FIELDS = tuple(
    dict.fromkeys(
        field
        for fields in REALTIME_EVENT_FIELDS.values()
        for field in fields
    )
)

# Python-level types for every field in the Realtime event model.
# Timestamp fields remain UTC ISO-8601 strings in the JSON envelope.
CANONICAL_FIELD_TYPES = {
    "event_type": str,
    "source_provider": str,
    "source_entity_id": str,
    "event_ts": str,
    "ingest_ts": str,
    "is_deleted": bool,
    "vehicle_id": str,
    "trip_id": str,
    "route_id": str,
    "latitude": float,
    "longitude": float,
    "speed": float,
    "bearing": float,
    "stop_id": str,
    "stop_sequence": int,
    "scheduled_arrival_ts": str,
    "scheduled_departure_ts": str,
    "predicted_arrival_ts": str,
    "predicted_departure_ts": str,
    "delay_seconds": int,
    "schedule_relationship": str,
    "trip_schedule_relationship": str,
    "alert_id": str,
    "active_start_ts": str,
    "active_end_ts": str,
    "cause": str,
    "effect": str,
    "description": str,
    "affected_route_id": str,
    "affected_trip_id": str,
    "affected_stop_id": str,
}

CANONICAL_REQUIRED_FIELDS = REALTIME_COMMON_FIELDS

UTC_TIMESTAMP_FIELDS = (
    "event_ts",
    "ingest_ts",
    "scheduled_arrival_ts",
    "scheduled_departure_ts",
    "predicted_arrival_ts",
    "predicted_departure_ts",
    "active_start_ts",
    "active_end_ts",
)


def build_realtime_spark_schema():
    """Build the Spark JSON schema from the canonical Realtime contract."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    spark_types = {
        str: StringType,
        int: IntegerType,
        float: DoubleType,
        bool: BooleanType,
    }

    return StructType(
        [
            StructField(
                field,
                spark_types[CANONICAL_FIELD_TYPES[field]](),
                True,
            )
            for field in CANONICAL_REALTIME_FIELDS
        ]
    )
