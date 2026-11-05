from __future__ import annotations

import json

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
    create_spark_session,
)
from transport_lakehouse.streaming.streaming_silver import (  # noqa: E402
    build_service_alert_silver,
    build_trip_update_silver,
    build_vehicle_position_silver,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = create_spark_session()
    yield session
    session.stop()


@pytest.fixture
def event_stream(spark: SparkSession):
    events = [
        {
            "event_type": "vehicle_position",
            "source_provider": "at",
            "source_entity_id": "entity-001",
            "event_ts": "2026-10-29T06:00:00+00:00",
            "ingest_ts": "2026-10-29T06:00:05+00:00",
            "vehicle_id": "vehicle-001",
            "trip_id": "trip-001",
            "route_id": "route-001",
            "latitude": -36.85,
            "longitude": 174.76,
            "speed": 12.5,
            "bearing": 180.0,
        },
        {
            "event_type": "trip_update",
            "source_provider": "at",
            "source_entity_id": "trip-entity-001",
            "event_ts": "2026-10-29T06:01:00+00:00",
            "ingest_ts": "2026-10-29T06:01:05+00:00",
            "trip_id": "trip-002",
            "route_id": "route-002",
            "vehicle_id": "vehicle-002",
            "stop_id": "stop-003",
            "stop_sequence": 3,
            "scheduled_arrival_ts": "2026-10-29T06:05:00+00:00",
            "scheduled_departure_ts": "2026-10-29T06:06:00+00:00",
            "predicted_arrival_ts": "2026-10-29T06:07:00+00:00",
            "predicted_departure_ts": "2026-10-29T06:08:00+00:00",
            "delay_seconds": 120,
            "schedule_relationship": "SCHEDULED",
            "trip_schedule_relationship": "SCHEDULED",
        },
        {
            "event_type": "service_alert",
            "source_provider": "at",
            "source_entity_id": "alert-entity-001",
            "event_ts": "2026-10-29T06:02:00+00:00",
            "ingest_ts": "2026-10-29T06:02:05+00:00",
            "alert_id": "alert-001",
            "active_start_ts": "2026-10-29T06:00:00+00:00",
            "active_end_ts": "2026-10-29T08:00:00+00:00",
            "cause": "CONSTRUCTION",
            "effect": "DETOUR",
            "description": "Road works",
            "affected_route_id": "route-003",
            "affected_trip_id": "trip-003",
            "affected_stop_id": "stop-004",
        },
    ]

    rows = [
        (
            json.dumps(event),
            f"identity-{index}",
            "resolved",
            "transport.events.validated",
            0,
            index,
        )
        for index, event in enumerate(events)
    ]

    return spark.createDataFrame(
        rows,
        [
            "message_value",
            "dedup_identity",
            "identity_status",
            "topic",
            "partition",
            "offset",
        ],
    )


def test_vehicle_position_silver_fields_and_types(event_stream) -> None:
    from pyspark.sql.types import DoubleType, TimestampType

    result = build_vehicle_position_silver(event_stream)
    rows = result.collect()

    assert len(rows) == 1
    row = rows[0]

    assert row["event_type"] == "vehicle_position"
    assert row["source_provider"] == "at"
    assert row["source_entity_id"] == "entity-001"
    assert row["vehicle_id"] == "vehicle-001"
    assert row["route_id"] == "route-001"
    assert row["latitude"] == pytest.approx(-36.85)
    assert row["longitude"] == pytest.approx(174.76)
    assert isinstance(result.schema["latitude"].dataType, DoubleType)
    assert isinstance(result.schema["event_ts"].dataType, TimestampType)
    assert row["dedup_identity"] == "identity-0"
    assert row["offset"] == 0


def test_trip_update_silver_preserves_contract_fields(event_stream) -> None:
    from pyspark.sql.types import IntegerType, TimestampType

    result = build_trip_update_silver(event_stream)
    rows = result.collect()

    assert len(rows) == 1
    row = rows[0]

    assert row["trip_id"] == "trip-002"
    assert row["route_id"] == "route-002"
    assert row["stop_id"] == "stop-003"
    assert row["stop_sequence"] == 3
    assert row["delay_seconds"] == 120
    assert row["trip_schedule_relationship"] == "SCHEDULED"
    assert isinstance(result.schema["stop_sequence"].dataType, IntegerType)
    assert isinstance(
        result.schema["predicted_arrival_ts"].dataType,
        TimestampType,
    )
    assert row["scheduled_arrival_ts"] < row["predicted_arrival_ts"]


def test_trip_level_cancellation_without_stop_update_is_preserved(
    spark: SparkSession,
) -> None:
    event = {
        "event_type": "trip_update",
        "source_provider": "at",
        "source_entity_id": "trip-entity-cancelled",
        "event_ts": "2026-10-29T06:01:00+00:00",
        "ingest_ts": "2026-10-29T06:01:05+00:00",
        "trip_id": "trip-cancelled",
        "route_id": "route-002",
        "vehicle_id": None,
        "stop_id": None,
        "stop_sequence": None,
        "delay_seconds": None,
        "schedule_relationship": None,
        "trip_schedule_relationship": "CANCELED",
    }
    source = spark.createDataFrame(
        [(json.dumps(event),)],
        ["message_value"],
    )

    row = build_trip_update_silver(source).first()

    assert row["trip_id"] == "trip-cancelled"
    assert row["stop_id"] is None
    assert row["stop_sequence"] is None
    assert row["trip_schedule_relationship"] == "CANCELED"


def test_service_alert_silver_preserves_affected_entities(
    event_stream,
) -> None:
    result = build_service_alert_silver(event_stream)
    rows = result.collect()

    assert len(rows) == 1
    row = rows[0]

    assert row["alert_id"] == "alert-001"
    assert row["cause"] == "CONSTRUCTION"
    assert row["effect"] == "DETOUR"
    assert row["description"] == "Road works"
    assert row["affected_route_id"] == "route-003"
    assert row["affected_trip_id"] == "trip-003"
    assert row["affected_stop_id"] == "stop-004"


def test_event_types_are_isolated_between_silver_outputs(
    event_stream,
) -> None:
    vehicle = build_vehicle_position_silver(event_stream)
    trip = build_trip_update_silver(event_stream)
    alert = build_service_alert_silver(event_stream)

    assert vehicle.count() == 1
    assert trip.count() == 1
    assert alert.count() == 1

    assert "stop_id" not in vehicle.columns
    assert "latitude" not in trip.columns
    assert "vehicle_id" not in alert.columns


def test_missing_event_timestamp_is_not_replaced_by_ingest_timestamp(
    spark: SparkSession,
) -> None:
    event = {
        "event_type": "vehicle_position",
        "source_provider": "at",
        "event_ts": None,
        "ingest_ts": "2026-10-29T06:00:05+00:00",
        "vehicle_id": "vehicle-001",
    }

    source = spark.createDataFrame(
        [(json.dumps(event),)],
        ["message_value"],
    )

    row = build_vehicle_position_silver(source).first()

    assert row["event_ts"] is None
    assert row["ingest_ts"] is not None


def test_vehicle_position_does_not_infer_missing_route_id(
    spark: SparkSession,
) -> None:
    event = {
        "event_type": "vehicle_position",
        "source_provider": "at",
        "source_entity_id": "entity-001",
        "event_ts": "2026-10-29T06:00:00+00:00",
        "ingest_ts": "2026-10-29T06:00:05+00:00",
        "vehicle_id": "vehicle-001",
        "latitude": -36.85,
        "longitude": 174.76,
    }

    source = spark.createDataFrame(
        [(json.dumps(event),)],
        ["message_value"],
    )

    row = build_vehicle_position_silver(source).first()

    assert row["route_id"] is None


def test_silver_excludes_unresolved_identity_events(
    spark: SparkSession,
) -> None:
    event = {
        "event_type": "vehicle_position",
        "source_provider": "at",
        "source_entity_id": "entity-001",
        "event_ts": "2026-10-29T06:00:00+00:00",
        "ingest_ts": "2026-10-29T06:00:05+00:00",
        "vehicle_id": "vehicle-001",
    }

    source = spark.createDataFrame(
        [
            (json.dumps(event), "resolved"),
            (json.dumps(event), "identity_unresolved"),
        ],
        ["message_value", "identity_status"],
    )

    result = build_vehicle_position_silver(source)

    assert result.count() == 1
    assert result.first()["identity_status"] == "resolved"


def test_service_alert_timestamps_are_spark_timestamps(
    event_stream,
) -> None:
    from pyspark.sql.types import TimestampType

    result = build_service_alert_silver(event_stream)

    assert isinstance(result.schema["active_start_ts"].dataType, TimestampType)
    assert isinstance(result.schema["active_end_ts"].dataType, TimestampType)

    row = result.first()
    assert row["active_start_ts"] is not None
    assert row["active_end_ts"] is not None


def test_silver_requires_message_value_column(
    spark: SparkSession,
) -> None:
    source = spark.createDataFrame(
        [("vehicle-001",)],
        ["vehicle_id"],
    )

    with pytest.raises(ValueError, match="message_value"):
        build_vehicle_position_silver(source)


@pytest.mark.parametrize(
    ("event_type", "builder"),
    [
        ("vehicle_position", build_vehicle_position_silver),
        ("trip_update", build_trip_update_silver),
        ("service_alert", build_service_alert_silver),
    ],
)
def test_all_streaming_silver_outputs_preserve_tombstones(
    spark: SparkSession,
    event_type: str,
    builder,
) -> None:
    event = {
        "event_type": event_type,
        "source_provider": "at",
        "source_entity_id": f"{event_type}-deleted-001",
        "event_ts": "2026-10-29T06:00:00+00:00",
        "ingest_ts": "2026-10-29T06:00:05+00:00",
        "is_deleted": True,
    }

    source = spark.createDataFrame(
        [(json.dumps(event),)],
        ["message_value"],
    )

    row = builder(source).first()

    assert row is not None
    assert row["event_type"] == event_type
    assert row["is_deleted"] is True


@pytest.mark.parametrize(
    ("event_type", "builder"),
    [
        ("vehicle_position", build_vehicle_position_silver),
        ("trip_update", build_trip_update_silver),
        ("service_alert", build_service_alert_silver),
    ],
)
def test_streaming_silver_fields_match_canonical_contract(
    event_stream,
    event_type: str,
    builder,
) -> None:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        TimestampType,
    )

    from transport_lakehouse.contracts import (
        CANONICAL_FIELD_TYPES,
        REALTIME_EVENT_FIELDS,
        UTC_TIMESTAMP_FIELDS,
    )

    result = builder(event_stream)

    metadata_fields = {
        "dedup_identity",
        "identity_status",
        "topic",
        "partition",
        "offset",
        "raw_message",
    }

    expected_fields = set(REALTIME_EVENT_FIELDS[event_type])

    # Each event must contain exactly its canonical fields plus metadata.
    assert set(result.columns) == expected_fields | metadata_fields

    spark_types = {
        str: StringType,
        int: IntegerType,
        float: DoubleType,
        bool: BooleanType,
    }

    for field in expected_fields:
        expected_type = (
            TimestampType
            if field in UTC_TIMESTAMP_FIELDS
            else spark_types[CANONICAL_FIELD_TYPES[field]]
        )

        assert type(result.schema[field].dataType) is expected_type, (
            f"{event_type}.{field} must use "
            f"{expected_type.__name__}, "
            f"got {type(result.schema[field].dataType).__name__}"
        )
