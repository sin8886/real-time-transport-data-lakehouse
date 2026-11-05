from __future__ import annotations

from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
    create_spark_session,
)
from transport_lakehouse.streaming.streaming_gold import (  # noqa: E402
    build_route_delay_gold,
    build_vehicle_activity_gold,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = create_spark_session()
    yield session
    session.stop()


@pytest.fixture
def trip_updates(spark: SparkSession):
    rows = [
        ("route-001", datetime(2026, 10, 29, 6, 1), 120),
        ("route-001", datetime(2026, 10, 29, 6, 2), 60),
        ("route-001", datetime(2026, 10, 29, 6, 4), -30),
        ("route-002", datetime(2026, 10, 29, 6, 3), 90),
        ("route-001", datetime(2026, 10, 29, 6, 7), 180),
        (None, datetime(2026, 10, 29, 6, 2), 100),
        ("", datetime(2026, 10, 29, 6, 2), 100),
        ("route-003", None, 100),
        ("route-003", datetime(2026, 10, 29, 6, 2), None),
    ]

    return spark.createDataFrame(
        rows,
        ["route_id", "event_ts", "delay_seconds"],
    )


@pytest.mark.parametrize(
    "duration",
    ["5 minutes", "15 minutes", "60 minutes"],
)
def test_route_delay_gold_aggregates_by_route_and_window(
    trip_updates,
    duration: str,
) -> None:
    result = build_route_delay_gold(trip_updates, duration)

    assert result.columns == [
        "window_start",
        "window_end",
        "route_id",
        "avg_delay_seconds",
        "event_count",
    ]

    rows = result.collect()
    route_1 = [row for row in rows if row["route_id"] == "route-001"]
    route_2 = [row for row in rows if row["route_id"] == "route-002"]

    assert len(route_1) >= 1
    assert len(route_2) >= 1

    if duration == "5 minutes":
        first_window = next(
            row
            for row in route_1
            if row["window_start"] == datetime(2026, 10, 29, 6, 0)
        )
        assert first_window["event_count"] == 3
        assert first_window["avg_delay_seconds"] == pytest.approx(50.0)


def test_route_delay_gold_preserves_negative_delays(trip_updates) -> None:
    result = build_route_delay_gold(trip_updates, "5 minutes")

    first_window = next(
        row
        for row in result.collect()
        if row["route_id"] == "route-001"
        and row["window_start"] == datetime(2026, 10, 29, 6, 0)
    )

    assert first_window["avg_delay_seconds"] == pytest.approx(50.0)
    assert first_window["event_count"] == 3


def test_route_delay_gold_rejects_unsupported_window(trip_updates) -> None:
    with pytest.raises(ValueError, match="window_duration"):
        build_route_delay_gold(trip_updates, "10 minutes")


def test_route_delay_gold_rejects_empty_watermark(trip_updates) -> None:
    with pytest.raises(ValueError, match="Watermark"):
        build_route_delay_gold(trip_updates, "5 minutes", " ")


def test_route_delay_gold_requires_input_columns(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [("route-001",)],
        ["route_id"],
    )

    with pytest.raises(ValueError, match="missing required columns"):
        build_route_delay_gold(source, "5 minutes")


@pytest.mark.parametrize(
    "duration",
    ["5 minutes", "15 minutes", "60 minutes"],
)
def test_route_delay_gold_preserves_streaming_input(
    spark: SparkSession,
    duration: str,
) -> None:
    source = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)
        .load()
    )

    trip_updates = source.selectExpr(
        "'route-001' AS route_id",
        "timestamp AS event_ts",
        "CAST(value % 300 AS INT) AS delay_seconds",
    )

    result = build_route_delay_gold(trip_updates, duration)

    assert trip_updates.isStreaming
    assert result.isStreaming
    assert result.columns == [
        "window_start",
        "window_end",
        "route_id",
        "avg_delay_seconds",
        "event_count",
    ]
@pytest.fixture
def vehicle_positions(spark: SparkSession):
    rows = [
        ("vehicle-001", "route-001", datetime(2026, 10, 29, 6, 1), 4.0),
        ("vehicle-001", "route-001", datetime(2026, 10, 29, 6, 2), 4.0),
        ("vehicle-002", "route-001", datetime(2026, 10, 29, 6, 3), 8.0),
        ("vehicle-003", "route-001", datetime(2026, 10, 29, 6, 4), None),
        ("vehicle-004", "route-002", datetime(2026, 10, 29, 6, 2), 2.0),
        ("", "route-001", datetime(2026, 10, 29, 6, 2), 3.0),
        ("vehicle-005", "", datetime(2026, 10, 29, 6, 2), 3.0),
        ("vehicle-006", "route-001", None, 3.0),
    ]

    return spark.createDataFrame(
        rows,
        ["vehicle_id", "route_id", "event_ts", "speed"],
    )


def test_vehicle_activity_gold_aggregates_by_route_and_window(
    vehicle_positions,
) -> None:
    result = build_vehicle_activity_gold(
        vehicle_positions,
        "5 minutes",
    )

    assert result.columns == [
        "window_start",
        "route_id",
        "active_vehicle_count",
        "avg_speed",
        "congestion_rate",
    ]

    rows = result.collect()

    route_1 = next(
        row for row in rows if row["route_id"] == "route-001"
    )
    route_2 = next(
        row for row in rows if row["route_id"] == "route-002"
    )

    assert route_1["active_vehicle_count"] == 3
    assert route_1["avg_speed"] == pytest.approx(6.0)
    assert route_1["congestion_rate"] == pytest.approx(0.5)

    assert route_2["active_vehicle_count"] == 1
    assert route_2["avg_speed"] == pytest.approx(2.0)
    assert route_2["congestion_rate"] == pytest.approx(1.0)


def test_vehicle_activity_gold_rejects_unsupported_window(
    vehicle_positions,
) -> None:
    with pytest.raises(ValueError, match="window_duration"):
        build_vehicle_activity_gold(vehicle_positions, "10 minutes")


def test_vehicle_activity_gold_rejects_empty_watermark(
    vehicle_positions,
) -> None:
    with pytest.raises(ValueError, match="Watermark"):
        build_vehicle_activity_gold(
            vehicle_positions,
            "5 minutes",
            " ",
        )


def test_vehicle_activity_gold_rejects_negative_threshold(
    vehicle_positions,
) -> None:
    with pytest.raises(ValueError, match="low_speed_threshold"):
        build_vehicle_activity_gold(
            vehicle_positions,
            "5 minutes",
            low_speed_threshold=-1,
        )


def test_vehicle_activity_gold_requires_input_columns(
    spark: SparkSession,
) -> None:
    source = spark.createDataFrame(
        [("route-001",)],
        ["route_id"],
    )

    with pytest.raises(ValueError, match="missing required columns"):
        build_vehicle_activity_gold(source, "5 minutes")


@pytest.mark.parametrize(
    "duration",
    ["5 minutes", "15 minutes", "60 minutes"],
)
def test_vehicle_activity_gold_preserves_streaming_input(
    spark: SparkSession,
    duration: str,
) -> None:
    source = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)
        .load()
    )

    vehicle_positions = source.selectExpr(
        "CAST(value % 10 AS STRING) AS vehicle_id",
        "'route-001' AS route_id",
        "timestamp AS event_ts",
        "CAST(value % 10 AS DOUBLE) AS speed",
    )

    result = build_vehicle_activity_gold(
        vehicle_positions,
        duration,
    )

    assert vehicle_positions.isStreaming
    assert result.isStreaming
    assert result.columns == [
        "window_start",
        "route_id",
        "active_vehicle_count",
        "avg_speed",
        "congestion_rate",
    ]