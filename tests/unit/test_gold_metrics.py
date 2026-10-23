from __future__ import annotations

import pytest


pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from transport_lakehouse.processing.gold_metrics import (  # noqa: E402
    filter_active_trips,
    get_active_service_ids,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    from transport_lakehouse.processing.spark_batch import (
        create_spark_session,
    )

    session = create_spark_session()
    yield session
    session.stop()


def test_get_active_service_ids_applies_calendar_and_exceptions() -> None:
    calendar = [
        {
            "service_id": "WEEKDAY",
            "monday": "1",
            "tuesday": "1",
            "wednesday": "1",
            "thursday": "1",
            "friday": "1",
            "saturday": "0",
            "sunday": "0",
            "start_date": "20261001",
            "end_date": "20261031",
        },
        {
            "service_id": "SPECIAL",
            "monday": "0",
            "tuesday": "0",
            "wednesday": "0",
            "thursday": "0",
            "friday": "0",
            "saturday": "0",
            "sunday": "0",
            "start_date": "20261001",
            "end_date": "20261031",
        },
    ]

    exceptions = [
        {
            "service_id": "WEEKDAY",
            "date": "20261022",
            "exception_type": "2",
        },
        {
            "service_id": "SPECIAL",
            "date": "20261022",
            "exception_type": "1",
        },
    ]

    result = get_active_service_ids(
        calendar=calendar,
        calendar_dates=exceptions,
        service_date="2026-10-22",
    )

    assert result == {"SPECIAL"}


def test_filter_active_trips(spark: SparkSession) -> None:
    trips = spark.createDataFrame(
        [
            ("T1", "R1", "WEEKDAY", "0"),
            ("T2", "R1", "SPECIAL", "0"),
            ("T3", "R2", "WEEKEND", "1"),
        ],
        ["trip_id", "route_id", "service_id", "direction_id"],
    )

    result = filter_active_trips(
        trips=trips,
        active_service_ids={"WEEKDAY", "SPECIAL"},
    )

    assert {row["trip_id"] for row in result.collect()} == {"T1", "T2"}


def test_build_gold_metrics(spark: SparkSession) -> None:
    from transport_lakehouse.processing.gold_metrics import (
        build_gold_metrics,
    )

    events = spark.createDataFrame(
        [
            ("T1", "R1", "S1"),
            ("T1", "R1", "S2"),
            ("T1", "R1", "S3"),
            ("T2", "R1", "S1"),
            ("T2", "R1", "S2"),
            ("T3", "R2", "S1"),
        ],
        ["trip_id", "route_id", "stop_id"],
    )

    daily, route, stop = build_gold_metrics(
        events=events,
        service_date="2026-10-22",
    )

    daily_row = daily.first()

    assert daily_row["scheduled_trip_count"] == 3
    assert daily_row["active_route_count"] == 2
    assert daily_row["active_stop_count"] == 3

    route_r1 = route.filter("route_id = 'R1'").first()

    assert route_r1["scheduled_trip_count"] == 2
    assert route_r1["stop_event_count"] == 5
    assert route_r1["served_stop_count"] == 3

    stop_s1 = stop.filter("stop_id = 'S1'").first()

    assert stop_s1["scheduled_trip_count"] == 3
    assert stop_s1["stop_event_count"] == 3
