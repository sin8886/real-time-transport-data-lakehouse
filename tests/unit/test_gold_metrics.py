from __future__ import annotations

import pytest


pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from transport_lakehouse.processing.gold_metrics import (  # noqa: E402
    build_service_reliability_gold,
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


def test_build_service_reliability_gold(spark: SparkSession) -> None:
    trips = spark.createDataFrame(
        [
            ("T1", "R1", "ACTIVE"),
            ("T2", "R1", "ACTIVE"),
            ("T3", "R1", "ACTIVE"),
            ("T4", "R2", "ACTIVE"),
            ("T5", "R3", "ACTIVE"),
        ],
        ["trip_id", "route_id", "service_id"],
    )
    trip_updates = spark.createDataFrame(
        [
            ("2026-10-21T01:00:00+13:00", "T1", "R1", 100, "SCHEDULED", "stop-1", 1),
            ("2026-10-21T02:00:00+13:00", "T1", "R1", 600, "SCHEDULED", "stop-2", 2),
            ("2026-10-21T03:00:00+13:00", "T2", "R1", 300, "SCHEDULED", "stop-1", 1),
            ("2026-10-21T04:00:00+13:00", "T3", "R1", None, "CANCELED", None, None),
            ("2026-10-21T05:00:00+13:00", "T4", "R2", 0, None, "stop-1", 1),
        ],
        [
            "event_ts",
            "trip_id",
            "route_id",
            "delay_seconds",
            "trip_schedule_relationship",
            "stop_id",
            "stop_sequence",
        ],
    )
    calendar = [
        {
            "service_id": "ACTIVE",
            "monday": "0",
            "tuesday": "0",
            "wednesday": "1",
            "thursday": "0",
            "friday": "0",
            "saturday": "0",
            "sunday": "0",
            "start_date": "20261001",
            "end_date": "20261031",
        }
    ]

    result = build_service_reliability_gold(
        trips=trips,
        trip_updates=trip_updates,
        calendar=calendar,
        calendar_dates=[],
        service_date="2026-10-21",
    )
    rows = {
        row["route_id"]: row
        for row in result.collect()
    }

    assert rows["R1"]["scheduled_trips"] == 3
    assert rows["R1"]["observed_trips"] == 3
    assert rows["R1"]["delayed_trips"] == 1
    assert rows["R1"]["cancelled_trips"] == 1
    assert rows["R1"]["reliability_rate"] == pytest.approx(33.333333, rel=1e-5)

    assert rows["R2"]["scheduled_trips"] == 1
    assert rows["R2"]["observed_trips"] == 1
    assert rows["R2"]["delayed_trips"] == 0
    assert rows["R2"]["cancelled_trips"] == 0
    assert rows["R2"]["reliability_rate"] == pytest.approx(100.0)

    assert rows["R3"]["scheduled_trips"] == 1
    assert rows["R3"]["observed_trips"] == 0
    assert rows["R3"]["delayed_trips"] == 0
    assert rows["R3"]["cancelled_trips"] == 0
    assert rows["R3"]["reliability_rate"] is None


def test_service_reliability_filters_updates_by_auckland_date_and_schedule(
    spark: SparkSession,
) -> None:
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    trips = spark.createDataFrame(
        [
            ("T1", "R1", "ACTIVE"),
            ("T2", "R1", "ACTIVE"),
        ],
        ["trip_id", "route_id", "service_id"],
    )
    trip_updates = spark.createDataFrame(
        [
            # 2026-10-20T11:30Z is 2026-10-21 in Auckland.
            ("2026-10-20T11:30:00Z", "T1", "R1", 100, None),
            # 2026-10-21T11:30Z is 2026-10-22 in Auckland.
            ("2026-10-21T11:30:00Z", "T1", "R1", 900, None),
            # Same date, but not a scheduled trip.
            ("2026-10-20T12:00:00Z", "T9", "R1", 900, None),
            # Same trip ID, but the route does not match the schedule.
            ("2026-10-20T13:00:00Z", "T2", "R9", 900, None),
            # Null event time is not an observation.
            (None, "T2", "R1", 900, None),
        ],
        StructType(
            [
                StructField("event_ts", StringType(), True),
                StructField("trip_id", StringType(), True),
                StructField("route_id", StringType(), True),
                StructField("delay_seconds", IntegerType(), True),
                StructField(
                    "trip_schedule_relationship",
                    StringType(),
                    True,
                ),
            ]
        ),
    )
    calendar = [
        {
            "service_id": "ACTIVE",
            "monday": "0",
            "tuesday": "0",
            "wednesday": "1",
            "thursday": "0",
            "friday": "0",
            "saturday": "0",
            "sunday": "0",
            "start_date": "20261001",
            "end_date": "20261031",
        }
    ]

    result = build_service_reliability_gold(
        trips=trips,
        trip_updates=trip_updates,
        calendar=calendar,
        calendar_dates=[],
        service_date="2026-10-21",
    )
    rows = {row["route_id"]: row for row in result.collect()}

    assert rows["R1"]["scheduled_trips"] == 2
    assert rows["R1"]["observed_trips"] == 1
    assert rows["R1"]["delayed_trips"] == 0
    assert rows["R1"]["cancelled_trips"] == 0
    assert rows["R1"]["reliability_rate"] == pytest.approx(100.0)


@pytest.mark.parametrize(
    ("event_ts", "service_date"),
    [
        ("2026-09-26T13:30:00Z", "2026-09-27"),
        ("2026-09-26T14:30:00Z", "2026-09-27"),
        ("2026-04-04T13:30:00Z", "2026-04-05"),
        ("2026-04-04T14:30:00Z", "2026-04-05"),
        ("2026-06-15T12:30:00Z", "2026-06-16"),
    ],
)
def test_service_reliability_handles_auckland_timezone_boundaries(
    spark: SparkSession,
    event_ts: str,
    service_date: str,
) -> None:
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

    trips = spark.createDataFrame(
        [("T1", "R1", "TIMEZONE_TEST")],
        ["trip_id", "route_id", "service_id"],
    )
    trip_updates = spark.createDataFrame(
        [(event_ts, "T1", "R1", 0, None)],
        StructType(
            [
                StructField("event_ts", StringType(), True),
                StructField("trip_id", StringType(), True),
                StructField("route_id", StringType(), True),
                StructField("delay_seconds", IntegerType(), True),
                StructField(
                    "trip_schedule_relationship",
                    StringType(),
                    True,
                ),
            ]
        ),
    )
    calendar_dates = [
        {
            "service_id": "TIMEZONE_TEST",
            "date": service_date.replace("-", ""),
            "exception_type": "1",
        }
    ]

    row = build_service_reliability_gold(
        trips=trips,
        trip_updates=trip_updates,
        calendar=[],
        calendar_dates=calendar_dates,
        service_date=service_date,
    ).first()

    assert row["observed_trips"] == 1
    assert row["scheduled_trips"] == 1
    assert row["reliability_rate"] == pytest.approx(100.0)
