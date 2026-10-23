from __future__ import annotations

import pytest


pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from transport_lakehouse.processing.spark_batch import (  # noqa: E402
    build_trip_stop_events,
    create_spark_session,
    load_gtfs_tables,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = create_spark_session()
    yield session
    session.stop()


@pytest.fixture
def gtfs_dataframes(spark: SparkSession):
    routes = spark.createDataFrame(
        [
            ("R1", "70", "3"),
            ("R2", "NX1", "3"),
        ],
        ["route_id", "route_short_name", "route_type"],
    )

    trips = spark.createDataFrame(
        [
            ("T1", "R1", "WEEKDAY", "0"),
            ("T2", "R2", "WEEKEND", "1"),
        ],
        ["trip_id", "route_id", "service_id", "direction_id"],
    )

    stop_times = spark.createDataFrame(
        [
            ("T1", "S1", "1", "08:00:00", "08:00:00"),
            ("T1", "S2", "2", "08:05:00", "08:05:00"),
            ("T2", "S2", "1", "09:00:00", "09:00:00"),
        ],
        [
            "trip_id",
            "stop_id",
            "stop_sequence",
            "arrival_time",
            "departure_time",
        ],
    )

    stops = spark.createDataFrame(
        [
            ("S1", "Central Station", "-36.8485", "174.7633"),
            ("S2", "Britomart", "-36.8440", "174.7680"),
        ],
        ["stop_id", "stop_name", "stop_lat", "stop_lon"],
    )

    return routes, trips, stop_times, stops


def test_build_trip_stop_events_joins_all_four_tables(
    gtfs_dataframes,
) -> None:
    routes, trips, stop_times, stops = gtfs_dataframes

    result = build_trip_stop_events(routes, trips, stop_times, stops)
    rows = result.orderBy("trip_id", "stop_sequence").collect()

    assert result.count() == 3
    assert len(rows) == 3

    first = rows[0].asDict()
    assert first["route_id"] == "R1"
    assert first["route_short_name"] == "70"
    assert first["trip_id"] == "T1"
    assert first["stop_id"] == "S1"
    assert first["stop_name"] == "Central Station"
    assert first["stop_sequence"] == "1"


def test_build_trip_stop_events_has_expected_columns(
    gtfs_dataframes,
) -> None:
    routes, trips, stop_times, stops = gtfs_dataframes

    result = build_trip_stop_events(routes, trips, stop_times, stops)

    assert result.columns == [
        "route_id",
        "route_short_name",
        "route_type",
        "trip_id",
        "service_id",
        "direction_id",
        "stop_id",
        "stop_sequence",
        "arrival_time",
        "departure_time",
        "stop_name",
        "stop_lat",
        "stop_lon",
    ]


def test_build_trip_stop_events_rejects_missing_columns(
    gtfs_dataframes,
) -> None:
    routes, trips, stop_times, stops = gtfs_dataframes
    invalid_stops = stops.drop("stop_name")

    with pytest.raises(ValueError, match="stop_name"):
        build_trip_stop_events(routes, trips, stop_times, invalid_stops)


def test_load_gtfs_tables_reads_csv_files(
    spark: SparkSession,
    tmp_path,
) -> None:
    files = {
        "routes.txt": ("route_id,route_short_name,route_type\nR1,70,3\n"),
        "trips.txt": ("trip_id,route_id,service_id,direction_id\nT1,R1,WK,0\n"),
        "stop_times.txt": (
            "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n"
            "T1,S1,1,08:00:00,08:00:00\n"
        ),
        "stops.txt": (
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "S1,Central Station,-36.8485,174.7633\n"
        ),
    }

    for filename, content in files.items():
        (tmp_path / filename).write_text(content, encoding="utf-8")

    tables = load_gtfs_tables(spark, tmp_path)

    assert set(tables) == {"routes", "trips", "stop_times", "stops"}
    assert tables["routes"].first()["route_id"] == "R1"
    assert tables["routes"].schema["route_type"].dataType.simpleString() == "string"
