from __future__ import annotations

import sys
from pathlib import Path

import pytest


pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from spark_lab import (  # noqa: E402
    create_spark_session,
    load_routes,
    run_spark_sql,
    summarize_routes,
    validate_parquet,
    write_partitioned_parquet,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = create_spark_session()
    yield spark
    spark.stop()


@pytest.fixture
def routes(spark: SparkSession):
    input_path = Path(__file__).parents[1] / "data" / "spark_lab" / "routes.csv"
    return load_routes(spark, input_path)


def test_summarize_routes_counts_positive_delays(routes) -> None:
    rows = summarize_routes(routes).collect()
    summary = {row.route_id: row.asDict() for row in rows}

    assert summary[70]["delayed_record_count"] == 2
    assert summary[70]["delayed_trip_count"] == 2
    assert summary[71]["delayed_record_count"] == 2
    assert summary[72]["delayed_record_count"] == 1
    assert summary[73]["delayed_record_count"] == 1


def test_summarize_routes_average_delay_is_correct(routes) -> None:
    rows = summarize_routes(routes).collect()
    averages = {row.route_id: row.average_delay_seconds for row in rows}

    assert averages[70] == 37.5
    assert averages[71] == 15.0
    assert averages[72] == 60.0
    assert averages[73] == 5.0


def test_spark_sql_returns_all_route_records(routes) -> None:
    rows = run_spark_sql(routes).collect()
    counts = {row.route_id: row.record_count for row in rows}

    assert counts == {70: 4, 71: 3, 72: 3, 73: 2}


def test_partitioned_parquet_round_trip(routes, tmp_path: Path) -> None:
    output_path = tmp_path / "route_data"
    write_partitioned_parquet(routes, output_path)

    loaded = validate_parquet(output_path)
    assert loaded.count() == 12
    assert set(loaded.columns) == {
        "route_id",
        "route_name",
        "trip_id",
        "stop_id",
        "delay_seconds",
    }
    assert (output_path / "route_id=70").is_dir()
    assert (output_path / "route_id=73").is_dir()
