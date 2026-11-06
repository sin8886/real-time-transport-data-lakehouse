from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from benchmarks.spark_tuning import (
    REPARTITION_CASES,
    SHUFFLE_PARTITION_CASES,
    repartition_dataframe,
    set_shuffle_partitions,
    tuning_configuration,
)


def test_shuffle_partition_cases_are_explicit() -> None:
    assert SHUFFLE_PARTITION_CASES == (8, 16, 32, 200)


def test_repartition_cases_are_explicit() -> None:
    assert REPARTITION_CASES == (8, 16)


def test_set_shuffle_partitions() -> None:
    class FakeSpark:
        def __init__(self):
            self.values = {}

            class Conf:
                def __init__(self, values):
                    self.values = values

                def set(self, key, value):
                    self.values[key] = value

            self.conf = Conf(self.values)

    spark = FakeSpark()
    set_shuffle_partitions(spark, 16)

    assert spark.values["spark.sql.shuffle.partitions"] == "16"


def test_set_shuffle_partitions_rejects_invalid_value() -> None:
    class FakeSpark:
        conf = object()

    with pytest.raises(ValueError, match="at least 1"):
        set_shuffle_partitions(FakeSpark(), 0)


def test_tuning_configuration_reads_effective_values() -> None:
    class FakeConf:
        def get(self, key):
            return f"value-for-{key}"

    class FakeSpark:
        conf = FakeConf()

    result = tuning_configuration(FakeSpark())

    assert result["spark.sql.shuffle.partitions"] == (
        "value-for-spark.sql.shuffle.partitions"
    )
    assert len(result) == 4


def test_repartition_dataframe_requires_partition_count() -> None:
    with pytest.raises(ValueError, match="must be specified"):
        repartition_dataframe(object())


def test_repartition_dataframe_rejects_invalid_partition_count() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        repartition_dataframe(object(), partitions=0)


def test_repartition_dataframe_rejects_unknown_column() -> None:
    class FakeDataFrame:
        columns = ["route_id", "trip_id"]

    with pytest.raises(ValueError, match="Unknown repartition column"):
        repartition_dataframe(
            FakeDataFrame(),
            partitions=8,
            partition_column="unknown",
        )


def test_repartition_dataframe_uses_requested_partition_count() -> None:
    class FakeDataFrame:
        columns = ["route_id"]

        def repartition(self, *args):
            return args

    dataframe = FakeDataFrame()

    assert repartition_dataframe(dataframe, partitions=8) == (8,)
    assert repartition_dataframe(
        dataframe,
        partitions=16,
        partition_column="route_id",
    ) == (16, "route_id")


def test_validate_results_dir_accepts_external_directory(tmp_path):
    from benchmarks.spark_tuning import validate_results_dir

    result = validate_results_dir(tmp_path / "benchmark-results")

    assert result == (tmp_path / "benchmark-results").resolve()


def test_validate_results_dir_rejects_gold_directory():
    from benchmarks.spark_tuning import PROJECT_ROOT, validate_results_dir

    gold_dir = PROJECT_ROOT / "data" / "gold"

    with pytest.raises(ValueError, match="must not be written under data/gold"):
        validate_results_dir(gold_dir)


def test_validate_results_dir_rejects_gold_subdirectory():
    from benchmarks.spark_tuning import PROJECT_ROOT, validate_results_dir

    gold_subdir = PROJECT_ROOT / "data" / "gold" / "benchmark-results"

    with pytest.raises(ValueError, match="must not be written under data/gold"):
        validate_results_dir(gold_subdir)


def test_build_markdown_report_contains_benchmark_sections():
    from benchmarks.spark_tuning import build_markdown_report

    case = {
        "median_count_action_ms": 100.0,
        "min_count_action_ms": 90.0,
        "max_count_action_ms": 110.0,
    }

    result = {
        "status": "completed",
        "environment": {
            "timestamp_utc": "2026-11-06T00:00:00Z",
            "spark_version": "3.5.9",
            "pyspark_version": "3.5.9",
            "python_version": "3.12.3",
            "java_version": "17",
            "master": "local[*]",
            "cpu_count": 4,
            "effective_sql_conf": {
                "spark.sql.shuffle.partitions": "200",
            },
        },
        "input": {
            "provider": "at",
            "service_date": "2026-10-22",
            "events_rows": 1000,
            "active_trip_join_rows": 500,
        },
        "shuffle_benchmark": {"8": case},
        "input_partition_benchmark": {"cases": {"8": case}},
        "broadcast_benchmark": {"default": case},
        "repartition_benchmark": {"8": case},
        "validation": {
            "all_cases_validated": True,
            "events_expected_rows": 1000,
            "active_trip_expected_rows": 500,
        },
        "methodology": {
            "warmup_runs": 1,
            "measured_runs_per_case": 3,
            "primary_metric": "median_count_action_ms",
            "shuffle_isolation": "AQE and automatic broadcast disabled.",
            "production_code_modified": False,
        },
        "limitations": ["Local execution only."],
    }

    report = build_markdown_report(result)

    assert "# Spark Tuning Benchmark v2" in report
    assert "Shuffle partition benchmark" in report
    assert "Input partition benchmark" in report
    assert "Broadcast benchmark" in report
    assert "Output repartition benchmark" in report
    assert "All cases validated: `True`" in report
    assert "100.000" in report



def test_build_active_trip_context_reuses_trips_and_reads_only_calendars(
    tmp_path,
    monkeypatch,
):
    from benchmarks import spark_tuning

    trips = object()
    active_trip_ids = object()

    parsed_calendar = [{"service_id": "weekday"}]
    parsed_calendar_dates = [{"service_id": "holiday"}]

    parser_calls = []
    filter_calls = []

    def fake_parse_gtfs_table(path):
        parser_calls.append(path.name)

        if path.name == "calendar.txt":
            return parsed_calendar

        if path.name == "calendar_dates.txt":
            return parsed_calendar_dates

        raise AssertionError(f"Unexpected GTFS table: {path.name}")

    def fake_get_active_service_ids(**kwargs):
        assert kwargs["calendar"] is parsed_calendar
        assert kwargs["calendar_dates"] is parsed_calendar_dates
        assert kwargs["service_date"] == spark_tuning.SERVICE_DATE
        return {"weekday", "holiday"}

    class FakeActiveTrips:
        def select(self, column):
            assert column == "trip_id"
            return active_trip_ids

        def count(self):
            return 42

    def fake_filter_active_trips(dataframe, service_ids):
        filter_calls.append((dataframe, service_ids))
        return FakeActiveTrips()

    monkeypatch.setattr(
        spark_tuning,
        "parse_gtfs_table",
        fake_parse_gtfs_table,
    )
    monkeypatch.setattr(
        spark_tuning,
        "get_active_service_ids",
        fake_get_active_service_ids,
    )
    monkeypatch.setattr(
        spark_tuning,
        "filter_active_trips",
        fake_filter_active_trips,
    )
    monkeypatch.setattr(
        spark_tuning,
        "schema_summary",
        lambda dataframe: ["trip_id"],
    )

    result_ids, metadata = spark_tuning.build_active_trip_context(
        trips,
        tmp_path,
    )

    assert parser_calls == ["calendar.txt", "calendar_dates.txt"]
    assert len(filter_calls) == 1
    assert filter_calls[0][0] is trips
    assert filter_calls[0][1] == {"weekday", "holiday"}
    assert result_ids is active_trip_ids
    assert metadata["active_service_id_count"] == 2
    assert metadata["active_trip_rows"] == 42
    assert metadata["active_trip_schema"] == ["trip_id"]
