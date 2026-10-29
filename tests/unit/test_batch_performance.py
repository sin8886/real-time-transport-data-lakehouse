from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from benchmarks.batch_performance import (
    compare_schema,
    file_statistics,
    parquet_files,
    partition_statistics,
    schema_summary,
    validate_route_filter,
    validate_result,
    validate_results_dir,
)


def test_parquet_files_excludes_non_data_files(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    (output / "part-00000.parquet").write_bytes(b"one")
    (output / "_SUCCESS").write_bytes(b"")
    (output / ".part-00000.parquet.crc").write_bytes(b"crc")

    assert parquet_files(output) == [output / "part-00000.parquet"]
    result = file_statistics(output, spark=object())

    assert result["total_bytes"] == 3
    assert result["files"] == [{"path": "part-00000.parquet", "bytes": 3}]


def test_partition_statistics_uses_precomputed_route_counts(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    (output / "route_id=R1").mkdir(parents=True)
    (output / "route_id=R2").mkdir()

    result = partition_statistics(
        output,
        "route_id",
        route_counts_by_value={"R1": 4},
        spark=object(),
    )

    assert result["directory_count"] == 2
    assert result["non_empty_partition_count"] == 1
    assert result["empty_partition_count"] == 1
    assert result["partitions"] == [
        {"value": "R1", "row_count": 4},
        {"value": "R2", "row_count": 0},
    ]


def test_validate_result_rejects_row_count_and_schema_mismatch() -> None:
    dataframe = type(
        "DataFrameStub",
        (),
        {
            "count": lambda self: 2,
            "schema": type(
                "SchemaStub",
                (),
                {
                    "fields": [
                        type(
                            "FieldStub",
                            (),
                            {
                                "name": "route_id",
                                "dataType": type(
                                    "TypeStub", (), {"simpleString": lambda self: "string"}
                                )(),
                                "nullable": False,
                            },
                        )()
                    ]
                },
            )(),
        },
    )()

    assert validate_result(dataframe, 2, schema_summary(dataframe))["row_count"] == 2
    with pytest.raises(ValueError, match="row count"):
        validate_result(dataframe, 1, schema_summary(dataframe))


def test_partition_schema_allows_spark_projected_partition_column() -> None:
    expected = [
        {"name": "route_id", "type": "string", "nullable": True},
        {"name": "trip_id", "type": "string", "nullable": True},
    ]
    actual = [
        {"name": "trip_id", "type": "string", "nullable": True},
        {"name": "route_id", "type": "string", "nullable": False},
    ]

    result = compare_schema(actual, expected, partition_column="route_id")

    assert result["matches"] is True
    assert result["comparison"].startswith("partition column")


def test_partition_schema_rejects_type_changes() -> None:
    expected = [{"name": "route_id", "type": "string", "nullable": True}]
    actual = [{"name": "route_id", "type": "int", "nullable": True}]

    result = compare_schema(actual, expected, partition_column="route_id")

    assert result["matches"] is False
    assert result["differences"][0]["kind"] == "type"


def test_validate_route_filter_requires_a_non_empty_value() -> None:
    class EmptyDataFrame:
        def select(self, *_args):
            return self

        def where(self, *_args):
            return self

        def first(self):
            return None

    with pytest.raises(ValueError, match="non-empty route_id"):
        validate_route_filter(EmptyDataFrame())


def test_validate_results_dir_protects_gold_path(tmp_path: Path) -> None:
    assert validate_results_dir(tmp_path) == tmp_path.resolve()
    with pytest.raises(ValueError, match="data/gold"):
        validate_results_dir(Path("data/gold"))


def test_result_schema_is_json_serializable() -> None:
    class DataType:
        def simpleString(self):
            return "string"

    class Field:
        name = "route_id"
        dataType = DataType()
        nullable = False

    class Schema:
        fields = [Field()]

    payload = schema_summary(type("DataFrame", (), {"schema": Schema()})())
    json.dumps(payload)
    assert payload == [
        {"name": "route_id", "type": "string", "nullable": False}
    ]
