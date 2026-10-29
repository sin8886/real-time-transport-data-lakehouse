"""Manual, isolated Spark Batch Performance Benchmark v1."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from transport_lakehouse.processing.gold_metrics import (  # noqa: E402
    filter_active_trips,
    get_active_service_ids,
)
from transport_lakehouse.processing.parse_static import parse_static_feed  # noqa: E402
from transport_lakehouse.processing.spark_batch import (  # noqa: E402
    build_trip_stop_events,
    create_spark_session,
    load_gtfs_tables,
)


RUNS = 3
FILE_SIZE_CASES = (1, 2, 8)
PARQUET_SUFFIX = ".parquet"
PLAN_NODES = (
    "Exchange",
    "BroadcastHashJoin",
    "SortMergeJoin",
    "HashAggregate",
    "FileScan",
)


def elapsed_ms(action: Callable[[], Any]) -> tuple[Any, float]:
    """Run an action and return its value and wall-clock duration."""
    started = perf_counter()
    value = action()
    return value, round((perf_counter() - started) * 1000, 3)


def parquet_files(root: str | Path) -> list[Path]:
    """Return visible Parquet data files below a benchmark output directory."""
    return sorted(
        path
        for path in Path(root).rglob(f"*{PARQUET_SUFFIX}")
        if path.is_file() and not path.name.startswith(".")
    )


def file_statistics(root: str | Path, spark: Any | None = None) -> dict[str, Any]:
    """Collect file counts and byte-size statistics without Spark actions."""
    files = parquet_files(root)
    entries = []
    for path in files:
        entries.append(
            {
                "path": path.relative_to(root).as_posix(),
                "bytes": path.stat().st_size,
            }
        )

    sizes = [entry["bytes"] for entry in entries]
    return {
        "file_count": len(entries),
        "total_bytes": sum(sizes),
        "min_bytes": min(sizes) if sizes else 0,
        "max_bytes": max(sizes) if sizes else 0,
        "mean_bytes": statistics.mean(sizes) if sizes else 0,
        "median_bytes": statistics.median(sizes) if sizes else 0,
        "files": entries,
    }


def partition_statistics(
    root: str | Path,
    partition_column: str,
    spark: Any | None = None,
    *,
    route_counts_by_value: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Inspect key=value directories using one precomputed row distribution."""
    root_path = Path(root)
    route_counts_by_value = route_counts_by_value or {}
    directories = sorted(
        path
        for path in root_path.rglob(f"{partition_column}=*")
        if path.is_dir()
    )
    partitions = []
    for directory in directories:
        value = directory.name.split("=", 1)[1]
        rows = route_counts_by_value.get(value, 0)
        partitions.append({"value": value, "row_count": rows})

    return {
        "directory_count": len(directories),
        "non_empty_partition_count": sum(
            partition["row_count"] > 0
            for partition in partitions
        ),
        "empty_partition_count": sum(
            partition["row_count"] == 0 for partition in partitions
        ),
        "partitions": partitions,
    }


def schema_summary(dataframe: Any) -> list[dict[str, Any]]:
    """Return a JSON-safe schema summary."""
    return [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in dataframe.schema.fields
    ]


def explain_summary(dataframe: Any) -> dict[str, Any]:
    """Capture formatted explain output and identify nodes that actually occur."""
    buffer = __import__("io").StringIO()
    with contextlib.redirect_stdout(buffer):
        dataframe.explain("formatted")
    plan = buffer.getvalue()
    return {
        "text": plan,
        "nodes": {node: node in plan for node in PLAN_NODES},
        "has_partition_filters": "PartitionFilters:" in plan,
    }


def validate_route_filter(events: Any) -> str:
    """Select one real, non-empty route id for every comparable read."""
    route_id = events.select("route_id").where("route_id IS NOT NULL").first()
    if route_id is None or route_id["route_id"] in (None, ""):
        raise ValueError("No non-empty route_id exists in the benchmark input.")
    return str(route_id["route_id"])


def validate_results_dir(results_dir: str | Path) -> Path:
    """Reject result paths inside the production Gold tree."""
    resolved = Path(results_dir).expanduser().resolve()
    gold_root = (PROJECT_ROOT / "data" / "gold").resolve()
    if resolved == gold_root or gold_root in resolved.parents:
        raise ValueError("Benchmark results must not be written under data/gold.")
    return resolved


def compare_schema(
    actual_schema: list[dict[str, Any]],
    expected_schema: list[dict[str, Any]],
    partition_column: str | None = None,
) -> dict[str, Any]:
    """Compare schemas while documenting Spark's partition-column projection."""
    expected_by_name = {field["name"]: field for field in expected_schema}
    actual_by_name = {field["name"]: field for field in actual_schema}
    differences = []

    if set(actual_by_name) != set(expected_by_name):
        differences.append(
            {
                "kind": "columns",
                "expected": list(expected_by_name),
                "actual": list(actual_by_name),
            }
        )

    for name in sorted(set(actual_by_name) & set(expected_by_name)):
        expected_field = expected_by_name[name]
        actual_field = actual_by_name[name]
        if actual_field["type"] != expected_field["type"]:
            differences.append(
                {
                    "kind": "type",
                    "column": name,
                    "expected": expected_field["type"],
                    "actual": actual_field["type"],
                }
            )
        if (
            actual_field["nullable"] != expected_field["nullable"]
            and name != partition_column
        ):
            differences.append(
                {
                    "kind": "nullable",
                    "column": name,
                    "expected": expected_field["nullable"],
                    "actual": actual_field["nullable"],
                }
            )

    expected_order = [field["name"] for field in expected_schema]
    actual_order = [field["name"] for field in actual_schema]
    allowed_order = expected_order
    order_explanation = "exact"
    if partition_column and partition_column in expected_order:
        allowed_order = [
            name for name in expected_order if name != partition_column
        ] + [partition_column]
        order_explanation = (
            "partition column may be projected from key=value directories "
            "and appended by Spark"
        )
    if actual_order != expected_order and actual_order != allowed_order:
        differences.append(
            {
                "kind": "order",
                "expected": expected_order,
                "actual": actual_order,
                "allowed": allowed_order,
            }
        )

    return {
        "matches": not differences,
        "differences": differences,
        "comparison": order_explanation,
    }


def route_counts(dataframe: Any) -> dict[str, int]:
    """Return a compact route-level data consistency signature."""
    return {
        str(row["route_id"]): int(row["count"])
        for row in dataframe.groupBy("route_id").count().collect()
    }


def validate_result(
    dataframe: Any,
    expected_rows: int,
    expected_schema: list[dict[str, Any]],
    expected_route_counts: dict[str, int] | None = None,
    partition_column: str | None = None,
) -> dict[str, Any]:
    """Validate rows, schema, and route distribution before comparisons."""
    actual_rows = dataframe.count()
    actual_schema = schema_summary(dataframe)
    if actual_rows != expected_rows:
        raise ValueError(
            f"Result row count mismatch: expected {expected_rows}, got {actual_rows}"
        )
    schema_result = compare_schema(
        actual_schema,
        expected_schema,
        partition_column=partition_column,
    )
    if not schema_result["matches"]:
        raise ValueError(
            "Result schema differs from the benchmark reference: "
            f"{schema_result['differences']}"
        )
    actual_route_counts = None
    if expected_route_counts is not None:
        actual_route_counts = route_counts(dataframe)
        if actual_route_counts != expected_route_counts:
            raise ValueError(
                "Result route distribution differs from the benchmark reference."
            )
    return {
        "row_count": actual_rows,
        "schema": actual_schema,
        "schema_comparison": schema_result,
        "route_counts": actual_route_counts,
    }


def measure_parquet_read(
    spark: Any,
    output_path: Path,
    route_id: str,
) -> dict[str, Any]:
    """Measure all-row and fixed route-filtered Parquet reads."""
    all_df = spark.read.parquet(str(output_path))
    all_rows, read_all_ms = elapsed_ms(all_df.count)
    escaped_route_id = route_id.replace("'", "''")
    filtered_df = spark.read.parquet(str(output_path)).where(
        f"route_id = '{escaped_route_id}'"
    )
    filtered_rows, read_filtered_ms = elapsed_ms(filtered_df.count)
    return {
        "read_all_ms": read_all_ms,
        "read_filtered_ms": read_filtered_ms,
        "read_all_rows": all_rows,
        "filtered_rows": filtered_rows,
        "input_partitions": all_df.rdd.getNumPartitions(),
        "filtered_plan": explain_summary(filtered_df),
    }


def write_events(
    events: Any,
    output_path: Path,
    layout: str,
    file_repartition: int | None = None,
) -> dict[str, Any]:
    """Write one isolated temporary Parquet case and report its write timing."""
    if output_path.exists():
        raise FileExistsError(f"Benchmark output already exists: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe = events
    if file_repartition is not None:
        dataframe = dataframe.repartition(file_repartition)
    writer = dataframe.write.mode("error")
    if layout == "route_id":
        writer = writer.partitionBy("route_id")
    _, write_ms = elapsed_ms(lambda: writer.parquet(str(output_path)))
    return {
        "write_ms": write_ms,
        "output_partitions": dataframe.rdd.getNumPartitions(),
        "repartition_shuffle_ms": None,
        "repartition_shuffle_status": (
            "unavailable; write timing includes any repartition shuffle"
            if file_repartition is not None
            else "not_applicable"
        ),
    }


def run_case(
    spark: Any,
    events: Any,
    output_path: Path,
    route_id: str,
    expected_rows: int,
    expected_schema: list[dict[str, Any]],
    expected_route_counts: dict[str, int],
    layout: str = "unpartitioned",
    file_repartition: int | None = None,
) -> dict[str, Any]:
    """Run one write/read case and reject inconsistent results."""
    write_result = write_events(
        events,
        output_path,
        layout=layout,
        file_repartition=file_repartition,
    )
    output_df = spark.read.parquet(str(output_path))
    validation = validate_result(
        output_df,
        expected_rows,
        expected_schema,
        expected_route_counts=expected_route_counts,
        partition_column="route_id" if layout == "route_id" else None,
    )
    read_result = measure_parquet_read(spark, output_path, route_id)
    if read_result["filtered_rows"] == 0:
        raise ValueError(f"Route filter returned no rows for route_id={route_id}")
    return {
        **write_result,
        **validation,
        **read_result,
        "files": file_statistics(output_path, spark),
        "partition_statistics": (
            partition_statistics(
                output_path,
                "route_id",
                route_counts_by_value=validation["route_counts"],
            )
            if layout == "route_id"
            else None
        ),
    }


def run_repeated_case(
    case_runner: Callable[[Path], dict[str, Any]],
    work_root: Path,
    case_name: str,
    runs: int = RUNS,
) -> dict[str, Any]:
    """Run one warm-up and repeated measured cases in separate directories."""
    warmup_path = work_root / f"{case_name}__warmup"
    warmup = case_runner(warmup_path)
    measured = []
    for index in range(1, runs + 1):
        measured.append(
            {
                "run": index,
                "warmup": False,
                "metrics": case_runner(work_root / f"{case_name}__run{index}"),
            }
        )
    return {
        "warmup": {"warmup": True, "metrics": warmup},
        "runs": measured,
        "median_write_ms": statistics.median(
            run["metrics"]["write_ms"] for run in measured
        ),
        "median_read_all_ms": statistics.median(
            run["metrics"]["read_all_ms"] for run in measured
        ),
        "median_read_filtered_ms": statistics.median(
            run["metrics"]["read_filtered_ms"] for run in measured
        ),
    }


def collect_environment(spark: Any) -> dict[str, Any]:
    """Capture environment and effective Spark configuration."""
    conf = dict(spark.sparkContext.getConf().getAll())
    effective_keys = (
        "spark.sql.shuffle.partitions",
        "spark.default.parallelism",
        "spark.sql.adaptive.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.executor.memory",
        "spark.driver.memory",
    )
    effective_conf = {
        key: conf.get(key, "not_explicitly_set") for key in effective_keys
    }
    try:
        java_version = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        java_text = (java_version.stderr or java_version.stdout).splitlines()
        java_version_value = java_text[0] if java_text else "unavailable"
    except (OSError, subprocess.SubprocessError):
        java_version_value = "unavailable"
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "pyspark_version": __import__("pyspark").__version__,
        "python_version": platform.python_version(),
        "java_version": java_version_value,
        "os": platform.platform(),
        "cpu_count": os.cpu_count(),
        "master": spark.sparkContext.master,
        "spark_conf": dict(sorted(conf.items())),
        "effective_spark_conf": effective_conf,
    }


def build_active_trip_join(
    spark: Any,
    gtfs_dir: Path,
) -> tuple[Any, dict]:
    """Build the exact active-trip Join used by scripts/run_gold_batch.py."""
    parsed_tables = parse_static_feed(gtfs_dir)
    active_service_ids = get_active_service_ids(
        calendar=parsed_tables["calendar"],
        calendar_dates=parsed_tables["calendar_dates"],
        service_date="2026-10-22",
    )
    tables = load_gtfs_tables(spark, gtfs_dir)
    active_trips = filter_active_trips(tables["trips"], active_service_ids)
    active_trip_ids = active_trips.select("trip_id")
    return active_trip_ids, {
        "active_service_id_count": len(active_service_ids),
        "active_trip_rows": active_trips.count(),
        "active_trip_schema": schema_summary(active_trips),
    }


def measure_join(name: str, builder: Callable[[], Any], inputs: dict[str, Any]) -> dict[str, Any]:
    """Measure lazy construction separately from the count action."""
    result, construction_ms = elapsed_ms(builder)
    plan = explain_summary(result)
    output_rows, action_ms = elapsed_ms(result.count)
    return {
        "name": name,
        "inputs": inputs,
        "construction_ms": construction_ms,
        "count_action_ms": action_ms,
        "output_rows": output_rows,
        "output_schema": schema_summary(result),
        "output_partitions": result.rdd.getNumPartitions(),
        "plan": plan,
    }


def run_benchmark(
    input_dir: Path,
    results_dir: Path,
    runs: int = RUNS,
) -> Path:
    """Run the full real-data benchmark and write JSON outside its temp files."""
    if runs < 1:
        raise ValueError("runs must be at least 1")
    results_dir = validate_results_dir(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    results_path = results_dir / "batch_performance_v1.json"
    if results_path.exists():
        raise FileExistsError(f"Results file already exists: {results_path}")

    work_root = Path(tempfile.mkdtemp(prefix="transport-benchmark-v1-"))
    spark = create_spark_session()
    try:
        tables = load_gtfs_tables(spark, input_dir)
        input_counts = {}
        input_schemas = {}
        for name, dataframe in tables.items():
            input_counts[name] = dataframe.count()
            input_schemas[name] = schema_summary(dataframe)
        events = build_trip_stop_events(
            routes=tables["routes"],
            trips=tables["trips"],
            stop_times=tables["stop_times"],
            stops=tables["stops"],
        )
        expected_rows, _ = elapsed_ms(events.count)
        expected_schema = schema_summary(events)
        expected_route_counts = route_counts(events)
        route_id = validate_route_filter(events)

        partition_cases = {}
        for name, layout in (
            ("baseline_unpartitioned", "unpartitioned"),
            ("partitioned_by_route_id", "route_id"),
        ):
            partition_cases[name] = run_repeated_case(
                lambda path, selected_layout=layout: run_case(
                    spark,
                    events,
                    path,
                    route_id,
                    expected_rows,
                    expected_schema,
                    expected_route_counts,
                    layout=selected_layout,
                ),
                work_root,
                name,
                runs,
            )

        file_size_cases = {}
        for partitions in FILE_SIZE_CASES:
            name = f"repartition_{partitions}"
            file_size_cases[name] = run_repeated_case(
                lambda path, count=partitions: run_case(
                    spark,
                    events,
                    path,
                    route_id,
                    expected_rows,
                    expected_schema,
                    expected_route_counts,
                    file_repartition=count,
                ),
                work_root,
                name,
                runs,
            )

        active_trip_ids, active_metadata = build_active_trip_join(spark, input_dir)
        join_results = {
            "day17_four_table_join": measure_join(
                "day17_four_table_join",
                lambda: build_trip_stop_events(
                    routes=tables["routes"],
                    trips=tables["trips"],
                    stop_times=tables["stop_times"],
                    stops=tables["stops"],
                ),
                input_counts,
            ),
            "active_trip_filter_join": measure_join(
                "active_trip_filter_join",
                lambda: events.join(
                    active_trip_ids,
                    on="trip_id",
                    how="inner",
                ),
                {**input_counts, "active_trips": active_metadata["active_trip_rows"]},
            ),
        }

        result = {
            "benchmark": "batch_performance_v1",
            "status": "completed",
            "input": {
                "provider": "at",
                "input_dir": str(input_dir),
                "route_filter": route_id,
                "row_counts": input_counts,
                "schemas": input_schemas,
                "events_rows": expected_rows,
                "events_schema": expected_schema,
            },
            "environment": collect_environment(spark),
            "partition_benchmark": partition_cases,
            "file_size_benchmark": file_size_cases,
            "join_benchmark": {
                "active_trip_context": active_metadata,
                "results": join_results,
            },
            "limitations": [
                "OS file caches were not cleared.",
                "local[*] results do not represent a production cluster.",
                "repartition shuffle time is unavailable as a separate reliable measure.",
                "service_date was not added to Day 17 events.",
            ],
        }
        results_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        return results_path
    finally:
        spark.stop()
        shutil.rmtree(work_root, ignore_errors=False)


def parse_args() -> argparse.Namespace:
    """Parse the explicit manual benchmark command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=PROJECT_ROOT / "data/gtfs/at")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=PROJECT_ROOT / "reports/benchmarks",
    )
    parser.add_argument("--runs", type=int, default=RUNS)
    return parser.parse_args()


def main() -> None:
    """Run the real-data benchmark only when explicitly invoked."""
    args = parse_args()
    print(run_benchmark(args.input_dir, args.results_dir, args.runs))


if __name__ == "__main__":
    main()
