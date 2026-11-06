"""Manual, isolated Spark Tuning Benchmark v2."""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
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
from transport_lakehouse.processing.static_parser import parse_gtfs_table  # noqa: E402
from transport_lakehouse.processing.spark_batch import (  # noqa: E402
    build_trip_stop_events,
    create_spark_session,
    load_gtfs_tables,
)

SHUFFLE_PARTITION_CASES = (8, 16, 32, 200)
INPUT_PARTITION_CASES = (8, 16, 32)
REPARTITION_CASES = (8, 16)
RUNS = 3
SERVICE_DATE = "2026-10-22"

SPARK_SQL_CONF_KEYS = (
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.coalescePartitions.enabled",
)

PLAN_NODES = (
    "Exchange",
    "BroadcastExchange",
    "BroadcastHashJoin",
    "SortMergeJoin",
    "AQEShuffleRead",
    "AdaptiveSparkPlan",
)


def tuning_configuration(spark: Any) -> dict[str, str]:
    """Read effective SQL configuration values."""
    result = {}
    for key in SPARK_SQL_CONF_KEYS:
        try:
            result[key] = spark.conf.get(key)
        except Exception:
            result[key] = "unavailable"
    return result


def validate_results_dir(results_dir: str | Path) -> Path:
    """Reject benchmark result paths inside the production Gold tree."""
    resolved = Path(results_dir).expanduser().resolve()
    gold_root = (PROJECT_ROOT / "data" / "gold").resolve()

    if resolved == gold_root or gold_root in resolved.parents:
        raise ValueError(
            "Benchmark results must not be written under data/gold."
        )

    return resolved


def set_shuffle_partitions(spark: Any, partitions: int) -> None:
    """Set SQL shuffle partition count for an experiment."""
    if partitions < 1:
        raise ValueError("partitions must be at least 1")
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))


def build_tuned_trip_stop_events(
    routes: Any,
    trips: Any,
    stop_times: Any,
    stops: Any,
    *,
    broadcast_trips: bool = False,
) -> Any:
    """Build production-equivalent events with an optional trips broadcast hint."""
    if not broadcast_trips:
        return build_trip_stop_events(
            routes=routes,
            trips=trips,
            stop_times=stop_times,
            stops=stops,
        )

    from pyspark.sql import functions as F

    joined = (
        routes.alias("r")
        .join(
            F.broadcast(trips).alias("t"),
            F.col("r.route_id") == F.col("t.route_id"),
            "inner",
        )
        .join(
            stop_times.alias("st"),
            F.col("t.trip_id") == F.col("st.trip_id"),
            "inner",
        )
        .join(
            stops.alias("s"),
            F.col("st.stop_id") == F.col("s.stop_id"),
            "inner",
        )
    )

    return joined.select(
        F.col("r.route_id").alias("route_id"),
        F.col("r.route_short_name").alias("route_short_name"),
        F.col("r.route_type").alias("route_type"),
        F.col("t.trip_id").alias("trip_id"),
        F.col("t.service_id").alias("service_id"),
        F.col("t.direction_id").alias("direction_id"),
        F.col("st.stop_id").alias("stop_id"),
        F.col("st.stop_sequence").alias("stop_sequence"),
        F.col("st.arrival_time").alias("arrival_time"),
        F.col("st.departure_time").alias("departure_time"),
        F.col("s.stop_name").alias("stop_name"),
        F.col("s.stop_lat").alias("stop_lat"),
        F.col("s.stop_lon").alias("stop_lon"),
    )


def build_active_trip_filter_join(
    events: Any,
    active_trip_ids: Any,
    *,
    force_broadcast: bool = False,
) -> Any:
    """Join events with active trips using an optional broadcast hint."""
    if force_broadcast:
        from pyspark.sql import functions as F

        active_trip_ids = F.broadcast(active_trip_ids)

    return events.join(active_trip_ids, on="trip_id", how="inner")


def executed_plan_summary(dataframe: Any) -> dict[str, Any]:
    """Capture the executed physical plan after an action."""
    plan = dataframe._jdf.queryExecution().executedPlan().toString()

    return {
        "text": plan,
        "nodes": {
            node: node in plan
            for node in PLAN_NODES
        },
        "adaptive_final_plan": "isFinalPlan=true" in plan,
    }


def schema_summary(dataframe: Any) -> list[dict[str, Any]]:
    """Return a JSON-serializable schema description."""
    return [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in dataframe.schema.fields
    ]


def measure_dataframe(
    name: str,
    dataframe: Any,
    *,
    expected_rows: int,
    expected_schema: list[dict[str, Any]],
    spark: Any,
) -> dict[str, Any]:
    """Execute, validate, and capture the actual count query plan."""
    from pyspark.sql import functions as F

    actual_schema = schema_summary(dataframe)

    if actual_schema != expected_schema:
        raise ValueError(f"{name} returned a schema different from baseline")

    count_query = dataframe.agg(
        F.count(F.lit(1)).alias("_benchmark_count")
    )

    started = perf_counter()
    count_row = count_query.collect()[0]
    action_ms = round((perf_counter() - started) * 1000, 3)
    output_rows = int(count_row["_benchmark_count"])

    if output_rows != expected_rows:
        raise ValueError(
            f"{name} returned {output_rows} rows; expected {expected_rows}"
        )

    return {
        "name": name,
        "count_action_ms": action_ms,
        "output_rows": output_rows,
        "output_partitions": dataframe.rdd.getNumPartitions(),
        "output_schema": actual_schema,
        "spark_conf": tuning_configuration(spark),
        "executed_plan": executed_plan_summary(count_query),
    }

def repartition_dataframe(
    dataframe: Any,
    *,
    partitions: int | None = None,
    partition_column: str | None = None,
) -> Any:
    """Create an isolated output repartition experiment."""
    if partitions is None:
        raise ValueError("partitions must be specified")
    if partitions < 1:
        raise ValueError("partitions must be at least 1")

    if partition_column is not None:
        if partition_column not in dataframe.columns:
            raise ValueError(
                f"Unknown repartition column: {partition_column}"
            )
        return dataframe.repartition(partitions, partition_column)

    return dataframe.repartition(partitions)


def run_repeated_case(
    spark: Any,
    case_name: str,
    builder: Callable[[], Any],
    *,
    expected_rows: int,
    expected_schema: list[dict[str, Any]],
    runs: int = RUNS,
) -> dict[str, Any]:
    """Run one warmup and repeated measured executions."""
    if runs < 1:
        raise ValueError("runs must be at least 1")

    def execute(run_name: str) -> dict[str, Any]:
        construction_started = perf_counter()
        dataframe = builder()
        construction_ms = round(
            (perf_counter() - construction_started) * 1000,
            3,
        )
        metrics = measure_dataframe(
            run_name,
            dataframe,
            expected_rows=expected_rows,
            expected_schema=expected_schema,
            spark=spark,
        )
        metrics["construction_ms"] = construction_ms
        return metrics

    warmup = execute(f"{case_name}_warmup")
    measured = [
        {
            "run": index,
            "metrics": execute(f"{case_name}_run{index}"),
        }
        for index in range(1, runs + 1)
    ]

    timings = [
        item["metrics"]["count_action_ms"]
        for item in measured
    ]

    return {
        "warmup": warmup,
        "runs": measured,
        "median_count_action_ms": round(statistics.median(timings), 3),
        "min_count_action_ms": round(min(timings), 3),
        "max_count_action_ms": round(max(timings), 3),
    }


def collect_environment(spark: Any) -> dict[str, Any]:
    """Capture runtime environment and Spark configuration."""
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
        key: conf.get(key, "not_explicitly_set")
        for key in effective_keys
    }

    try:
        java_result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        java_lines = (
            java_result.stderr or java_result.stdout
        ).splitlines()
        java_version = java_lines[0] if java_lines else "unavailable"
    except (OSError, subprocess.SubprocessError):
        java_version = "unavailable"

    import pyspark

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "pyspark_version": pyspark.__version__,
        "python_version": platform.python_version(),
        "java_version": java_version,
        "os": platform.platform(),
        "cpu_count": os.cpu_count(),
        "master": spark.sparkContext.master,
        "spark_conf": dict(sorted(conf.items())),
        "effective_spark_conf": effective_conf,
        "effective_sql_conf": tuning_configuration(spark),
    }


def build_markdown_report(result: dict[str, Any]) -> str:
    """Build a concise human-readable benchmark report."""

    def render_cases(title: str, cases: dict[str, Any]) -> list[str]:
        lines = [
            f"## {title}",
            "",
            "| Case | Median (ms) | Min (ms) | Max (ms) |",
            "|---|---:|---:|---:|",
        ]

        for name, metrics in cases.items():
            lines.append(
                f"| {name} "
                f"| {metrics['median_count_action_ms']:.3f} "
                f"| {metrics['min_count_action_ms']:.3f} "
                f"| {metrics['max_count_action_ms']:.3f} |"
            )

        lines.append("")
        return lines

    environment = result["environment"]
    input_data = result["input"]

    lines = [
        "# Spark Tuning Benchmark v2",
        "",
        f"- Status: `{result['status']}`",
        f"- Generated (UTC): `{environment['timestamp_utc']}`",
        f"- Input provider: `{input_data['provider']}`",
        f"- Service date: `{input_data['service_date']}`",
        f"- Events rows: **{input_data['events_rows']:,}**",
        f"- Active trip events: **{input_data['active_trip_join_rows']:,}**",
        "",
        "## Environment",
        "",
        f"- Spark: `{environment['spark_version']}`",
        f"- PySpark: `{environment['pyspark_version']}`",
        f"- Python: `{environment['python_version']}`",
        f"- Java: `{environment['java_version']}`",
        f"- Master: `{environment['master']}`",
        f"- CPU count: `{environment['cpu_count']}`",
        "",
        "### Effective Spark SQL configuration",
        "",
        "| Configuration | Value |",
        "|---|---|",
    ]

    for key, value in environment["effective_sql_conf"].items():
        lines.append(f"| `{key}` | `{value}` |")

    lines.append("")
    lines.extend(
        render_cases("Shuffle partition benchmark", result["shuffle_benchmark"])
    )
    lines.extend(
        render_cases(
            "Input partition benchmark",
            result["input_partition_benchmark"]["cases"],
        )
    )
    lines.extend(
        render_cases("Broadcast benchmark", result["broadcast_benchmark"])
    )
    lines.extend(
        render_cases("Output repartition benchmark", result["repartition_benchmark"])
    )

    lines.extend([
        "## Validation",
        "",
        f"- All cases validated: `{result['validation']['all_cases_validated']}`",
        f"- Expected event rows: `{result['validation']['events_expected_rows']}`",
        f"- Expected active event rows: `{result['validation']['active_trip_expected_rows']}`",
        "- Each case validates output row count and schema against its baseline.",
        "",
        "## Methodology",
        "",
        f"- Warmup runs per case: `{result['methodology']['warmup_runs']}`",
        f"- Measured runs per case: `{result['methodology']['measured_runs_per_case']}`",
        f"- Primary metric: `{result['methodology']['primary_metric']}`",
        f"- Shuffle isolation: `{result['methodology']['shuffle_isolation']}`",
        f"- Production code modified: `{result['methodology']['production_code_modified']}`",
        "",
        "## Limitations",
        "",
    ])

    for limitation in result["limitations"]:
        lines.append(f"- {limitation}")

    lines.append("")
    return "\n".join(lines)


def build_active_trip_context(
    trips: Any,
    input_dir: Path,
) -> tuple[Any, dict[str, Any]]:
    """Build active trip IDs using the existing Trips DataFrame."""
    calendar = parse_gtfs_table(input_dir / "calendar.txt")
    calendar_dates = parse_gtfs_table(input_dir / "calendar_dates.txt")

    active_service_ids = get_active_service_ids(
        calendar=calendar,
        calendar_dates=calendar_dates,
        service_date=SERVICE_DATE,
    )

    active_trips = filter_active_trips(
        trips,
        active_service_ids,
    )
    active_trip_ids = active_trips.select("trip_id")

    metadata = {
        "service_date": SERVICE_DATE,
        "active_service_id_count": len(active_service_ids),
        "active_trip_rows": active_trips.count(),
        "active_trip_schema": schema_summary(active_trips),
    }

    return active_trip_ids, metadata


def run_benchmark(
    input_dir: Path,
    results_dir: Path,
    runs: int = RUNS,
) -> Path:
    """Run real-data tuning experiments and write a JSON report."""
    if runs < 1:
        raise ValueError("runs must be at least 1")

    input_dir = Path(input_dir)
    results_dir = validate_results_dir(results_dir)

    if not input_dir.is_dir():
        raise FileNotFoundError(f"GTFS input directory not found: {input_dir}")

    results_dir.mkdir(parents=True, exist_ok=True)
    results_path = results_dir / "spark_tuning_v2.json"
    markdown_path = results_dir / "spark_tuning_v2.md"

    if results_path.exists() or markdown_path.exists():
        raise FileExistsError(
            "A Spark tuning v2 report already exists in: "
            f"{results_dir}"
        )

    spark = create_spark_session()

    try:
        tables = load_gtfs_tables(spark, input_dir)

        input_counts = {
            name: dataframe.count()
            for name, dataframe in tables.items()
        }

        events = build_trip_stop_events(
            routes=tables["routes"],
            trips=tables["trips"],
            stop_times=tables["stop_times"],
            stops=tables["stops"],
        )

        expected_rows = events.count()
        expected_schema = schema_summary(events)

        active_trip_ids, active_metadata = build_active_trip_context(
            tables["trips"],
            input_dir,
        )

        active_events = build_active_trip_filter_join(
            events,
            active_trip_ids,
        )
        active_expected_rows = active_events.count()
        active_expected_schema = schema_summary(active_events)

        original_shuffle_partitions = spark.conf.get(
            "spark.sql.shuffle.partitions"
        )
        original_aqe = spark.conf.get("spark.sql.adaptive.enabled")
        original_broadcast_threshold = spark.conf.get(
            "spark.sql.autoBroadcastJoinThreshold"
        )

        shuffle_results = {}
        try:
            # Isolate shuffle partition effects from AQE and auto broadcast.
            spark.conf.set("spark.sql.adaptive.enabled", "false")
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

            for partitions in SHUFFLE_PARTITION_CASES:
                set_shuffle_partitions(spark, partitions)
                shuffle_results[str(partitions)] = run_repeated_case(
                    spark,
                    f"shuffle_partitions_{partitions}",
                    lambda: build_trip_stop_events(
                        routes=tables["routes"],
                        trips=tables["trips"],
                        stop_times=tables["stop_times"],
                        stops=tables["stops"],
                    ),
                    expected_rows=expected_rows,
                    expected_schema=expected_schema,
                    runs=runs,
                )
        finally:
            spark.conf.set(
                "spark.sql.shuffle.partitions",
                original_shuffle_partitions,
            )
            spark.conf.set("spark.sql.adaptive.enabled", original_aqe)
            spark.conf.set(
                "spark.sql.autoBroadcastJoinThreshold",
                original_broadcast_threshold,
            )

        input_partition_results = {}
        for partitions in INPUT_PARTITION_CASES:
            input_partition_results[str(partitions)] = run_repeated_case(
                spark,
                f"stop_times_input_partitions_{partitions}",
                lambda count=partitions: build_trip_stop_events(
                    routes=tables["routes"],
                    trips=tables["trips"],
                    stop_times=tables["stop_times"].repartition(count),
                    stops=tables["stops"],
                ),
                expected_rows=expected_rows,
                expected_schema=expected_schema,
                runs=runs,
            )

        broadcast_results = {
            "four_table_join_default": run_repeated_case(
                spark,
                "four_table_join_default",
                lambda: build_tuned_trip_stop_events(
                    tables["routes"],
                    tables["trips"],
                    tables["stop_times"],
                    tables["stops"],
                ),
                expected_rows=expected_rows,
                expected_schema=expected_schema,
                runs=runs,
            ),
            "four_table_join_broadcast_trips": run_repeated_case(
                spark,
                "four_table_join_broadcast_trips",
                lambda: build_tuned_trip_stop_events(
                    tables["routes"],
                    tables["trips"],
                    tables["stop_times"],
                    tables["stops"],
                    broadcast_trips=True,
                ),
                expected_rows=expected_rows,
                expected_schema=expected_schema,
                runs=runs,
            ),
            "active_trip_join_default": run_repeated_case(
                spark,
                "active_trip_join_default",
                lambda: build_active_trip_filter_join(
                    events,
                    active_trip_ids,
                ),
                expected_rows=active_expected_rows,
                expected_schema=active_expected_schema,
                runs=runs,
            ),
            "active_trip_join_broadcast": run_repeated_case(
                spark,
                "active_trip_join_broadcast",
                lambda: build_active_trip_filter_join(
                    events,
                    active_trip_ids,
                    force_broadcast=True,
                ),
                expected_rows=active_expected_rows,
                expected_schema=active_expected_schema,
                runs=runs,
            ),
        }

        repartition_results = {}
        for partitions in REPARTITION_CASES:
            repartition_results[str(partitions)] = run_repeated_case(
                spark,
                f"events_repartition_{partitions}",
                lambda count=partitions: repartition_dataframe(
                    events,
                    partitions=count,
                ),
                expected_rows=expected_rows,
                expected_schema=expected_schema,
                runs=runs,
            )

        result = {
            "benchmark": "spark_tuning_v2",
            "status": "completed",
            "input": {
                "provider": "at",
                "input_dir": str(input_dir),
                "service_date": SERVICE_DATE,
                "row_counts": input_counts,
                "events_rows": expected_rows,
                "events_schema": expected_schema,
                "active_trip_context": active_metadata,
                "active_trip_join_rows": active_expected_rows,
            },
            "environment": collect_environment(spark),
            "shuffle_benchmark": shuffle_results,
            "input_partition_benchmark": {
                "target_table": "stop_times",
                "cases": input_partition_results,
            },
            "broadcast_benchmark": broadcast_results,
            "repartition_benchmark": repartition_results,
            "validation": {
                "events_expected_rows": expected_rows,
                "events_schema": expected_schema,
                "active_trip_expected_rows": active_expected_rows,
                "active_trip_schema": active_expected_schema,
                "all_cases_validated": True,
            },
            "methodology": {
                "warmup_runs": 1,
                "measured_runs_per_case": runs,
                "primary_metric": "median_count_action_ms",
                "input_partition_note": (
                    "Only stop_times is repartitioned. Its repartition "
                    "shuffle is included in the measured count action."
                ),
                "repartition_note": (
                    "Output repartition shuffle is included in the "
                    "measured count action."
                ),
                "shuffle_isolation": (
                    "AQE disabled and automatic broadcast disabled only "
                    "during shuffle partition experiments; original "
                    "settings restored afterward."
                ),
                "production_code_modified": False,
            },
            "limitations": [
                "Results are from local[*], not a distributed cluster.",
                "OS file-system caches were not cleared.",
                "AQE and automatic broadcast are disabled only for the "
                "isolated shuffle partition experiments.",
                "Other experiments use the session's normal Spark SQL settings.",
                "Count action time includes lazy transformations and shuffles.",
                "Input partition experiments repartition stop_times only.",
            ],
        }

        results_path.write_text(
            json.dumps(result, indent=2),
            encoding="utf-8",
        )
        markdown_path.write_text(
            build_markdown_report(result),
            encoding="utf-8",
        )
        return results_path

    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=PROJECT_ROOT / "data/gtfs/at",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=PROJECT_ROOT / "reports/benchmarks",
    )
    parser.add_argument("--runs", type=int, default=RUNS)
    return parser.parse_args()


def main() -> None:
    """Run the benchmark only when explicitly invoked."""
    args = parse_args()
    print(run_benchmark(args.input_dir, args.results_dir, args.runs))


if __name__ == "__main__":
    main()
