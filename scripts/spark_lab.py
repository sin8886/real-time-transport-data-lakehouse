"""A small local PySpark lab for transport route data."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


REQUIRED_COLUMNS = {
    "route_id",
    "route_name",
    "trip_id",
    "stop_id",
    "delay_seconds",
}


def _require_pyspark() -> tuple[Any, Any]:
    """Import PySpark when needed and provide an actionable error if absent."""
    try:
        from pyspark.sql import SparkSession, functions as F
    except ImportError as error:
        raise RuntimeError(
            "PySpark is required for this lab. Install it in .venv with: "
            "'.\\.venv\\Scripts\\python.exe -m pip install \"pyspark>=3.5,<4.0\"'"
        ) from error
    return SparkSession, F


def create_spark_session() -> Any:
    """Create a Spark session that runs locally instead of using the cluster."""
    SparkSession, _ = _require_pyspark()
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.appName("TransportSparkLab")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_routes(spark: Any, input_path: str | Path) -> Any:
    """Load route observations from a headered CSV with an inferred numeric delay."""
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(input_path))
    )
    df.printSchema()
    df.show(5, truncate=False)
    return df


def summarize_routes(df: Any) -> Any:
    """Summarize records, trips, and average delay by route."""
    _, F = _require_pyspark()
    return (
        df.select("route_id", "route_name", "trip_id", "delay_seconds")
        .filter(F.col("delay_seconds") > 0)
        .groupBy("route_id", "route_name")
        .agg(
            F.count("*").alias("delayed_record_count"),
            F.countDistinct("trip_id").alias("delayed_trip_count"),
            F.avg("delay_seconds").alias("average_delay_seconds"),
        )
        .orderBy("route_id")
    )


def run_spark_sql(df: Any) -> Any:
    """Run a SQL aggregation over a temporary route view."""
    df.createOrReplaceTempView("transport_routes")
    result = df.sparkSession.sql(
        """
        SELECT
            route_id,
            COUNT(*) AS record_count,
            AVG(delay_seconds) AS average_delay_seconds
        FROM transport_routes
        GROUP BY route_id
        ORDER BY route_id
        """
    )
    result.show(truncate=False)
    return result


def _require_windows_hadoop_tools(spark: Any) -> None:
    """Require a user-configured winutils installation before local writes."""
    if os.name != "nt":
        return

    hadoop_home = os.environ.get("HADOOP_HOME")
    winutils_path = Path(hadoop_home) / "bin" / "winutils.exe" if hadoop_home else None
    if winutils_path is None or not winutils_path.is_file():
        raise RuntimeError(
            "Writing Parquet on Windows requires Hadoop's winutils.exe. "
            "Set HADOOP_HOME to a trusted local Hadoop tools directory "
            "containing bin\\winutils.exe before starting Spark. Do not "
            "download an unverified executable; build the matching Hadoop "
            "Windows native tools from the Apache Hadoop source, or run this "
            "lab in WSL/Linux. HADOOP_HOME was not changed by this lab."
        )

    # Hadoop reads this Java system property after the Spark JVM has started.
    spark.sparkContext._jvm.java.lang.System.setProperty(
        "hadoop.home.dir", str(Path(hadoop_home))
    )


def write_partitioned_parquet(df: Any, output_path: str | Path) -> None:
    """Write route data as disposable, route-partitioned Parquet output."""
    # partitionBy creates a storage layout, not Spark's in-memory execution partitions.
    _require_windows_hadoop_tools(df.sparkSession)
    df.write.mode("overwrite").partitionBy("route_id").parquet(str(output_path))


def validate_parquet(output_path: str | Path) -> Any:
    """Read route Parquet output and validate its columns, rows, and partitions."""
    SparkSession, _ = _require_pyspark()
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("An active Spark session is required to validate Parquet.")

    df = spark.read.parquet(str(output_path))
    missing_columns = REQUIRED_COLUMNS.difference(df.columns)
    if missing_columns:
        raise ValueError(
            f"Parquet output is missing columns: {sorted(missing_columns)}"
        )
    if df.count() == 0:
        raise ValueError("Parquet output contains no route records.")
    partition_dir = Path(output_path) / "route_id=70"
    if not partition_dir.is_dir():
        raise ValueError(
            f"Expected route partition directory is missing: {partition_dir}"
        )
    df.printSchema()
    df.show(5, truncate=False)
    return df


def main() -> None:
    """Run the complete local Spark lab."""
    project_root = Path(__file__).resolve().parents[1]
    input_path = project_root / "data" / "spark_lab" / "routes.csv"
    output_path = project_root / "data" / "spark_lab" / "output" / "route_data"
    spark = create_spark_session()
    try:
        routes = load_routes(spark, input_path)
        print("Positive-delay route summary:")
        summarize_routes(routes).show(truncate=False)
        print("Spark SQL route summary:")
        run_spark_sql(routes)
        write_partitioned_parquet(routes, output_path)
        print(f"Validated Parquet output at: {output_path}")
        validate_parquet(output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
