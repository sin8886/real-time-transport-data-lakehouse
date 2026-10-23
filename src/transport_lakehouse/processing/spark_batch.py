"""Spark batch transformations for GTFS Static data."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


TABLE_COLUMNS = {
    "routes": {"route_id", "route_short_name", "route_type"},
    "trips": {"trip_id", "route_id", "service_id", "direction_id"},
    "stop_times": {
        "trip_id",
        "stop_id",
        "stop_sequence",
        "arrival_time",
        "departure_time",
    },
    "stops": {"stop_id", "stop_name", "stop_lat", "stop_lon"},
}


def create_spark_session() -> Any:
    """Create a local Spark session for GTFS batch processing."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(
            "PySpark is required. Install it with: pip install 'pyspark>=3.5,<4.0'"
        ) from error

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    return (
        SparkSession.builder.appName("TransportLakehouseBatch")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_gtfs_tables(spark: Any, gtfs_dir: str | Path) -> dict[str, Any]:
    """Load the four required GTFS Static CSV tables as string columns."""
    root = Path(gtfs_dir)

    tables: dict[str, Any] = {}
    for table_name, required_columns in TABLE_COLUMNS.items():
        input_path = root / f"{table_name}.txt"

        if not input_path.is_file():
            raise FileNotFoundError(f"Required GTFS file was not found: {input_path}")

        df = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .option("encoding", "UTF-8")
            .csv(str(input_path))
        )

        missing_columns = required_columns.difference(df.columns)
        if missing_columns:
            raise ValueError(
                f"{table_name}.txt is missing required columns: "
                f"{sorted(missing_columns)}"
            )

        tables[table_name] = df

    return tables


def build_trip_stop_events(
    routes: Any,
    trips: Any,
    stop_times: Any,
    stops: Any,
) -> Any:
    """Join GTFS tables into one row per trip-stop event."""
    from pyspark.sql import functions as F

    table_inputs = {
        "routes": routes,
        "trips": trips,
        "stop_times": stop_times,
        "stops": stops,
    }

    for table_name, df in table_inputs.items():
        missing_columns = TABLE_COLUMNS[table_name].difference(df.columns)
        if missing_columns:
            raise ValueError(
                f"{table_name} DataFrame is missing required columns: "
                f"{sorted(missing_columns)}"
            )

    joined = (
        routes.alias("r")
        .join(
            trips.alias("t"),
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


def run_spark_batch(spark: Any, gtfs_dir: str | Path) -> Any:
    """Load GTFS Static tables and build the enriched trip-stop DataFrame."""
    tables = load_gtfs_tables(spark, gtfs_dir)

    return build_trip_stop_events(
        routes=tables["routes"],
        trips=tables["trips"],
        stop_times=tables["stop_times"],
        stops=tables["stops"],
    )
