"""Consume Kafka vehicle events and upsert the latest state into PostgreSQL."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import (  # noqa: E402
    avg,
    col,
    count,
    from_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
    DEFAULT_BOOTSTRAP_SERVERS,
    read_kafka_stream,
    select_message_columns,
)

SCHEMA_PATH = Path(
    "/opt/transport-lakehouse/src/transport_lakehouse/sql/realtime_serving_v1.sql"
)

VEHICLE_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("source_provider", StringType()),
        StructField("event_ts", StringType()),
        StructField("ingest_ts", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("bearing", DoubleType()),
        StructField("is_deleted", BooleanType()),
    ]
)

TRIP_UPDATE_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("source_provider", StringType()),
        StructField("event_ts", StringType()),
        StructField("route_id", StringType()),
        StructField("delay_seconds", DoubleType()),
    ]
)


def _connection() -> Any:
    import psycopg

    return psycopg.connect(
        dbname=os.getenv("POSTGRES_DB", "transport_lakehouse"),
        user=os.getenv("POSTGRES_USER", "transport"),
        password=os.getenv("POSTGRES_PASSWORD", "transport_local_dev"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )


def _ensure_schema() -> None:
    with _connection() as connection:
        connection.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
        connection.commit()


def _write_batch(batch: Any, batch_id: int) -> None:
    rows = batch.collect()
    if not rows:
        return
    sql = """
        INSERT INTO dashboard_active_vehicles (
            provider, vehicle_id, trip_id, route_id, latitude, longitude,
            speed, bearing, event_ts
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (provider, vehicle_id) DO UPDATE SET
            trip_id = EXCLUDED.trip_id,
            route_id = EXCLUDED.route_id,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            speed = EXCLUDED.speed,
            bearing = EXCLUDED.bearing,
            event_ts = EXCLUDED.event_ts,
            ingested_at = CURRENT_TIMESTAMP
    """
    values = [
        (
            row.source_provider,
            row.vehicle_id,
            row.trip_id,
            row.route_id,
            row.latitude,
            row.longitude,
            row.speed,
            row.bearing,
            row.event_ts,
        )
        for row in rows
        if row.source_provider and row.vehicle_id and row.event_ts
    ]
    if not values:
        return
    with _connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, values)
        connection.commit()
    print(f"Persisted {len(values)} vehicles in batch {batch_id}", flush=True)


def _write_delay_batch(batch: Any, batch_id: int) -> None:
    rows = batch.collect()
    values = [
        (
            row.window_start,
            row.window_end,
            row.source_provider,
            row.route_id,
            row.avg_delay_seconds,
            row.event_count,
        )
        for row in rows
        if row.source_provider and row.route_id and row.window_start
    ]
    if not values:
        return
    sql = """
        INSERT INTO dashboard_route_delay_trends (
            window_start, window_end, provider, route_id,
            avg_delay_seconds, event_count
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, provider, route_id)
        DO UPDATE SET
            avg_delay_seconds = EXCLUDED.avg_delay_seconds,
            event_count = EXCLUDED.event_count
    """
    with _connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, values)
        connection.commit()
    print(f"Persisted {len(values)} delay windows in batch {batch_id}", flush=True)


def main() -> None:
    _ensure_schema()
    spark = (
        SparkSession.builder.appName("TransportRealtimeServing")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    stream = select_message_columns(
        read_kafka_stream(
            spark,
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
            ),
            topics=("transport.vehicle_positions.raw",),
            starting_offsets="latest",
        )
    )
    parsed = stream.withColumn(
        "event",
        from_json(col("message_value"), VEHICLE_SCHEMA),
    ).select("event.*")
    vehicles = parsed.filter(
        (col("event_type") == "vehicle_position")
        & col("vehicle_id").isNotNull()
        & col("event_ts").isNotNull()
        & (~col("is_deleted"))
    )
    query = (
        vehicles.writeStream.foreachBatch(_write_batch)
        .outputMode("update")
        .option(
            "checkpointLocation",
            os.getenv(
                "REALTIME_CHECKPOINT",
                "/opt/transport-lakehouse-checkpoints/dashboard/realtime-vehicles",
            ),
        )
        .trigger(processingTime="10 seconds")
        .start()
    )
    trip_stream = select_message_columns(
        read_kafka_stream(
            spark,
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
            ),
            topics=("transport.trip_updates.raw",),
            starting_offsets="latest",
        )
    )
    trip_events = (
        trip_stream.withColumn(
            "event",
            from_json(col("message_value"), TRIP_UPDATE_SCHEMA),
        )
        .select("event.*")
        .filter(
            (col("event_type") == "trip_update")
            & col("source_provider").isNotNull()
            & col("route_id").isNotNull()
            & col("event_ts").isNotNull()
            & col("delay_seconds").isNotNull()
        )
        .withColumn("event_ts", to_timestamp("event_ts"))
    )
    delay_windows = (
        trip_events.withWatermark("event_ts", "5 minutes")
        .groupBy(window("event_ts", "5 minutes"), "source_provider", "route_id")
        .agg(
            avg("delay_seconds").alias("avg_delay_seconds"),
            count("*").alias("event_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "source_provider",
            "route_id",
            "avg_delay_seconds",
            "event_count",
        )
    )
    delay_query = (
        delay_windows.writeStream.foreachBatch(_write_delay_batch)
        .outputMode("update")
        .option(
            "checkpointLocation",
            os.getenv(
                "REALTIME_DELAY_CHECKPOINT",
                os.getenv(
                    "REALTIME_CHECKPOINT",
                    "/opt/transport-lakehouse-checkpoints/dashboard/"
                    "realtime-vehicles",
                )
                + "-delays",
            ),
        )
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()
    delay_query.awaitTermination()


if __name__ == "__main__":
    main()
