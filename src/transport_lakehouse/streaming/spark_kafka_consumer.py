"""Spark Structured Streaming consumer for raw transport Kafka topics."""

from __future__ import annotations

from typing import Any


RAW_TOPICS = (
    "transport.vehicle_positions.raw",
    "transport.trip_updates.raw",
    "transport.service_alerts.raw",
)

DEFAULT_BOOTSTRAP_SERVERS = "kafka:29092"


def create_spark_session() -> Any:
    """Create a Spark session; the master is supplied by spark-submit."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("TransportLakehouseKafkaStreaming")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_kafka_stream(
    spark: Any,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topics: tuple[str, ...] = RAW_TOPICS,
) -> Any:
    """Subscribe to raw Kafka topics and return the streaming DataFrame."""
    if not bootstrap_servers.strip():
        raise ValueError("Kafka bootstrap servers cannot be empty.")

    if not topics or any(not topic.strip() for topic in topics):
        raise ValueError("At least one non-empty Kafka topic is required.")

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", "earliest")
        .load()
    )


def select_message_columns(kafka_stream: Any) -> Any:
    """Select Kafka metadata and decode key/value as UTF-8 strings."""
    return kafka_stream.selectExpr(
        "CAST(key AS STRING) AS message_key",
        "CAST(value AS STRING) AS message_value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    )


def start_console_query(message_stream: Any) -> Any:
    """Start a console sink for the Day 29 streaming smoke test."""
    return (
        message_stream.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="5 seconds")
        .start()
    )
