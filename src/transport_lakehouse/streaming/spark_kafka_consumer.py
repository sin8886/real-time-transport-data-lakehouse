"""Spark Structured Streaming consumers for transport Kafka topics."""

from __future__ import annotations

from typing import Any


RAW_TOPICS = (
    "transport.vehicle_positions.raw",
    "transport.trip_updates.raw",
    "transport.service_alerts.raw",
)

VALIDATED_TOPIC = "transport.events.validated"
DLQ_TOPIC = "transport.events.dlq"

DEFAULT_BOOTSTRAP_SERVERS = "kafka:29092"


def create_spark_session() -> Any:
    """Create a Spark session; the master is supplied by spark-submit."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("TransportLakehouseKafkaStreaming")
        .config("spark.ui.showConsoleProgress", "false")
        .config(
            "spark.executorEnv.PYTHONPATH",
            "/opt/transport-lakehouse/src",
        )
        .getOrCreate()
    )


def read_kafka_stream(
    spark: Any,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topics: tuple[str, ...] = RAW_TOPICS,
    starting_offsets: str = "earliest",
) -> Any:
    """Subscribe to Kafka topics with configurable initial offsets.

    The starting_offsets setting only applies when a new checkpoint is
    created. Existing checkpoints determine offsets during recovery.
    """
    if not bootstrap_servers.strip():
        raise ValueError("Kafka bootstrap servers cannot be empty.")

    if not topics or any(not topic.strip() for topic in topics):
        raise ValueError("At least one non-empty Kafka topic is required.")

    if starting_offsets not in {"earliest", "latest"}:
        raise ValueError("starting_offsets must be 'earliest' or 'latest'.")

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", starting_offsets)
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


def validate_kafka_stream(message_stream: Any) -> Any:
    """Apply the canonical event validator to each Kafka message."""
    from pyspark.sql.functions import udf
    from pyspark.sql.types import (
        BooleanType,
        StringType,
        StructField,
        StructType,
    )

    from transport_lakehouse.streaming.stream_validation import (
        validate_raw_message,
    )

    result_schema = StructType(
        [
            StructField("is_valid", BooleanType(), False),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True),
        ]
    )

    def validate_message(raw_message: str | None, source_topic: str) -> tuple:
        result = validate_raw_message(
            raw_message,
            source_topic=source_topic,
        )
        return (
            result.is_valid,
            result.error_type,
            result.error_message,
        )

    validate_udf = udf(validate_message, result_schema)

    return message_stream.withColumn(
        "validation",
        validate_udf("message_value", "topic"),
    )


def split_validated_and_dlq(validated_stream: Any) -> tuple[Any, Any]:
    """Split validated events from rejected messages."""
    from pyspark.sql.functions import col, lit, struct, to_json

    valid_stream = (
        validated_stream.filter(col("validation.is_valid"))
        .select(
            col("message_key").cast("string").alias("key"),
            col("message_value").cast("string").alias("value"),
        )
        .withColumn("topic", lit(VALIDATED_TOPIC))
    )

    dlq_stream = (
        validated_stream.filter(~col("validation.is_valid"))
        .select(
            col("message_key"),
            col("message_value"),
            col("topic").alias("source_topic"),
            col("partition").alias("source_partition"),
            col("offset").alias("source_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("validation.error_type").alias("error_type"),
            col("validation.error_message").alias("error_message"),
        )
        .select(
            col("message_key").cast("string").alias("key"),
            to_json(
                struct(
                    col("message_value").alias("raw_message"),
                    col("source_topic"),
                    col("source_partition"),
                    col("source_offset"),
                    col("kafka_timestamp"),
                    col("error_type"),
                    col("error_message"),
                )
            ).alias("value"),
        )
        .withColumn("topic", lit(DLQ_TOPIC))
    )

    return valid_stream, dlq_stream


def start_kafka_sink(
    output_stream: Any,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    checkpoint_location: str = "/tmp/transport-day30-checkpoint",
) -> Any:
    """Write records to the Kafka topic specified by each row."""
    if not bootstrap_servers.strip():
        raise ValueError("Kafka bootstrap servers cannot be empty.")
    if not checkpoint_location.strip():
        raise ValueError("Checkpoint location cannot be empty.")

    return (
        output_stream.select("key", "value", "topic")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="5 seconds")
        .start()
    )


def start_validation_query(
    valid_stream: Any,
    dlq_stream: Any,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    checkpoint_location: str = "/tmp/transport-day30-checkpoint",
) -> Any:
    """Route valid and rejected records through one Kafka sink query."""
    combined_stream = valid_stream.unionByName(dlq_stream)
    return start_kafka_sink(
        combined_stream,
        bootstrap_servers=bootstrap_servers,
        checkpoint_location=checkpoint_location,
    )

def parse_event_time(message_stream: Any) -> Any:
    """Parse canonical event_ts JSON field into Spark event_time."""
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StringType, StructField, StructType

    event_schema = StructType(
        [StructField("event_ts", StringType(), True)]
    )

    return (
        message_stream.withColumn(
            "_event",
            from_json(col("message_value"), event_schema),
        )
        .withColumn(
            "event_time",
            col("_event.event_ts").cast("timestamp"),
        )
        .drop("_event")
    )


def apply_event_time_watermark(
    event_stream: Any,
    delay_threshold: str = "5 minutes",
) -> Any:
    """Apply a watermark to the parsed event-time column."""
    if not delay_threshold.strip():
        raise ValueError("Watermark delay threshold cannot be empty.")

    return event_stream.withWatermark("event_time", delay_threshold)
