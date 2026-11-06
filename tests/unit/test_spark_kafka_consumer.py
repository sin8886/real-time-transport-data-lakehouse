from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
    DEFAULT_BOOTSTRAP_SERVERS,
    RAW_TOPICS,
    create_spark_session,
    read_kafka_stream,
)



@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = create_spark_session()
    yield session
    session.stop()


class FakeReader:
    def __init__(self) -> None:
        self.options: dict[str, str] = {}

    def format(self, value: str) -> FakeReader:
        self.format_value = value
        return self

    def option(self, key: str, value: str) -> FakeReader:
        self.options[key] = value
        return self

    def load(self) -> str:
        return "fake-stream"


class FakeSpark:
    def __init__(self) -> None:
        self.reader = FakeReader()
        self.readStream = self.reader


def test_default_starting_offsets_is_earliest() -> None:
    spark = FakeSpark()

    result = read_kafka_stream(spark)

    assert result == "fake-stream"
    assert spark.reader.format_value == "kafka"
    assert spark.reader.options["startingOffsets"] == "earliest"
    assert spark.reader.options["subscribe"] == ",".join(RAW_TOPICS)
    assert (
        spark.reader.options["kafka.bootstrap.servers"]
        == DEFAULT_BOOTSTRAP_SERVERS
    )


def test_latest_starting_offsets_is_supported() -> None:
    spark = FakeSpark()

    read_kafka_stream(
        spark,
        topics=("transport.vehicle_positions.raw",),
        starting_offsets="latest",
    )

    assert spark.reader.options["startingOffsets"] == "latest"


@pytest.mark.parametrize("starting_offsets", ["", "middle", "LATEST"])
def test_invalid_starting_offsets_is_rejected(
    starting_offsets: str,
) -> None:
    spark = FakeSpark()

    with pytest.raises(
        ValueError,
        match="starting_offsets must be 'earliest' or 'latest'",
    ):
        read_kafka_stream(
            spark,
            starting_offsets=starting_offsets,
        )


def test_parse_event_time_converts_canonical_timestamp(spark) -> None:
    from pyspark.sql.types import TimestampType

    from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
        parse_event_time,
    )

    source = spark.createDataFrame(
        [
            ('{"event_ts":"2026-10-29T06:00:02+00:00"}',),
        ],
        ["message_value"],
    )

    result = parse_event_time(source)

    assert isinstance(result.schema["event_time"].dataType, TimestampType)
    assert result.first()["event_time"] is not None


def test_parse_event_time_handles_missing_timestamp(spark) -> None:
    from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
        parse_event_time,
    )

    source = spark.createDataFrame(
        [('{"event_type":"vehicle_position"}',)],
        ["message_value"],
    )

    result = parse_event_time(source)

    assert result.first()["event_time"] is None


def test_apply_event_time_watermark(spark) -> None:
    from pyspark.sql.types import TimestampType

    from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
        apply_event_time_watermark,
    )

    source = spark.createDataFrame(
        [],
        "event_time timestamp",
    )

    result = apply_event_time_watermark(source, "5 minutes")

    assert isinstance(result.schema["event_time"].dataType, TimestampType)


def test_apply_event_time_watermark_rejects_empty_threshold(
    spark,
) -> None:
    from transport_lakehouse.streaming.spark_kafka_consumer import (  # noqa: E402
        apply_event_time_watermark,
    )

    source = spark.createDataFrame(
        [],
        "event_time timestamp",
    )

    with pytest.raises(
        ValueError,
        match="Watermark delay threshold cannot be empty",
    ):
        apply_event_time_watermark(source, "   ")


def test_build_dedup_identity_for_vehicle_position(spark) -> None:
    import json

    from transport_lakehouse.streaming.spark_kafka_consumer import (
        build_dedup_identity,
    )

    event = {
        "event_type": "vehicle_position",
        "source_provider": "at",
        "source_entity_id": "entity-001",
        "event_ts": "2026-10-29T06:00:00+00:00",
        "vehicle_id": "vehicle-001",
    }
    source = spark.createDataFrame(
        [(json.dumps(event),)],
        ["message_value"],
    )

    result = build_dedup_identity(source).first()

    assert result["identity_status"] == "resolved"
    assert result["dedup_identity"] is not None
    assert "entity-001" in result["dedup_identity"]
    assert "vehicle-001" not in result["dedup_identity"]


def test_build_dedup_identity_for_trip_update(spark) -> None:
    import json

    from transport_lakehouse.streaming.spark_kafka_consumer import (
        build_dedup_identity,
    )

    event = {
        "event_type": "trip_update",
        "source_provider": "at",
        "trip_id": "trip-001",
        "stop_sequence": 3,
        "stop_id": "stop-003",
        "event_ts": "2026-10-29T06:00:00+00:00",
    }
    source = spark.createDataFrame(
        [(json.dumps(event),)],
        ["message_value"],
    )

    result = build_dedup_identity(source).first()

    assert result["identity_status"] == "resolved"
    assert "trip-001" in result["dedup_identity"]
    assert "stop_sequence" in result["dedup_identity"]
    assert '"3"' in result["dedup_identity"]


def test_build_dedup_identity_for_service_alert(spark) -> None:
    import json

    from transport_lakehouse.streaming.spark_kafka_consumer import (
        build_dedup_identity,
    )

    event = {
        "event_type": "service_alert",
        "source_provider": "at",
        "alert_id": "alert-001",
        "event_ts": "2026-10-29T06:00:00+00:00",
    }
    source = spark.createDataFrame(
        [(json.dumps(event),)],
        ["message_value"],
    )

    result = build_dedup_identity(source).first()

    assert result["identity_status"] == "resolved"
    assert "alert-001" in result["dedup_identity"]


def test_build_dedup_identity_marks_missing_identity_unresolved(
    spark,
) -> None:
    import json

    from transport_lakehouse.streaming.spark_kafka_consumer import (
        build_dedup_identity,
    )

    events = [
        {
            "event_type": "vehicle_position",
            "source_provider": "at",
            "event_ts": "2026-10-29T06:00:00+00:00",
            "vehicle_id": "vehicle-001",
        },
        {
            "event_type": "trip_update",
            "source_provider": "at",
            "trip_id": "trip-001",
            "event_ts": "2026-10-29T06:00:00+00:00",
        },
        {
            "event_type": "service_alert",
            "source_provider": "at",
            "event_ts": "2026-10-29T06:00:00+00:00",
        },
    ]
    source = spark.createDataFrame(
        [(json.dumps(event),) for event in events],
        ["message_value"],
    )

    results = build_dedup_identity(source).collect()

    assert len(results) == 3
    assert all(
        result["identity_status"] == "identity_unresolved"
        for result in results
    )
    assert all(result["dedup_identity"] is None for result in results)


def test_build_dedup_identity_uses_stop_id_when_sequence_missing(
    spark,
) -> None:
    import json

    from transport_lakehouse.streaming.spark_kafka_consumer import (
        build_dedup_identity,
    )

    event = {
        "event_type": "trip_update",
        "source_provider": "at",
        "trip_id": "trip-001",
        "stop_id": "stop-003",
        "event_ts": "2026-10-29T06:00:00+00:00",
    }
    source = spark.createDataFrame(
        [(json.dumps(event),)],
        ["message_value"],
    )

    result = build_dedup_identity(source).first()

    assert result["identity_status"] == "resolved"
    assert "stop_id" in result["dedup_identity"]
    assert "stop-003" in result["dedup_identity"]


def test_read_kafka_stream_rejects_empty_bootstrap_servers() -> None:
    spark = FakeSpark()

    with pytest.raises(ValueError, match="Kafka bootstrap servers cannot be empty"):
        read_kafka_stream(spark, bootstrap_servers="  ")


@pytest.mark.parametrize("topics", [(), (" ",)])
def test_read_kafka_stream_rejects_empty_topics(
    topics: tuple[str, ...],
) -> None:
    spark = FakeSpark()

    with pytest.raises(
        ValueError,
        match="At least one non-empty Kafka topic is required",
    ):
        read_kafka_stream(spark, topics=topics)


def test_read_kafka_stream_propagates_load_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = FakeSpark()

    def fail_load() -> None:
        raise RuntimeError("Kafka source unavailable")

    monkeypatch.setattr(spark.reader, "load", fail_load)

    with pytest.raises(RuntimeError, match="Kafka source unavailable"):
        read_kafka_stream(spark)


class FakeStreamingWriter:
    def __init__(self, *, fail_start: bool = False) -> None:
        self.options: dict[str, str] = {}
        self.format_value: str | None = None
        self.output_mode: str | None = None
        self.trigger_options: dict[str, str] = {}
        self.fail_start = fail_start

    def format(self, value: str) -> FakeStreamingWriter:
        self.format_value = value
        return self

    def option(self, key: str, value: str) -> FakeStreamingWriter:
        self.options[key] = value
        return self

    def outputMode(self, value: str) -> FakeStreamingWriter:
        self.output_mode = value
        return self

    def trigger(self, **kwargs: str) -> FakeStreamingWriter:
        self.trigger_options.update(kwargs)
        return self

    def start(self) -> str:
        if self.fail_start:
            raise RuntimeError("Kafka sink unavailable")
        return "fake-query"


class FakeOutputStream:
    def __init__(self, *, fail_start: bool = False) -> None:
        self.writer = FakeStreamingWriter(fail_start=fail_start)
        self.writeStream = self.writer
        self.selected_columns: tuple[str, ...] | None = None
        self.union_stream: FakeOutputStream | None = None

    def select(self, *columns: str) -> FakeOutputStream:
        self.selected_columns = columns
        return self

    def unionByName(
        self,
        other: FakeOutputStream,
    ) -> FakeOutputStream:
        self.union_stream = other
        return self


def test_start_kafka_sink_passes_checkpoint_location() -> None:
    from transport_lakehouse.streaming.spark_kafka_consumer import (
        start_kafka_sink,
    )

    stream = FakeOutputStream()

    result = start_kafka_sink(
        stream,
        bootstrap_servers="localhost:9092",
        checkpoint_location="/tmp/test-kafka-checkpoint",
    )

    assert result == "fake-query"
    assert stream.selected_columns == ("key", "value", "topic")
    assert stream.writer.format_value == "kafka"
    assert (
        stream.writer.options["checkpointLocation"]
        == "/tmp/test-kafka-checkpoint"
    )
    assert stream.writer.options["kafka.bootstrap.servers"] == "localhost:9092"
    assert stream.writer.output_mode == "append"
    assert stream.writer.trigger_options == {"processingTime": "5 seconds"}


def test_start_kafka_sink_propagates_start_failure() -> None:
    from transport_lakehouse.streaming.spark_kafka_consumer import (
        start_kafka_sink,
    )

    stream = FakeOutputStream(fail_start=True)

    with pytest.raises(RuntimeError, match="Kafka sink unavailable"):
        start_kafka_sink(
            stream,
            checkpoint_location="/tmp/test-kafka-checkpoint",
        )


def test_start_validation_query_forwards_checkpoint_location() -> None:
    from transport_lakehouse.streaming.spark_kafka_consumer import (
        start_validation_query,
    )

    valid_stream = FakeOutputStream()
    dlq_stream = FakeOutputStream()

    result = start_validation_query(
        valid_stream,
        dlq_stream,
        bootstrap_servers="localhost:9092",
        checkpoint_location="/tmp/validation-checkpoint",
    )

    assert result == "fake-query"
    assert valid_stream.union_stream is dlq_stream
    assert (
        valid_stream.writer.options["checkpointLocation"]
        == "/tmp/validation-checkpoint"
    )


@pytest.mark.parametrize(
    ("bootstrap_servers", "checkpoint_location", "error_message"),
    [
        ("  ", "/tmp/test-checkpoint", "Kafka bootstrap servers cannot be empty"),
        ("localhost:9092", "  ", "Checkpoint location cannot be empty"),
    ],
)
def test_start_kafka_sink_rejects_empty_required_settings(
    bootstrap_servers: str,
    checkpoint_location: str,
    error_message: str,
) -> None:
    from transport_lakehouse.streaming.spark_kafka_consumer import (
        start_kafka_sink,
    )

    stream = FakeOutputStream()

    with pytest.raises(ValueError, match=error_message):
        start_kafka_sink(
            stream,
            bootstrap_servers=bootstrap_servers,
            checkpoint_location=checkpoint_location,
        )
