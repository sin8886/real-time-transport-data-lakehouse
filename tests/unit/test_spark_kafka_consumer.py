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
