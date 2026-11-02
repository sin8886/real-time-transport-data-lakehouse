from __future__ import annotations

import pytest

from transport_lakehouse.streaming.spark_kafka_consumer import (
    DEFAULT_BOOTSTRAP_SERVERS,
    RAW_TOPICS,
    read_kafka_stream,
)


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
