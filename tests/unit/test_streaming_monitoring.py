from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from transport_lakehouse.streaming.monitoring import (
    calculate_spark_observed_offset_gap,
    extract_progress_metrics,
    parse_kafka_offsets,
)

@pytest.mark.parametrize(
    ("raw_offsets", "expected"),
    [
        (
            '{"topic-a":{"0":100,"1":200},"topic-b":{"0":50}}',
            {
                "topic-a": {"0": 100, "1": 200},
                "topic-b": {"0": 50},
            },
        ),
        ("{}", {}),
        ("", {}),
        ("null", {}),
        ("not-json", {}),
    ],
)
def test_parse_kafka_offsets(
    raw_offsets: str,
    expected: dict[str, dict[str, int]],
) -> None:
    assert parse_kafka_offsets(raw_offsets) == expected


def test_parse_kafka_offsets_ignores_invalid_partition_values() -> None:
    raw_offsets = (
        '{"topic-a":{"0":100,"1":"invalid","2":-1,"3":true}}'
    )

    assert parse_kafka_offsets(raw_offsets) == {
        "topic-a": {"0": 100}
    }


def test_calculate_spark_observed_offset_gap() -> None:
    end_offset = (
        '{"topic-a":{"0":100,"1":200},"topic-b":{"0":50}}'
    )
    latest_offset = (
        '{"topic-a":{"0":110,"1":205},"topic-b":{"0":80}}'
    )

    # (110 - 100) + (205 - 200) + (80 - 50) = 45
    assert (
        calculate_spark_observed_offset_gap(
            end_offset,
            latest_offset,
        )
        == 45
    )


def test_offset_gap_ignores_unmatched_topics_and_partitions() -> None:
    end_offset = '{"topic-a":{"0":100,"1":200}}'
    latest_offset = (
        '{"topic-a":{"0":110},"topic-b":{"0":500}}'
    )

    assert (
        calculate_spark_observed_offset_gap(
            end_offset,
            latest_offset,
        )
        == 10
    )


def test_offset_gap_never_returns_negative_values() -> None:
    end_offset = '{"topic-a":{"0":100}}'
    latest_offset = '{"topic-a":{"0":90}}'

    assert (
        calculate_spark_observed_offset_gap(
            end_offset,
            latest_offset,
        )
        == 0
    )


def test_offset_gap_handles_missing_or_invalid_offsets() -> None:
    assert calculate_spark_observed_offset_gap("", "") == 0
    assert (
        calculate_spark_observed_offset_gap(
            '{"topic-a":{"0":100}}',
            "invalid-json",
        )
        == 0
    )



def test_extract_progress_metrics() -> None:
    progress = SimpleNamespace(
        id="query-001",
        runId="run-001",
        name="transport-stream",
        timestamp="2026-11-06T08:00:00.000Z",
        batchId=12,
        batchDuration=1500,
        durationMs={
            "triggerExecution": 1400,
            "latestOffset": 50,
        },
        numInputRows=100,
        inputRowsPerSecond=80.0,
        processedRowsPerSecond=75.0,
        eventTime={
            "watermark": "2026-11-06T07:59:00.000Z",
        },
        sources=[
            SimpleNamespace(
                description="KafkaV2[Subscribe[transport.raw]]",
                endOffset='{"transport.raw":{"0":100}}',
                latestOffset='{"transport.raw":{"0":115}}',
                numInputRows=100,
                inputRowsPerSecond=80.0,
                processedRowsPerSecond=75.0,
            )
        ],
        sink=SimpleNamespace(
            description="ForeachBatchSink",
            numOutputRows=95,
        ),
        stateOperators=[
            SimpleNamespace(
                operatorName="stateStoreSave",
                numRowsTotal=200,
                numRowsUpdated=100,
                numRowsRemoved=5,
                numRowsDroppedByWatermark=2,
                memoryUsedBytes=4096,
            )
        ],
    )

    metrics = extract_progress_metrics(progress)

    assert metrics["query_id"] == "query-001"
    assert metrics["run_id"] == "run-001"
    assert metrics["query_name"] == "transport-stream"
    assert metrics["batch_id"] == 12
    assert metrics["batch_duration_ms"] == 1500
    assert metrics["trigger_execution_ms"] == 1400
    assert metrics["num_input_rows"] == 100
    assert metrics["input_rows_per_second"] == 80.0
    assert metrics["processed_rows_per_second"] == 75.0
    assert metrics["watermark"] == "2026-11-06T07:59:00.000Z"
    assert metrics["num_output_rows"] == 95

    assert metrics["sources"][0]["spark_observed_offset_gap"] == 15
    assert metrics["state_operators"][0]["num_rows_total"] == 200
    assert metrics["state_operators"][0]["memory_used_bytes"] == 4096


def test_extract_progress_metrics_handles_missing_optional_fields() -> None:
    progress = SimpleNamespace(
        id="query-002",
        runId="run-002",
        name=None,
        timestamp="2026-11-06T08:00:00.000Z",
        batchId=0,
        batchDuration=0,
        durationMs={},
        numInputRows=0,
        inputRowsPerSecond=0.0,
        processedRowsPerSecond=0.0,
        eventTime={},
        sources=[],
        sink=SimpleNamespace(numOutputRows=0),
        stateOperators=[],
    )

    metrics = extract_progress_metrics(progress)

    assert metrics["query_id"] == "query-002"
    assert metrics["query_name"] is None
    assert metrics["watermark"] is None
    assert metrics["trigger_execution_ms"] is None
    assert metrics["sources"] == []
    assert metrics["state_operators"] == []
    assert metrics["num_output_rows"] == 0





def test_listener_logs_query_started() -> None:
    from transport_lakehouse.streaming.monitoring import (
        TransportStreamingListener,
    )

    logger = Mock()
    listener = TransportStreamingListener(logger=logger)

    event = SimpleNamespace(
        id="query-001",
        runId="run-001",
        name="transport-stream",
        timestamp="2026-11-06T08:00:00.000Z",
    )

    listener.onQueryStarted(event)

    logger.info.assert_called_once()
    assert "started" in logger.info.call_args.args[0]


def test_listener_logs_progress_metrics() -> None:
    import json

    from transport_lakehouse.streaming.monitoring import (
        TransportStreamingListener,
    )

    logger = Mock()
    listener = TransportStreamingListener(logger=logger)

    progress = SimpleNamespace(
        id="query-001",
        runId="run-001",
        name="transport-stream",
        timestamp="2026-11-06T08:00:00.000Z",
        batchId=3,
        batchDuration=1000,
        durationMs={"triggerExecution": 900},
        numInputRows=20,
        inputRowsPerSecond=20.0,
        processedRowsPerSecond=18.0,
        eventTime={"watermark": "2026-11-06T07:59:00.000Z"},
        sources=[],
        sink=SimpleNamespace(numOutputRows=20),
        stateOperators=[],
    )

    listener.onQueryProgress(SimpleNamespace(progress=progress))

    logger.info.assert_called_once()
    message = logger.info.call_args.args[0]
    payload = json.loads(logger.info.call_args.args[1])

    assert "progress" in message
    assert payload["query_id"] == "query-001"
    assert payload["batch_id"] == 3
    assert payload["num_input_rows"] == 20


def test_listener_logs_query_terminated_with_error() -> None:
    from transport_lakehouse.streaming.monitoring import (
        TransportStreamingListener,
    )

    logger = Mock()
    listener = TransportStreamingListener(logger=logger)

    event = SimpleNamespace(
        id="query-001",
        runId="run-001",
        exception="Kafka connection failed",
        errorClassOnException="KAFKA_CONNECTION_ERROR",
    )

    listener.onQueryTerminated(event)

    logger.error.assert_called_once()

    message = logger.error.call_args.args[0]
    arguments = logger.error.call_args.args[1:]

    assert "terminated with error" in message
    assert "query_id=%s" in message
    assert "run_id=%s" in message
    assert "error_class=%s" in message

    assert arguments == (
        "query-001",
        "run-001",
        "KAFKA_CONNECTION_ERROR",
        "Kafka connection failed",
    )


def test_listener_logs_normal_query_termination() -> None:
    from transport_lakehouse.streaming.monitoring import (
        TransportStreamingListener,
    )

    logger = Mock()
    listener = TransportStreamingListener(logger=logger)

    event = SimpleNamespace(
        id="query-001",
        runId="run-001",
        exception=None,
        errorClassOnException=None,
    )

    listener.onQueryTerminated(event)

    logger.info.assert_called_once()
    assert "terminated" in logger.info.call_args.args[0]

def test_listener_logs_query_idle() -> None:
    from transport_lakehouse.streaming.monitoring import (
        TransportStreamingListener,
    )

    logger = Mock()
    listener = TransportStreamingListener(logger=logger)

    event = SimpleNamespace(
        id="query-001",
        runId="run-001",
    )

    listener.onQueryIdle(event)

    logger.info.assert_called_once()
    assert "idle" in logger.info.call_args.args[0]
    assert logger.info.call_args.args[1:] == (
        "query-001",
        "run-001",
    )
def test_configure_monitoring_logger_does_not_duplicate_handlers() -> None:
    from transport_lakehouse.streaming.monitoring import (
        configure_monitoring_logger,
    )

    logger_name = "transport_lakehouse.test_monitoring_unit"

    logger = configure_monitoring_logger(logger_name)
    first_count = len(logger.handlers)

    logger = configure_monitoring_logger(logger_name)
    second_count = len(logger.handlers)

    assert first_count == 1
    assert second_count == 1
    assert logger.propagate is False