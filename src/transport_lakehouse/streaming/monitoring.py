from __future__ import annotations

import json
import logging

def parse_kafka_offsets(
    raw_offsets: str | None,
) -> dict[str, dict[str, int]]:
    """Parse Spark Kafka source offsets into topic/partition mappings."""
    if not isinstance(raw_offsets, str) or not raw_offsets.strip():
        return {}

    try:
        payload = json.loads(raw_offsets)
    except (json.JSONDecodeError, TypeError):
        return {}

    if not isinstance(payload, dict):
        return {}

    parsed: dict[str, dict[str, int]] = {}

    for topic, partitions in payload.items():
        if not isinstance(topic, str) or not isinstance(partitions, dict):
            continue

        valid_partitions: dict[str, int] = {}

        for partition, offset in partitions.items():
            if (
                isinstance(partition, str)
                and isinstance(offset, int)
                and not isinstance(offset, bool)
                and offset >= 0
            ):
                valid_partitions[partition] = offset

        if valid_partitions:
            parsed[topic] = valid_partitions

    return parsed


def calculate_spark_observed_offset_gap(
    end_offset: str | None,
    latest_offset: str | None,
) -> int:
    """Calculate the positive offset gap observed by Spark.

    This is not broker-level Kafka consumer lag. It compares only matching
    topic/partition offsets present in both Spark progress snapshots.
    """
    end_offsets = parse_kafka_offsets(end_offset)
    latest_offsets = parse_kafka_offsets(latest_offset)

    total_gap = 0

    for topic, partitions in end_offsets.items():
        latest_partitions = latest_offsets.get(topic, {})

        for partition, end_value in partitions.items():
            latest_value = latest_partitions.get(partition)

            if latest_value is not None:
                total_gap += max(0, latest_value - end_value)

    return total_gap



def _get_value(obj: object, name: str, default: object = None) -> object:
    """Read an optional attribute without assuming a concrete Spark class."""
    return getattr(obj, name, default)


def _extract_source_metrics(source: object) -> dict[str, object]:
    """Extract metrics for one Spark streaming source."""
    end_offset = _get_value(source, "endOffset")
    latest_offset = _get_value(source, "latestOffset")

    return {
        "description": _get_value(source, "description"),
        "num_input_rows": _get_value(source, "numInputRows", 0),
        "input_rows_per_second": _get_value(
            source,
            "inputRowsPerSecond",
            0.0,
        ),
        "processed_rows_per_second": _get_value(
            source,
            "processedRowsPerSecond",
            0.0,
        ),
        "spark_observed_offset_gap": (
            calculate_spark_observed_offset_gap(
                end_offset if isinstance(end_offset, str) else None,
                latest_offset if isinstance(latest_offset, str) else None,
            )
        ),
    }


def _extract_state_operator_metrics(
    operator: object,
) -> dict[str, object]:
    """Extract state store metrics from one state operator."""
    return {
        "operator_name": _get_value(operator, "operatorName"),
        "num_rows_total": _get_value(operator, "numRowsTotal", 0),
        "num_rows_updated": _get_value(operator, "numRowsUpdated", 0),
        "num_rows_removed": _get_value(operator, "numRowsRemoved", 0),
        "num_rows_dropped_by_watermark": _get_value(
            operator,
            "numRowsDroppedByWatermark",
            0,
        ),
        "memory_used_bytes": _get_value(operator, "memoryUsedBytes", 0),
    }


def extract_progress_metrics(progress: object) -> dict[str, object]:
    """Extract a JSON-friendly monitoring snapshot from Spark progress."""
    duration_ms = _get_value(progress, "durationMs", {})
    if not isinstance(duration_ms, dict):
        duration_ms = {}

    event_time = _get_value(progress, "eventTime", {})
    if not isinstance(event_time, dict):
        event_time = {}

    sources = _get_value(progress, "sources", [])
    if not isinstance(sources, (list, tuple)):
        sources = []

    state_operators = _get_value(progress, "stateOperators", [])
    if not isinstance(state_operators, (list, tuple)):
        state_operators = []

    sink = _get_value(progress, "sink")

    return {
        "query_id": _get_value(progress, "id"),
        "run_id": _get_value(progress, "runId"),
        "query_name": _get_value(progress, "name"),
        "timestamp": _get_value(progress, "timestamp"),
        "batch_id": _get_value(progress, "batchId"),
        "batch_duration_ms": _get_value(progress, "batchDuration"),
        "trigger_execution_ms": duration_ms.get("triggerExecution"),
        "num_input_rows": _get_value(progress, "numInputRows", 0),
        "input_rows_per_second": _get_value(
            progress,
            "inputRowsPerSecond",
            0.0,
        ),
        "processed_rows_per_second": _get_value(
            progress,
            "processedRowsPerSecond",
            0.0,
        ),
        "watermark": event_time.get("watermark"),
        "num_output_rows": _get_value(sink, "numOutputRows", 0),
        "sources": [
            _extract_source_metrics(source)
            for source in sources
        ],
        "state_operators": [
            _extract_state_operator_metrics(operator)
            for operator in state_operators
        ],
    }





try:
    from pyspark.sql.streaming.listener import (
        StreamingQueryListener as _StreamingQueryListener,
    )
except ImportError:

    class _StreamingQueryListener:
        """Fallback base class when PySpark is unavailable."""

        def __init__(self) -> None:
            raise ImportError(
                "PySpark is required to create TransportStreamingListener"
            )


def configure_monitoring_logger(
    name: str = "transport_lakehouse.streaming.monitoring",
    level: int = logging.INFO,
) -> logging.Logger:
    """Configure a dedicated console logger for streaming monitoring."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s"
            )
        )
        logger.addHandler(handler)

    return logger


class TransportStreamingListener(_StreamingQueryListener):
    """Log Spark Structured Streaming lifecycle and progress metrics."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        super().__init__()
        self.logger = logger or configure_monitoring_logger()

    def onQueryStarted(self, event: object) -> None:
        self.logger.info(
            "Streaming query started: query_id=%s run_id=%s name=%s",
            _get_value(event, "id"),
            _get_value(event, "runId"),
            _get_value(event, "name"),
        )

    def onQueryProgress(self, event: object) -> None:
        progress = _get_value(event, "progress")
        if progress is None:
            self.logger.warning(
                "Streaming query progress event has no progress payload"
            )
            return

        metrics = extract_progress_metrics(progress)

        self.logger.info(
            "Streaming query progress: %s",
            json.dumps(metrics, sort_keys=True, default=str),
        )
    def onQueryIdle(self, event: object) -> None:
        self.logger.info(
            "Streaming query idle: query_id=%s run_id=%s",
            _get_value(event, "id"),
            _get_value(event, "runId"),
        )

    def onQueryTerminated(self, event: object) -> None:
        query_id = _get_value(event, "id")
        run_id = _get_value(event, "runId")
        exception = _get_value(event, "exception")
        error_class = _get_value(event, "errorClassOnException")

        if exception is not None:
            self.logger.error(
                (
                    "Streaming query terminated with error: "
                    "query_id=%s run_id=%s error_class=%s exception=%s"
                ),
                query_id,
                run_id,
                error_class,
                exception,
            )
            return

        self.logger.info(
            "Streaming query terminated: query_id=%s run_id=%s",
            query_id,
            run_id,
        )
