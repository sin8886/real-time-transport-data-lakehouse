"""Gold aggregations for realtime transport events."""

from __future__ import annotations

from typing import Any


SUPPORTED_WINDOW_DURATIONS = ("5 minutes", "15 minutes", "60 minutes")
DEFAULT_WATERMARK_DELAY = "5 minutes"


def build_route_delay_gold(
    trip_update_stream: Any,
    window_duration: str,
    watermark_delay: str = DEFAULT_WATERMARK_DELAY,
) -> Any:
    """Aggregate Trip Update delays by route and event-time window.

    The input is expected to be the Trip Update Silver DataFrame.
    Watermarking is applied to event_ts at the Gold aggregation boundary.
    """
    from pyspark.sql.functions import avg, col, count, length, trim, window

    if window_duration not in SUPPORTED_WINDOW_DURATIONS:
        raise ValueError(
            "window_duration must be one of: "
            + ", ".join(SUPPORTED_WINDOW_DURATIONS)
        )

    if not watermark_delay.strip():
        raise ValueError("Watermark delay cannot be empty.")

    required_columns = {"route_id", "event_ts", "delay_seconds"}
    missing_columns = required_columns.difference(trip_update_stream.columns)
    if missing_columns:
        raise ValueError(
            "trip_update_stream is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )

    valid_events = trip_update_stream.filter(
        col("route_id").isNotNull()
        & (length(trim(col("route_id"))) > 0)
        & col("event_ts").isNotNull()
        & col("delay_seconds").isNotNull()
    )

    watermarked_events = valid_events.withWatermark(
        "event_ts",
        watermark_delay,
    )

    return (
        watermarked_events.groupBy(
            window(col("event_ts"), window_duration).alias("window"),
            col("route_id"),
        )
        .agg(
            avg(col("delay_seconds")).alias("avg_delay_seconds"),
            count("*").alias("event_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("route_id"),
            col("avg_delay_seconds"),
            col("event_count"),
        )
    )
