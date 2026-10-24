"""Load Gold metrics into the PostgreSQL Serving v1 tables."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from transport_lakehouse.database import get_connection
from transport_lakehouse.processing.gold_writer import validate_service_date
from transport_lakehouse.processing.spark_batch import create_spark_session


UPSERT_DAILY_METRICS_SQL = """
    INSERT INTO daily_metrics (
        provider,
        service_date,
        scheduled_trip_count,
        active_route_count,
        active_stop_count
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (provider, service_date) DO UPDATE SET
        scheduled_trip_count = EXCLUDED.scheduled_trip_count,
        active_route_count = EXCLUDED.active_route_count,
        active_stop_count = EXCLUDED.active_stop_count
"""

UPSERT_ROUTE_METRICS_SQL = """
    INSERT INTO route_metrics (
        provider,
        service_date,
        route_id,
        scheduled_trip_count,
        stop_event_count,
        served_stop_count
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (provider, service_date, route_id) DO UPDATE SET
        scheduled_trip_count = EXCLUDED.scheduled_trip_count,
        stop_event_count = EXCLUDED.stop_event_count,
        served_stop_count = EXCLUDED.served_stop_count
"""

UPSERT_STOP_METRICS_SQL = """
    INSERT INTO stop_metrics (
        provider,
        service_date,
        stop_id,
        scheduled_trip_count,
        stop_event_count
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (provider, service_date, stop_id) DO UPDATE SET
        scheduled_trip_count = EXCLUDED.scheduled_trip_count,
        stop_event_count = EXCLUDED.stop_event_count
"""


def _read_gold_rows(
    spark: Any,
    dataset_dir: Path,
) -> list[dict[str, Any]]:
    parquet_files = sorted(dataset_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in: {dataset_dir}")

    dataframe = spark.read.parquet(*(str(path) for path in parquet_files))
    return [row.asDict() for row in dataframe.collect()]


def _as_service_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _daily_values(
    provider: str,
    rows: list[dict[str, Any]],
) -> list[tuple[Any, ...]]:
    return [
        (
            provider,
            _as_service_date(row["service_date"]),
            int(row["scheduled_trip_count"]),
            int(row["active_route_count"]),
            int(row["active_stop_count"]),
        )
        for row in rows
    ]


def _route_values(
    provider: str,
    rows: list[dict[str, Any]],
) -> list[tuple[Any, ...]]:
    return [
        (
            provider,
            _as_service_date(row["service_date"]),
            str(row["route_id"]),
            int(row["scheduled_trip_count"]),
            int(row["stop_event_count"]),
            int(row["served_stop_count"]),
        )
        for row in rows
    ]


def _stop_values(
    provider: str,
    rows: list[dict[str, Any]],
) -> list[tuple[Any, ...]]:
    return [
        (
            provider,
            _as_service_date(row["service_date"]),
            str(row["stop_id"]),
            int(row["scheduled_trip_count"]),
            int(row["stop_event_count"]),
        )
        for row in rows
    ]


def load_gold_metrics(
    provider: str,
    service_date: str,
    gold_root: str | Path = "data/gold",
    spark: Any | None = None,
) -> dict[str, int]:
    """Load all Gold metric datasets for one provider and date."""
    if provider not in {"at", "metlink"}:
        raise ValueError(f"Unsupported provider: {provider}")

    snapshot_date = validate_service_date(service_date)
    base_dir = Path(gold_root) / provider / f"service_date={snapshot_date}"
    spark_session = spark or create_spark_session()
    owns_spark = spark is None

    try:
        daily_rows = _read_gold_rows(spark_session, base_dir / "daily_metrics")
        route_rows = _read_gold_rows(spark_session, base_dir / "route_metrics")
        stop_rows = _read_gold_rows(spark_session, base_dir / "stop_metrics")

        daily_values = _daily_values(provider, daily_rows)
        route_values = _route_values(provider, route_rows)
        stop_values = _stop_values(provider, stop_rows)

        with get_connection() as connection:
            with connection.transaction():
                with connection.cursor() as cursor:
                    if daily_values:
                        cursor.executemany(
                            UPSERT_DAILY_METRICS_SQL,
                            daily_values,
                        )
                    if route_values:
                        cursor.executemany(
                            UPSERT_ROUTE_METRICS_SQL,
                            route_values,
                        )
                    if stop_values:
                        cursor.executemany(
                            UPSERT_STOP_METRICS_SQL,
                            stop_values,
                        )
    finally:
        if owns_spark:
            spark_session.stop()

    return {
        "daily_metrics": len(daily_values),
        "route_metrics": len(route_values),
        "stop_metrics": len(stop_values),
    }
