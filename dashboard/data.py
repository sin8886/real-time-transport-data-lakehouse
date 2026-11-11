"""Dashboard query layer for the local PostgreSQL serving database."""

from __future__ import annotations

import os
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import boto3
from psycopg.rows import dict_row


def _connection() -> psycopg.Connection:
    """Create a dashboard database connection from environment variables."""
    database = os.getenv("POSTGRES_DB", "transport_lakehouse")
    user = os.getenv("POSTGRES_USER", "transport")
    password = os.getenv("POSTGRES_PASSWORD", "transport_local_dev")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5433"))
    return psycopg.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
        connect_timeout=5,
        row_factory=dict_row,
    )


def _query(sql: str, params: Sequence[Any] = ()) -> pd.DataFrame:
    """Run a fixed dashboard query and return a dataframe."""
    with _connection() as connection:
        rows = connection.execute(sql, params).fetchall()
    return pd.DataFrame(rows)


def load_overview(provider: str = "at") -> dict[str, Any]:
    """Load the latest batch KPIs and serving freshness."""
    daily = _query(
        """
        SELECT provider, service_date, scheduled_trip_count,
               active_route_count, active_stop_count
        FROM daily_metrics
        WHERE provider = %s
        ORDER BY service_date DESC
        LIMIT 1
        """,
        (provider,),
    )
    latest_run = _query(
        """
        SELECT pipeline_name, provider, status, records_in, records_out,
               completed_at
        FROM pipeline_runs
        WHERE provider = %s
        ORDER BY completed_at DESC
        LIMIT 1
        """,
        (provider,),
    )
    return {"daily": daily, "latest_run": latest_run}


def load_route_metrics(
    provider: str = "at",
    service_date: date | str | None = None,
) -> pd.DataFrame:
    """Load route-level Gold metrics with optional service-date filtering."""
    if service_date is None:
        return _query(
            """
            SELECT service_date, route_id, scheduled_trip_count,
                   stop_event_count, served_stop_count
            FROM route_metrics
            WHERE provider = %s
            ORDER BY stop_event_count DESC
            """,
            (provider,),
        )
    return _query(
        """
        SELECT service_date, route_id, scheduled_trip_count,
               stop_event_count, served_stop_count
        FROM route_metrics
        WHERE provider = %s AND service_date = %s
        ORDER BY stop_event_count DESC
        """,
        (provider, service_date),
    )


def load_stop_metrics(
    provider: str = "at",
    service_date: date | str | None = None,
) -> pd.DataFrame:
    """Load stop-level Gold metrics with optional service-date filtering."""
    if service_date is None:
        return _query(
            """
            SELECT service_date, stop_id, scheduled_trip_count,
                   stop_event_count
            FROM stop_metrics
            WHERE provider = %s
            ORDER BY stop_event_count DESC
            """,
            (provider,),
        )
    return _query(
        """
        SELECT service_date, stop_id, scheduled_trip_count,
               stop_event_count
        FROM stop_metrics
        WHERE provider = %s AND service_date = %s
        ORDER BY stop_event_count DESC
        """,
        (provider, service_date),
    )


def load_pipeline_runs(provider: str = "at") -> pd.DataFrame:
    """Load recent pipeline audit records."""
    return _query(
        """
        SELECT pipeline_name, provider, status, records_in, records_out,
               failed_records, started_at, completed_at, duration_ms
        FROM pipeline_runs
        WHERE provider = %s
        ORDER BY completed_at DESC
        LIMIT 20
        """,
        (provider,),
    )


def load_active_vehicles() -> pd.DataFrame:
    """Load recently observed vehicle positions from realtime serving."""
    return _query(
        """
        SELECT provider, vehicle_id, trip_id, route_id, latitude, longitude,
               speed, bearing, event_ts, ingested_at
        FROM dashboard_active_vehicles
        WHERE event_ts >= CURRENT_TIMESTAMP - INTERVAL '10 minutes'
        ORDER BY event_ts DESC
        """
    )


def load_serving_health() -> pd.DataFrame:
    """Load serving freshness and row-count signals for the dashboard."""
    return _query(
        """
        SELECT COUNT(*)::bigint AS vehicle_rows,
               COUNT(*) FILTER (
                   WHERE event_ts >= CURRENT_TIMESTAMP - INTERVAL '10 minutes'
               )::bigint AS fresh_vehicle_rows,
               MAX(event_ts) AS latest_event_ts,
               MAX(ingested_at) AS latest_ingested_at
        FROM dashboard_active_vehicles
        """
    )


def load_route_delay_trends(
    provider: str = "at",
    window_minutes: int = 60,
) -> pd.DataFrame:
    """Load persisted, observed Trip Update delay windows."""
    if window_minutes not in {5, 15, 60}:
        raise ValueError("window_minutes must be one of 5, 15, or 60")
    return _query(
        """
        SELECT window_start, window_end, route_id, avg_delay_seconds, event_count
        FROM dashboard_route_delay_trends
        WHERE provider = %s
          AND window_start >= CURRENT_TIMESTAMP - make_interval(mins => %s)
        ORDER BY window_start, route_id
        """,
        (provider, window_minutes),
    )


def load_s3_health() -> dict[str, Any]:
    """Read-only S3 lake health probe using the configured AWS identity."""
    bucket = os.getenv("S3_BUCKET_NAME", "sin8886-transport-lakehouse")
    region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
    try:
        client = boto3.client("s3", region_name=region)
        response = client.list_objects_v2(Bucket=bucket, MaxKeys=1000)
        objects = response.get("Contents", [])
        latest = max((item["LastModified"] for item in objects), default=None)
        return {
            "status": "Healthy",
            "bucket": bucket,
            "object_count": len(objects),
            "latest_modified": latest,
            "error": None,
        }
    except Exception as error:
        status = (
            "Not configured"
            if "credentials" in str(error).lower()
            else "Unavailable"
        )
        return {
            "status": status,
            "bucket": bucket,
            "object_count": None,
            "latest_modified": None,
            "error": str(error),
        }


def format_timestamp(value: Any) -> str:
    """Format UTC database timestamps in Auckland local time."""
    if value is None or pd.isna(value):
        return "Not available"
    if isinstance(value, (datetime, date)):
        if isinstance(value, datetime):
            timestamp = value
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=ZoneInfo("UTC"))
            timestamp = timestamp.astimezone(ZoneInfo("Pacific/Auckland"))
            return timestamp.strftime("%Y-%m-%d %H:%M:%S NZST/NZDT")
        return value.isoformat()
    return str(value)
