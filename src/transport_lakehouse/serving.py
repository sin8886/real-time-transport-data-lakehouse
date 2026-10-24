"""Load Silver dimensions into the PostgreSQL Serving v1 tables."""

from __future__ import annotations

from typing import Any

from transport_lakehouse.database import get_connection
from transport_lakehouse.processing.snapshot_pipeline import SnapshotPipeline


UPSERT_ROUTES_SQL = """
    INSERT INTO dim_route (
        provider,
        route_id,
        agency_id,
        route_short_name,
        route_long_name,
        route_type
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (provider, route_id) DO UPDATE SET
        agency_id = EXCLUDED.agency_id,
        route_short_name = EXCLUDED.route_short_name,
        route_long_name = EXCLUDED.route_long_name,
        route_type = EXCLUDED.route_type
"""

UPSERT_STOPS_SQL = """
    INSERT INTO dim_stop (
        provider,
        stop_id,
        stop_name,
        latitude,
        longitude,
        zone_id,
        wheelchair_boarding
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (provider, stop_id) DO UPDATE SET
        stop_name = EXCLUDED.stop_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        zone_id = EXCLUDED.zone_id,
        wheelchair_boarding = EXCLUDED.wheelchair_boarding
"""


def _route_values(
    provider: str,
    records: list[dict[str, Any]],
) -> list[tuple[Any, ...]]:
    return [
        (
            provider,
            record["route_id"],
            record.get("agency_id"),
            record.get("route_short_name"),
            record.get("route_long_name"),
            record.get("route_type"),
        )
        for record in records
    ]


def _stop_values(
    provider: str,
    records: list[dict[str, Any]],
) -> list[tuple[Any, ...]]:
    return [
        (
            provider,
            record["stop_id"],
            record.get("stop_name"),
            record.get("latitude"),
            record.get("longitude"),
            record.get("zone_id"),
            record.get("wheelchair_boarding"),
        )
        for record in records
    ]


def load_serving_dimensions(
    provider: str,
    ingest_date: str,
    pipeline: SnapshotPipeline | None = None,
) -> dict[str, int]:
    """Load Silver routes and stops into PostgreSQL Serving v1."""
    snapshot_pipeline = pipeline or SnapshotPipeline()
    result = snapshot_pipeline.run(provider=provider, ingest_date=ingest_date)
    if not result["passed"]:
        raise ValueError("Snapshot quality checks failed")

    route_values = _route_values(provider, result["silver_tables"]["routes"])
    stop_values = _stop_values(provider, result["silver_tables"]["stops"])

    with get_connection() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                if route_values:
                    cursor.executemany(UPSERT_ROUTES_SQL, route_values)
                if stop_values:
                    cursor.executemany(UPSERT_STOPS_SQL, stop_values)

    return {
        "routes": len(route_values),
        "stops": len(stop_values),
    }
