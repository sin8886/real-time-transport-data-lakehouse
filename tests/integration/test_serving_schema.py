"""Integration tests for the PostgreSQL serving schema."""

from pathlib import Path

import psycopg
import pytest

from transport_lakehouse.database import get_connection


SCHEMA_PATH = (
    Path(__file__).parents[2] / "src" / "transport_lakehouse" / "sql" / "serving_v1.sql"
)
TABLE_NAMES = {
    "dim_route",
    "dim_stop",
    "daily_metrics",
    "route_metrics",
    "stop_metrics",
}


@pytest.fixture
def postgres_connection():
    """Provide an autocommit connection, skipping when PostgreSQL is unavailable."""
    try:
        with get_connection() as connection:
            connection.autocommit = True
            yield connection
    except (ConnectionError, ValueError) as exc:
        pytest.skip(f"PostgreSQL integration service unavailable: {exc}")


def initialize_schema(connection: psycopg.Connection) -> None:
    """Create the serving schema from the checked-in DDL."""
    connection.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


@pytest.mark.integration
def test_serving_schema_is_idempotent_and_has_expected_constraints(
    postgres_connection: psycopg.Connection,
) -> None:
    """Create the schema twice and verify its keys and foreign keys."""
    initialize_schema(postgres_connection)
    initialize_schema(postgres_connection)

    tables = postgres_connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = ANY(%s)
        """,
        (list(TABLE_NAMES),),
    ).fetchall()
    assert {row[0] for row in tables} == TABLE_NAMES

    constraints = postgres_connection.execute(
        """
        SELECT conrelid::regclass::text, contype, pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE connamespace = 'public'::regnamespace
          AND conrelid::regclass::text = ANY(%s)
        """,
        (list(TABLE_NAMES),),
    ).fetchall()

    primary_keys = {row[0] for row in constraints if row[1] == "p"}
    assert primary_keys == TABLE_NAMES

    foreign_keys = {
        (row[0], row[2].replace(" ", "")) for row in constraints if row[1] == "f"
    }
    assert (
        "route_metrics",
        "FOREIGNKEY(provider,route_id)REFERENCESdim_route(provider,route_id)",
    ) in foreign_keys
    assert (
        "stop_metrics",
        "FOREIGNKEY(provider,stop_id)REFERENCESdim_stop(provider,stop_id)",
    ) in foreign_keys


@pytest.mark.integration
@pytest.mark.parametrize(
    ("table_name", "columns", "values"),
    [
        (
            "daily_metrics",
            "provider, service_date, scheduled_trip_count, active_route_count, active_stop_count",
            "'schema_test', '2026-10-24', -1, 0, 0",
        ),
        (
            "daily_metrics",
            "provider, service_date, scheduled_trip_count, active_route_count, active_stop_count",
            "'schema_test', '2026-10-24', 0, -1, 0",
        ),
        (
            "daily_metrics",
            "provider, service_date, scheduled_trip_count, active_route_count, active_stop_count",
            "'schema_test', '2026-10-24', 0, 0, -1",
        ),
        (
            "route_metrics",
            "provider, service_date, route_id, scheduled_trip_count, stop_event_count, served_stop_count",
            "'schema_test', '2026-10-24', 'route-1', -1, 0, 0",
        ),
        (
            "route_metrics",
            "provider, service_date, route_id, scheduled_trip_count, stop_event_count, served_stop_count",
            "'schema_test', '2026-10-24', 'route-1', 0, -1, 0",
        ),
        (
            "route_metrics",
            "provider, service_date, route_id, scheduled_trip_count, stop_event_count, served_stop_count",
            "'schema_test', '2026-10-24', 'route-1', 0, 0, -1",
        ),
        (
            "stop_metrics",
            "provider, service_date, stop_id, scheduled_trip_count, stop_event_count",
            "'schema_test', '2026-10-24', 'stop-1', -1, 0",
        ),
        (
            "stop_metrics",
            "provider, service_date, stop_id, scheduled_trip_count, stop_event_count",
            "'schema_test', '2026-10-24', 'stop-1', 0, -1",
        ),
    ],
)
def test_metric_count_constraints_reject_negative_values(
    postgres_connection: psycopg.Connection,
    table_name: str,
    columns: str,
    values: str,
) -> None:
    """Reject negative values in every metric count column family."""
    initialize_schema(postgres_connection)

    if table_name == "route_metrics":
        postgres_connection.execute(
            """
            INSERT INTO dim_route (provider, route_id)
            VALUES ('schema_test', 'route-1')
            ON CONFLICT DO NOTHING
            """
        )
    elif table_name == "stop_metrics":
        postgres_connection.execute(
            """
            INSERT INTO dim_stop (provider, stop_id)
            VALUES ('schema_test', 'stop-1')
            ON CONFLICT DO NOTHING
            """
        )

    with pytest.raises(psycopg.errors.CheckViolation):
        postgres_connection.execute(
            f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        )
