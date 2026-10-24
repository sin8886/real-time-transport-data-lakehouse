"""Unit tests for the PostgreSQL Serving v1 dimension loader."""

from unittest.mock import MagicMock, patch

import psycopg
import pytest

from transport_lakehouse.serving import (
    UPSERT_ROUTES_SQL,
    UPSERT_STOPS_SQL,
    load_serving_dimensions,
)


@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mocked transactional PostgreSQL connection."""
    connection = MagicMock()
    connection.transaction.return_value.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = (
        connection.cursor.return_value
    )
    return connection


@pytest.fixture
def mock_pipeline() -> MagicMock:
    """Create a pipeline with representative Silver dimensions."""
    pipeline = MagicMock()
    pipeline.run.return_value = {
        "passed": True,
        "silver_tables": {
            "routes": [
                {
                    "route_id": "R1",
                    "agency_id": "A1",
                    "route_short_name": "1",
                    "route_long_name": "Route 1",
                    "route_type": 3,
                }
            ],
            "stops": [
                {
                    "stop_id": "S1",
                    "stop_name": "Station 1",
                    "latitude": -36.85,
                    "longitude": 174.76,
                    "zone_id": "A",
                    "wheelchair_boarding": 1,
                }
            ],
        },
    }
    return pipeline


def test_load_serving_dimensions_maps_fields_and_provider(
    mock_connection: MagicMock,
    mock_pipeline: MagicMock,
) -> None:
    """Map Silver routes and stops with the requested provider."""
    connection_context = MagicMock()
    connection_context.__enter__.return_value = mock_connection
    connection_context.__exit__.return_value = False

    with patch(
        "transport_lakehouse.serving.get_connection",
        return_value=connection_context,
    ):
        result = load_serving_dimensions("at", "2026-10-22", mock_pipeline)

    assert result == {"routes": 1, "stops": 1}
    mock_pipeline.run.assert_called_once_with(
        provider="at",
        ingest_date="2026-10-22",
    )
    cursor = mock_connection.cursor.return_value
    assert cursor.executemany.call_args_list[0].args == (
        UPSERT_ROUTES_SQL,
        [("at", "R1", "A1", "1", "Route 1", 3)],
    )
    assert cursor.executemany.call_args_list[1].args == (
        UPSERT_STOPS_SQL,
        [("at", "S1", "Station 1", -36.85, 174.76, "A", 1)],
    )
    assert "ON CONFLICT (provider, route_id) DO UPDATE" in UPSERT_ROUTES_SQL
    assert "ON CONFLICT (provider, stop_id) DO UPDATE" in UPSERT_STOPS_SQL
    mock_connection.transaction.assert_called_once_with()


def test_load_serving_dimensions_handles_empty_dimensions(
    mock_connection: MagicMock,
) -> None:
    """Return zero counts without executing inserts for empty Silver data."""
    pipeline = MagicMock()
    pipeline.run.return_value = {
        "passed": True,
        "silver_tables": {
            "routes": [],
            "stops": [],
        },
    }
    connection_context = MagicMock()
    connection_context.__enter__.return_value = mock_connection
    connection_context.__exit__.return_value = False

    with patch(
        "transport_lakehouse.serving.get_connection",
        return_value=connection_context,
    ):
        result = load_serving_dimensions("metlink", "2026-10-22", pipeline)

    assert result == {"routes": 0, "stops": 0}
    mock_connection.cursor.return_value.executemany.assert_not_called()


def test_load_serving_dimensions_rejects_failed_quality_checks(
    mock_connection: MagicMock,
) -> None:
    """Reject failed Silver quality checks before opening the database."""
    pipeline = MagicMock()
    pipeline.run.return_value = {
        "passed": False,
        "silver_tables": {
            "routes": [{"route_id": "R1"}],
            "stops": [{"stop_id": "S1"}],
        },
    }

    with (
        patch("transport_lakehouse.serving.get_connection") as mock_get_connection,
        pytest.raises(ValueError, match="Snapshot quality checks failed"),
    ):
        load_serving_dimensions("at", "2026-10-22", pipeline)

    mock_get_connection.assert_not_called()
    mock_connection.cursor.return_value.executemany.assert_not_called()


def test_load_serving_dimensions_rolls_back_database_errors(
    mock_connection: MagicMock,
    mock_pipeline: MagicMock,
) -> None:
    """Propagate database errors and leave transaction rollback to Psycopg."""
    database_error = psycopg.OperationalError("database unavailable")
    mock_connection.cursor.return_value.executemany.side_effect = [None, database_error]
    transaction_context = mock_connection.transaction.return_value
    transaction_context.__exit__.return_value = False
    connection_context = MagicMock()
    connection_context.__enter__.return_value = mock_connection
    connection_context.__exit__.return_value = False

    with (
        patch(
            "transport_lakehouse.serving.get_connection",
            return_value=connection_context,
        ),
        pytest.raises(psycopg.OperationalError),
    ):
        load_serving_dimensions("at", "2026-10-22", mock_pipeline)

    transaction_context.__exit__.assert_called_once()
    assert transaction_context.__exit__.call_args.args[0] is psycopg.OperationalError
