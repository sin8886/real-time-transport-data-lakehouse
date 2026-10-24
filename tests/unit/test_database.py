"""Unit tests for PostgreSQL connection utilities."""

from unittest.mock import Mock, patch

import psycopg
import pytest

from transport_lakehouse.database import get_connection


@patch.dict(
    "os.environ",
    {
        "POSTGRES_DB": "test_db",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_password",
    },
    clear=True,
)
@patch("transport_lakehouse.database.psycopg.connect")
def test_get_connection_uses_environment_and_closes_connection(
    mock_connect: Mock,
) -> None:
    """Connect with environment settings and close the connection on exit."""
    connection = Mock()
    mock_connect.return_value = connection

    with get_connection() as result:
        assert result is connection

    mock_connect.assert_called_once_with(
        dbname="test_db",
        user="test_user",
        password="test_password",
        host="localhost",
        port=5433,
    )
    connection.close.assert_called_once_with()


@patch.dict(
    "os.environ",
    {
        "POSTGRES_DB": "test_db",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_password",
        "POSTGRES_PORT": "55432",
    },
    clear=True,
)
@patch("transport_lakehouse.database.psycopg.connect")
def test_get_connection_converts_custom_port_to_integer(
    mock_connect: Mock,
) -> None:
    """Pass a custom PostgreSQL port as an integer."""
    mock_connect.return_value = Mock()

    with get_connection():
        pass

    assert mock_connect.call_args.kwargs["port"] == 55432
    assert isinstance(mock_connect.call_args.kwargs["port"], int)


@patch.dict("os.environ", {}, clear=True)
def test_get_connection_requires_credentials() -> None:
    """Reject missing PostgreSQL environment variables."""
    with pytest.raises(ValueError, match="POSTGRES_DB"):
        with get_connection():
            pass


@patch.dict(
    "os.environ",
    {
        "POSTGRES_DB": "test_db",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_password",
        "POSTGRES_PORT": "not-a-port",
    },
    clear=True,
)
@patch("transport_lakehouse.database.psycopg.connect")
def test_get_connection_rejects_invalid_port(mock_connect: Mock) -> None:
    """Reject an invalid PostgreSQL port before attempting to connect."""
    with pytest.raises(ValueError, match="POSTGRES_PORT must be a valid integer"):
        with get_connection():
            pass

    mock_connect.assert_not_called()


@patch.dict(
    "os.environ",
    {
        "POSTGRES_DB": "test_db",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_password",
    },
    clear=True,
)
@patch("transport_lakehouse.database.psycopg.connect")
def test_get_connection_reports_connection_errors(mock_connect: Mock) -> None:
    """Expose connection failures without leaking credentials."""
    mock_connect.side_effect = psycopg.OperationalError("connection failed")

    with pytest.raises(ConnectionError, match="Unable to connect to PostgreSQL"):
        with get_connection():
            pass
