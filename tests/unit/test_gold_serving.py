"""Unit tests for the PostgreSQL Gold metrics loader."""

from datetime import date
from unittest.mock import MagicMock, call, patch

import psycopg
import pytest

from transport_lakehouse.gold_serving import (
    UPSERT_DAILY_METRICS_SQL,
    UPSERT_ROUTE_METRICS_SQL,
    UPSERT_STOP_METRICS_SQL,
    load_gold_metrics,
)


def _row(values: dict[str, object]) -> MagicMock:
    row = MagicMock()
    row.asDict.return_value = values
    return row


@pytest.fixture
def mock_spark(tmp_path):
    """Create a Spark mock and all Gold Parquet file markers."""
    base_dir = tmp_path / "at" / "service_date=2026-10-22"
    for dataset_name, file_names in {
        "daily_metrics": ["part-00000.parquet"],
        "route_metrics": ["part-00000.parquet"],
        "stop_metrics": ["part-00000.parquet", "part-00001.parquet"],
    }.items():
        dataset_dir = base_dir / dataset_name
        dataset_dir.mkdir(parents=True)
        for file_name in file_names:
            (dataset_dir / file_name).touch()

    spark = MagicMock()
    spark.read.parquet.side_effect = [
        MagicMock(
            collect=MagicMock(
                return_value=[
                    _row(
                        {
                            "service_date": "2026-10-22",
                            "scheduled_trip_count": 10,
                            "active_route_count": 2,
                            "active_stop_count": 3,
                        }
                    )
                ]
            )
        ),
        MagicMock(
            collect=MagicMock(
                return_value=[
                    _row(
                        {
                            "service_date": "2026-10-22",
                            "route_id": 101,
                            "scheduled_trip_count": "4",
                            "stop_event_count": 20,
                            "served_stop_count": 3,
                        }
                    )
                ]
            )
        ),
        MagicMock(
            collect=MagicMock(
                return_value=[
                    _row(
                        {
                            "service_date": "2026-10-22",
                            "stop_id": "S1",
                            "scheduled_trip_count": 4,
                            "stop_event_count": 4,
                        }
                    ),
                    _row(
                        {
                            "service_date": "2026-10-22",
                            "stop_id": "S2",
                            "scheduled_trip_count": 2,
                            "stop_event_count": 2,
                        }
                    ),
                ]
            )
        ),
    ]
    return spark, tmp_path


@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mocked transactional PostgreSQL connection."""
    connection = MagicMock()
    connection.transaction.return_value.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = (
        connection.cursor.return_value
    )
    return connection


def test_load_gold_metrics_maps_all_parts_and_upserts(
    mock_spark,
    mock_connection: MagicMock,
) -> None:
    """Read every Parquet part and map Gold fields to Serving rows."""
    spark, gold_root = mock_spark
    connection_context = MagicMock()
    connection_context.__enter__.return_value = mock_connection
    connection_context.__exit__.return_value = False

    with patch(
        "transport_lakehouse.gold_serving.get_connection",
        return_value=connection_context,
    ):
        result = load_gold_metrics("at", "2026-10-22", gold_root, spark)

    assert result == {
        "daily_metrics": 1,
        "route_metrics": 1,
        "stop_metrics": 2,
    }
    assert spark.read.parquet.call_args_list[2].args[-2:] == (
        str(gold_root / "at/service_date=2026-10-22/stop_metrics/part-00000.parquet"),
        str(gold_root / "at/service_date=2026-10-22/stop_metrics/part-00001.parquet"),
    )

    cursor = mock_connection.cursor.return_value
    assert cursor.executemany.call_args_list == [
        call(
            UPSERT_DAILY_METRICS_SQL,
            [("at", date(2026, 10, 22), 10, 2, 3)],
        ),
        call(
            UPSERT_ROUTE_METRICS_SQL,
            [("at", date(2026, 10, 22), "101", 4, 20, 3)],
        ),
        call(
            UPSERT_STOP_METRICS_SQL,
            [
                ("at", date(2026, 10, 22), "S1", 4, 4),
                ("at", date(2026, 10, 22), "S2", 2, 2),
            ],
        ),
    ]


def test_load_gold_metrics_handles_empty_datasets(
    tmp_path,
    mock_connection: MagicMock,
) -> None:
    """Return zero counts without executing inserts for empty datasets."""
    base_dir = tmp_path / "at" / "service_date=2026-10-22"
    spark = MagicMock()
    for dataset_name in ("daily_metrics", "route_metrics", "stop_metrics"):
        dataset_dir = base_dir / dataset_name
        dataset_dir.mkdir(parents=True)
        (dataset_dir / "part-00000.parquet").touch()
        spark.read.parquet.return_value = MagicMock(collect=MagicMock(return_value=[]))

    connection_context = MagicMock()
    connection_context.__enter__.return_value = mock_connection
    connection_context.__exit__.return_value = False
    with patch(
        "transport_lakehouse.gold_serving.get_connection",
        return_value=connection_context,
    ):
        result = load_gold_metrics("at", "2026-10-22", tmp_path, spark)

    assert result == {"daily_metrics": 0, "route_metrics": 0, "stop_metrics": 0}
    mock_connection.cursor.return_value.executemany.assert_not_called()


def test_load_gold_metrics_rolls_back_on_database_error(
    mock_spark,
    mock_connection: MagicMock,
) -> None:
    """Propagate database errors through the transaction context."""
    spark, gold_root = mock_spark
    error = psycopg.OperationalError("database unavailable")
    mock_connection.cursor.return_value.executemany.side_effect = error
    transaction_context = mock_connection.transaction.return_value
    transaction_context.__exit__.return_value = False
    connection_context = MagicMock()
    connection_context.__enter__.return_value = mock_connection
    connection_context.__exit__.return_value = False

    with (
        patch(
            "transport_lakehouse.gold_serving.get_connection",
            return_value=connection_context,
        ),
        pytest.raises(psycopg.OperationalError),
    ):
        load_gold_metrics("at", "2026-10-22", gold_root, spark)

    transaction_context.__exit__.assert_called_once()
    assert transaction_context.__exit__.call_args.args[0] is psycopg.OperationalError


def test_load_gold_metrics_rolls_back_when_stop_metrics_write_fails(
    mock_spark,
    mock_connection: MagicMock,
) -> None:
    """Roll back after daily and route writes succeed but stop write fails."""
    spark, gold_root = mock_spark
    cursor = mock_connection.cursor.return_value
    database_error = psycopg.OperationalError("stop metrics unavailable")
    cursor.executemany.side_effect = [None, None, database_error]
    transaction_context = mock_connection.transaction.return_value
    transaction_context.__exit__.return_value = False
    connection_context = MagicMock()
    connection_context.__enter__.return_value = mock_connection
    connection_context.__exit__.return_value = False

    with (
        patch(
            "transport_lakehouse.gold_serving.get_connection",
            return_value=connection_context,
        ),
        pytest.raises(psycopg.OperationalError),
    ):
        load_gold_metrics("at", "2026-10-22", gold_root, spark)

    assert cursor.executemany.call_args_list == [
        call(
            UPSERT_DAILY_METRICS_SQL,
            [("at", date(2026, 10, 22), 10, 2, 3)],
        ),
        call(
            UPSERT_ROUTE_METRICS_SQL,
            [("at", date(2026, 10, 22), "101", 4, 20, 3)],
        ),
        call(
            UPSERT_STOP_METRICS_SQL,
            [
                ("at", date(2026, 10, 22), "S1", 4, 4),
                ("at", date(2026, 10, 22), "S2", 2, 2),
            ],
        ),
    ]
    transaction_context.__exit__.assert_called_once()
    assert transaction_context.__exit__.call_args.args[0] is psycopg.OperationalError
