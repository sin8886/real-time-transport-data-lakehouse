from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from transport_lakehouse.audit import (
    INSERT_PIPELINE_RUN_SQL,
    extract_record_counts,
    record_pipeline_run,
)


def test_extract_record_counts_uses_records_and_failed_records() -> None:
    result = {
        "parsed_records": {"routes": 3, "stops": 4},
        "silver_records": {"routes": 2, "stops": 4},
        "quality": {
            "failed_checks": 99,
            "tables": {
                "routes": {
                    "schema": {"failed_records": 2},
                    "unique": {"failed_records": 1},
                },
                "stops": {"null": {"failed_records": 3}},
            },
        },
    }

    assert extract_record_counts(result) == {
        "records_in": 7,
        "records_out": 6,
        "failed_records": 6,
    }


def test_record_pipeline_run_uses_independent_transaction() -> None:
    connection = MagicMock()
    connection_context = MagicMock()
    connection_context.__enter__.return_value = connection
    connection_context.__exit__.return_value = False
    connection.transaction.return_value.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    run_id = uuid4()
    started_at = datetime(2026, 10, 26, tzinfo=timezone.utc)

    with patch(
        "transport_lakehouse.audit.get_connection",
        return_value=connection_context,
    ):
        record_pipeline_run(
            run_id=run_id,
            pipeline_name="incremental_static_batch",
            provider="at",
            ingest_date="2026-10-26",
            started_at=started_at,
            completed_at=started_at,
            duration_ms=10,
            status="success",
            records_in=7,
            records_out=6,
            failed_records=0,
        )

    connection.transaction.assert_called_once_with()
    cursor.execute.assert_called_once()
    assert cursor.execute.call_args.args == (
        INSERT_PIPELINE_RUN_SQL,
        (
            run_id,
            "incremental_static_batch",
            "at",
            "2026-10-26",
            started_at,
            started_at,
            10,
            "success",
            7,
            6,
            0,
            None,
        ),
    )


def test_record_pipeline_run_rejects_unknown_status() -> None:
    with pytest.raises(ValueError, match="Unsupported pipeline status"):
        record_pipeline_run(
            run_id=uuid4(),
            pipeline_name="incremental_static_batch",
            provider="at",
            ingest_date=None,
            started_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
            duration_ms=0,
            status="running",
            records_in=0,
            records_out=0,
            failed_records=0,
        )
