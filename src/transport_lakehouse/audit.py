"""PostgreSQL pipeline audit recording."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from transport_lakehouse.database import get_connection


INSERT_PIPELINE_RUN_SQL = """
    INSERT INTO pipeline_runs (
        run_id,
        pipeline_name,
        provider,
        ingest_date,
        started_at,
        completed_at,
        duration_ms,
        status,
        records_in,
        records_out,
        failed_records,
        error_message
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def record_pipeline_run(
    *,
    run_id: UUID,
    pipeline_name: str,
    provider: str | None,
    ingest_date: str | None,
    started_at: datetime,
    completed_at: datetime,
    duration_ms: int,
    status: str,
    records_in: int,
    records_out: int,
    failed_records: int,
    error_message: str | None = None,
) -> None:
    """Insert one audit record using an independent connection and transaction."""
    if status not in {"success", "skipped", "failed"}:
        raise ValueError(f"Unsupported pipeline status: {status}")

    values = (
        run_id,
        pipeline_name,
        provider,
        ingest_date,
        started_at,
        completed_at,
        duration_ms,
        status,
        records_in,
        records_out,
        failed_records,
        error_message,
    )

    with get_connection() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(INSERT_PIPELINE_RUN_SQL, values)


def extract_record_counts(result: dict[str, Any]) -> dict[str, int]:
    """Extract input, output, and failed-record counts from a pipeline result."""
    parsed_records = result.get("parsed_records", {})
    silver_records = result.get("silver_records", {})
    quality = result.get("quality", {})
    tables = quality.get("tables", {})

    records_in = sum(parsed_records.values())
    records_out = sum(silver_records.values())
    failed_records = sum(
        check.get("failed_records", 0)
        for table_checks in tables.values()
        for check in table_checks.values()
    )

    return {
        "records_in": records_in,
        "records_out": records_out,
        "failed_records": failed_records,
    }


class PipelineAuditError(RuntimeError):
    """Raised when an audit record cannot be persisted."""

    def __init__(
        self,
        message: str,
        *,
        audit_error: Exception,
        business_error: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.audit_error = audit_error
        self.business_error = business_error
