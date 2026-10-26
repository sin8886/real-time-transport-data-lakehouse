"""Incremental GTFS Static batch orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from pathlib import Path
from typing import Any, Callable
from uuid import UUID, uuid4

from transport_lakehouse.audit import (
    PipelineAuditError,
    extract_record_counts,
    record_pipeline_run,
)
from transport_lakehouse.ingestion.static_gtfs import StaticGTFSIngestion
from transport_lakehouse.processing.snapshot_pipeline import SnapshotPipeline
from transport_lakehouse.processing.watermark import WatermarkStore


class IncrementalBatchPipeline:
    """Process a GTFS feed only when its content differs from the watermark."""

    def __init__(
        self,
        source_dir: str | Path,
        storage: Any | None = None,
        provider: str = "at",
        work_dir: str | Path = "data/snapshots",
        watermark_path: str | Path = "data/state/static_gtfs_watermarks.json",
        snapshot_pipeline: SnapshotPipeline | None = None,
        audit_writer: Callable[..., None] = record_pipeline_run,
    ) -> None:
        self.ingestion = StaticGTFSIngestion(
            source_dir=source_dir,
            storage=storage,
            provider=provider,
        )
        self.provider = provider
        self.watermarks = WatermarkStore(watermark_path)
        self.snapshot_pipeline = snapshot_pipeline or SnapshotPipeline(
            storage=storage,
            work_dir=work_dir,
        )
        self.audit_writer = audit_writer

    def run(self, ingest_date: str | None = None) -> dict[str, Any]:
        """Detect, process, and record one changed feed."""
        run_id = uuid4()
        started_at = datetime.now(timezone.utc)
        started_clock = perf_counter()
        completed_at = started_at
        snapshot_date: str | None = None
        counts = {"records_in": 0, "records_out": 0, "failed_records": 0}
        status = "failed"
        result: dict[str, Any] | None = None
        response: dict[str, Any] | None = None

        try:
            snapshot_date = self.ingestion._validate_ingest_date(ingest_date)
            fingerprint = self.ingestion.feed_fingerprint()
            previous = self.watermarks.read(self.provider)

            if previous is not None and previous["fingerprint"] == fingerprint:
                status = "skipped"
                response = {
                    "provider": self.provider,
                    "ingest_date": previous["ingest_date"],
                    "fingerprint": fingerprint,
                    "changed": False,
                    "processed": False,
                    "skipped": True,
                }
            else:
                self.ingestion.ingest(snapshot_date)
                result = self.snapshot_pipeline.run(self.provider, snapshot_date)
                counts = extract_record_counts(result)
                if not result.get("passed", False):
                    raise RuntimeError(
                        f"Snapshot processing failed for "
                        f"{self.provider}/{snapshot_date}"
                    )

                watermark = {
                    "fingerprint": fingerprint,
                    "ingest_date": snapshot_date,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }
                self.watermarks.write(self.provider, watermark)
                status = "success"
                response = {
                    "provider": self.provider,
                    "ingest_date": snapshot_date,
                    "fingerprint": fingerprint,
                    "changed": True,
                    "processed": True,
                    "skipped": False,
                    "result": result,
                }
        except Exception as business_error:
            completed_at = datetime.now(timezone.utc)
            self._write_audit(
                run_id=run_id,
                snapshot_date=snapshot_date,
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=self._duration_ms(started_clock),
                status="failed",
                counts=counts,
                error_message=str(business_error),
                business_error=business_error,
            )
            raise

        completed_at = datetime.now(timezone.utc)
        self._write_audit(
            run_id=run_id,
            snapshot_date=snapshot_date,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=self._duration_ms(started_clock),
            status=status,
            counts=counts,
        )
        return response or {}

    @staticmethod
    def _duration_ms(started_clock: float) -> int:
        return max(0, round((perf_counter() - started_clock) * 1000))

    def _write_audit(
        self,
        *,
        run_id: UUID,
        snapshot_date: str | None,
        started_at: datetime,
        completed_at: datetime,
        duration_ms: int,
        status: str,
        counts: dict[str, int],
        error_message: str | None = None,
        business_error: Exception | None = None,
    ) -> None:
        try:
            self.audit_writer(
                run_id=run_id,
                pipeline_name="incremental_static_batch",
                provider=self.provider,
                ingest_date=snapshot_date,
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=duration_ms,
                status=status,
                records_in=counts["records_in"],
                records_out=counts["records_out"],
                failed_records=counts["failed_records"],
                error_message=error_message,
            )
        except Exception as audit_error:
            if business_error is not None:
                raise PipelineAuditError(
                    f"Pipeline failed and audit recording failed: {audit_error}",
                    audit_error=audit_error,
                    business_error=business_error,
                ) from business_error
            raise PipelineAuditError(
                f"Pipeline succeeded but audit recording failed: {audit_error}",
                audit_error=audit_error,
            ) from audit_error
