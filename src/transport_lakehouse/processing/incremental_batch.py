"""Incremental GTFS Static batch orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

    def run(self, ingest_date: str | None = None) -> dict[str, Any]:
        """Detect, process, and record one changed feed."""
        snapshot_date = self.ingestion._validate_ingest_date(ingest_date)
        fingerprint = self.ingestion.feed_fingerprint()
        previous = self.watermarks.read(self.provider)

        if previous is not None and previous["fingerprint"] == fingerprint:
            return {
                "provider": self.provider,
                "ingest_date": previous["ingest_date"],
                "fingerprint": fingerprint,
                "changed": False,
                "processed": False,
                "skipped": True,
            }

        self.ingestion.ingest(snapshot_date)
        result = self.snapshot_pipeline.run(self.provider, snapshot_date)
        if not result.get("passed", False):
            raise RuntimeError(
                f"Snapshot processing failed for {self.provider}/{snapshot_date}"
            )

        watermark = {
            "fingerprint": fingerprint,
            "ingest_date": snapshot_date,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        self.watermarks.write(self.provider, watermark)

        return {
            "provider": self.provider,
            "ingest_date": snapshot_date,
            "fingerprint": fingerprint,
            "changed": True,
            "processed": True,
            "skipped": False,
            "result": result,
        }
