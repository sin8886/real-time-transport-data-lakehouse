
"""Historical GTFS snapshot processing pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from transport_lakehouse.processing.parse_static import parse_static_feed
from transport_lakehouse.processing.silver_dimensions import (
    build_silver_dimensions,
)
from transport_lakehouse.quality.data_quality import (
    run_dimension_quality_checks,
)
from transport_lakehouse.storage.s3 import S3Storage


class SnapshotPipeline:
    """Process a historical GTFS snapshot from S3 Bronze."""

    def __init__(
        self,
        storage: S3Storage | None = None,
        work_dir: str | Path = "data/snapshots",
    ) -> None:
        self.storage = storage or S3Storage()
        self.work_dir = Path(work_dir)

    def run(
        self,
        provider: str,
        ingest_date: str,
    ) -> dict[str, Any]:
        """Download, parse, transform, and validate one snapshot."""
        if provider not in {"at", "metlink"}:
            raise ValueError(f"Unsupported provider: {provider}")

        try:
            parsed_date = date.fromisoformat(ingest_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("ingest_date must use YYYY-MM-DD format") from exc

        if parsed_date.isoformat() != ingest_date:
            raise ValueError("ingest_date must use YYYY-MM-DD format")

        snapshot_dir = (
            self.work_dir
            / provider
            / f"ingest_date={ingest_date}"
        )

        downloaded_files = self.storage.download_snapshot(
            provider=provider,
            ingest_date=ingest_date,
            local_dir=snapshot_dir,
        )

        parsed_tables = parse_static_feed(snapshot_dir)

        silver_tables = build_silver_dimensions(parsed_tables)

        quality_result = run_dimension_quality_checks(silver_tables)

        return {
            "provider": provider,
            "ingest_date": ingest_date,
            "snapshot_dir": str(snapshot_dir),
            "downloaded_files": len(downloaded_files),
            "parsed_records": {
                table_name: len(records)
                for table_name, records in parsed_tables.items()
            },
            "silver_records": {
                table_name: len(records)
                for table_name, records in silver_tables.items()
            },
            "quality": quality_result,
            "passed": quality_result["passed"],
            "silver_tables": silver_tables,
        }
