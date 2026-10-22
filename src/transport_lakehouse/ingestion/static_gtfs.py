
"""Ingestion utilities for static GTFS feeds."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from transport_lakehouse.storage.s3 import S3Storage


class StaticGTFSIngestion:
    """Upload static GTFS files as date-partitioned Bronze snapshots."""

    def __init__(
        self,
        source_dir: str | Path,
        storage: S3Storage | None = None,
        provider: str = "at",
    ) -> None:
        self.source_dir = Path(source_dir)
        self.storage = storage or S3Storage()
        self.provider = provider

        if provider not in {"at", "metlink"}:
            raise ValueError(f"Unsupported provider: {provider}")

        if not self.source_dir.is_dir():
            raise NotADirectoryError(
                f"GTFS source directory not found: {self.source_dir}"
            )

    @staticmethod
    def _validate_ingest_date(ingest_date: date | str | None) -> str:
        """Return ingest date as an ISO-8601 date string."""
        if ingest_date is None:
            return date.today().isoformat()

        if isinstance(ingest_date, date):
            return ingest_date.isoformat()

        if isinstance(ingest_date, str):
            try:
                parsed_date = date.fromisoformat(ingest_date)
            except ValueError as exc:
                raise ValueError(
                    "ingest_date must use YYYY-MM-DD format"
                ) from exc

            if parsed_date.isoformat() != ingest_date:
                raise ValueError("ingest_date must use YYYY-MM-DD format")

            return parsed_date.isoformat()

        raise TypeError("ingest_date must be a date, string, or None")

    def ingest(self, ingest_date: date | str | None = None) -> list[str]:
        """Upload GTFS text files into a date-partitioned Bronze snapshot."""
        snapshot_date = self._validate_ingest_date(ingest_date)

        gtfs_files = sorted(self.source_dir.glob("*.txt"))

        if not gtfs_files:
            raise FileNotFoundError(
                f"No GTFS text files found in: {self.source_dir}"
            )

        uploaded_keys = []

        for file_path in gtfs_files:
            object_key = (
                f"{self.provider}/static/"
                f"ingest_date={snapshot_date}/{file_path.name}"
            )

            uploaded_key = self.storage.upload_file(
                local_path=file_path,
                layer="bronze",
                key=object_key,
            )

            uploaded_keys.append(uploaded_key)

        return uploaded_keys
