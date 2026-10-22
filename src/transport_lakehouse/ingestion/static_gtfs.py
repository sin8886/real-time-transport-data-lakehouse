"""Ingestion utilities for static GTFS feeds."""

from __future__ import annotations

from pathlib import Path

from transport_lakehouse.storage.s3 import S3Storage


class StaticGTFSIngestion:
    """Upload static GTFS files to the Bronze layer."""

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

    def ingest(self) -> list[str]:
        """Upload all GTFS text files to the Bronze layer."""
        gtfs_files = sorted(self.source_dir.glob("*.txt"))

        if not gtfs_files:
            raise FileNotFoundError(f"No GTFS text files found in: {self.source_dir}")

        uploaded_keys = []

        for file_path in gtfs_files:
            object_key = f"{self.provider}/static/{file_path.name}"

            uploaded_key = self.storage.upload_file(
                local_path=file_path,
                layer="bronze",
                key=object_key,
            )

            uploaded_keys.append(uploaded_key)

        return uploaded_keys
