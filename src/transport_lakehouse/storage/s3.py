"""S3 storage utilities for the transport data lakehouse."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import boto3
from botocore.client import BaseClient


LakeLayer = Literal["bronze", "silver", "gold"]


class S3Storage:
    """Manage files in the project's S3 data lake bucket."""

    VALID_LAYERS = {"bronze", "silver", "gold"}

    def __init__(
        self,
        bucket_name: str | None = None,
        region_name: str | None = None,
        client: BaseClient | None = None,
    ) -> None:
        self.bucket_name = bucket_name or os.getenv(
            "S3_BUCKET_NAME", "sin8886-transport-lakehouse"
        )
        self.region_name = region_name or os.getenv(
            "AWS_DEFAULT_REGION", "ap-southeast-2"
        )
        self.client = client or boto3.client(
            "s3",
            region_name=self.region_name,
        )

    @classmethod
    def build_key(cls, layer: LakeLayer, key: str) -> str:
        """Build an S3 object key under a valid lake layer."""
        if layer not in cls.VALID_LAYERS:
            raise ValueError(f"Invalid lake layer: {layer}")

        clean_key = key.lstrip("/")
        if not clean_key:
            raise ValueError("Object key cannot be empty")

        return f"{layer}/{clean_key}"

    def upload_file(
        self,
        local_path: str | Path,
        layer: LakeLayer,
        key: str,
    ) -> str:
        """Upload a local file and return its S3 object key."""
        path = Path(local_path)

        if not path.is_file():
            raise FileNotFoundError(f"Local file not found: {path}")

        object_key = self.build_key(layer, key)

        self.client.upload_file(
            str(path),
            self.bucket_name,
            object_key,
        )
        return object_key

    def download_file(
        self,
        key: str,
        local_path: str | Path,
    ) -> None:
        """Download an S3 object to a local file."""
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        self.client.download_file(
            self.bucket_name,
            key,
            str(destination),
        )

    def download_snapshot(
        self,
        provider: str,
        ingest_date: str,
        local_dir: str | Path,
    ) -> list[Path]:
        """Download all GTFS files belonging to a historical snapshot."""
        from datetime import date

        if provider not in {"at", "metlink"}:
            raise ValueError(f"Unsupported provider: {provider}")

        try:
            parsed_date = date.fromisoformat(ingest_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("ingest_date must use YYYY-MM-DD format") from exc

        if parsed_date.isoformat() != ingest_date:
            raise ValueError("ingest_date must use YYYY-MM-DD format")

        prefix = f"bronze/{provider}/static/ingest_date={ingest_date}/"

        response = self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
        )

        object_keys = [
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith(".txt")
        ]

        if not object_keys:
            raise FileNotFoundError(f"No GTFS snapshot found: {provider}/{ingest_date}")

        destination_dir = Path(local_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = []

        for object_key in sorted(object_keys):
            filename = object_key.removeprefix(prefix)

            if not filename or "/" in filename or "\\" in filename:
                raise ValueError(f"Invalid snapshot object key: {object_key}")

            destination = destination_dir / filename

            self.download_file(
                key=object_key,
                local_path=destination,
            )

            downloaded_files.append(destination)

        return downloaded_files
