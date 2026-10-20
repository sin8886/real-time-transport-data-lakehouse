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
