"""Integration tests for real AWS S3 storage."""

import os
from pathlib import Path
from uuid import uuid4

import boto3
import pytest

from transport_lakehouse.storage.s3 import S3Storage


@pytest.mark.integration
def test_s3_upload_download_round_trip(tmp_path: Path) -> None:
    """Upload and download a file using the real AWS S3 service."""
    if os.getenv("RUN_S3_INTEGRATION") != "1":
        pytest.skip("Set RUN_S3_INTEGRATION=1 to run real S3 tests")

    bucket_name = os.getenv(
        "S3_BUCKET_NAME",
        "sin8886-transport-lakehouse",
    )
    region_name = os.getenv(
        "AWS_DEFAULT_REGION",
        "ap-southeast-2",
    )

    storage = S3Storage(
        bucket_name=bucket_name,
        region_name=region_name,
    )

    object_key = f"bronze/integration-tests/{uuid4().hex}.txt"
    source_file = tmp_path / "source.txt"
    downloaded_file = tmp_path / "downloaded" / "result.txt"
    expected_content = "S3 integration test: transport lakehouse"

    source_file.write_text(expected_content, encoding="utf-8")

    try:
        uploaded_key = storage.upload_file(
            source_file,
            "bronze",
            object_key.removeprefix("bronze/"),
        )

        assert uploaded_key == object_key

        storage.download_file(
            uploaded_key,
            downloaded_file,
        )

        assert downloaded_file.read_text(encoding="utf-8") == expected_content

    finally:
        s3_client = boto3.client("s3", region_name=region_name)
        s3_client.delete_object(
            Bucket=bucket_name,
            Key=object_key,
        )
