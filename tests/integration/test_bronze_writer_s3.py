"""Integration tests for Bronze GTFS snapshots in real AWS S3."""

import hashlib
import os
from pathlib import Path
from uuid import uuid4

import boto3
import pytest

from transport_lakehouse.ingestion.static_gtfs import StaticGTFSIngestion
from transport_lakehouse.storage.s3 import S3Storage


class IsolatedBronzeStorage:
    """Redirect Bronze uploads into a unique S3 test prefix."""

    def __init__(
        self,
        storage: S3Storage,
        prefix: str,
    ) -> None:
        self.storage = storage
        self.prefix = prefix
        self.uploaded_keys: list[str] = []

    def upload_file(
        self,
        local_path: str | Path,
        layer: str,
        key: str,
    ) -> str:
        isolated_key = f"{self.prefix}/{key}"

        uploaded_key = self.storage.upload_file(
            local_path=local_path,
            layer=layer,
            key=isolated_key,
        )

        self.uploaded_keys.append(uploaded_key)
        return uploaded_key


def sha256_file(path: Path) -> str:
    """Calculate a file's SHA-256 checksum."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


@pytest.mark.integration
def test_bronze_gtfs_snapshot_s3_round_trip(
    tmp_path: Path,
) -> None:
    """Upload and restore an isolated Bronze GTFS snapshot."""
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

    provider = "metlink"
    ingest_date = "2099-01-01"
    test_prefix = f"integration-tests/{uuid4().hex}"

    source_dir = tmp_path / "gtfs_source"
    restored_dir = tmp_path / "gtfs_restored"
    source_dir.mkdir()

    test_files = {
        "routes.txt": "route_id,route_short_name\nR_TEST,1\n",
        "stops.txt": "stop_id,stop_name\nS_TEST,Integration Stop\n",
        "trips.txt": (
            "route_id,service_id,trip_id\n"
            "R_TEST,SERVICE_TEST,T_TEST\n"
        ),
    }

    for filename, content in test_files.items():
        (source_dir / filename).write_text(
            content,
            encoding="utf-8",
        )

    storage = S3Storage(
        bucket_name=bucket_name,
        region_name=region_name,
    )
    isolated_storage = IsolatedBronzeStorage(
        storage=storage,
        prefix=test_prefix,
    )

    ingestion = StaticGTFSIngestion(
        source_dir=source_dir,
        storage=isolated_storage,
        provider=provider,
    )

    s3_client = boto3.client("s3", region_name=region_name)

    try:
        uploaded_keys = ingestion.ingest(ingest_date)

        expected_keys = [
            f"bronze/{test_prefix}/"
            f"{provider}/static/ingest_date={ingest_date}/{filename}"
            for filename in sorted(test_files)
        ]

        assert uploaded_keys == expected_keys
        assert set(uploaded_keys) == set(isolated_storage.uploaded_keys)

        for object_key in uploaded_keys:
            response = s3_client.head_object(
                Bucket=bucket_name,
                Key=object_key,
            )
            assert response["ContentLength"] > 0

        # List only this test's isolated objects.
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"bronze/{test_prefix}/",
        )

        isolated_keys = sorted(
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith(".txt")
        )

        assert isolated_keys == sorted(uploaded_keys)

        # Restore files from the isolated S3 prefix.
        restored_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = []

        for object_key in isolated_keys:
            filename = object_key.rsplit("/", 1)[-1]
            destination = restored_dir / filename

            storage.download_file(
                key=object_key,
                local_path=destination,
            )

            downloaded_files.append(destination)

        assert len(downloaded_files) == len(test_files)

        for downloaded_file in downloaded_files:
            original_file = source_dir / downloaded_file.name

            assert original_file.is_file()
            assert sha256_file(downloaded_file) == sha256_file(
                original_file
            )

    finally:
        if isolated_storage.uploaded_keys:
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    "Objects": [
                        {"Key": key}
                        for key in isolated_storage.uploaded_keys
                    ],
                    "Quiet": True,
                },
            )
