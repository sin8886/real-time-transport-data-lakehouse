"""Integration tests for Silver Parquet uploads to real AWS S3."""

import os
from datetime import date
from pathlib import Path
from uuid import uuid4

import boto3
import pytest
from pyspark.sql import SparkSession

from transport_lakehouse.processing.silver_writer import (
    upload_silver_parquet,
    write_silver_parquet,
)
from transport_lakehouse.storage.s3 import S3Storage


class IsolatedS3Storage:
    """Redirect Silver uploads into a unique integration-test prefix."""

    def __init__(self, storage: S3Storage, prefix: str) -> None:
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
            local_path,
            layer,
            isolated_key,
        )

        self.uploaded_keys.append(uploaded_key)
        return uploaded_key


@pytest.mark.integration
def test_silver_parquet_s3_round_trip(tmp_path: Path) -> None:
    """Write, upload, download, and read real Silver Parquet datasets."""
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

    provider = "at"
    ingest_date = date.today().isoformat()
    test_prefix = f"integration-tests/{uuid4().hex}"

    storage = S3Storage(
        bucket_name=bucket_name,
        region_name=region_name,
    )
    isolated_storage = IsolatedS3Storage(storage, test_prefix)

    s3_client = boto3.client("s3", region_name=region_name)

    silver_dimensions = {
        "routes": [
            {
                "route_id": "TEST_R1",
                "agency_id": "TEST_A1",
                "route_short_name": "1",
                "route_long_name": "Integration Test Route",
                "route_type": 3,
            }
        ],
        "stops": [
            {
                "stop_id": "TEST_S1",
                "stop_name": "Integration Test Stop",
                "latitude": -36.8485,
                "longitude": 174.7633,
                "zone_id": None,
                "wheelchair_boarding": 0,
            }
        ],
        "trips": [
            {
                "trip_id": "TEST_T1",
                "route_id": "TEST_R1",
                "service_id": "TEST_SERVICE",
                "trip_headsign": "Integration Test",
                "direction_id": 0,
                "shape_id": "TEST_SHAPE",
            }
        ],
    }

    spark = (
        SparkSession.builder
        .appName("silver-s3-integration-test")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        output_paths = write_silver_parquet(
            silver_dimensions=silver_dimensions,
            output_dir=tmp_path / "silver",
            provider=provider,
            ingest_date=ingest_date,
            spark=spark,
        )

        uploaded_keys = upload_silver_parquet(
            output_paths=output_paths,
            storage=isolated_storage,
            provider=provider,
            ingest_date=ingest_date,
        )

        assert uploaded_keys
        assert set(uploaded_keys) == set(isolated_storage.uploaded_keys)

        for dataset in ("routes", "stops", "trips"):
            expected_prefix = (
                f"silver/{test_prefix}/{provider}/"
                f"ingest_date={ingest_date}/{dataset}/"
            )

            dataset_keys = [
                key for key in uploaded_keys
                if key.startswith(expected_prefix)
            ]

            assert dataset_keys, f"No S3 Parquet files uploaded for {dataset}"

            downloaded_dir = tmp_path / "downloaded" / dataset
            downloaded_dir.mkdir(parents=True)

            for index, object_key in enumerate(dataset_keys):
                response = s3_client.head_object(
                    Bucket=bucket_name,
                    Key=object_key,
                )
                assert response["ContentLength"] > 0

                downloaded_file = downloaded_dir / f"part-{index}.parquet"

                storage.download_file(
                    object_key,
                    downloaded_file,
                )

            dataframe = spark.read.parquet(str(downloaded_dir))

            assert dataframe.count() == 1

    finally:
        try:
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
        finally:
            spark.stop()
