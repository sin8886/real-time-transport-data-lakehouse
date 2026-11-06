"""Integration tests for Gold Parquet uploads to real AWS S3."""

import os
from datetime import date
from pathlib import Path
from uuid import uuid4

import boto3
import pytest
from pyspark.sql import SparkSession

from transport_lakehouse.processing.gold_writer import (
    upload_gold_parquet,
    write_gold_parquet,
)
from transport_lakehouse.storage.s3 import S3Storage


class IsolatedS3Storage:
    """Redirect Gold uploads into a unique integration-test prefix."""

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
            local_path=local_path,
            layer=layer,
            key=isolated_key,
        )

        self.uploaded_keys.append(uploaded_key)
        return uploaded_key


@pytest.mark.integration
def test_gold_parquet_s3_round_trip(tmp_path: Path) -> None:
    """Write, upload, download, and read real Gold Parquet datasets."""
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
    service_date = date.today().isoformat()
    test_prefix = f"integration-tests/{uuid4().hex}"

    storage = S3Storage(
        bucket_name=bucket_name,
        region_name=region_name,
    )
    isolated_storage = IsolatedS3Storage(storage, test_prefix)

    s3_client = boto3.client("s3", region_name=region_name)

    spark = (
        SparkSession.builder
        .appName("gold-s3-integration-test")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    daily_metrics = spark.createDataFrame(
        [
            (service_date, 12, 180.0),
        ],
        ["service_date", "total_trips", "average_delay_seconds"],
    )

    route_metrics = spark.createDataFrame(
        [
            ("TEST_R1", 8, 210.0),
        ],
        ["route_id", "trip_count", "average_delay_seconds"],
    )

    stop_metrics = spark.createDataFrame(
        [
            ("TEST_S1", 15, 120.0),
        ],
        ["stop_id", "arrival_count", "average_delay_seconds"],
    )

    try:
        output_paths = write_gold_parquet(
            daily_metrics=daily_metrics,
            route_metrics=route_metrics,
            stop_metrics=stop_metrics,
            output_dir=tmp_path / "gold",
            service_date=service_date,
        )

        uploaded_keys = upload_gold_parquet(
            output_paths=output_paths,
            storage=isolated_storage,
            provider=provider,
            service_date=service_date,
        )

        assert uploaded_keys
        assert set(uploaded_keys) == set(isolated_storage.uploaded_keys)

        for dataset in (
            "daily_metrics",
            "route_metrics",
            "stop_metrics",
        ):
            expected_prefix = (
                f"gold/{test_prefix}/{provider}/"
                f"service_date={service_date}/{dataset}/"
            )

            dataset_keys = [
                key for key in uploaded_keys
                if key.startswith(expected_prefix)
            ]

            assert dataset_keys, (
                f"No S3 Parquet files uploaded for {dataset}"
            )

            downloaded_dir = tmp_path / "downloaded" / dataset
            downloaded_dir.mkdir(parents=True)

            for index, object_key in enumerate(dataset_keys):
                response = s3_client.head_object(
                    Bucket=bucket_name,
                    Key=object_key,
                )
                assert response["ContentLength"] > 0

                downloaded_file = (
                    downloaded_dir / f"part-{index}.parquet"
                )

                storage.download_file(
                    object_key,
                    downloaded_file,
                )

            dataframe = spark.read.parquet(str(downloaded_dir))

            assert dataframe.count() == 1, (
                f"Unexpected row count for {dataset}"
            )

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
