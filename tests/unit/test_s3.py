"""Unit tests for S3 storage utilities."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from transport_lakehouse.storage.s3 import S3Storage


@pytest.fixture
def mock_s3_client() -> Mock:
    """Create a mocked S3 client."""
    return Mock()


@pytest.fixture
def storage(mock_s3_client: Mock) -> S3Storage:
    """Create S3Storage with a mocked client."""
    return S3Storage(
        bucket_name="test-bucket",
        region_name="ap-southeast-2",
        client=mock_s3_client,
    )


@pytest.mark.parametrize(
    ("layer", "key", "expected"),
    [
        ("bronze", "gtfs/data.zip", "bronze/gtfs/data.zip"),
        ("silver", "trips/clean.parquet", "silver/trips/clean.parquet"),
        ("gold", "analytics/daily.parquet", "gold/analytics/daily.parquet"),
    ],
)
def test_build_key(layer: str, key: str, expected: str) -> None:
    """Build object keys for all valid lake layers."""
    assert S3Storage.build_key(layer, key) == expected


def test_build_key_rejects_invalid_layer() -> None:
    """Reject unsupported lake layers."""
    with pytest.raises(ValueError, match="Invalid lake layer"):
        S3Storage.build_key("invalid", "data.csv")  # type: ignore[arg-type]


def test_build_key_rejects_empty_key() -> None:
    """Reject empty object keys."""
    with pytest.raises(ValueError, match="Object key cannot be empty"):
        S3Storage.build_key("bronze", "///")


def test_upload_file(storage: S3Storage, mock_s3_client: Mock, tmp_path: Path) -> None:
    """Upload a local file to the expected S3 location."""
    local_file = tmp_path / "sample.csv"
    local_file.write_text("id,value\n1,100\n", encoding="utf-8")

    result = storage.upload_file(
        local_file,
        "bronze",
        "gtfs/sample.csv",
    )

    assert result == "bronze/gtfs/sample.csv"
    mock_s3_client.upload_file.assert_called_once_with(
        str(local_file),
        "test-bucket",
        "bronze/gtfs/sample.csv",
    )


def test_upload_file_not_found(storage: S3Storage, tmp_path: Path) -> None:
    """Raise an error when the local file does not exist."""
    missing_file = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError, match="Local file not found"):
        storage.upload_file(missing_file, "bronze", "missing.csv")


def test_download_file(
    storage: S3Storage,
    mock_s3_client: Mock,
    tmp_path: Path,
) -> None:
    """Download an S3 object to a local destination."""
    destination = tmp_path / "downloads" / "sample.csv"

    storage.download_file(
        "bronze/gtfs/sample.csv",
        destination,
    )

    assert destination.parent.is_dir()
    mock_s3_client.download_file.assert_called_once_with(
        "test-bucket",
        "bronze/gtfs/sample.csv",
        str(destination),
    )

def test_download_snapshot(
    storage: S3Storage,
    mock_s3_client: Mock,
    tmp_path: Path,
) -> None:
    """Download all GTFS files from a specific historical snapshot."""
    mock_s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "bronze/at/static/ingest_date=2026-10-22/routes.txt"},
            {"Key": "bronze/at/static/ingest_date=2026-10-22/stops.txt"},
            {"Key": "bronze/at/static/ingest_date=2026-10-22/trips.txt"},
            {"Key": "bronze/at/static/ingest_date=2026-10-22/metadata.json"},
        ]
    }

    destination = tmp_path / "snapshot"

    downloaded_files = storage.download_snapshot(
        provider="at",
        ingest_date="2026-10-22",
        local_dir=destination,
    )

    assert downloaded_files == [
        destination / "routes.txt",
        destination / "stops.txt",
        destination / "trips.txt",
    ]

    mock_s3_client.list_objects_v2.assert_called_once_with(
        Bucket="test-bucket",
        Prefix="bronze/at/static/ingest_date=2026-10-22/",
    )

    assert mock_s3_client.download_file.call_count == 3


def test_download_snapshot_not_found(
    storage: S3Storage,
    mock_s3_client: Mock,
    tmp_path: Path,
) -> None:
    """Raise an error when the requested snapshot does not exist."""
    mock_s3_client.list_objects_v2.return_value = {}

    with pytest.raises(FileNotFoundError, match="No GTFS snapshot found"):
        storage.download_snapshot(
            provider="at",
            ingest_date="2026-10-22",
            local_dir=tmp_path / "snapshot",
        )

    mock_s3_client.download_file.assert_not_called()


def test_download_snapshot_rejects_invalid_provider(
    storage: S3Storage,
    tmp_path: Path,
) -> None:
    """Reject unsupported GTFS providers."""
    with pytest.raises(ValueError, match="Unsupported provider"):
        storage.download_snapshot(
            provider="invalid",
            ingest_date="2026-10-22",
            local_dir=tmp_path,
        )


@pytest.mark.parametrize(
    "invalid_date",
    ["2026-2-22", "22-10-2026", "not-a-date"],
)
def test_download_snapshot_rejects_invalid_date(
    storage: S3Storage,
    tmp_path: Path,
    invalid_date: str,
) -> None:
    """Reject invalid snapshot date formats."""
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        storage.download_snapshot(
            provider="at",
            ingest_date=invalid_date,
            local_dir=tmp_path,
        )
