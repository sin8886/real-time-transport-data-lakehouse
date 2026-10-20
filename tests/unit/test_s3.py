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
