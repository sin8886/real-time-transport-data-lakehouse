from pathlib import Path
from unittest.mock import Mock

import pytest

from transport_lakehouse.processing.gold_writer import (
    upload_gold_parquet,
    validate_service_date,
    write_gold_parquet,
)
from transport_lakehouse.storage.s3 import S3Storage


def test_validate_service_date() -> None:
    assert validate_service_date("2026-10-22") == "2026-10-22"


@pytest.mark.parametrize(
    "service_date",
    ["20261022", "2026-13-22", "2026-2-2"],
)
def test_validate_service_date_rejects_invalid_values(
    service_date: str,
) -> None:
    with pytest.raises(ValueError):
        validate_service_date(service_date)


def test_write_gold_parquet_creates_three_datasets(
    tmp_path: Path,
) -> None:
    daily = Mock()
    route = Mock()
    stop = Mock()

    paths = write_gold_parquet(
        daily_metrics=daily,
        route_metrics=route,
        stop_metrics=stop,
        output_dir=tmp_path,
        service_date="2026-10-22",
    )

    expected_base = tmp_path / "service_date=2026-10-22"

    assert paths == {
        "daily_metrics": expected_base / "daily_metrics",
        "route_metrics": expected_base / "route_metrics",
        "stop_metrics": expected_base / "stop_metrics",
    }

    daily.write.mode.return_value.parquet.assert_called_once_with(
        str(expected_base / "daily_metrics")
    )
    route.write.mode.return_value.parquet.assert_called_once_with(
        str(expected_base / "route_metrics")
    )
    stop.write.mode.return_value.parquet.assert_called_once_with(
        str(expected_base / "stop_metrics")
    )


def test_upload_gold_parquet(
    tmp_path: Path,
) -> None:
    storage = Mock(spec=S3Storage)
    dataset_dir = tmp_path / "daily_metrics"
    dataset_dir.mkdir()

    parquet_file = dataset_dir / "part-00000.parquet"
    parquet_file.write_bytes(b"test parquet data")

    storage.upload_file.return_value = (
        "gold/at/service_date=2026-10-22/daily_metrics/part-00000.parquet"
    )

    uploaded_keys = upload_gold_parquet(
        output_paths={"daily_metrics": dataset_dir},
        storage=storage,
        provider="at",
        service_date="2026-10-22",
    )

    expected_key = "gold/at/service_date=2026-10-22/daily_metrics/part-00000.parquet"

    assert uploaded_keys == [expected_key]
    storage.upload_file.assert_called_once_with(
        local_path=parquet_file,
        layer="gold",
        key=("at/service_date=2026-10-22/daily_metrics/part-00000.parquet"),
    )


def test_upload_gold_parquet_rejects_invalid_provider(
    tmp_path: Path,
) -> None:
    storage = Mock(spec=S3Storage)

    with pytest.raises(ValueError, match="Unsupported provider"):
        upload_gold_parquet(
            output_paths={},
            storage=storage,
            provider="invalid",
            service_date="2026-10-22",
        )


def test_upload_gold_parquet_rejects_unsupported_dataset(
    tmp_path: Path,
) -> None:
    storage = Mock(spec=S3Storage)

    with pytest.raises(ValueError, match="Unsupported Gold dataset"):
        upload_gold_parquet(
            output_paths={"unknown": tmp_path},
            storage=storage,
            provider="at",
            service_date="2026-10-22",
        )


def test_upload_gold_parquet_rejects_missing_directory(
    tmp_path: Path,
) -> None:
    storage = Mock(spec=S3Storage)

    with pytest.raises(FileNotFoundError, match="directory not found"):
        upload_gold_parquet(
            output_paths={"daily_metrics": tmp_path / "missing"},
            storage=storage,
            provider="at",
            service_date="2026-10-22",
        )


def test_upload_gold_parquet_rejects_empty_directory(
    tmp_path: Path,
) -> None:
    storage = Mock(spec=S3Storage)
    dataset_dir = tmp_path / "daily_metrics"
    dataset_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="No Parquet files"):
        upload_gold_parquet(
            output_paths={"daily_metrics": dataset_dir},
            storage=storage,
            provider="at",
            service_date="2026-10-22",
        )
