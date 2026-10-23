from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from transport_lakehouse.ingestion.static_gtfs import StaticGTFSIngestion


def test_ingest_uploads_all_gtfs_files_as_snapshot(tmp_path: Path) -> None:
    source_dir = tmp_path / "at"
    source_dir.mkdir()

    (source_dir / "routes.txt").write_text("route_id,route_short_name\n")
    (source_dir / "stops.txt").write_text("stop_id,stop_name\n")
    (source_dir / "trips.txt").write_text("trip_id,route_id\n")

    mock_storage = MagicMock()
    mock_storage.upload_file.side_effect = lambda local_path, layer, key: (
        f"{layer}/{key.lstrip('/')}"
    )

    ingestion = StaticGTFSIngestion(
        source_dir=source_dir,
        storage=mock_storage,
        provider="at",
    )

    uploaded_keys = ingestion.ingest("2026-10-22")

    assert uploaded_keys == [
        "bronze/at/static/ingest_date=2026-10-22/routes.txt",
        "bronze/at/static/ingest_date=2026-10-22/stops.txt",
        "bronze/at/static/ingest_date=2026-10-22/trips.txt",
    ]

    assert mock_storage.upload_file.call_count == 3
    assert mock_storage.upload_file.call_args_list[0].kwargs == {
        "local_path": source_dir / "routes.txt",
        "layer": "bronze",
        "key": "at/static/ingest_date=2026-10-22/routes.txt",
    }


def test_ingest_accepts_date_object(tmp_path: Path) -> None:
    source_dir = tmp_path / "at"
    source_dir.mkdir()
    (source_dir / "routes.txt").write_text("route_id\n")

    mock_storage = MagicMock()
    mock_storage.upload_file.side_effect = lambda local_path, layer, key: (
        f"{layer}/{key.lstrip('/')}"
    )

    ingestion = StaticGTFSIngestion(
        source_dir=source_dir,
        storage=mock_storage,
    )

    uploaded_keys = ingestion.ingest(date(2026, 10, 22))

    assert uploaded_keys == ["bronze/at/static/ingest_date=2026-10-22/routes.txt"]


def test_ingest_raises_error_when_directory_is_empty(tmp_path: Path) -> None:
    source_dir = tmp_path / "empty"
    source_dir.mkdir()

    ingestion = StaticGTFSIngestion(
        source_dir=source_dir,
        storage=MagicMock(),
    )

    with pytest.raises(FileNotFoundError):
        ingestion.ingest("2026-10-22")


def test_ingestion_rejects_invalid_provider(tmp_path: Path) -> None:
    source_dir = tmp_path / "at"
    source_dir.mkdir()

    with pytest.raises(ValueError, match="Unsupported provider"):
        StaticGTFSIngestion(
            source_dir=source_dir,
            storage=MagicMock(),
            provider="invalid",
        )


def test_ingestion_rejects_missing_directory(tmp_path: Path) -> None:
    source_dir = tmp_path / "missing"

    with pytest.raises(NotADirectoryError):
        StaticGTFSIngestion(
            source_dir=source_dir,
            storage=MagicMock(),
        )


@pytest.mark.parametrize(
    "invalid_date",
    ["2026-2-22", "22-10-2026", "not-a-date"],
)
def test_ingestion_rejects_invalid_date_format(
    tmp_path: Path,
    invalid_date: str,
) -> None:
    source_dir = tmp_path / "at"
    source_dir.mkdir()
    (source_dir / "routes.txt").write_text("route_id\n")

    ingestion = StaticGTFSIngestion(
        source_dir=source_dir,
        storage=MagicMock(),
    )

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        ingestion.ingest(invalid_date)
