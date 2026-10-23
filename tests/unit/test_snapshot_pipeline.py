from pathlib import Path
from unittest.mock import MagicMock

import pytest

from transport_lakehouse.processing.snapshot_pipeline import SnapshotPipeline


def test_snapshot_pipeline_processes_historical_snapshot(
    tmp_path: Path,
) -> None:
    snapshot_dir = tmp_path / "at" / "ingest_date=2026-10-22"
    snapshot_dir.mkdir(parents=True)

    table_names = (
        "agency",
        "routes",
        "stops",
        "trips",
        "stop_times",
        "calendar",
        "calendar_dates",
    )

    for table_name in table_names:
        (snapshot_dir / f"{table_name}.txt").write_text(
            "id\n",
            encoding="utf-8",
        )

    mock_storage = MagicMock()

    mock_storage.download_snapshot.return_value = [
        snapshot_dir / f"{table_name}.txt" for table_name in table_names
    ]

    pipeline = SnapshotPipeline(
        storage=mock_storage,
        work_dir=tmp_path,
    )

    result = pipeline.run(
        provider="at",
        ingest_date="2026-10-22",
    )

    assert result["provider"] == "at"
    assert result["ingest_date"] == "2026-10-22"
    assert result["downloaded_files"] == 7
    assert result["passed"] is True

    assert result["quality"]["total_checks"] == 11
    assert result["quality"]["passed_checks"] == 11
    assert result["quality"]["failed_checks"] == 0

    mock_storage.download_snapshot.assert_called_once()


def test_snapshot_pipeline_rejects_invalid_provider(
    tmp_path: Path,
) -> None:
    pipeline = SnapshotPipeline(
        storage=MagicMock(),
        work_dir=tmp_path,
    )

    with pytest.raises(ValueError, match="Unsupported provider"):
        pipeline.run("invalid", "2026-10-22")


def test_snapshot_pipeline_rejects_invalid_date(
    tmp_path: Path,
) -> None:
    pipeline = SnapshotPipeline(
        storage=MagicMock(),
        work_dir=tmp_path,
    )

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        pipeline.run("at", "2026-2-22")
