import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from transport_lakehouse.processing.incremental_batch import IncrementalBatchPipeline


def build_pipeline(tmp_path: Path) -> tuple[IncrementalBatchPipeline, MagicMock, Path]:
    source_dir = tmp_path / "feed"
    source_dir.mkdir()
    (source_dir / "routes.txt").write_text("route_id\nr1\n", encoding="utf-8")
    (source_dir / "stops.txt").write_text("stop_id\ns1\n", encoding="utf-8")

    storage = MagicMock()
    snapshot_pipeline = MagicMock()
    snapshot_pipeline.run.return_value = {"passed": True}
    watermark_path = tmp_path / "state" / "watermarks.json"
    pipeline = IncrementalBatchPipeline(
        source_dir=source_dir,
        storage=storage,
        snapshot_pipeline=snapshot_pipeline,
        watermark_path=watermark_path,
    )
    return pipeline, snapshot_pipeline, watermark_path


def test_first_run_processes_and_persists_watermark(tmp_path: Path) -> None:
    pipeline, snapshot_pipeline, watermark_path = build_pipeline(tmp_path)

    result = pipeline.run("2026-10-26")

    assert result["changed"] is True
    assert result["processed"] is True
    assert snapshot_pipeline.run.call_args.args == ("at", "2026-10-26")
    assert (
        json.loads(watermark_path.read_text(encoding="utf-8"))["feeds"]["at"][
            "ingest_date"
        ]
        == "2026-10-26"
    )


def test_unchanged_feed_is_skipped(tmp_path: Path) -> None:
    pipeline, snapshot_pipeline, watermark_path = build_pipeline(tmp_path)
    pipeline.run("2026-10-26")
    snapshot_pipeline.reset_mock()

    result = pipeline.run("2026-10-27")

    assert result["skipped"] is True
    snapshot_pipeline.run.assert_not_called()
    assert (
        json.loads(watermark_path.read_text(encoding="utf-8"))["feeds"]["at"][
            "ingest_date"
        ]
        == "2026-10-26"
    )


def test_changed_feed_is_processed_again(tmp_path: Path) -> None:
    pipeline, snapshot_pipeline, _ = build_pipeline(tmp_path)
    pipeline.run("2026-10-26")
    snapshot_pipeline.reset_mock()
    (tmp_path / "feed" / "routes.txt").write_text(
        "route_id\nr1\nr2\n", encoding="utf-8"
    )

    result = pipeline.run("2026-10-27")

    assert result["changed"] is True
    snapshot_pipeline.run.assert_called_once_with("at", "2026-10-27")


def test_processing_failure_does_not_update_watermark(tmp_path: Path) -> None:
    pipeline, snapshot_pipeline, watermark_path = build_pipeline(tmp_path)
    snapshot_pipeline.run.return_value = {"passed": False}

    with pytest.raises(RuntimeError, match="Snapshot processing failed"):
        pipeline.run("2026-10-26")

    assert not watermark_path.exists()


def test_processing_exception_does_not_update_existing_watermark(
    tmp_path: Path,
) -> None:
    pipeline, snapshot_pipeline, watermark_path = build_pipeline(tmp_path)
    pipeline.run("2026-10-26")
    original = watermark_path.read_text(encoding="utf-8")
    snapshot_pipeline.run.side_effect = RuntimeError("processing error")
    (tmp_path / "feed" / "routes.txt").write_text(
        "route_id\nr1\nr2\n", encoding="utf-8"
    )

    with pytest.raises(RuntimeError, match="processing error"):
        pipeline.run("2026-10-27")

    assert watermark_path.read_text(encoding="utf-8") == original


def test_corrupt_watermark_fails_clearly(tmp_path: Path) -> None:
    pipeline, _, watermark_path = build_pipeline(tmp_path)
    watermark_path.parent.mkdir()
    watermark_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid watermark file"):
        pipeline.run("2026-10-26")


@pytest.mark.parametrize(
    "invalid_watermark",
    [
        {
            "fingerprint": "not-a-sha256",
            "ingest_date": "2026-10-26",
            "processed_at": "2026-10-26T20:00:00+00:00",
        },
        {
            "fingerprint": "0" * 64,
            "ingest_date": "2026-2-26",
            "processed_at": "2026-10-26T20:00:00+00:00",
        },
        {
            "fingerprint": "0" * 64,
            "ingest_date": "2026-10-26",
        },
    ],
)
def test_invalid_watermark_schema_fails_clearly(
    tmp_path: Path,
    invalid_watermark: dict[str, str],
) -> None:
    pipeline, _, watermark_path = build_pipeline(tmp_path)
    watermark_path.parent.mkdir()
    watermark_path.write_text(
        json.dumps({"version": 1, "feeds": {"at": invalid_watermark}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid watermark file"):
        pipeline.run("2026-10-26")


def test_invalid_watermark_write_does_not_replace_existing_file(
    tmp_path: Path,
) -> None:
    pipeline, _, watermark_path = build_pipeline(tmp_path)
    pipeline.run("2026-10-26")
    original = watermark_path.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid watermark"):
        pipeline.watermarks.write(
            "at",
            {
                "fingerprint": "invalid",
                "ingest_date": "2026-10-27",
                "processed_at": "2026-10-26T20:00:00+00:00",
            },
        )

    assert watermark_path.read_text(encoding="utf-8") == original


def test_repeated_run_after_success_is_idempotent(tmp_path: Path) -> None:
    pipeline, snapshot_pipeline, _ = build_pipeline(tmp_path)

    first = pipeline.run("2026-10-26")
    second = pipeline.run("2026-10-26")

    assert first["fingerprint"] == second["fingerprint"]
    assert snapshot_pipeline.run.call_count == 1
