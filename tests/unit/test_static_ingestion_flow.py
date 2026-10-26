from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from transport_lakehouse.flows.static_ingestion_flow import (
    run_incremental_static_batch,
    serve_daily_static_ingestion,
    static_ingestion_flow,
)


def test_flow_returns_pipeline_result_and_passes_parameters(tmp_path: Path) -> None:
    pipeline = MagicMock()
    pipeline.run.return_value = {"processed": True, "changed": True}

    with patch(
        "transport_lakehouse.flows.static_ingestion_flow.IncrementalBatchPipeline",
        return_value=pipeline,
    ) as pipeline_class:
        result = static_ingestion_flow(
            source_dir=str(tmp_path / "feed"),
            provider="metlink",
            ingest_date="2026-10-26",
            work_dir=str(tmp_path / "snapshots"),
            watermark_path=str(tmp_path / "watermark.json"),
        )

    assert result == {"processed": True, "changed": True}
    pipeline_class.assert_called_once_with(
        source_dir=tmp_path / "feed",
        provider="metlink",
        work_dir=tmp_path / "snapshots",
        watermark_path=tmp_path / "watermark.json",
    )
    pipeline.run.assert_called_once_with(ingest_date="2026-10-26")


def test_task_retries_after_failure_and_then_returns_result(tmp_path: Path) -> None:
    pipeline = MagicMock()
    pipeline.run.side_effect = [
        RuntimeError("temporary failure"),
        {"processed": True},
    ]

    with patch(
        "transport_lakehouse.flows.static_ingestion_flow.IncrementalBatchPipeline",
        return_value=pipeline,
    ):
        result = static_ingestion_flow(
            source_dir=str(tmp_path / "feed"),
            ingest_date="2026-10-26",
        )

    assert result == {"processed": True}
    assert pipeline.run.call_count == 2


def test_task_retries_are_configured_without_flow_retries() -> None:
    assert run_incremental_static_batch.retries == 2
    assert run_incremental_static_batch.retry_delay_seconds == 1
    assert static_ingestion_flow.retries == 0


def test_task_propagates_error_after_retries_are_exhausted(tmp_path: Path) -> None:
    pipeline = MagicMock()
    pipeline.run.side_effect = RuntimeError("permanent failure")

    with (
        patch(
            "transport_lakehouse.flows.static_ingestion_flow.IncrementalBatchPipeline",
            return_value=pipeline,
        ),
        pytest.raises(RuntimeError, match="permanent failure"),
    ):
        static_ingestion_flow(
            source_dir=str(tmp_path / "feed"),
            ingest_date="2026-10-26",
        )

    assert pipeline.run.call_count == 3


def test_flow_does_not_add_audit_writer(tmp_path: Path) -> None:
    pipeline = MagicMock()
    pipeline.run.return_value = {"skipped": True}

    with patch(
        "transport_lakehouse.flows.static_ingestion_flow.IncrementalBatchPipeline",
        return_value=pipeline,
    ) as pipeline_class:
        static_ingestion_flow(source_dir=str(tmp_path / "feed"))

    assert "audit_writer" not in pipeline_class.call_args.kwargs


def test_daily_schedule_uses_local_serve_without_webserver(
    tmp_path: Path,
) -> None:
    with patch.object(static_ingestion_flow, "serve") as serve:
        serve_daily_static_ingestion(
            source_dir=str(tmp_path / "feed"),
            provider="metlink",
            work_dir=str(tmp_path / "snapshots"),
            watermark_path=str(tmp_path / "watermark.json"),
        )

    serve.assert_called_once_with(
        name="daily-static-ingestion",
        cron="0 2 * * *",
        parameters={
            "source_dir": str(tmp_path / "feed"),
            "provider": "metlink",
            "work_dir": str(tmp_path / "snapshots"),
            "watermark_path": str(tmp_path / "watermark.json"),
        },
        webserver=False,
    )
