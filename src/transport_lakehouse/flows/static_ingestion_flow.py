"""Prefect orchestration for the incremental Static GTFS pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from prefect import flow, task

from transport_lakehouse.processing.incremental_batch import (
    IncrementalBatchPipeline,
)


@task(
    name="run-incremental-static-batch",
    retries=2,
    retry_delay_seconds=1,
)
def run_incremental_static_batch(
    source_dir: str,
    provider: str = "at",
    ingest_date: str | None = None,
    work_dir: str = "data/snapshots",
    watermark_path: str = "data/state/static_gtfs_watermarks.json",
) -> dict[str, Any]:
    """Run the existing incremental pipeline as a retryable Prefect task."""
    pipeline = IncrementalBatchPipeline(
        source_dir=Path(source_dir),
        provider=provider,
        work_dir=Path(work_dir),
        watermark_path=Path(watermark_path),
    )
    return pipeline.run(ingest_date=ingest_date)


@flow(name="static-ingestion-flow")
def static_ingestion_flow(
    source_dir: str,
    provider: str = "at",
    ingest_date: str | None = None,
    work_dir: str = "data/snapshots",
    watermark_path: str = "data/state/static_gtfs_watermarks.json",
) -> dict[str, Any]:
    """Orchestrate one parameterized incremental Static GTFS run."""
    return run_incremental_static_batch(
        source_dir=source_dir,
        provider=provider,
        ingest_date=ingest_date,
        work_dir=work_dir,
        watermark_path=watermark_path,
    )


def serve_daily_static_ingestion(
    *,
    source_dir: str,
    provider: str = "at",
    work_dir: str = "data/snapshots",
    watermark_path: str = "data/state/static_gtfs_watermarks.json",
) -> None:
    """Serve the flow with the local daily schedule."""
    static_ingestion_flow.serve(
        name="daily-static-ingestion",
        cron="0 2 * * *",
        parameters={
            "source_dir": source_dir,
            "provider": provider,
            "work_dir": work_dir,
            "watermark_path": watermark_path,
        },
        webserver=False,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run or serve the incremental Static GTFS Prefect flow."
    )
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--provider", default="at", choices=("at", "metlink"))
    parser.add_argument("--ingest-date")
    parser.add_argument("--work-dir", default="data/snapshots")
    parser.add_argument(
        "--watermark-path",
        default="data/state/static_gtfs_watermarks.json",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Serve a local daily schedule instead of running immediately.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = _parse_args()
    if arguments.serve:
        serve_daily_static_ingestion(
            source_dir=arguments.source_dir,
            provider=arguments.provider,
            work_dir=arguments.work_dir,
            watermark_path=arguments.watermark_path,
        )
    else:
        static_ingestion_flow(
            source_dir=arguments.source_dir,
            provider=arguments.provider,
            ingest_date=arguments.ingest_date,
            work_dir=arguments.work_dir,
            watermark_path=arguments.watermark_path,
        )
