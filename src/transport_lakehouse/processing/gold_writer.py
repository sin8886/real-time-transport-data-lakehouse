"""Write Gold metrics to local Parquet files and S3."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
from transport_lakehouse.storage.s3 import S3Storage


def validate_service_date(service_date: str) -> str:
    """Validate and return a YYYY-MM-DD service date."""
    try:
        parsed_date = date.fromisoformat(service_date)
    except (TypeError, ValueError) as exc:
        raise ValueError("service_date must use YYYY-MM-DD format") from exc

    if parsed_date.isoformat() != service_date:
        raise ValueError("service_date must use YYYY-MM-DD format")

    return parsed_date.isoformat()


def write_gold_parquet(
    daily_metrics: Any,
    route_metrics: Any,
    stop_metrics: Any,
    output_dir: str | Path,
    service_date: str,
) -> dict[str, Path]:
    """Write daily, route, and stop metrics to local Parquet directories."""
    snapshot_date = validate_service_date(service_date)
    base_dir = Path(output_dir) / f"service_date={snapshot_date}"

    datasets = {
        "daily_metrics": daily_metrics,
        "route_metrics": route_metrics,
        "stop_metrics": stop_metrics,
    }

    output_paths = {}

    for dataset_name, dataframe in datasets.items():
        dataset_dir = base_dir / dataset_name

        dataframe.write.mode("overwrite").parquet(str(dataset_dir))

        output_paths[dataset_name] = dataset_dir

    return output_paths


def upload_gold_parquet(
    output_paths: dict[str, Path],
    storage: S3Storage,
    provider: str,
    service_date: str,
) -> list[str]:
    """Upload Gold Parquet files to the S3 Gold layer."""
    snapshot_date = validate_service_date(service_date)

    if provider not in {"at", "metlink"}:
        raise ValueError(f"Unsupported provider: {provider}")

    uploaded_keys = []

    for dataset_name, dataset_dir in output_paths.items():
        if dataset_name not in {
            "daily_metrics",
            "route_metrics",
            "stop_metrics",
        }:
            raise ValueError(f"Unsupported Gold dataset: {dataset_name}")

        if not dataset_dir.is_dir():
            raise FileNotFoundError(f"Gold dataset directory not found: {dataset_dir}")

        parquet_files = sorted(dataset_dir.rglob("*.parquet"))

        if not parquet_files:
            raise FileNotFoundError(f"No Parquet files found in: {dataset_dir}")

        for file_path in parquet_files:
            relative_path = file_path.relative_to(dataset_dir)

            object_key = (
                f"{provider}/service_date={snapshot_date}/"
                f"{dataset_name}/{relative_path.as_posix()}"
            )

            uploaded_key = storage.upload_file(
                local_path=file_path,
                layer="gold",
                key=object_key,
            )

            uploaded_keys.append(uploaded_key)

    return uploaded_keys
