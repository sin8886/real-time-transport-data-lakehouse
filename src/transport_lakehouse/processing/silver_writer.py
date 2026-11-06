from datetime import date
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from transport_lakehouse.contracts import (
    STATIC_DIMENSION_FIELDS,
    STATIC_DIMENSION_FIELD_TYPES,
)
from transport_lakehouse.processing.spark_batch import create_spark_session


_DATASET_NAMES = ("routes", "stops", "trips")

_SPARK_TYPES = {
    str: StringType,
    int: IntegerType,
    float: DoubleType,
}


def validate_ingest_date(ingest_date: str) -> str:
    """Validate an ISO-8601 ingestion date."""
    try:
        parsed = date.fromisoformat(ingest_date)
    except (TypeError, ValueError) as exc:
        raise ValueError("ingest_date must use YYYY-MM-DD format") from exc

    if parsed.isoformat() != ingest_date:
        raise ValueError("ingest_date must use YYYY-MM-DD format")

    return ingest_date


def _build_dimension_schema(dataset: str) -> StructType:
    if dataset not in STATIC_DIMENSION_FIELDS:
        raise ValueError(f"Unsupported Silver dataset: {dataset}")

    fields = STATIC_DIMENSION_FIELDS[dataset]
    field_types = STATIC_DIMENSION_FIELD_TYPES[dataset]

    return StructType(
        [
            StructField(
                field_name,
                _SPARK_TYPES[field_types[field_name]](),
                True,
            )
            for field_name in fields
        ]
    )


def write_silver_parquet(
    silver_dimensions: dict[str, list[dict[str, Any]]],
    output_dir: str | Path,
    provider: str,
    ingest_date: str,
    spark: SparkSession | None = None,
) -> dict[str, str]:
    """Write canonical Silver dimensions as locally partitioned Parquet."""
    if provider not in {"at", "metlink"}:
        raise ValueError("provider must be 'at' or 'metlink'")

    validate_ingest_date(ingest_date)

    if set(silver_dimensions) != set(_DATASET_NAMES):
        raise ValueError(
            f"silver_dimensions must contain exactly: {', '.join(_DATASET_NAMES)}"
        )

    owns_spark_session = spark is None
    if spark is None:
        spark = create_spark_session()

    base_path = Path(output_dir) / provider / f"ingest_date={ingest_date}"
    output_paths: dict[str, str] = {}

    try:
        for dataset in _DATASET_NAMES:
            records = silver_dimensions[dataset]
            if not records:
                raise ValueError(f"Silver dataset '{dataset}' cannot be empty")

            schema = _build_dimension_schema(dataset)
            expected_fields = set(STATIC_DIMENSION_FIELDS[dataset])

            for record in records:
                if set(record) != expected_fields:
                    raise ValueError(
                        f"Records in '{dataset}' must match its canonical fields"
                    )

            dataframe = spark.createDataFrame(records, schema=schema)
            dataset_path = base_path / dataset

            (
                dataframe.write
                .mode("overwrite")
                .parquet(str(dataset_path))
            )
            output_paths[dataset] = str(dataset_path)

    finally:
        if owns_spark_session:
            spark.stop()

    return output_paths


def upload_silver_parquet(
    output_paths: dict[str, str],
    storage: Any,
    provider: str,
    ingest_date: str,
) -> list[str]:
    """Upload locally written Silver Parquet files to S3."""
    if provider not in {"at", "metlink"}:
        raise ValueError("provider must be 'at' or 'metlink'")

    validate_ingest_date(ingest_date)

    if set(output_paths) != set(_DATASET_NAMES):
        raise ValueError(
            f"output_paths must contain exactly: {', '.join(_DATASET_NAMES)}"
        )

    validated_files: dict[str, list[Path]] = {}

    # Validate every dataset before uploading anything.
    for dataset in _DATASET_NAMES:
        dataset_path = Path(output_paths[dataset])

        if not dataset_path.is_dir():
            raise FileNotFoundError(
                f"Silver Parquet directory does not exist: {dataset_path}"
            )

        parquet_files = sorted(dataset_path.rglob("*.parquet"))

        if not parquet_files:
            raise FileNotFoundError(
                f"No Parquet files found in Silver dataset: {dataset_path}"
            )

        validated_files[dataset] = parquet_files

    uploaded_keys: list[str] = []

    # Upload only after all datasets pass validation.
    for dataset in _DATASET_NAMES:
        dataset_path = Path(output_paths[dataset])

        for parquet_file in validated_files[dataset]:
            relative_path = parquet_file.relative_to(dataset_path).as_posix()
            key = (
                f"{provider}/ingest_date={ingest_date}/"
                f"{dataset}/{relative_path}"
            )

            uploaded_key = storage.upload_file(
                str(parquet_file),
                layer="silver",
                key=key,
            )

            uploaded_keys.append(uploaded_key)

    return uploaded_keys
