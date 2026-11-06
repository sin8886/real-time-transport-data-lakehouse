from unittest.mock import Mock

import pytest

from transport_lakehouse.processing.silver_writer import (
    upload_silver_parquet,
    validate_ingest_date,
    write_silver_parquet,
)


@pytest.fixture
def silver_dimensions():
    return {
        "routes": [
            {
                "route_id": "R1",
                "agency_id": "AT",
                "route_short_name": "1",
                "route_long_name": "Route 1",
                "route_type": 3,
            }
        ],
        "stops": [
            {
                "stop_id": "S1",
                "stop_name": "Central",
                "latitude": -36.85,
                "longitude": 174.76,
                "zone_id": None,
                "wheelchair_boarding": 0,
            }
        ],
        "trips": [
            {
                "trip_id": "T1",
                "route_id": "R1",
                "service_id": "WEEKDAY",
                "trip_headsign": "Central",
                "direction_id": 0,
                "shape_id": "SH1",
            }
        ],
    }


def test_validate_ingest_date():
    assert validate_ingest_date("2026-11-06") == "2026-11-06"


@pytest.mark.parametrize("invalid_date", ["2026/11/06", "2026-2-06", "invalid"])
def test_validate_ingest_date_rejects_invalid_values(invalid_date):
    with pytest.raises(ValueError):
        validate_ingest_date(invalid_date)


def test_write_silver_parquet_uses_canonical_schemas(
    tmp_path,
    silver_dimensions,
):
    spark = Mock()
    dataframes = {
        dataset: Mock()
        for dataset in ("routes", "stops", "trips")
    }
    spark.createDataFrame.side_effect = dataframes.values()

    result = write_silver_parquet(
        silver_dimensions=silver_dimensions,
        output_dir=tmp_path,
        provider="at",
        ingest_date="2026-11-06",
        spark=spark,
    )

    assert set(result) == {"routes", "stops", "trips"}

    for dataset, dataframe in dataframes.items():
        dataframe.write.mode.return_value.parquet.assert_called_once_with(
            str(
                tmp_path
                / "at"
                / "ingest_date=2026-11-06"
                / dataset
            )
        )

    schemas = [
        call.kwargs["schema"]
        for call in spark.createDataFrame.call_args_list
    ]
    assert [field.name for field in schemas[0].fields] == [
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
    ]
    assert schemas[0]["route_type"].dataType.simpleString() == "int"
    assert schemas[1]["latitude"].dataType.simpleString() == "double"
    assert schemas[2]["direction_id"].dataType.simpleString() == "int"


def test_write_silver_parquet_rejects_missing_dataset(
    tmp_path,
    silver_dimensions,
):
    silver_dimensions.pop("trips")

    with pytest.raises(ValueError, match="exactly"):
        write_silver_parquet(
            silver_dimensions,
            tmp_path,
            "at",
            "2026-11-06",
            spark=Mock(),
        )


def test_write_silver_parquet_rejects_empty_dataset(
    tmp_path,
    silver_dimensions,
):
    silver_dimensions["routes"] = []

    with pytest.raises(ValueError, match="cannot be empty"):
        write_silver_parquet(
            silver_dimensions,
            tmp_path,
            "at",
            "2026-11-06",
            spark=Mock(),
        )


def test_write_silver_parquet_rejects_invalid_provider(
    tmp_path,
    silver_dimensions,
):
    with pytest.raises(ValueError, match="provider"):
        write_silver_parquet(
            silver_dimensions,
            tmp_path,
            "unknown",
            "2026-11-06",
            spark=Mock(),
        )


def test_upload_silver_parquet_uploads_all_datasets(tmp_path):
    output_paths = {}

    for dataset in ("routes", "stops", "trips"):
        dataset_dir = tmp_path / dataset
        dataset_dir.mkdir()
        (dataset_dir / "part-00000.parquet").write_bytes(b"parquet")
        output_paths[dataset] = str(dataset_dir)

    storage = Mock()
    storage.upload_file.side_effect = (
        lambda local_path, layer, key: f"{layer}/{key}"
    )

    uploaded = upload_silver_parquet(
        output_paths,
        storage,
        provider="at",
        ingest_date="2026-11-06",
    )

    assert len(uploaded) == 3
    assert set(uploaded) == {
        "silver/at/ingest_date=2026-11-06/routes/part-00000.parquet",
        "silver/at/ingest_date=2026-11-06/stops/part-00000.parquet",
        "silver/at/ingest_date=2026-11-06/trips/part-00000.parquet",
    }
    assert storage.upload_file.call_count == 3


def test_upload_silver_parquet_rejects_missing_directory(tmp_path):
    output_paths = {
        dataset: str(tmp_path / dataset)
        for dataset in ("routes", "stops", "trips")
    }

    storage = Mock()

    with pytest.raises(FileNotFoundError):
        upload_silver_parquet(
            output_paths,
            storage,
            provider="at",
            ingest_date="2026-11-06",
        )

    storage.upload_file.assert_not_called()


def test_upload_silver_parquet_rejects_missing_parquet_file(tmp_path):
    output_paths = {}

    for dataset in ("routes", "stops", "trips"):
        dataset_dir = tmp_path / dataset
        dataset_dir.mkdir()
        output_paths[dataset] = str(dataset_dir)

    storage = Mock()

    with pytest.raises(FileNotFoundError, match="No Parquet files"):
        upload_silver_parquet(
            output_paths,
            storage,
            provider="at",
            ingest_date="2026-11-06",
        )


def test_upload_silver_parquet_rejects_invalid_dataset_keys(tmp_path):
    storage = Mock()

    with pytest.raises(ValueError, match="exactly"):
        upload_silver_parquet(
            {"routes": str(tmp_path)},
            storage,
            provider="at",
            ingest_date="2026-11-06",
        )


def test_upload_silver_parquet_uploads_multiple_files(tmp_path):
    output_paths = {}

    for dataset in ("routes", "stops", "trips"):
        dataset_dir = tmp_path / dataset
        dataset_dir.mkdir()

        (dataset_dir / "part-00000.parquet").write_bytes(b"parquet")
        (dataset_dir / "part-00001.parquet").write_bytes(b"parquet")

        output_paths[dataset] = str(dataset_dir)

    storage = Mock()
    storage.upload_file.side_effect = (
        lambda local_path, layer, key: f"{layer}/{key}"
    )

    uploaded = upload_silver_parquet(
        output_paths,
        storage,
        provider="at",
        ingest_date="2026-11-06",
    )

    assert len(uploaded) == 6
    assert storage.upload_file.call_count == 6
    assert all(key.startswith("silver/at/") for key in uploaded)
    assert len(set(uploaded)) == 6


def test_upload_silver_parquet_preserves_nested_directory(tmp_path):
    output_paths = {}

    for dataset in ("routes", "stops", "trips"):
        dataset_dir = tmp_path / dataset
        dataset_dir.mkdir()

        if dataset == "routes":
            nested_dir = dataset_dir / "nested"
            nested_dir.mkdir()
            (nested_dir / "part-00001.parquet").write_bytes(b"parquet")
        else:
            (dataset_dir / "part-00000.parquet").write_bytes(b"parquet")

        output_paths[dataset] = str(dataset_dir)

    storage = Mock()
    storage.upload_file.side_effect = (
        lambda local_path, layer, key: f"{layer}/{key}"
    )

    uploaded = upload_silver_parquet(
        output_paths,
        storage,
        provider="at",
        ingest_date="2026-11-06",
    )

    assert (
        "silver/at/ingest_date=2026-11-06/"
        "routes/nested/part-00001.parquet"
    ) in uploaded


def test_upload_silver_parquet_propagates_upload_error(tmp_path):
    output_paths = {}

    for dataset in ("routes", "stops", "trips"):
        dataset_dir = tmp_path / dataset
        dataset_dir.mkdir()
        (dataset_dir / "part-00000.parquet").write_bytes(b"parquet")
        output_paths[dataset] = str(dataset_dir)

    storage = Mock()
    storage.upload_file.side_effect = RuntimeError("S3 upload failed")

    with pytest.raises(RuntimeError, match="S3 upload failed"):
        upload_silver_parquet(
            output_paths,
            storage,
            provider="at",
            ingest_date="2026-11-06",
        )


def test_upload_silver_parquet_returns_storage_adapter_keys(tmp_path):
    output_paths = {}

    for dataset in ("routes", "stops", "trips"):
        dataset_dir = tmp_path / dataset
        dataset_dir.mkdir()
        (dataset_dir / "part-00000.parquet").write_bytes(b"parquet")
        output_paths[dataset] = str(dataset_dir)

    adapter_returned_keys = []

    def fake_upload(local_path, layer, key):
        uploaded_key = (
            f"{layer}/integration-tests/unique-prefix/{key}"
        )
        adapter_returned_keys.append(uploaded_key)
        return uploaded_key

    storage = Mock()
    storage.upload_file.side_effect = fake_upload

    uploaded = upload_silver_parquet(
        output_paths,
        storage,
        provider="at",
        ingest_date="2026-11-06",
    )

    assert len(uploaded) == 3
    assert uploaded == adapter_returned_keys
    assert storage.upload_file.call_count == 3
def test_write_silver_parquet_rejects_extra_record_field(
    tmp_path,
    silver_dimensions,
):
    silver_dimensions["routes"][0]["unexpected"] = "value"

    spark = Mock()

    with pytest.raises(ValueError, match="canonical fields"):
        write_silver_parquet(
            silver_dimensions,
            tmp_path,
            "at",
            "2026-11-06",
            spark=spark,
        )

    spark.createDataFrame.assert_not_called()


def test_write_silver_parquet_rejects_missing_record_field(
    tmp_path,
    silver_dimensions,
):
    del silver_dimensions["routes"][0]["agency_id"]

    spark = Mock()

    with pytest.raises(ValueError, match="canonical fields"):
        write_silver_parquet(
            silver_dimensions,
            tmp_path,
            "at",
            "2026-11-06",
            spark=spark,
        )

    spark.createDataFrame.assert_not_called()


def test_write_silver_parquet_stops_owned_spark_on_failure(
    tmp_path,
    silver_dimensions,
    monkeypatch,
):
    import transport_lakehouse.processing.silver_writer as module

    spark = Mock()
    dataframe = Mock()

    spark.createDataFrame.return_value = dataframe

    dataframe.write.mode.return_value.parquet.side_effect = (
        RuntimeError("Parquet write failed")
    )

    monkeypatch.setattr(
        module,
        "create_spark_session",
        lambda: spark,
    )

    with pytest.raises(RuntimeError, match="Parquet write failed"):
        module.write_silver_parquet(
            silver_dimensions,
            tmp_path,
            "at",
            "2026-11-06",
        )

    spark.stop.assert_called_once()