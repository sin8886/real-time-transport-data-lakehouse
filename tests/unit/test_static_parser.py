import pytest

from transport_lakehouse.processing.static_parser import parse_gtfs_table


def test_parse_gtfs_table_returns_records(tmp_path):
    file_path = tmp_path / "routes.txt"
    file_path.write_text(
        "route_id,route_short_name,route_type\n101-202,101,3\n102-203,102,2\n",
        encoding="utf-8",
    )

    records = parse_gtfs_table(file_path)

    assert len(records) == 2
    assert records[0]["route_id"] == "101-202"
    assert records[0]["route_short_name"] == "101"
    assert records[0]["route_type"] == "3"


def test_parse_gtfs_table_preserves_empty_fields(tmp_path):
    file_path = tmp_path / "agency.txt"
    file_path.write_text(
        "agency_id,agency_name,agency_email\nAM,AT Metro,\n",
        encoding="utf-8",
    )

    records = parse_gtfs_table(file_path)

    assert records[0]["agency_email"] == ""


def test_parse_gtfs_table_handles_utf8_bom(tmp_path):
    file_path = tmp_path / "stops.txt"
    file_path.write_text(
        "\ufeffstop_id,stop_name\n100,Central Station\n",
        encoding="utf-8",
    )

    records = parse_gtfs_table(file_path)

    assert records[0]["stop_id"] == "100"


def test_parse_gtfs_table_returns_empty_list_for_header_only_file(tmp_path):
    file_path = tmp_path / "trips.txt"
    file_path.write_text(
        "route_id,service_id,trip_id\n",
        encoding="utf-8",
    )

    records = parse_gtfs_table(file_path)

    assert records == []


def test_parse_gtfs_table_raises_error_for_empty_file(tmp_path):
    file_path = tmp_path / "empty.txt"
    file_path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="no header"):
        parse_gtfs_table(file_path)


def test_parse_gtfs_table_raises_error_for_missing_file(tmp_path):
    file_path = tmp_path / "missing.txt"

    with pytest.raises(FileNotFoundError):
        parse_gtfs_table(file_path)


def test_parse_gtfs_table_rejects_directory(tmp_path):
    with pytest.raises(ValueError, match="Expected a file"):
        parse_gtfs_table(tmp_path)
