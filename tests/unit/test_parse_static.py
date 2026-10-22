import pytest

from transport_lakehouse.processing.parse_static import (
    GTFS_CORE_TABLES,
    parse_static_feed,
)


def test_parse_static_feed_reads_all_core_tables(tmp_path):
    for table_name in GTFS_CORE_TABLES:
        file_path = tmp_path / f"{table_name}.txt"
        file_path.write_text(
            "id,name\n1,test\n2,test2\n",
            encoding="utf-8",
        )

    parsed_tables = parse_static_feed(tmp_path)

    assert set(parsed_tables) == set(GTFS_CORE_TABLES)

    for table_name in GTFS_CORE_TABLES:
        assert len(parsed_tables[table_name]) == 2


def test_parse_static_feed_raises_error_for_missing_table(tmp_path):
    for table_name in GTFS_CORE_TABLES[:-1]:
        file_path = tmp_path / f"{table_name}.txt"
        file_path.write_text(
            "id,name\n1,test\n",
            encoding="utf-8",
        )

    with pytest.raises(FileNotFoundError):
        parse_static_feed(tmp_path)
