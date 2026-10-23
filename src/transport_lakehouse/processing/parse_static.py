from pathlib import Path

from transport_lakehouse.processing.static_parser import parse_gtfs_table

"""Parse the required GTFS Static tables from a provider directory."""
GTFS_CORE_TABLES = (
    "agency",
    "routes",
    "stops",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
)


def parse_static_feed(source_dir: Path) -> dict[str, list[dict[str, str]]]:
    """Parse the five core GTFS Static tables from a provider directory."""
    parsed_tables = {}

    for table_name in GTFS_CORE_TABLES:
        file_path = source_dir / f"{table_name}.txt"
        parsed_tables[table_name] = parse_gtfs_table(file_path)

    return parsed_tables


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    source_dir = project_root / "data" / "gtfs" / "at"

    parsed_tables = parse_static_feed(source_dir)

    print("GTFS Static Parsing Results")
    print("=" * 40)

    for table_name, records in parsed_tables.items():
        print(f"{table_name}: {len(records):,} records")

    print("=" * 40)
    print("Static parsing completed successfully.")


if __name__ == "__main__":
    main()
