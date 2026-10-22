import csv
from pathlib import Path


def parse_gtfs_table(file_path: Path) -> list[dict[str, str]]:
    """
    Parse a GTFS Static text file into a list of dictionaries.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"GTFS file not found: {file_path}")

    if not file_path.is_file():
        raise ValueError(f"Expected a file, got: {file_path}")

    with file_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError(f"GTFS file has no header: {file_path}")

        return list(reader)
