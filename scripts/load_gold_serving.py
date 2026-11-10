"""Load the Auckland Gold snapshot into PostgreSQL for the dashboard."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from transport_lakehouse.gold_serving import load_gold_metrics  # noqa: E402
from transport_lakehouse.database import get_connection  # noqa: E402
from transport_lakehouse.serving import load_serving_dimensions  # noqa: E402

SCHEMA_PATH = PROJECT_ROOT / "src" / "transport_lakehouse" / "sql" / "serving_v1.sql"


def initialize_schema() -> None:
    """Create the idempotent Serving schema before loading dashboard data."""
    with get_connection() as connection:
        connection.autocommit = True
        connection.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load local Gold Parquet metrics into PostgreSQL."
    )
    parser.add_argument("--provider", default="at", choices=("at", "metlink"))
    parser.add_argument("--service-date", default="2026-10-22")
    parser.add_argument(
        "--ingest-date",
        default="2026-10-22",
        help="Static snapshot date used to refresh route and stop dimensions.",
    )
    parser.add_argument(
        "--source-dir",
        default="data/gtfs/at",
        help="Local GTFS directory used for dimensions; avoids an S3 download.",
    )
    parser.add_argument("--gold-root", default="data/gold")
    args = parser.parse_args()

    initialize_schema()
    dimensions = load_serving_dimensions(
        provider=args.provider,
        ingest_date=args.ingest_date,
        source_dir=args.source_dir,
    )
    counts = load_gold_metrics(
        provider=args.provider,
        service_date=args.service_date,
        gold_root=args.gold_root,
    )
    print(
        f"Loaded {args.provider} Serving data: "
        f"dimensions={dimensions}, metrics={counts}"
    )


if __name__ == "__main__":
    main()
