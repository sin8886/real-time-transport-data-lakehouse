from typing import Any


# Expected Silver dimension schemas.
EXPECTED_SCHEMAS = {
    "routes": {
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
    },
    "stops": {
        "stop_id",
        "stop_name",
        "latitude",
        "longitude",
        "zone_id",
        "wheelchair_boarding",
    },
    "trips": {
        "trip_id",
        "route_id",
        "service_id",
        "trip_headsign",
        "direction_id",
        "shape_id",
    },
}

PRIMARY_KEYS = {
    "routes": "route_id",
    "stops": "stop_id",
    "trips": "trip_id",
}

REQUIRED_FIELDS = {
    "routes": ["route_id"],
    "stops": ["stop_id", "stop_name", "latitude", "longitude"],
    "trips": ["trip_id", "route_id", "service_id"],
}


def _make_result(
    check_name: str,
    errors: list[str],
) -> dict[str, Any]:
    """Create a standardized data quality check result."""
    return {
        "check_name": check_name,
        "passed": len(errors) == 0,
        "failed_records": len(errors),
        "errors": errors,
    }


def check_schema(
    table_name: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Check whether records contain all expected fields."""
    if table_name not in EXPECTED_SCHEMAS:
        return _make_result(
            "schema",
            [f"Unknown table: {table_name}"],
        )

    expected_fields = EXPECTED_SCHEMAS[table_name]
    errors = []

    for index, record in enumerate(records):
        missing_fields = expected_fields - set(record.keys())

        if missing_fields:
            errors.append(f"Record {index}: missing fields {sorted(missing_fields)}")

    return _make_result("schema", errors)


def check_nulls(
    table_name: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Check required fields for null or empty values."""
    if table_name not in REQUIRED_FIELDS:
        return _make_result(
            "null",
            [f"Unknown table: {table_name}"],
        )

    errors = []

    for index, record in enumerate(records):
        for field in REQUIRED_FIELDS[table_name]:
            value = record.get(field)

            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(
                    f"Record {index}: required field '{field}' is null or empty"
                )

    return _make_result("null", errors)


def check_unique(
    table_name: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Check whether the primary key is unique."""
    if table_name not in PRIMARY_KEYS:
        return _make_result(
            "unique",
            [f"Unknown table: {table_name}"],
        )

    primary_key = PRIMARY_KEYS[table_name]
    seen = set()
    errors = []

    for index, record in enumerate(records):
        value = record.get(primary_key)

        if value is None:
            continue

        if value in seen:
            errors.append(f"Record {index}: duplicate {primary_key} '{value}'")
        else:
            seen.add(value)

    return _make_result("unique", errors)


def check_geo(
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Validate latitude and longitude ranges for stop records."""
    errors = []

    for index, record in enumerate(records):
        latitude = record.get("latitude")
        longitude = record.get("longitude")

        if latitude is None or longitude is None:
            continue

        if not -90 <= latitude <= 90:
            errors.append(f"Record {index}: invalid latitude '{latitude}'")

        if not -180 <= longitude <= 180:
            errors.append(f"Record {index}: invalid longitude '{longitude}'")

    return _make_result("geo", errors)


def check_referential_integrity(
    routes: list[dict[str, Any]],
    trips: list[dict[str, Any]],
) -> dict[str, Any]:
    """Check whether every trip references an existing route."""
    route_ids = {
        record.get("route_id")
        for record in routes
        if record.get("route_id") is not None
    }

    errors = []

    for index, trip in enumerate(trips):
        route_id = trip.get("route_id")

        if route_id is not None and route_id not in route_ids:
            errors.append(
                f"Trip record {index}: route_id '{route_id}' does not exist in routes"
            )

    return _make_result("referential", errors)


def run_dimension_quality_checks(
    silver_tables: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Run all DQ v1 checks for Silver dimension tables."""
    results = {}

    for table_name in ("routes", "stops", "trips"):
        records = silver_tables[table_name]

        results[table_name] = {
            "schema": check_schema(table_name, records),
            "null": check_nulls(table_name, records),
            "unique": check_unique(table_name, records),
        }

    results["stops"]["geo"] = check_geo(silver_tables["stops"])

    results["trips"]["referential"] = check_referential_integrity(
        silver_tables["routes"],
        silver_tables["trips"],
    )

    all_checks = [
        check for table_results in results.values() for check in table_results.values()
    ]

    return {
        "passed": all(check["passed"] for check in all_checks),
        "total_checks": len(all_checks),
        "passed_checks": sum(check["passed"] for check in all_checks),
        "failed_checks": sum(not check["passed"] for check in all_checks),
        "tables": results,
    }
