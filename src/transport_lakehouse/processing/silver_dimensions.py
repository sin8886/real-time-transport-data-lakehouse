from typing import Any


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    cleaned = str(value).strip()
    return cleaned if cleaned else None


def _optional_int(value: Any) -> int | None:
    cleaned = _clean_text(value)
    return int(cleaned) if cleaned is not None else None


def _optional_float(value: Any) -> float | None:
    cleaned = _clean_text(value)
    return float(cleaned) if cleaned is not None else None


def build_silver_routes(
    records: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Build standardized Silver route dimension records."""
    return [
        {
            "route_id": _clean_text(record.get("route_id")),
            "agency_id": _clean_text(record.get("agency_id")),
            "route_short_name": _clean_text(record.get("route_short_name")),
            "route_long_name": _clean_text(record.get("route_long_name")),
            "route_type": _optional_int(record.get("route_type")),
        }
        for record in records
    ]


def build_silver_stops(
    records: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Build standardized Silver stop dimension records."""
    return [
        {
            "stop_id": _clean_text(record.get("stop_id")),
            "stop_name": _clean_text(record.get("stop_name")),
            "latitude": _optional_float(record.get("stop_lat")),
            "longitude": _optional_float(record.get("stop_lon")),
            "zone_id": _clean_text(record.get("zone_id")),
            "wheelchair_boarding": _optional_int(record.get("wheelchair_boarding")),
        }
        for record in records
    ]


def build_silver_trips(
    records: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Build standardized Silver trip dimension records."""
    return [
        {
            "trip_id": _clean_text(record.get("trip_id")),
            "route_id": _clean_text(record.get("route_id")),
            "service_id": _clean_text(record.get("service_id")),
            "trip_headsign": _clean_text(record.get("trip_headsign")),
            "direction_id": _optional_int(record.get("direction_id")),
            "shape_id": _clean_text(record.get("shape_id")),
        }
        for record in records
    ]


def build_silver_dimensions(
    parsed_tables: dict[str, list[dict[str, str]]],
) -> dict[str, list[dict[str, Any]]]:
    """Build the three Silver dimension tables from parsed GTFS records."""
    return {
        "routes": build_silver_routes(parsed_tables["routes"]),
        "stops": build_silver_stops(parsed_tables["stops"]),
        "trips": build_silver_trips(parsed_tables["trips"]),
    }
