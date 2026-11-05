"""Canonical Silver transformations for GTFS Static dimensions."""

from __future__ import annotations

from typing import Any

from transport_lakehouse.contracts import (
    STATIC_DIMENSION_FIELDS,
    STATIC_DIMENSION_FIELD_TYPES,
)


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


def _project_static_record(
    dimension_name: str,
    values: dict[str, Any],
) -> dict[str, Any]:
    """Validate and project one record against its canonical contract."""
    fields = STATIC_DIMENSION_FIELDS[dimension_name]
    field_types = STATIC_DIMENSION_FIELD_TYPES[dimension_name]

    if set(values) != set(fields):
        raise ValueError(
            f"{dimension_name} Silver fields do not match canonical contract"
        )

    result = {field: values[field] for field in fields}

    for field, value in result.items():
        expected_type = field_types[field]

        if value is not None and type(value) is not expected_type:
            raise TypeError(
                f"{dimension_name}.{field} must be "
                f"{expected_type.__name__} or None, "
                f"got {type(value).__name__}"
            )

    return result


def build_silver_routes(
    records: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Build standardized Silver route dimension records."""
    return [
        _project_static_record(
            "routes",
            {
                "route_id": _clean_text(record.get("route_id")),
                "agency_id": _clean_text(record.get("agency_id")),
                "route_short_name": _clean_text(record.get("route_short_name")),
                "route_long_name": _clean_text(record.get("route_long_name")),
                "route_type": _optional_int(record.get("route_type")),
            },
        )
        for record in records
    ]


def build_silver_stops(
    records: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Build standardized Silver stop dimension records."""
    return [
        _project_static_record(
            "stops",
            {
                "stop_id": _clean_text(record.get("stop_id")),
                "stop_name": _clean_text(record.get("stop_name")),
                "latitude": _optional_float(record.get("stop_lat")),
                "longitude": _optional_float(record.get("stop_lon")),
                "zone_id": _clean_text(record.get("zone_id")),
                "wheelchair_boarding": _optional_int(
                    record.get("wheelchair_boarding")
                ),
            },
        )
        for record in records
    ]


def build_silver_trips(
    records: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Build standardized Silver trip dimension records."""
    return [
        _project_static_record(
            "trips",
            {
                "trip_id": _clean_text(record.get("trip_id")),
                "route_id": _clean_text(record.get("route_id")),
                "service_id": _clean_text(record.get("service_id")),
                "trip_headsign": _clean_text(record.get("trip_headsign")),
                "direction_id": _optional_int(record.get("direction_id")),
                "shape_id": _clean_text(record.get("shape_id")),
            },
        )
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
