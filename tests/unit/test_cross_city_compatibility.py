"""Cross-city compatibility tests for AT and Metlink GTFS Static feeds."""

from __future__ import annotations

import pytest

from transport_lakehouse.contracts import (
    SHARED_ENTITY_FIELDS,
    STATIC_DIMENSION_FIELDS,
    STATIC_DIMENSION_FIELD_TYPES,
)
from transport_lakehouse.processing.silver_dimensions import (
    build_silver_dimensions,
)


@pytest.fixture(params=["at", "metlink"], ids=["auckland-at", "wellington-metlink"])
def provider_static_records(request):
    """Provide representative provider-specific GTFS Static records."""
    provider = request.param

    routes = [
        {
            "route_id": "R100",
            "agency_id": "AGENCY1",
            "route_short_name": "100",
            "route_long_name": "Central Route",
            "route_type": "712" if provider == "metlink" else "3",
            **(
                {"contract_id": "CONTRACT1"}
                if provider == "at"
                else {"route_color": "00364a"}
            ),
        }
    ]

    stops = [
        {
            "stop_id": "S100",
            "stop_name": "Central Station",
            "stop_lat": "-36.8500",
            "stop_lon": "174.7600",
            "zone_id": "",
            **(
                {"wheelchair_boarding": "0"}
                if provider == "at"
                else {"stop_timezone": "Pacific/Auckland"}
            ),
        }
    ]

    trips = [
        {
            "trip_id": "T100",
            "route_id": "R100",
            "service_id": "WEEKDAY",
            "trip_headsign": "Central",
            "direction_id": "0",
            "shape_id": "SHAPE100",
            **(
                {"trip_short_name": "AT100"}
                if provider == "at"
                else {"etm_id": "109"}
            ),
        }
    ]

    return {
        "provider": provider,
        "records": {
            "routes": routes,
            "stops": stops,
            "trips": trips,
        },
    }


def test_provider_projects_to_canonical_static_schema(
    provider_static_records,
):
    """Both providers must produce exactly the canonical dimension fields."""
    silver = build_silver_dimensions(
        provider_static_records["records"]
    )

    for dimension, records in silver.items():
        expected_fields = set(STATIC_DIMENSION_FIELDS[dimension])

        assert records
        assert all(set(record) == expected_fields for record in records)


def test_provider_projects_to_canonical_field_types(
    provider_static_records,
):
    """Both providers must produce the same Python field types."""
    silver = build_silver_dimensions(
        provider_static_records["records"]
    )

    for dimension, records in silver.items():
        expected_types = STATIC_DIMENSION_FIELD_TYPES[dimension]

        for record in records:
            for field, expected_type in expected_types.items():
                value = record[field]

                assert value is None or type(value) is expected_type, (
                    f"{provider_static_records['provider']}."
                    f"{dimension}.{field} has invalid type"
                )


def test_provider_specific_fields_do_not_leak_into_canonical_output(
    provider_static_records,
):
    """Provider-only GTFS fields must not enter canonical Silver records."""
    silver = build_silver_dimensions(
        provider_static_records["records"]
    )

    for dimension, records in silver.items():
        for record in records:
            assert set(record) == set(STATIC_DIMENSION_FIELDS[dimension])

    if provider_static_records["provider"] == "at":
        assert "contract_id" not in silver["routes"][0]
        assert "trip_short_name" not in silver["trips"][0]
    else:
        assert "route_color" not in silver["routes"][0]
        assert "etm_id" not in silver["trips"][0]


@pytest.fixture
def metlink_static_records():
    """Provide Metlink-specific records with omitted optional fields."""
    return {
        "routes": [
            {
                "route_id": "R100",
                "agency_id": "AGENCY1",
                "route_short_name": "100",
                "route_long_name": "Central Route",
                "route_type": "712",
            }
        ],
        "stops": [
            {
                "stop_id": "S100",
                "stop_name": "Central Station",
                "stop_lat": "-41.28",
                "stop_lon": "174.77",
                "zone_id": "5",
            }
        ],
        "trips": [
            {
                "trip_id": "T100",
                "route_id": "R100",
                "service_id": "WEEKDAY",
                "trip_headsign": "",
                "direction_id": "0",
                "shape_id": "SHAPE100",
                "etm_id": "109",
            }
        ],
    }


def test_metlink_missing_wheelchair_boarding_is_nullable(
    metlink_static_records,
):
    """Metlink may omit wheelchair_boarding from its source stops table."""
    silver = build_silver_dimensions(metlink_static_records)

    assert silver["stops"][0]["wheelchair_boarding"] is None


def test_metlink_extended_route_type_is_preserved_as_integer(
    metlink_static_records,
):
    """Extended GTFS route_type values must not be incorrectly restricted."""
    silver = build_silver_dimensions(metlink_static_records)

    assert silver["routes"][0]["route_type"] == 712
    assert type(silver["routes"][0]["route_type"]) is int


def test_shared_entity_identifiers_remain_strings(
    provider_static_records,
):
    """Static entity identifiers must retain canonical string types."""
    silver = build_silver_dimensions(
        provider_static_records["records"]
    )

    for field in SHARED_ENTITY_FIELDS:
        dimension = {
            "route_id": "routes",
            "stop_id": "stops",
            "trip_id": "trips",
        }[field]

        assert type(silver[dimension][0][field]) is str

    assert type(silver["trips"][0]["route_id"]) is str


def test_missing_optional_fields_remain_nullable():
    """Missing optional GTFS fields must be projected as None."""
    records = {
        "routes": [
            {
                "route_id": "R200",
                "agency_id": "AGENCY1",
                "route_short_name": "200",
                "route_long_name": "Test Route",
                "route_type": "712",
            }
        ],
        "stops": [
            {
                "stop_id": "S200",
                "stop_name": "Test Stop",
                "stop_lat": "-41.28",
                "stop_lon": "174.77",
            }
        ],
        "trips": [
            {
                "trip_id": "T200",
                "route_id": "R200",
                "service_id": "WEEKDAY",
                "direction_id": "0",
            }
        ],
    }

    silver = build_silver_dimensions(records)

    assert silver["stops"][0]["zone_id"] is None
    assert silver["stops"][0]["wheelchair_boarding"] is None
    assert silver["trips"][0]["trip_headsign"] is None
    assert silver["trips"][0]["shape_id"] is None
