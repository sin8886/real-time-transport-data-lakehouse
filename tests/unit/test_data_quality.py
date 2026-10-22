from transport_lakehouse.quality.data_quality import (
    check_geo,
    check_nulls,
    check_referential_integrity,
    check_schema,
    check_unique,
    run_dimension_quality_checks,
)


def test_check_schema_passes():
    records = [
        {
            "route_id": "R1",
            "agency_id": "A1",
            "route_short_name": "1",
            "route_long_name": "Route 1",
            "route_type": 3,
        }
    ]

    result = check_schema("routes", records)

    assert result["passed"] is True
    assert result["failed_records"] == 0


def test_check_schema_detects_missing_fields():
    result = check_schema(
        "routes",
        [{"route_id": "R1"}],
    )

    assert result["passed"] is False
    assert "agency_id" in result["errors"][0]


def test_check_nulls_detects_empty_required_fields():
    records = [
        {
            "route_id": "",
            "agency_id": "A1",
            "route_short_name": "1",
            "route_long_name": "Route 1",
            "route_type": 3,
        }
    ]

    result = check_nulls("routes", records)

    assert result["passed"] is False
    assert result["failed_records"] == 1


def test_check_unique_detects_duplicates():
    records = [
        {"route_id": "R1"},
        {"route_id": "R1"},
        {"route_id": "R2"},
    ]

    result = check_unique("routes", records)

    assert result["passed"] is False
    assert result["failed_records"] == 1


def test_check_geo_passes_valid_coordinates():
    records = [
        {"latitude": -36.85, "longitude": 174.76},
    ]

    result = check_geo(records)

    assert result["passed"] is True


def test_check_geo_detects_invalid_coordinates():
    records = [
        {"latitude": 100, "longitude": 200},
    ]

    result = check_geo(records)

    assert result["passed"] is False
    assert result["failed_records"] == 2


def test_check_referential_integrity_passes():
    routes = [{"route_id": "R1"}]
    trips = [{"trip_id": "T1", "route_id": "R1"}]

    result = check_referential_integrity(routes, trips)

    assert result["passed"] is True


def test_check_referential_integrity_detects_missing_route():
    routes = [{"route_id": "R1"}]
    trips = [{"trip_id": "T1", "route_id": "R2"}]

    result = check_referential_integrity(routes, trips)

    assert result["passed"] is False
    assert result["failed_records"] == 1


def test_run_dimension_quality_checks():
    silver_tables = {
        "routes": [
            {
                "route_id": "R1",
                "agency_id": "A1",
                "route_short_name": "1",
                "route_long_name": "Route 1",
                "route_type": 3,
            }
        ],
        "stops": [
            {
                "stop_id": "S1",
                "stop_name": "Station 1",
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
                "trip_headsign": "City",
                "direction_id": 0,
                "shape_id": "SH1",
            }
        ],
    }

    result = run_dimension_quality_checks(silver_tables)

    assert result["passed"] is True
    assert result["total_checks"] == 11
    assert result["passed_checks"] == 11
    assert result["failed_checks"] == 0
