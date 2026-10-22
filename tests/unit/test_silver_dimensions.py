from transport_lakehouse.processing.silver_dimensions import (
    build_silver_dimensions,
    build_silver_routes,
    build_silver_stops,
    build_silver_trips,
)


def test_build_silver_routes():
    records = [
        {
            "route_id": "101-202",
            "agency_id": "NZB",
            "route_short_name": "101",
            "route_long_name": "Route 101",
            "route_type": "3",
            "route_desc": "Unused field",
        }
    ]

    result = build_silver_routes(records)

    assert result == [
        {
            "route_id": "101-202",
            "agency_id": "NZB",
            "route_short_name": "101",
            "route_long_name": "Route 101",
            "route_type": 3,
        }
    ]


def test_build_silver_stops():
    records = [
        {
            "stop_id": "100-56c57897",
            "stop_name": "Papatoetoe Train Station",
            "stop_lat": "-36.97766",
            "stop_lon": "174.84925",
            "zone_id": "",
            "wheelchair_boarding": "0",
        }
    ]

    result = build_silver_stops(records)

    assert result == [
        {
            "stop_id": "100-56c57897",
            "stop_name": "Papatoetoe Train Station",
            "latitude": -36.97766,
            "longitude": 174.84925,
            "zone_id": None,
            "wheelchair_boarding": 0,
        }
    ]


def test_build_silver_trips():
    records = [
        {
            "trip_id": "1001-14703-19500-2-bb1d9386",
            "route_id": "147-203",
            "service_id": "Weekday-1",
            "trip_headsign": "Waitakere Township to Henderson",
            "direction_id": "0",
            "shape_id": "1001-14703-5b92fb16",
        }
    ]

    result = build_silver_trips(records)

    assert result[0]["trip_id"] == "1001-14703-19500-2-bb1d9386"
    assert result[0]["direction_id"] == 0
    assert result[0]["route_id"] == "147-203"


def test_empty_optional_values_become_none():
    result = build_silver_routes(
        [
            {
                "route_id": "101",
                "agency_id": "",
                "route_short_name": "",
                "route_long_name": "",
                "route_type": "",
            }
        ]
    )

    assert result[0]["agency_id"] is None
    assert result[0]["route_short_name"] is None
    assert result[0]["route_type"] is None


def test_build_silver_dimensions():
    parsed_tables = {
        "routes": [{"route_id": "101", "route_type": "3"}],
        "stops": [{"stop_id": "100", "stop_lat": "-36.9", "stop_lon": "174.8"}],
        "trips": [{"trip_id": "T1", "direction_id": "0"}],
    }

    result = build_silver_dimensions(parsed_tables)

    assert set(result) == {"routes", "stops", "trips"}
    assert result["routes"][0]["route_type"] == 3
    assert result["stops"][0]["latitude"] == -36.9
    assert result["trips"][0]["direction_id"] == 0
