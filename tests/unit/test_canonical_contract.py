from __future__ import annotations

import pytest

from transport_lakehouse.contracts import (
    CANONICAL_FIELD_TYPES,
    CANONICAL_REALTIME_FIELDS,
    CANONICAL_REQUIRED_FIELDS,
    REALTIME_COMMON_FIELDS,
    REALTIME_EVENT_FIELDS,
    SHARED_ENTITY_FIELDS,
    STATIC_DIMENSION_FIELDS,
    build_realtime_spark_schema,
)


def test_realtime_event_types_share_common_envelope() -> None:
    for fields in REALTIME_EVENT_FIELDS.values():
        for field in REALTIME_COMMON_FIELDS:
            assert field in fields


def test_canonical_realtime_fields_cover_every_event_field() -> None:
    expected = {
        field
        for fields in REALTIME_EVENT_FIELDS.values()
        for field in fields
    }

    assert set(CANONICAL_REALTIME_FIELDS) == expected
    assert set(CANONICAL_FIELD_TYPES) == expected


def test_realtime_required_fields_are_common_envelope() -> None:
    assert CANONICAL_REQUIRED_FIELDS == REALTIME_COMMON_FIELDS


def test_static_dimensions_share_entity_identifiers() -> None:
    assert "route_id" in STATIC_DIMENSION_FIELDS["routes"]
    assert "stop_id" in STATIC_DIMENSION_FIELDS["stops"]
    assert "trip_id" in STATIC_DIMENSION_FIELDS["trips"]

    assert set(SHARED_ENTITY_FIELDS) == {"route_id", "trip_id", "stop_id"}


def test_static_and_realtime_grains_remain_separate() -> None:
    assert "event_type" not in STATIC_DIMENSION_FIELDS["routes"]
    assert "event_type" not in STATIC_DIMENSION_FIELDS["stops"]
    assert "event_type" not in STATIC_DIMENSION_FIELDS["trips"]


def test_spark_schema_matches_canonical_realtime_contract() -> None:
    pytest.importorskip("pyspark")

    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
    )

    schema = build_realtime_spark_schema()

    assert tuple(schema.fieldNames()) == CANONICAL_REALTIME_FIELDS

    actual_types = {
        field.name: type(field.dataType)
        for field in schema.fields
    }

    expected_types = {
        str: StringType,
        int: IntegerType,
        float: DoubleType,
        bool: BooleanType,
    }

    for name, python_type in CANONICAL_FIELD_TYPES.items():
        assert actual_types[name] is expected_types[python_type]

    assert actual_types["is_deleted"] is BooleanType


def test_static_dimension_types_match_canonical_fields() -> None:
    from transport_lakehouse.contracts import (
        SHARED_ENTITY_FIELD_TYPES,
        STATIC_DIMENSION_FIELD_TYPES,
    )

    for dimension_name, fields in STATIC_DIMENSION_FIELDS.items():
        assert set(STATIC_DIMENSION_FIELD_TYPES[dimension_name]) == set(fields)

    for dimension_types in STATIC_DIMENSION_FIELD_TYPES.values():
        for field, expected_type in SHARED_ENTITY_FIELD_TYPES.items():
            if field in dimension_types:
                assert dimension_types[field] is expected_type


def test_shared_entity_types_match_realtime_contract() -> None:
    from transport_lakehouse.contracts import SHARED_ENTITY_FIELD_TYPES

    for field, expected_type in SHARED_ENTITY_FIELD_TYPES.items():
        assert CANONICAL_FIELD_TYPES[field] is expected_type


def test_static_and_realtime_share_string_entity_ids() -> None:
    from transport_lakehouse.contracts import (
        SHARED_ENTITY_FIELD_TYPES,
        STATIC_DIMENSION_FIELD_TYPES,
    )

    for field in SHARED_ENTITY_FIELDS:
        assert SHARED_ENTITY_FIELD_TYPES[field] is str
        assert CANONICAL_FIELD_TYPES[field] is str

    assert STATIC_DIMENSION_FIELD_TYPES["routes"]["route_id"] is str
    assert STATIC_DIMENSION_FIELD_TYPES["stops"]["stop_id"] is str
    assert STATIC_DIMENSION_FIELD_TYPES["trips"]["trip_id"] is str
    assert STATIC_DIMENSION_FIELD_TYPES["trips"]["route_id"] is str


def test_static_contract_rejects_invalid_runtime_types() -> None:
    from transport_lakehouse.processing.silver_dimensions import (
        _project_static_record,
    )

    with pytest.raises(TypeError, match="stops.latitude must be float"):
        _project_static_record(
            "stops",
            {
                "stop_id": "S1",
                "stop_name": None,
                "latitude": "-36.9",
                "longitude": 174.8,
                "zone_id": None,
                "wheelchair_boarding": None,
            },
        )
