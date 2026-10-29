from datetime import datetime, timezone
import json
from unittest.mock import Mock

import pytest

from transport_lakehouse.ingestion.realtime_fetcher import (
    RealtimeFetcher,
    RealtimeHTTPError,
    RealtimeParseError,
    _normalize_schedule_relationship,
    parse_json_feed,
    parse_protobuf_feed,
)


INGEST_TS = "2026-10-29T06:00:00+00:00"


def entity_payload() -> dict:
    return {
        "response": {
            "entity": [
                {
                    "id": "v-1",
                    "vehicle": {
                        "timestamp": 1793253600,
                        "vehicle": {"id": "bus-1"},
                        "trip": {"trip_id": "trip-1", "route_id": "route-1"},
                        "position": {
                            "latitude": -36.85,
                            "longitude": 174.76,
                            "bearing": 90.0,
                        },
                    },
                },
                {
                    "id": "t-1",
                    "trip_update": {
                        "timestamp": 1793253601,
                        "trip": {"trip_id": "trip-1", "route_id": "route-1"},
                        "vehicle": {"id": "bus-1"},
                        "stop_time_update": [
                            {
                                "stop_id": "stop-1",
                                "arrival": {"time": 1793253660, "delay": -30},
                                "schedule_relationship": "SCHEDULED",
                            },
                            {
                                "stop_sequence": 2,
                                "departure": {"time": 1793253720},
                                "schedule_relationship": "SKIPPED",
                            },
                        ],
                    },
                },
                {
                    "id": "a-1",
                    "timestamp": 1793253602,
                    "alert": {
                        "active_period": [{"start": 1793253600}],
                        "cause": "CONSTRUCTION",
                        "effect": "DETOUR",
                        "description_text": {
                            "translation": [{"text": "Use another stop"}]
                        },
                        "informed_entity": [{"route_id": "route-1"}],
                    },
                },
            ]
        }
    }


def test_json_feed_normalizes_all_three_event_types() -> None:
    events = parse_json_feed(entity_payload(), ingest_ts=INGEST_TS)

    assert [event["event_type"] for event in events] == [
        "vehicle_position",
        "trip_update",
        "trip_update",
        "service_alert",
    ]
    assert events[0]["vehicle_id"] == "bus-1"
    assert events[0]["longitude"] == 174.76
    assert len([event for event in events if event["event_type"] == "trip_update"]) == 2
    assert events[1]["trip_id"] == "trip-1"
    assert events[1]["vehicle_id"] == "bus-1"
    assert events[1]["delay_seconds"] == -30
    assert events[1]["schedule_relationship"] == "SCHEDULED"
    assert events[1]["predicted_arrival_ts"]
    assert events[2]["schedule_relationship"] == "SKIPPED"
    assert events[2]["predicted_departure_ts"]
    assert events[3]["description"] == "Use another stop"
    assert events[3]["active_end_ts"] is None
    for event in events:
        assert event["source_provider"] == "AT"
        assert event["ingest_ts"] == INGEST_TS


def test_json_feed_allows_missing_optional_fields() -> None:
    payload = {"entity": [{"id": "a-1", "alert": {"informed_entity": []}}]}

    event = parse_json_feed(payload, ingest_ts=INGEST_TS)[0]

    assert event["event_ts"] is None
    assert event["cause"] is None
    assert event["affected_route_id"] is None


@pytest.mark.parametrize(
    "description_text",
    [
        None,
        {},
        {"translation": None},
        {"translation": []},
    ],
)
def test_alert_description_missing_or_empty_is_nullable(
    description_text: object,
) -> None:
    payload = {
        "entity": [
            {
                "id": "a-1",
                "alert": {"description_text": description_text},
            }
        ]
    }

    event = parse_json_feed(payload, ingest_ts=INGEST_TS)[0]

    assert event["description"] is None


def test_alert_description_extracts_first_translation_text() -> None:
    payload = {
        "entity": [
            {
                "id": "a-1",
                "alert": {
                    "description_text": {
                        "translation": [
                            {"text": "Use another stop"},
                            {"text": "Additional translation"},
                        ]
                    }
                },
            }
        ]
    }

    event = parse_json_feed(payload, ingest_ts=INGEST_TS)[0]

    assert event["description"] == "Use another stop"


@pytest.mark.parametrize(
    "description_text",
    [
        {"translation": ["invalid"]},
        {"translation": [None]},
        {"translation": [{"text": 42}]},
    ],
)
def test_alert_description_rejects_invalid_translation_structure(
    description_text: object,
) -> None:
    with pytest.raises(RealtimeParseError, match="description"):
        parse_json_feed(
            {
                "entity": [
                    {
                        "id": "a-1",
                        "alert": {"description_text": description_text},
                    }
                ]
            },
            ingest_ts=INGEST_TS,
        )


@pytest.mark.parametrize("field", ["latitude", "longitude", "speed"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_vehicle_numeric_fields_reject_non_finite_values(
    field: str, value: float
) -> None:
    payload = entity_payload()
    payload["response"]["entity"][0]["vehicle"]["position"][field] = value

    with pytest.raises(RealtimeParseError, match=field):
        parse_json_feed(payload, ingest_ts=INGEST_TS)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", -36.85),
        ("longitude", 174.76),
        ("speed", 12),
        ("latitude", None),
        ("longitude", None),
        ("speed", None),
    ],
)
def test_vehicle_numeric_fields_accept_finite_values_and_none(
    field: str, value: int | float | None
) -> None:
    payload = entity_payload()
    payload["response"]["entity"][0]["vehicle"]["position"][field] = value

    event = parse_json_feed(payload, ingest_ts=INGEST_TS)[0]

    assert event[field] == value


def test_alert_entity_timestamp_accepts_at_numeric_epoch_string() -> None:
    payload = {
        "entity": [
            {
                "id": "alert-1",
                "timestamp": "1793253602",
                "alert": {"active_period": [{"start": None, "end": None}]},
            }
        ]
    }

    event = parse_json_feed(payload, ingest_ts=INGEST_TS)[0]

    assert event["event_ts"] == "2026-10-29T06:00:02+00:00"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("active_period", "not-a-list"),
        ("informed_entity", "not-a-list"),
    ],
)
def test_alert_rejects_malformed_nested_structures(field: str, value: object) -> None:
    alert = {"active_period": [], "informed_entity": []}
    alert[field] = value

    with pytest.raises(RealtimeParseError, match=f"alert\\.{field}"):
        parse_json_feed(
            {"entity": [{"id": "alert-1", "alert": alert}]},
            ingest_ts=INGEST_TS,
        )


def test_trip_update_contains_nullable_contract_keys() -> None:
    event = parse_json_feed(
        _trip_update_payload({"stop_id": "stop-1"}),
        ingest_ts=INGEST_TS,
    )[0]

    assert event["vehicle_id"] is None
    assert event["scheduled_arrival_ts"] is None
    assert event["scheduled_departure_ts"] is None


@pytest.mark.parametrize(
    ("bearing", "expected"),
    [
        (0, 0),
        (38.5, 38.5),
        ("240", 240),
        ("38.5", 38.5),
        (None, None),
    ],
)
def test_vehicle_bearing_accepts_numeric_values_and_strings(
    bearing: object, expected: int | float | None
) -> None:
    payload = entity_payload()
    payload["response"]["entity"][0]["vehicle"]["position"]["bearing"] = bearing

    event = parse_json_feed(payload, ingest_ts=INGEST_TS)[0]

    assert event["bearing"] == expected


@pytest.mark.parametrize(
    "bearing",
    [True, "", "not-a-number", [], {}, float("nan"), float("inf"), float("-inf")],
)
def test_vehicle_bearing_rejects_invalid_values(bearing: object) -> None:
    payload = entity_payload()
    payload["response"]["entity"][0]["vehicle"]["position"]["bearing"] = bearing

    with pytest.raises(RealtimeParseError, match="bearing"):
        parse_json_feed(payload, ingest_ts=INGEST_TS)


def _trip_update_payload(stop_time_update: object) -> dict:
    return {
        "entity": [
            {
                "id": "trip-update-1",
                "trip_update": {
                    "timestamp": 1793253601,
                    "trip": {"trip_id": "trip-1", "route_id": "route-1"},
                    "stop_time_update": stop_time_update,
                },
            }
        ]
    }


def test_trip_update_accepts_single_stop_time_update_mapping() -> None:
    event = parse_json_feed(
        _trip_update_payload(
            {
                "stop_id": "stop-1",
                "stop_sequence": 1,
                "arrival": {"time": 1793253660, "delay": -30},
                "departure": {"time": 1793253720},
                "schedule_relationship": "SCHEDULED",
            }
        ),
        ingest_ts=INGEST_TS,
    )[0]

    assert event["event_type"] == "trip_update"
    assert event["trip_id"] == "trip-1"
    assert event["stop_id"] == "stop-1"
    assert event["stop_sequence"] == 1
    assert event["delay_seconds"] == -30
    assert event["schedule_relationship"] == "SCHEDULED"
    assert event["event_ts"] == "2026-10-29T06:00:01+00:00"
    assert event["ingest_ts"] == INGEST_TS


def test_trip_update_accepts_stop_time_update_list() -> None:
    events = parse_json_feed(
        _trip_update_payload(
            [
                {"stop_id": "stop-1"},
                {"stop_sequence": 2},
            ]
        ),
        ingest_ts=INGEST_TS,
    )

    assert len(events) == 2
    assert [event["stop_id"] for event in events] == ["stop-1", None]
    assert [event["stop_sequence"] for event in events] == [None, 2]


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, "SCHEDULED"), ("SCHEDULED", "SCHEDULED"), ("SKIPPED", "SKIPPED"), (None, None)],
)
def test_schedule_relationship_accepts_standard_values(
    value: object, expected: str | None
) -> None:
    events = parse_json_feed(
        _trip_update_payload(
            {
                "stop_id": "stop-1",
                "schedule_relationship": value,
            }
        ),
        ingest_ts=INGEST_TS,
    )

    assert events[0]["schedule_relationship"] == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, "SCHEDULED"), (3, "CANCELED"), ("CANCELED", "CANCELED"), (None, None)],
)
def test_trip_schedule_relationship_uses_trip_descriptor_enum(
    value: object, expected: str | None
) -> None:
    from google.transit import gtfs_realtime_pb2

    payload = _trip_update_payload({"stop_id": "stop-1"})
    payload["entity"][0]["trip_update"]["trip"]["schedule_relationship"] = value

    events = parse_json_feed(payload, ingest_ts=INGEST_TS)

    assert events[0]["schedule_relationship"] is None
    assert (
        _normalize_schedule_relationship(
            value,
            "trip.schedule_relationship",
            gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship,
        )
        == expected
    )


def test_trip_schedule_relationship_rejects_unknown_enum_value() -> None:
    payload = _trip_update_payload({"stop_id": "stop-1"})
    payload["entity"][0]["trip_update"]["trip"]["schedule_relationship"] = 99

    with pytest.raises(RealtimeParseError, match="trip.schedule_relationship"):
        parse_json_feed(payload, ingest_ts=INGEST_TS)


@pytest.mark.parametrize("value", [True, 99, 1.5, ["SCHEDULED"]])
def test_schedule_relationship_rejects_unsupported_values(value: object) -> None:
    with pytest.raises(RealtimeParseError, match="schedule_relationship"):
        parse_json_feed(
            _trip_update_payload(
                {
                    "stop_id": "stop-1",
                    "schedule_relationship": value,
                }
            ),
            ingest_ts=INGEST_TS,
        )


def test_trip_update_skips_missing_stop_time_update() -> None:
    payload = {
        "entity": [
            {
                "id": "missing-stop-update",
                "trip_update": {
                    "trip": {"trip_id": "trip-without-stops"},
                    "timestamp": 1793253601,
                    "delay": 60,
                },
            },
            {
                "id": "valid-trip-update",
                "trip_update": {
                    "trip": {"trip_id": "trip-1"},
                    "stop_time_update": {"stop_id": "stop-1"},
                },
            },
        ]
    }

    events = parse_json_feed(payload, ingest_ts=INGEST_TS)

    assert len(events) == 1
    assert events[0]["source_entity_id"] == "valid-trip-update"


def test_trip_update_skips_empty_stop_time_update() -> None:
    events = parse_json_feed(
        _trip_update_payload([]),
        ingest_ts=INGEST_TS,
    )

    assert events == []


def test_cancelled_trip_without_stop_updates_does_not_create_event() -> None:
    payload = _trip_update_payload([])
    payload["entity"][0]["trip_update"]["trip"]["schedule_relationship"] = 3

    assert parse_json_feed(payload, ingest_ts=INGEST_TS) == []


def test_deleted_stop_level_trip_update_preserves_tombstone() -> None:
    payload = _trip_update_payload({"stop_id": "stop-1"})
    payload["entity"][0]["is_deleted"] = True

    event = parse_json_feed(payload, ingest_ts=INGEST_TS)[0]

    assert event["is_deleted"] is True


@pytest.mark.parametrize("stop_time_update", ["unsupported", 42])
def test_trip_update_rejects_unsupported_stop_time_update(
    stop_time_update: object,
) -> None:
    with pytest.raises(RealtimeParseError, match="stop_time_update"):
        parse_json_feed(
            _trip_update_payload(stop_time_update),
            ingest_ts=INGEST_TS,
        )


def test_trip_update_still_rejects_invalid_stop_fields() -> None:
    with pytest.raises(RealtimeParseError, match="stop_sequence"):
        parse_json_feed(
            _trip_update_payload(
                {"stop_id": "stop-1", "stop_sequence": "not-an-integer"}
            ),
            ingest_ts=INGEST_TS,
        )


def test_empty_feed_returns_no_events() -> None:
    assert parse_json_feed({"response": {"entity": []}}, ingest_ts=INGEST_TS) == []


def test_invalid_json_and_invalid_entity_are_rejected() -> None:
    with pytest.raises(RealtimeParseError, match="valid JSON"):
        parse_json_feed(b"not-json")
    with pytest.raises(RealtimeParseError, match="exactly one"):
        parse_json_feed({"entity": [{"id": "bad"}]})


def test_protobuf_feed_normalizes_valid_message() -> None:
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add(id="v-1")
    entity.vehicle.vehicle.id = "bus-1"
    entity.vehicle.trip.trip_id = "trip-1"
    entity.vehicle.position.latitude = -36.85
    entity.vehicle.position.longitude = 174.76
    entity.vehicle.timestamp = 1793253600

    events = parse_protobuf_feed(feed.SerializeToString(), ingest_ts=INGEST_TS)

    assert events[0]["event_type"] == "vehicle_position"
    assert events[0]["vehicle_id"] == "bus-1"
    assert events[0]["event_ts"].endswith("+00:00")


def _parity_protobuf_payload() -> bytes:
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"

    vehicle_entity = feed.entity.add(id="v-1")
    vehicle_entity.vehicle.vehicle.id = "bus-1"
    vehicle_entity.vehicle.trip.trip_id = "trip-1"
    vehicle_entity.vehicle.trip.route_id = "route-1"
    vehicle_entity.vehicle.position.latitude = -36.85
    vehicle_entity.vehicle.position.longitude = 174.76
    vehicle_entity.vehicle.position.bearing = 90.0
    vehicle_entity.vehicle.timestamp = 1793253600

    trip_entity = feed.entity.add(id="t-1")
    trip_entity.trip_update.trip.trip_id = "trip-1"
    trip_entity.trip_update.trip.route_id = "route-1"
    trip_entity.trip_update.vehicle.id = "bus-1"
    trip_entity.trip_update.timestamp = 1793253601
    first = trip_entity.trip_update.stop_time_update.add(stop_id="stop-1")
    first.arrival.time = 1793253660
    first.arrival.delay = -30
    first.schedule_relationship = first.SCHEDULED
    second = trip_entity.trip_update.stop_time_update.add(stop_sequence=2)
    second.departure.time = 1793253720
    second.schedule_relationship = second.SKIPPED

    alert_entity = feed.entity.add(id="a-1")
    alert_entity.alert.cause = alert_entity.alert.CONSTRUCTION
    alert_entity.alert.effect = alert_entity.alert.DETOUR
    alert_entity.alert.description_text.translation.add(text="Use another stop")
    alert_entity.alert.informed_entity.add(route_id="route-1")
    alert_entity.alert.active_period.add(start=1793253600)
    return feed.SerializeToString()


def test_json_and_protobuf_have_canonical_parity_for_supported_fields() -> None:
    json_payload = entity_payload()
    json_payload["response"]["entity"][2].pop("timestamp")
    json_events = parse_json_feed(json_payload, ingest_ts=INGEST_TS)
    protobuf_events = parse_protobuf_feed(
        _parity_protobuf_payload(), ingest_ts=INGEST_TS
    )

    assert len(json_events) == len(protobuf_events) == 4
    for json_event, protobuf_event in zip(json_events, protobuf_events):
        assert json_event.keys() == protobuf_event.keys()
        for field in json_event:
            if isinstance(json_event[field], float):
                assert json_event[field] == pytest.approx(protobuf_event[field])
            else:
                assert json_event[field] == protobuf_event[field]


@pytest.mark.parametrize(
    "payload",
    [
        {"entity": [{"id": "v-1", "vehicle": {"timestamp": "not-a-time"}}]},
        {
            "entity": [
                {
                    "id": "t-1",
                    "trip_update": {
                        "trip": {"trip_id": "trip-1"},
                        "stop_time_update": [
                            {"stop_id": "stop-1", "arrival": {"time": 1.5}}
                        ],
                    },
                }
            ]
        },
    ],
)
def test_invalid_timestamps_are_rejected(payload: dict) -> None:
    with pytest.raises(RealtimeParseError, match="timestamp"):
        parse_json_feed(payload, ingest_ts=INGEST_TS)


def test_numeric_timestamp_string_is_only_supported_as_epoch() -> None:
    payload = {"entity": [{"id": "v-1", "vehicle": {"timestamp": "1793253600"}}]}

    event = parse_json_feed(payload, ingest_ts=INGEST_TS)[0]

    assert event["event_ts"] == "2026-10-29T06:00:00+00:00"


@pytest.mark.parametrize("value", ["1793253600.0", "2026-10-29T06:00:00"])
def test_timestamp_string_still_requires_integer_epoch_or_timezone(
    value: str,
) -> None:
    with pytest.raises(RealtimeParseError, match="timestamp"):
        parse_json_feed(
            {"entity": [{"id": "v-1", "vehicle": {"timestamp": value}}]},
            ingest_ts=INGEST_TS,
        )


def test_timestamp_error_identifies_vehicle_field_and_entity() -> None:
    payload = {"entity": [{"id": "vehicle-1", "vehicle": {"timestamp": "bad"}}]}

    with pytest.raises(
        RealtimeParseError,
        match=r"vehicle_position entity_id='vehicle-1'.*field='vehicle.timestamp'.*type=str",
    ):
        parse_json_feed(payload, ingest_ts=INGEST_TS)


def test_timestamp_error_identifies_trip_stop_field_and_entity() -> None:
    payload = _trip_update_payload(
        {"stop_id": "stop-1", "arrival": {"time": "bad"}}
    )

    with pytest.raises(
        RealtimeParseError,
        match=r"trip_update entity_id='trip-update-1'.*"
        r"field='stop_time_update.arrival.time'.*type=str",
    ):
        parse_json_feed(payload, ingest_ts=INGEST_TS)


def test_timestamp_error_identifies_alert_period_field_and_entity() -> None:
    payload = {
        "entity": [
            {
                "id": "alert-1",
                "alert": {"active_period": [{"start": "bad"}]},
            }
        ]
    }

    with pytest.raises(
        RealtimeParseError,
        match=r"service_alert entity_id='alert-1'.*"
        r"field='alert.active_period\[0\].start'.*type=str",
    ):
        parse_json_feed(payload, ingest_ts=INGEST_TS)


@pytest.mark.parametrize("delay", [1.5, True])
def test_delay_seconds_must_be_an_integer(delay: object) -> None:
    payload = entity_payload()
    payload["response"]["entity"][1]["trip_update"]["stop_time_update"][0]["arrival"][
        "delay"
    ] = delay

    with pytest.raises(RealtimeParseError, match="delay_seconds"):
        parse_json_feed(payload, ingest_ts=INGEST_TS)


def test_ingest_timestamp_must_be_iso8601() -> None:
    with pytest.raises(RealtimeParseError, match="ingest_ts"):
        parse_json_feed(
            {"entity": [{"id": "a-1", "alert": {}}]},
            ingest_ts="not-a-time",
        )


def test_fetcher_uses_injected_http_and_does_not_log_secret() -> None:
    response = Mock()
    response.content = json.dumps(entity_payload()).encode()
    response.headers = {"Content-Type": "application/json"}
    response.raise_for_status.return_value = None
    http_get = Mock(return_value=response)
    fetcher = RealtimeFetcher(
        api_key="test-placeholder-not-a-real-key",
        http_get=http_get,
        clock=lambda: datetime(2026, 10, 29, 6, tzinfo=timezone.utc),
    )

    events = fetcher.fetch()

    assert len(events) == 4
    request = http_get.call_args
    assert request.kwargs["headers"]["Accept"] == "application/json"
    assert (
        request.kwargs["headers"]["Ocp-Apim-Subscription-Key"]
        == "test-placeholder-not-a-real-key"
    )
    assert events[0]["ingest_ts"] == INGEST_TS


def test_fetcher_http_error_timeout_and_missing_key() -> None:
    response = Mock()
    response.raise_for_status.side_effect = __import__("requests").HTTPError()
    with pytest.raises(RealtimeHTTPError, match="request failed") as error:
        RealtimeFetcher(
            api_key="test-placeholder-not-a-real-key",
            http_get=Mock(return_value=response),
        ).fetch()
    assert "test-placeholder-not-a-real-key" not in str(error.value)

    import requests

    with pytest.raises(RealtimeHTTPError, match="timed out"):
        RealtimeFetcher(
            api_key="key",
            http_get=Mock(side_effect=requests.Timeout()),
        ).fetch()
    with pytest.raises(ValueError, match="AT_API_KEY"):
        RealtimeFetcher(api_key="")


@pytest.mark.parametrize(
    ("content_type", "response_format", "expected_error"),
    [
        ("application/json; charset=utf-8", "json", None),
        ("application/x-protobuf; charset=binary", "protobuf", None),
        ("application/protobuf; charset=utf-8", "protobuf", None),
        ("application/protobuf", "protobuf", None),
        ("application/x-google-protobuf; charset=binary", "protobuf", None),
        ("application/x-google-protobuf", "json", "Content-Type"),
    ],
)
def test_fetcher_validates_content_type(
    content_type: str, response_format: str, expected_error: str | None
) -> None:
    response = Mock()
    response.content = json.dumps(entity_payload()).encode()
    response.headers = {"Content-Type": content_type}
    response.raise_for_status.return_value = None

    http_get = Mock(return_value=response)
    fetcher = RealtimeFetcher(api_key="test-placeholder", http_get=http_get)
    if expected_error:
        with pytest.raises(RealtimeHTTPError, match=expected_error):
            fetcher.fetch(response_format=response_format)
    else:
        response.content = (
            json.dumps(entity_payload()).encode()
            if response_format == "json"
            else _parity_protobuf_payload()
        )
        assert fetcher.fetch(response_format=response_format)
        assert (
            http_get.call_args.kwargs["headers"]["Accept"]
            == (
                "application/json"
                if response_format == "json"
                else "application/x-protobuf"
            )
        )


def test_fetcher_allows_missing_content_type_for_explicit_format() -> None:
    response = Mock()
    response.content = json.dumps(entity_payload()).encode()
    response.headers = {}
    response.raise_for_status.return_value = None

    fetcher = RealtimeFetcher(
        api_key="test-placeholder", http_get=Mock(return_value=response)
    )

    assert fetcher.fetch()
