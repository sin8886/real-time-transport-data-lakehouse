"""Fetch and normalize GTFS Realtime feeds into canonical events."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
from typing import Any, Callable, Mapping, Protocol

import requests
from google.protobuf.message import DecodeError

DEFAULT_AT_ENDPOINT = "https://api.at.govt.nz/realtime/legacy/"
GTFS_RT_PROTOBUF_MEDIA_TYPE = "application/x-protobuf"
LEGACY_GTFS_RT_PROTOBUF_MEDIA_TYPES = {
    "application/protobuf",
    "application/x-google-protobuf",
    "application/vnd.google.protobuf",
}
SUPPORTED_EVENT_TYPES = {
    "vehicle_position",
    "trip_update",
    "service_alert",
}


class RealtimeFetcherError(RuntimeError):
    """Base error for fetch and normalization failures."""


class RealtimeHTTPError(RealtimeFetcherError):
    """Raised when the realtime endpoint returns an HTTP error."""


class RealtimeParseError(RealtimeFetcherError):
    """Raised when a response is not a supported GTFS Realtime feed."""


class HTTPResponse(Protocol):
    status_code: int
    headers: Mapping[str, str]
    content: bytes

    def raise_for_status(self) -> None: ...


HTTPGet = Callable[..., HTTPResponse]


def _iso_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise RealtimeParseError("timestamp must be an integer epoch")
    if isinstance(value, int):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError) as error:
            raise RealtimeParseError("timestamp epoch is out of range") from error
    if isinstance(value, float):
        raise RealtimeParseError("timestamp epoch must be an integer")
    if isinstance(value, str):
        if value.isdigit():
            try:
                return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
            except (OverflowError, OSError, ValueError) as error:
                raise RealtimeParseError("timestamp epoch is out of range") from error
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise RealtimeParseError("timestamp must be ISO-8601") from error
        if parsed.tzinfo is None:
            raise RealtimeParseError("timestamp must include a timezone")
        return parsed.astimezone(timezone.utc).isoformat()
    raise RealtimeParseError(f"timestamp has unsupported type: {type(value).__name__}")


def _iso_timestamp_with_context(
    value: Any, field: str, event_type: str, entity_id: Any
) -> str | None:
    try:
        return _iso_timestamp(value)
    except RealtimeParseError as error:
        raise RealtimeParseError(
            f"{event_type} entity_id={entity_id!r} field={field!r} "
            f"type={type(value).__name__}: {error}"
        ) from error


def _validate_ingest_timestamp(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise RealtimeParseError("ingest_ts must be a non-empty ISO-8601 string")
    try:
        parsed = _iso_timestamp(value)
    except RealtimeParseError as error:
        raise RealtimeParseError(
            "ingest_ts must be a valid ISO-8601 timestamp"
        ) from error
    if parsed is None:
        raise RealtimeParseError("ingest_ts must be a non-empty ISO-8601 string")
    return parsed


def _string_or_none(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RealtimeParseError(f"{field} must be a string")
    return value


def _integer_or_none(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise RealtimeParseError(f"{field} must be an integer")
    return value


def _normalize_schedule_relationship(
    value: Any, field: str, enum_type: Any
) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise RealtimeParseError(f"{field} must be a string or enum")
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        try:
            return enum_type.Name(value)
        except (ImportError, ValueError) as error:
            raise RealtimeParseError(
                f"unknown {field} enum value: {value}"
            ) from error
    raise RealtimeParseError(f"{field} must be a string or enum")


def _number_or_none(value: Any, field: str) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RealtimeParseError(f"{field} must be numeric")
    if isinstance(value, float) and not math.isfinite(value):
        raise RealtimeParseError(f"{field} must be finite")
    return value


def _normalize_bearing(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise RealtimeParseError("bearing must be a finite number")
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            raise RealtimeParseError("bearing must be a finite number")
        return value
    if isinstance(value, str):
        if not value.strip():
            raise RealtimeParseError("bearing must be a finite number")
        try:
            numeric_value = float(value)
        except ValueError as error:
            raise RealtimeParseError("bearing must be a finite number") from error
        if not math.isfinite(numeric_value):
            raise RealtimeParseError("bearing must be a finite number")
        return (
            int(numeric_value)
            if numeric_value.is_integer() and "." not in value
            else numeric_value
        )
    raise RealtimeParseError("bearing must be a finite number")


def _json_value(message: Any, name: str, default: Any = None) -> Any:
    if isinstance(message, Mapping):
        return message.get(name, default)
    return getattr(message, name, default)


def _proto_scalar(message: Any, name: str) -> Any:
    try:
        return getattr(message, name) if message.HasField(name) else None
    except ValueError:
        if any(field.name == name for field, _ in message.ListFields()):
            return getattr(message, name)
        return None


def _nested_dict(message: Any, name: str) -> Mapping[str, Any]:
    value = _json_value(message, name, {})
    return value if isinstance(value, Mapping) else {}


def _canonical_envelope(
    event_type: str,
    provider: str,
    entity_id: Any,
    event_ts: Any,
    ingest_ts: str,
    deleted: Any,
    event_ts_field: str = "event_ts",
) -> dict[str, Any]:
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise RealtimeParseError(f"unsupported event type: {event_type}")
    if not isinstance(provider, str) or not provider:
        raise ValueError("provider must be a non-empty string")
    if deleted is not None and not isinstance(deleted, bool):
        raise RealtimeParseError("is_deleted must be a boolean")
    return {
        "event_type": event_type,
        "source_provider": provider,
        "source_entity_id": _string_or_none(entity_id, "source_entity_id"),
        "event_ts": _iso_timestamp_with_context(
            event_ts, event_ts_field, event_type, entity_id
        ),
        "ingest_ts": _validate_ingest_timestamp(ingest_ts),
        "is_deleted": deleted,
    }


def _normalize_vehicle(entity: Any, provider: str, ingest_ts: str) -> dict[str, Any]:
    vehicle = _json_value(entity, "vehicle", {})
    vehicle_identity = _nested_dict(vehicle, "vehicle")
    trip = _nested_dict(vehicle, "trip")
    position = _nested_dict(vehicle, "position")
    if isinstance(vehicle, Mapping) and "timestamp" in vehicle:
        event_ts = _json_value(vehicle, "timestamp")
        event_ts_field = "vehicle.timestamp"
    else:
        event_ts = _json_value(entity, "timestamp")
        event_ts_field = "entity.timestamp"
    event = _canonical_envelope(
        "vehicle_position",
        provider,
        _json_value(entity, "id"),
        event_ts,
        ingest_ts,
        _json_value(entity, "is_deleted"),
        event_ts_field,
    )
    event.update(
        {
            "vehicle_id": _string_or_none(
                _json_value(vehicle_identity, "id"), "vehicle_id"
            ),
            "trip_id": _string_or_none(_json_value(trip, "trip_id"), "trip_id"),
            "route_id": _string_or_none(_json_value(trip, "route_id"), "route_id"),
            "latitude": _number_or_none(_json_value(position, "latitude"), "latitude"),
            "longitude": _number_or_none(
                _json_value(position, "longitude"), "longitude"
            ),
            "speed": _number_or_none(_json_value(position, "speed"), "speed"),
            "bearing": _normalize_bearing(_json_value(position, "bearing")),
        }
    )
    return event


def _normalize_trip_update(
    entity: Any, provider: str, ingest_ts: str
) -> list[dict[str, Any]]:
    update = _json_value(entity, "trip_update", {})
    trip = _nested_dict(update, "trip")
    trip_id = _string_or_none(_json_value(trip, "trip_id"), "trip_id")
    vehicle = _nested_dict(update, "vehicle")
    from google.transit import gtfs_realtime_pb2

    _normalize_schedule_relationship(
        _json_value(trip, "schedule_relationship"),
        "trip.schedule_relationship",
        gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship,
    )
    stop_updates = _json_value(update, "stop_time_update", [])
    if isinstance(stop_updates, Mapping):
        stop_updates = [stop_updates]
    elif not isinstance(stop_updates, (list, tuple)):
        raise RealtimeParseError(
            "stop_time_update must be a mapping, list, or tuple"
        )
    if not stop_updates:
        return []
    if not trip_id:
        raise RealtimeParseError("trip_update is missing trip_id")

    result = []
    for stop_update in stop_updates:
        if not isinstance(stop_update, Mapping):
            raise RealtimeParseError("stop_time_update must contain objects")
        stop_id = _string_or_none(_json_value(stop_update, "stop_id"), "stop_id")
        stop_sequence = _json_value(stop_update, "stop_sequence")
        if stop_sequence is not None and (
            isinstance(stop_sequence, bool) or not isinstance(stop_sequence, int)
        ):
            raise RealtimeParseError("stop_sequence must be an integer")
        if stop_id is None and stop_sequence is None:
            raise RealtimeParseError(
                "trip_update stop-time update needs stop_id or stop_sequence"
            )
        arrival = _nested_dict(stop_update, "arrival")
        departure = _nested_dict(stop_update, "departure")
        event = _canonical_envelope(
            "trip_update",
            provider,
            _json_value(entity, "id"),
            _json_value(update, "timestamp"),
            ingest_ts,
            _json_value(entity, "is_deleted"),
            "trip_update.timestamp",
        )
        event.update(
            {
                "trip_id": trip_id,
                "route_id": _string_or_none(_json_value(trip, "route_id"), "route_id"),
                "vehicle_id": _string_or_none(
                    _json_value(vehicle, "id"), "vehicle_id"
                ),
                "stop_id": stop_id,
                "stop_sequence": stop_sequence,
                "scheduled_arrival_ts": None,
                "scheduled_departure_ts": None,
                "predicted_arrival_ts": _iso_timestamp_with_context(
                    _json_value(arrival, "time"),
                    "stop_time_update.arrival.time",
                    "trip_update",
                    _json_value(entity, "id"),
                ),
                "predicted_departure_ts": _iso_timestamp_with_context(
                    _json_value(departure, "time"),
                    "stop_time_update.departure.time",
                    "trip_update",
                    _json_value(entity, "id"),
                ),
                "delay_seconds": _integer_or_none(
                    arrival.get("delay")
                    if "delay" in arrival
                    else _json_value(update, "delay"),
                    "delay_seconds",
                ),
                "schedule_relationship": _normalize_schedule_relationship(
                    _json_value(stop_update, "schedule_relationship"),
                    "stop_time_update.schedule_relationship",
                    gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship,
                ),
            }
        )
        result.append(event)
    return result


def _normalize_alert(entity: Any, provider: str, ingest_ts: str) -> dict[str, Any]:
    alert = _json_value(entity, "alert", {})
    periods = _json_value(alert, "active_period", [])
    if not isinstance(periods, (list, tuple)):
        raise RealtimeParseError("alert.active_period must be a list")
    if any(not isinstance(period, Mapping) for period in periods):
        raise RealtimeParseError("alert.active_period must contain objects")
    period = periods[0] if isinstance(periods, (list, tuple)) and periods else {}
    informed = _json_value(alert, "informed_entity", [])
    if not isinstance(informed, (list, tuple)):
        raise RealtimeParseError("alert.informed_entity must be a list")
    if any(not isinstance(item, Mapping) for item in informed):
        raise RealtimeParseError("alert.informed_entity must contain objects")
    affected = informed[0] if isinstance(informed, (list, tuple)) and informed else {}
    description_text = _json_value(alert, "description_text")
    if description_text is None:
        description = None
    elif isinstance(description_text, Mapping):
        translations = description_text.get("translation")
        if translations is None:
            description = None
        elif not isinstance(translations, list):
            raise RealtimeParseError(
                "alert.description_text.translation must be a list"
            )
        elif not translations:
            description = None
        elif not isinstance(translations[0], Mapping):
            raise RealtimeParseError(
                "alert.description_text.translation must contain objects"
            )
        else:
            description = _string_or_none(
                translations[0].get("text"), "description"
            )
    else:
        description = _string_or_none(description_text, "description")
    event = _canonical_envelope(
        "service_alert",
        provider,
        _json_value(entity, "id"),
        _json_value(entity, "timestamp"),
        ingest_ts,
        _json_value(entity, "is_deleted"),
        "entity.timestamp",
    )
    event.update(
        {
            "alert_id": _string_or_none(_json_value(entity, "id"), "alert_id"),
            "active_start_ts": _iso_timestamp_with_context(
                _json_value(period, "start"),
                "alert.active_period[0].start",
                "service_alert",
                _json_value(entity, "id"),
            ),
            "active_end_ts": _iso_timestamp_with_context(
                _json_value(period, "end"),
                "alert.active_period[0].end",
                "service_alert",
                _json_value(entity, "id"),
            ),
            "cause": _string_or_none(_json_value(alert, "cause"), "cause"),
            "effect": _string_or_none(_json_value(alert, "effect"), "effect"),
            "description": description,
            "affected_route_id": _string_or_none(
                _json_value(affected, "route_id"), "affected_route_id"
            ),
            "affected_trip_id": _string_or_none(
                _json_value(affected, "trip_id"), "affected_trip_id"
            ),
            "affected_stop_id": _string_or_none(
                _json_value(affected, "stop_id"), "affected_stop_id"
            ),
        }
    )
    return event


def _normalize_entities(
    entities: Any, provider: str, ingest_ts: str
) -> list[dict[str, Any]]:
    if not isinstance(entities, list):
        raise RealtimeParseError("feed entity must be a list")
    events: list[dict[str, Any]] = []
    for entity in entities:
        if not isinstance(entity, Mapping):
            raise RealtimeParseError("feed entity must be an object")
        present = [
            kind for kind in ("vehicle", "trip_update", "alert") if kind in entity
        ]
        if len(present) != 1:
            raise RealtimeParseError(
                "each entity must contain exactly one supported event payload"
            )
        if present[0] == "vehicle":
            events.append(_normalize_vehicle(entity, provider, ingest_ts))
        elif present[0] == "trip_update":
            events.extend(_normalize_trip_update(entity, provider, ingest_ts))
        else:
            events.append(_normalize_alert(entity, provider, ingest_ts))
    return events


def parse_json_feed(
    payload: bytes | str | Mapping[str, Any],
    provider: str = "AT",
    ingest_ts: str | None = None,
) -> list[dict[str, Any]]:
    """Parse an AT-style JSON feed or standard GTFS-RT JSON object."""
    try:
        decoded = json.loads(payload) if isinstance(payload, (bytes, str)) else payload
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RealtimeParseError("response is not valid JSON") from error
    if not isinstance(decoded, Mapping):
        raise RealtimeParseError("JSON feed must be an object")
    response = decoded.get("response", decoded)
    if not isinstance(response, Mapping):
        raise RealtimeParseError("feed response must be an object")
    return _normalize_entities(
        response.get("entity", []),
        provider,
        ingest_ts or datetime.now(timezone.utc).isoformat(),
    )


def parse_protobuf_feed(
    payload: bytes, provider: str = "AT", ingest_ts: str | None = None
) -> list[dict[str, Any]]:
    """Parse a GTFS-RT FeedMessage protobuf payload."""
    try:
        from google.transit import gtfs_realtime_pb2
    except ImportError as error:
        raise RealtimeParseError(
            "gtfs-realtime-bindings is required for protobuf feeds"
        ) from error
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(payload)
    except DecodeError as error:
        raise RealtimeParseError("response is not valid GTFS-RT protobuf") from error
    entities = []
    for entity in feed.entity:
        data: dict[str, Any] = {
            "id": entity.id,
            "is_deleted": _proto_scalar(entity, "is_deleted"),
        }
        if entity.HasField("vehicle"):
            vehicle = entity.vehicle
            data["vehicle"] = {
                "vehicle": {
                    "id": (
                        _proto_scalar(vehicle.vehicle, "id")
                        if vehicle.HasField("vehicle")
                        else None
                    )
                },
                "trip": {
                    "trip_id": _proto_scalar(vehicle.trip, "trip_id"),
                    "route_id": _proto_scalar(vehicle.trip, "route_id"),
                },
                "position": {
                    "latitude": _proto_scalar(vehicle.position, "latitude"),
                    "longitude": _proto_scalar(vehicle.position, "longitude"),
                    "speed": _proto_scalar(vehicle.position, "speed"),
                    "bearing": _proto_scalar(vehicle.position, "bearing"),
                },
                "timestamp": _proto_scalar(vehicle, "timestamp"),
            }
        elif entity.HasField("trip_update"):
            update = entity.trip_update
            data["trip_update"] = {
                "trip": {
                    "trip_id": _proto_scalar(update.trip, "trip_id"),
                    "route_id": _proto_scalar(update.trip, "route_id"),
                    "schedule_relationship": (
                        gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                            _proto_scalar(update.trip, "schedule_relationship")
                        )
                        if _proto_scalar(update.trip, "schedule_relationship")
                        is not None
                        else None
                    ),
                },
                "vehicle": {
                    "id": (
                        _proto_scalar(update.vehicle, "id")
                        if update.HasField("vehicle")
                        else None
                    )
                },
                "timestamp": _proto_scalar(update, "timestamp"),
                "stop_time_update": [
                    {
                        "stop_id": _proto_scalar(item, "stop_id"),
                        "stop_sequence": _proto_scalar(item, "stop_sequence"),
                        "arrival": {
                            "time": (
                                _proto_scalar(item.arrival, "time")
                                if item.HasField("arrival")
                                else None
                            ),
                            "delay": (
                                _proto_scalar(item.arrival, "delay")
                                if item.HasField("arrival")
                                else None
                            ),
                        },
                        "departure": {
                            "time": (
                                _proto_scalar(item.departure, "time")
                                if item.HasField("departure")
                                else None
                            ),
                        },
                        "schedule_relationship": (
                            gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                                item.schedule_relationship
                            )
                            if item.HasField("schedule_relationship")
                            else None
                        ),
                    }
                    for item in update.stop_time_update
                ],
            }
        elif entity.HasField("alert"):
            alert = entity.alert
            data["timestamp"] = _proto_scalar(entity, "timestamp")
            data["alert"] = {
                "active_period": [
                    {
                        "start": period.start if period.HasField("start") else None,
                        "end": period.end if period.HasField("end") else None,
                    }
                    for period in alert.active_period
                ],
                "cause": (
                    gtfs_realtime_pb2.Alert.Cause.Name(cause)
                    if (cause := _proto_scalar(alert, "cause")) is not None
                    else None
                ),
                "effect": (
                    gtfs_realtime_pb2.Alert.Effect.Name(effect)
                    if (effect := _proto_scalar(alert, "effect")) is not None
                    else None
                ),
                "description_text": (
                    {
                        "translation": [
                            {"text": alert.description_text.translation[0].text}
                        ]
                    }
                    if alert.description_text.translation
                    else None
                ),
                "informed_entity": [
                    {
                        "route_id": _proto_scalar(item, "route_id"),
                        "trip_id": (
                            _proto_scalar(item.trip, "trip_id")
                            if item.HasField("trip")
                            else None
                        ),
                        "stop_id": _proto_scalar(item, "stop_id"),
                    }
                    for item in alert.informed_entity
                ],
            }
        else:
            raise RealtimeParseError("protobuf entity has no supported payload")
        entities.append(data)
    return _normalize_entities(
        entities, provider, ingest_ts or datetime.now(timezone.utc).isoformat()
    )


class RealtimeFetcher:
    """Fetch one GTFS-RT snapshot and return canonical event dictionaries."""

    def __init__(
        self,
        api_key: str | None = None,
        endpoint: str | None = None,
        provider: str = "AT",
        timeout: float = 10.0,
        http_get: HTTPGet | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("AT_API_KEY")
        if not self.api_key:
            raise ValueError("AT_API_KEY is not configured")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.endpoint = endpoint or os.getenv(
            "AT_REALTIME_ENDPOINT", DEFAULT_AT_ENDPOINT
        )
        self.provider = provider
        self.timeout = timeout
        self.http_get = http_get or requests.get
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def fetch(self, response_format: str = "json") -> list[dict[str, Any]]:
        """Fetch one snapshot; response_format is ``json`` or ``protobuf``."""
        if response_format not in {"json", "protobuf"}:
            raise ValueError("response_format must be 'json' or 'protobuf'")
        accept = (
            GTFS_RT_PROTOBUF_MEDIA_TYPE
            if response_format == "protobuf"
            else "application/json"
        )
        try:
            response = self.http_get(
                self.endpoint,
                headers={
                    "Accept": accept,
                    "Ocp-Apim-Subscription-Key": self.api_key,
                    "User-Agent": "real-time-transport-data-lakehouse/1.0",
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.Timeout as error:
            raise RealtimeHTTPError("realtime request timed out") from error
        except requests.RequestException as error:
            raise RealtimeHTTPError("realtime request failed") from error
        content_type = response.headers.get("Content-Type", "")
        media_type = content_type.split(";", 1)[0].strip().lower()
        expected_types = {
            "json": {"application/json", "application/gtfs+json"},
            "protobuf": {GTFS_RT_PROTOBUF_MEDIA_TYPE}
            | LEGACY_GTFS_RT_PROTOBUF_MEDIA_TYPES,
        }
        if media_type and media_type not in expected_types[response_format]:
            raise RealtimeHTTPError(
                f"response Content-Type {media_type!r} does not match "
                f"response_format={response_format!r}"
            )
        body = response.content
        ingest_ts = self.clock().astimezone(timezone.utc).isoformat()
        if response_format == "protobuf":
            return parse_protobuf_feed(body, self.provider, ingest_ts)
        return parse_json_feed(body, self.provider, ingest_ts)
