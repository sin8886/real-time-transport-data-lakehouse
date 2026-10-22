# Auckland Transport Source Validation

## Validation Scope

This validation covers the Auckland Transport GTFS Static snapshot available in `data/gtfs/at/` and one live request to the official Combined GTFS Realtime Feed:

`https://api.at.govt.nz/realtime/legacy/`

The request used the `AT_API_KEY` value loaded from the local `.env` file through the `Ocp-Apim-Subscription-Key` request header. The key is not recorded in this document, source code, logs, or output.

This is a Day 3 source validation only. It does not implement Kafka, Spark, S3, PostgreSQL, or a production pipeline.

## 1. GTFS Static Validation

The Static feed is available locally as CSV text files. The inspected files and observed contents are:

| File | Observed validation result |
| --- | --- |
| `agency.txt` | 17 data rows. Contains `agency_id`, agency names, URLs, timezone, language, phone, and fare contact fields. |
| `calendar.txt` | 123 data rows. Contains `service_id`, weekday flags, `start_date`, and `end_date`. |
| `calendar_dates.txt` | 807 data rows. Contains `service_id`, `date`, and `exception_type`; both exception types occur in the snapshot. |
| `feed_info.txt` | 1 data row. Identifies Auckland Transport as publisher and includes feed language, feed dates, and a feed version. |
| `routes.txt` | 215 data rows. Contains `route_id`, `agency_id`, route names, `route_type`, and additional feed fields including `contract_id`. |
| `trips.txt` | 37,894 data rows. Contains `route_id`, `service_id`, `trip_id`, direction, `shape_id`, and accessibility-related fields. |
| `stops.txt` | 7,010 data rows. Contains stop identifiers, names, latitude/longitude, location type, parent station, platform, accessibility, and validity dates. |
| `stop_times.txt` | 1,163,934 data rows. Contains `trip_id`, scheduled arrival/departure, `stop_id`, `stop_sequence`, pickup/drop-off fields, distance, and `timepoint`. |
| `shapes.txt` | 613,872 data rows. Contains ordered shape points and distance fields keyed by `shape_id`. |
| `transfers.txt` | 10 data rows. Contains source/destination stop identifiers, transfer type, and optional minimum transfer time. |
| `fare_attributes.txt` | File present; no data rows in this snapshot. |
| `fare_rules.txt` | File present; no data rows in this snapshot. |
| `frequencies.txt` | File present; no data rows in this snapshot. |

The primary relationship is present and consistent with the domain notes:

```text
routes.route_id
    -> trips.route_id
    -> stop_times.trip_id
    -> stops.stop_id
```

Additional relationships are visible through `trips.service_id` to the calendar files and `trips.shape_id` to `shapes.shape_id`. The current feed dates, feed version, row counts, and empty optional files are observations of this snapshot, not permanent AT guarantees.

## 2. Live Realtime Request

### Request Result

| Check | Result |
| --- | --- |
| Endpoint | Official Combined Feed: `https://api.at.govt.nz/realtime/legacy/` |
| Authentication | Successful with the local `AT_API_KEY` supplied in the subscription-key header. The key value was not logged. |
| HTTP status | `200` |
| Response format | Valid JSON; `Content-Type` was `application/json; charset=utf-8`. |
| API status | Top-level `status` was `OK`. |
| Response envelope | Top-level keys included `error`, `response`, and `status`; the `response` object contained `header` and `entity`. |
| Entity count | `2,386` in the validation request. Counts are live and will vary between requests. |

The API returns one combined feed rather than three separately requested legacy feeds. The validation script performs one request only and reads entities from `response.entity[]`.

### Entity Types and Observed Fields

| Entity type | Count in validation request | Observed field structure |
| --- | ---: | --- |
| Trip Update | 1,065 | Entity `id`, `is_deleted`, and `trip_update`; trip fields included `trip_id`, `route_id`, `direction_id`, `start_date`, `start_time`, and `schedule_relationship`. Stop-time updates included `stop_id`, `stop_sequence`, `arrival`, `departure`, and `schedule_relationship`. Trip update fields included `delay` and `timestamp` where present. |
| Vehicle Position | 1,135 | Entity `id`, `is_deleted`, and `vehicle`; vehicle fields included `id`, `label`, and `license_plate`; position fields included `latitude`, `longitude`, and `speed`; the entity also included `timestamp`. The feed documentation says Vehicle Position does not provide `stop_id`; the validation did not infer one. |
| Service Alert | 186 | Entity `id` and `timestamp`, with `alert` fields including `active_period`, `cause`, `description_text`, `effect`, `effect_detail`, `header_text`, `informed_entity`, and `severity_level`. `active_period` contained a list in the sampled response, with `start` and/or `end` fields potentially nullable according to AT documentation. |

The entity counts are evidence from one live request, not a fixed feed contract. The response contained all three required realtime categories: Trip Updates, Vehicle Positions, and Alerts.

## 3. Official AT Behaviors and Limitations

The following points are based on the AT Realtime API documentation used for this validation:

- The Realtime API aggregates Trip Updates, Vehicle Positions, and Service Alerts.
- JSON is the default representation; Protocol Buffers can be requested with the appropriate `Accept` header.
- Realtime field names follow the GTFS Realtime `snake_case` convention.
- AT states that the feed is updated at least every 30 seconds.
- The API returns all updates by default, with query parameters available to filter by vehicles or trips.
- `trip_id` and `route_id` refer to trips and routes published through the GTFS feed.
- `vehicle.id` is only a vehicle identifier. Vehicles can be used on different routes or swapped, and the ID format may change. It must not be used to infer a route.
- Vehicle Position messages provide trip, vehicle, position, and timestamp information. AT documentation says other fields such as `stop_id` are omitted.
- The documentation states that `bearing` is only provided when the AVL device supplies it, and that `odometer` and `speed` are currently not provided. The live JSON response from this validation request did include `speed` in the sampled Vehicle Position structure. This is a documentation-versus-live-response difference that must be handled as an observed optional field, not assumed to be permanently present or absent.
- Trip Update stop-time events contain both `delay` and `time`; delay can be negative reasonably often, particularly for buses.
- Trip Update `schedule_relationship` values documented for this feed include scheduled and cancelled relationships.
- AT documents that Service Alert `active_period.start` or `active_period.end` may be `null` rather than omitted.
- The official API page inspected for this validation did not publish a numeric request-rate limit. No numeric rate limit is claimed here. The project must confirm any applicable quota or throttling policy from the API portal/account terms before operationalizing repeated polling.

## 4. Validation Limitations and Day 3 Follow-up

The live request verifies authentication, HTTP access, JSON decoding, the Combined Feed envelope, and the presence of all three required entity categories. It does not establish a permanent guarantee for entity counts, field presence, feed availability, or rate limits.

The following remain source-policy or operational follow-up items:

- Confirm the account-specific API quota, throttling behavior, and terms of use through the AT developer portal.
- Observe refresh timing across repeated requests rather than treating one request as a latency or availability measurement.
- Confirm whether the documented and observed `speed` difference persists across multiple live responses and transport modes.
- Verify behavior for empty feeds, malformed responses, authentication failure, HTTP errors, and temporary source outage without logging secrets or full payloads.
- Confirm whether saved real GTFS Realtime snapshots can be retained and replayed under the applicable AT terms.

## 5. Validation Artifacts

- `scripts/validate_at_realtime.py` performs one request to the official Combined Feed, reads `response.entity[]`, reports HTTP status, top-level API status, entity count, entity-type counts, and redacted field summaries.
- `.env` supplies `AT_API_KEY` locally and is ignored by Git through `.gitignore`.
- This report records structural and behavioral observations only; it does not contain the API key or a complete API response.
