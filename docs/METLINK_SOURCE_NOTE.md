# Metlink Secondary Source Note

## Validation Scope

This Day 4 validation evaluates Metlink / Greater Wellington as a possible secondary GTFS source for cross-city compatibility testing. It uses the official Metlink Static download and the official Metlink Open Data Developer Portal only.

- Official Static documentation: `https://www.metlink.org.nz/legal/general-transit-feed-specification`
- Official Static download: `https://static.opendata.metlink.org.nz/v1/gtfs/full.zip`
- Official Developer Portal: `https://opendata.metlink.org.nz/`
- Local extracted feed: `data/gtfs/full/`

No Kafka, Spark, S3, PostgreSQL, or production pipeline was created for this validation.

## 1. Static Feed Availability

The official Static feed was downloaded and extracted successfully into `data/gtfs/full/`. The directory is readable and contains the following files:

```text
agency.txt
calendar.txt
calendar_dates.txt
feed_info.txt
routes.txt
shapes.txt
stops.txt
stop_patterns.txt
stop_pattern_trips.txt
stop_times.txt
transfers.txt
trips.txt
```

Observed row counts in this downloaded snapshot were:

| File                     | Data rows | Main observation                                                                                       |
| ------------------------ | --------: | ------------------------------------------------------------------------------------------------------ |
| `agency.txt`             |        23 | Agency identifiers, names, URLs, timezone, language, phone, and fare URL.                              |
| `routes.txt`             |       245 | Route identifiers, agency, route names, route type, URL, colors, and sort order.                       |
| `trips.txt`              |    19,157 | Route, service, trip, direction, shape, accessibility, and Metlink `etm_id` fields.                    |
| `stops.txt`              |     3,148 | Stop identifiers, names, coordinates, location type, parent station, and timezone.                     |
| `stop_times.txt`         |   664,608 | Trip-to-stop schedule with arrival/departure, stop sequence, pickup/drop-off, distance, and timepoint. |
| `calendar.txt`           |     1,892 | Weekly service flags and service date ranges.                                                          |
| `calendar_dates.txt`     |    23,623 | Date-specific service exceptions.                                                                      |
| `shapes.txt`             |   609,166 | Ordered geographic shape points and travelled distance.                                                |
| `feed_info.txt`          |         1 | Greater Wellington Regional Council publisher, feed dates, language, and feed version.                 |
| `transfers.txt`          |     1,035 | Stop transfer relationships, including optional trip identifiers and transfer time.                    |
| `stop_patterns.txt`      |    23,252 | Metlink-specific pattern file linking stops and ordered stop sequences.                                |
| `stop_pattern_trips.txt` |    19,157 | Metlink-specific relationship between stop patterns and trips.                                         |

The row counts describe this snapshot only and are not permanent feed guarantees.

## 2. Static Schema Comparison with Auckland Transport

The comparison used the existing AT feed in `data/gtfs/` and the Metlink feed in `data/gtfs/full/`.

| Core file            | Compatibility result           | Observed difference                                                                                        |
| -------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| `agency.txt`         | Compatible core GTFS structure | Metlink does not include AT's `agency_email` column in this snapshot.                                      |
| `routes.txt`         | Compatible core GTFS structure | Metlink does not include AT's `contract_id` column.                                                        |
| `trips.txt`          | Compatible core GTFS structure | Metlink includes `etm_id`; AT includes `trip_short_name` in the observed snapshot.                         |
| `stops.txt`          | Compatible core GTFS structure | Metlink does not include AT's `platform_code`, `wheelchair_boarding`, `start_date`, or `end_date` columns. |
| `stop_times.txt`     | Header-compatible              | The core schedule fields match exactly in the observed headers.                                            |
| `calendar.txt`       | Header-compatible              | The observed headers match exactly.                                                                        |
| `calendar_dates.txt` | Header-compatible              | The observed headers match exactly.                                                                        |
| `shapes.txt`         | Header-compatible              | The observed headers match exactly.                                                                        |

The core cross-city relationship is present in both feeds:

```text
routes.route_id
    -> trips.route_id
    -> stop_times.trip_id
    -> stops.stop_id
```

Both feeds also connect `trips.service_id` to calendar data and `trips.shape_id` to shape data. This supports the project conclusion that Metlink Static is suitable for later schema-compatibility testing at the domain level.

Compatibility does not mean identical schemas. A later implementation would need to handle optional or provider-specific columns without assuming that every AT-specific field exists in Metlink. This note does not define a new canonical schema.

## 3. Static Feed Observations

- Metlink publishes `feed_info.txt` with Greater Wellington Regional Council as the publisher in this snapshot.
- The observed feed contains bus, train, ferry, and cable-car-related agency/route data as represented by the feed's agencies and route types. A complete mode inventory was not required for this validation.
- `stop_patterns.txt` and `stop_pattern_trips.txt` are additional Metlink files and are not part of the AT core file set inspected for this comparison.
- The feed dates and feed version are snapshot metadata and must not be treated as permanent business rules.
- No claim is made here about future file presence, column ordering, row counts, or provider-specific field stability.

## 4. Realtime API Authentication and Official Portal

The official Metlink Open Data Developer Portal states that:

- A developer account is required to use the APIs.
- An API key is generated for the account and activated for subscribed public APIs.
- The portal provides a playground and example requests for subscribed APIs.
- API usage can be monitored on the account dashboard.
- API usage limits are associated with each API key, but the exact account-specific limits were not available from the public page used for this validation.

The local `.env` contains the Metlink API key. It was read only from the environment file and was not written to this document, source code, logs, or command output.

## 5. Realtime Validation Result

The three endpoint paths below were confirmed from the authenticated Metlink OpenAPI definition supplied for this validation. Each endpoint was called once using the local API key through the `x-api-key` request header. No API key or response body was printed or stored.

| Category          | Endpoint                       | HTTP status | Content-Type       | JSON parsed | Payload/entity | Entity count | Entity type       |
| ----------------- | ------------------------------ | ----------: | ------------------ | ----------- | -------------- | -----------: | ----------------- |
| Trip Updates      | `/v1/gtfs-rt/tripupdates`      |       `200` | `application/json` | Yes         | Present        |           50 | `trip_update`: 50 |
| Vehicle Positions | `/v1/gtfs-rt/vehiclepositions` |       `200` | `application/json` | Yes         | Present        |           50 | `vehicle`: 50     |
| Service Alerts    | `/v1/gtfs-rt/servicealerts`    |       `200` | `application/json` | Yes         | Present        |           12 | `alert`: 12       |

The successful `200` responses and valid JSON payloads confirm that the local API key was accepted for all three subscribed endpoints. The entity counts are observations from one live request and are not fixed feed limits.

The response format observed in this validation was JSON. Protobuf was not requested or observed in this run; protobuf behavior is **Not verified** here.

The following Realtime details remain **Not verified**:

- Account-specific rate limit and burst limit values.
- Update cadence measured across repeated requests.
- Whether the current entity counts or optional fields remain stable over time.
- Protobuf response behavior for these endpoints.

## 6. AT and Metlink Compatibility Assessment

### Compatible Areas

- Both feeds use the standard GTFS Static core files required for route, trip, stop, scheduled stop-time, calendar, calendar-exception, and shape understanding.
- Both feeds expose the same core identifiers: `route_id`, `trip_id`, `stop_id`, `service_id`, `shape_id`, and `stop_sequence` in the relevant files.
- Both feeds support the same primary relationship between planned route, trip, stop times, and stops.
- Both feeds use the same core `stop_times.txt`, `calendar.txt`, `calendar_dates.txt`, and `shapes.txt` headers in the observed snapshots.

### Differences Requiring Adaptation

- Optional columns differ between providers, especially in `agency.txt`, `routes.txt`, `trips.txt`, and `stops.txt`.
- Metlink has provider-specific `etm_id` and additional stop-pattern files.
- AT has provider-specific fields such as `contract_id` and additional stop validity/accessibility fields in the observed snapshot.
- Row counts, feed dates, route identifiers, agency identifiers, and provider-specific values are different by design.
- Realtime access was verified for Trip Updates, Vehicle Positions, and Service Alerts using the documented endpoints. Cross-provider field-level compatibility beyond the endpoint/entity presence was not assessed in this Day 4 validation.

## 7. Day 4 Conclusion

Metlink GTFS Static is available and structurally compatible with the AT core GTFS Static model for later cross-city testing. The compatibility is at the standard domain/file level, not an assertion that the provider schemas are identical.

Metlink Realtime access is verified for all three required categories in this run. The documented endpoints returned valid JSON payloads with `trip_update`, `vehicle`, and `alert` entities. Exact account limits, repeated-request cadence, and protobuf behavior remain **Not verified**.
