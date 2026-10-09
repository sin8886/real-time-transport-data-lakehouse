# GTFS Domain Notes

## 1. GTFS Overview

GTFS (General Transit Feed Specification) is an open data format for describing public transport services. It separates the planned network and timetable from changing operational information.

- **GTFS Static** describes the planned network: agencies, routes, trips, stops, scheduled stop times, service calendars, and geographic shapes. It is delivered as a collection of CSV text files.
- **GTFS Realtime** describes the current operational state of the network. It can report vehicle positions, trip progress and predictions, and service alerts. Auckland Transport's Realtime API can return JSON and also supports Protocol Buffers.

This project needs to understand both forms because the planned service provides the reference for interpreting current operations. Identifiers such as `trip_id`, `route_id`, and `stop_id` provide the relationships between scheduled data and realtime events.

## 2. GTFS Static Domain

The current Auckland Transport static snapshot contains standard GTFS files as well as additional optional files. The observations below describe this snapshot only; dates, feed version, row counts, and available fields may change in a later feed.

### Route

`routes.txt` describes transport services that passengers can recognize as routes. In this snapshot it contains `route_id`, `agency_id`, `route_short_name`, `route_long_name`, `route_type`, URL and color fields, and some AT-specific or feed-specific fields such as `contract_id`.

`route_id` identifies a route within this feed. It is the identifier used to connect a route to its trips. A route is not a vehicle: vehicle assignments can change during operations.

The snapshot includes route types with values such as `3` and `4`, corresponding to bus and ferry in the generic GTFS route-type vocabulary. The feed also contains multiple agencies, so `agency_id` is relevant when interpreting route ownership or service context.

### Trip

`trips.txt` describes individual scheduled journeys. Each row connects a `trip_id` to a `route_id` and a `service_id`. It also contains fields such as `trip_headsign`, `direction_id`, `block_id`, `shape_id`, `wheelchair_accessible`, and `bikes_allowed`.

- `trip_id` identifies one scheduled trip in this feed.
- `route_id` identifies the route on which the trip is scheduled to operate.
- `service_id` connects the trip to the calendar rules that determine when it is scheduled to run.
- `shape_id` connects the trip to its planned geographic path.

The same route can therefore have many trips, and trips on the same route can use different service calendars, directions, or shapes.

### Stop

`stops.txt` describes locations used by the transit network. The snapshot contains `stop_id`, `stop_code`, `stop_name`, `stop_lat`, `stop_lon`, `location_type`, `parent_station`, `platform_code`, `wheelchair_boarding`, and validity dates among other fields.

- `stop_id` identifies a stop or station within the feed.
- `stop_name` is the passenger-facing name.
- `stop_lat` and `stop_lon` describe the geographic location.
- `location_type` and `parent_station` distinguish stops from higher-level stations or grouped locations.
- `platform_code` can identify a platform where the feed provides one.

The current snapshot includes both ordinary stops and station-like records, and includes Auckland coordinates such as latitude values around `-36` and longitude values around `174`. These are observations from this snapshot, not a permanent constraint on future feeds.

### Stop Time

`stop_times.txt` links a scheduled `trip_id` to the stops visited by that trip. It contains `arrival_time`, `departure_time`, `stop_id`, `stop_sequence`, and additional fields including `stop_headsign`, pickup/drop-off types, `shape_dist_traveled`, and `timepoint`.

- `arrival_time` is the scheduled arrival time at the stop.
- `departure_time` is the scheduled departure time.
- `stop_id` identifies the stop visited by the trip.
- `stop_sequence` gives the order of the stop within the trip.

The first observed records use ordinary `HH:MM:SS` values and increasing stop sequences. Multiple records can have the same clock time at different stops, so the sequence is important for understanding the journey order. `stop_times.txt` is the largest operational schedule file in this snapshot, with approximately 1.16 million data rows.

### Service Calendar

`calendar.txt` defines recurring weekly service patterns. It contains a `service_id`, weekday flags, `start_date`, and `end_date`. A trip's `service_id` points to the calendar pattern that determines its normal operating dates.

`calendar_dates.txt` defines date-specific exceptions for a `service_id`. Its `exception_type` indicates whether service is added or removed for that date under the GTFS convention. The current snapshot contains both exception types. Calendar dates therefore refine the regular weekly pattern rather than replacing the trip relationship.

The current feed advertises dates in 2026, including a `feed_start_date` and `feed_end_date` in `feed_info.txt`. These are properties of this downloaded snapshot and must not be treated as permanent operating dates.

### Shape

`shapes.txt` describes the planned geographic path of a trip. It contains `shape_id`, `shape_pt_lat`, `shape_pt_lon`, `shape_pt_sequence`, and `shape_dist_traveled`.

A trip references a shape through `shape_id`. The ordered shape points represent the planned path and distance progression; they do not represent the live position of a vehicle. The current file contains approximately 614 thousand data rows, so one shape can consist of many ordered points.

### Observed Static Relationship

The central relationship observed in the files is:

```text
Route -> Trip -> Stop Times -> Stop
```

`Trip` also connects to `Service Calendar` through `service_id` and to a planned geographic path through `shape_id`.

### Other Files Present in the Snapshot

| File                  | Observation in the current snapshot                                                                                                                                          |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `agency.txt`          | Contains 17 agency records with fields such as `agency_id`, name, URL, timezone, language, phone, and fare contact fields. The records include AT Metro and other operators. |
| `feed_info.txt`       | Contains one feed metadata record with Auckland Transport as publisher, English as feed language, snapshot dates, and a feed version string.                                 |
| `transfers.txt`       | Contains 10 transfer records with `from_stop_id`, `to_stop_id`, `transfer_type`, and optional minimum transfer time.                                                         |
| `fare_attributes.txt` | Present, but contains only its header in this snapshot.                                                                                                                      |
| `fare_rules.txt`      | Present, but contains only its header in this snapshot.                                                                                                                      |
| `frequencies.txt`     | Present, but contains only its header in this snapshot.                                                                                                                      |

The empty optional files should not be interpreted as evidence that AT never provides fare or frequency information. They only describe what was present in this downloaded snapshot.

## 3. GTFS Realtime Domain

The following concepts are based on the Auckland Transport Realtime API documentation supplied for this project. No GTFS Realtime payload file is currently present in the workspace, so these are API-documented concepts rather than observations from a local realtime snapshot.

### Vehicle Position

A Vehicle Position describes the current or recently reported state of a vehicle. The AT Realtime representation includes:

- `vehicle`: vehicle identity, including `vehicle.id` when provided;
- `trip`: the trip associated with the vehicle, including identifiers such as `trip_id` and potentially `route_id`;
- `position`: geographic position such as `latitude` and `longitude`;
- `timestamp`: the time associated with the vehicle observation.

A Vehicle Position is an operational observation, not a replacement for the scheduled trip or stop-time data. If a current payload does not provide `stop_id` or another field, the project must not infer it from the vehicle's coordinates without an explicit basis.

### Trip Update

A Trip Update describes updated progress for a scheduled trip. It connects a `trip` with a sequence of `stop_time_update` records and may include a `vehicle` reference.

A `stop_time_update` identifies a stop using `stop_id`, `stop_sequence`, or both where provided. Its `arrival` and `departure` information can include predicted times and `delay`. The update can also include `schedule_relationship`, which describes how the realtime trip or stop-time update relates to the scheduled service.

The domain relationship is:

```text
Trip Update -> Trip -> Stop Time Update -> Stop
                              |
                              +-> arrival / departure / delay / schedule relationship
```

`trip_id` connects the update to the static trip. `route_id` can provide route context when present, but it should not be reconstructed from `vehicle.id`.

### Service Alert

A Service Alert describes a service condition affecting passengers or the network, such as a delay, cancellation, diversion, construction activity, or another operational disruption. An alert can be related to affected routes, trips, stops, or other transit entities depending on the feed content.

For Auckland Transport, these alerts can relate to bus, train, and ferry services. The API representation includes an `active_period` concept. In current AT data this field may be `null`, so a consumer must treat its presence and completeness as data-quality concerns rather than assuming every alert has a fully bounded active period.

## 4. Static vs Realtime

| Aspect               | GTFS Static                                                                             | GTFS Realtime                                                                                                |
| -------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Purpose              | Describes the planned transit network and schedule.                                     | Describes the current operational state.                                                                     |
| Typical information  | Routes, trips, stops, scheduled stop times, service calendars, and shapes.              | Vehicle positions, trip updates, predicted arrival/departure, delay, and service alerts.                     |
| Update behavior      | Relatively stable feed snapshots with validity dates and a feed version.                | Continuously changing feed data; AT documentation states that the feed is updated at least every 30 seconds. |
| Examples             | `routes.txt`, `trips.txt`, `stops.txt`, `stop_times.txt`, `calendar.txt`, `shapes.txt`. | Vehicle Position, Trip Update, and Service Alert messages, returned as JSON or Protocol Buffers.             |
| Role in this project | Provides the planned network and schedule reference.                                    | Provides current observations, predictions, and disruptions to compare with the plan.                        |

Static and Realtime need shared identifiers to be interpreted together. `trip_id` links a realtime trip update to a scheduled trip; `route_id` provides route context; and `stop_id` plus `stop_sequence` relates a realtime stop-time update to the planned stop sequence. These identifiers connect planned service with current operational state without treating realtime data as an independent network.

## 5. Core Domain Entities

| Entity           | Source                                               | Key Identifier                                 | Meaning                                                                              |
| ---------------- | ---------------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------ |
| Route            | Static `routes.txt`                                  | `route_id`                                     | A passenger-facing transit service or route grouping.                                |
| Trip             | Static `trips.txt`; Realtime Trip Update             | `trip_id`                                      | A scheduled journey and its current operational update when referenced in realtime.  |
| Stop             | Static `stops.txt`; Realtime stop-time update        | `stop_id`                                      | A stop, station, platform, or other scheduled location.                              |
| Stop Time        | Static `stop_times.txt`; Realtime `stop_time_update` | `trip_id` + `stop_sequence` and/or `stop_id`   | A trip's scheduled or updated relationship with a stop.                              |
| Vehicle          | Realtime Vehicle Position and Trip Update            | `vehicle.id`                                   | An operational vehicle identity; it is not a route identifier.                       |
| Vehicle Position | Realtime Vehicle Position                            | Vehicle/trip context + `timestamp`             | A reported geographic observation for a vehicle associated with a trip.              |
| Trip Update      | Realtime Trip Update                                 | `trip_id`                                      | Updated timing and relationship information for a trip and its stop-time updates.    |
| Service Alert    | Realtime Service Alert                               | Feed-provided alert identity or entity context | A passenger or operational alert affecting one or more transit services or entities. |

This table describes domain meaning only. It is not a final database schema or canonical event schema.

## 6. Important Identifiers

- **`route_id`**: identifies a route in the static feed and supplies route context for related trips and realtime records when present.
- **`trip_id`**: identifies a scheduled trip and is the main link between static trip data and a Trip Update.
- **`stop_id`**: identifies a stop or station record and links static stop times to a physical or logical location.
- **`vehicle.id`**: identifies a vehicle in a realtime message. AT documentation notes that vehicles may be swapped and that vehicle ID formats can change. It must not be used to infer a route.
- **`service_id`**: connects a static trip to recurring calendar rules and date exceptions.
- **`shape_id`**: connects a trip to its planned geographic shape.
- **`stop_sequence`**: identifies the order of a stop within a trip and helps match a realtime stop-time update to the scheduled sequence.

Identifiers are feed-scoped domain references. They should be interpreted using the relevant feed and message context rather than assumed to be globally permanent.

## 7. Event Time and Operational Time

The project needs to distinguish several kinds of time:

- **Scheduled time**: the planned arrival or departure in static `stop_times.txt`.
- **Predicted time**: the current expected arrival or departure reported by a Trip Update.
- **Actual or current observation time**: the time at which a vehicle position or other operational state is observed or reported.
- **`timestamp`**: a realtime timestamp associated with a feed entity or observation, such as a Vehicle Position timestamp.
- **Delay**: the difference between an observed or predicted time and the scheduled time, expressed according to the realtime message semantics.

Delay is not necessarily positive. A vehicle or stop-time prediction can be earlier than the scheduled time, so delay can be negative. The exact interpretation depends on whether the value describes arrival or departure and which scheduled reference is used.

These are domain concepts only. This section does not define watermarks, windows, state handling, or any other streaming implementation behavior.

## 8. Auckland Transport Specific Observations

The following facts are limited to the AT static snapshot and the AT Realtime API documentation supplied for this project.

- The static feed is delivered as GTFS CSV text files.
- The current static snapshot contains `routes.txt`, `trips.txt`, `stops.txt`, `stop_times.txt`, `shapes.txt`, `calendar.txt`, and related files listed above.
- The Realtime API can return JSON and also supports Protocol Buffers.
- Realtime field names use `snake_case` in the documented representation.
- AT documentation states that the Realtime feed is updated at least every 30 seconds.
- Trip Updates include trip and vehicle context and `stop_time_update` information, including arrival/departure and delay concepts where provided.
- Vehicle Positions include trip, vehicle, position, and timestamp information.
- A Vehicle Position should not be assumed to contain `stop_id` or other fields when the current payload does not provide them.
- Vehicle ID must not be used to infer a route because vehicles may be swapped and ID formats may change.
- Delay can be negative.
- `active_period` in a Service Alert may be `null` in current AT data.
- In this static snapshot, `fare_attributes.txt`, `fare_rules.txt`, and `frequencies.txt` are present but contain no data rows. This is a snapshot observation, not a permanent AT rule.
- The snapshot's feed dates and feed version are metadata for this downloaded feed and are not permanent business rules.

## 9. Static-Realtime Relationship

The complete domain chain can be read as two connected views of the same service:

```text
Static:
Route -> Trip -> Stop Times -> Stop

Realtime:
Trip Update -> Trip -> Stop Time Update -> predicted delay / arrival / departure

Vehicle Position -> Vehicle -> Trip -> Position -> Timestamp
```

Together, these concepts describe:

```text
planned service -> active trip -> current vehicle state -> observed or predicted service performance
```

The static feed establishes what was planned: which route a trip belongs to, which stops it should visit, in what order, and at what scheduled times. Realtime data adds what is happening now or what is currently predicted: where a vehicle is, how a trip is progressing, what arrival or departure is expected, and whether a service alert affects the network.

## 10. Domain Knowledge Relevant to This Project

These concepts are important because they define the meaning that later platform processing must preserve:

- **Kafka ingestion** needs event categories and stable identifiers so Vehicle Positions, Trip Updates, and Service Alerts can be recognized and related to the planned service.
- **Spark Structured Streaming** needs the distinction between event timestamps, scheduled times, predictions, and delays to reason about changing operational events.
- **Bronze / Silver / Gold** depends on separating original source observations from validated domain records and business-level results.
- **Route Delay Metrics** depends on joining trip and stop-time updates to route context and comparing predicted or observed timing with scheduled timing.
- **Vehicle Activity** depends on understanding vehicle identity, position, timestamp, and trip context without treating a vehicle ID as a route.
- **Service Reliability** depends on comparing scheduled service with observed trip behavior, delays, cancellations, and service alerts over an appropriate service period.

This section explains why the concepts matter; it does not define Kafka topics, Spark jobs, lake paths, or final schemas.

## 11. Day 2 Takeaways

- GTFS Static describes the planned transit network and schedule; GTFS Realtime describes the current operational state.
- The core static relationship is `Route -> Trip -> Stop Times -> Stop`.
- `service_id` determines when a trip is scheduled through weekly calendar rules and date exceptions.
- `shape_id` describes a planned path and is different from a live Vehicle Position.
- `trip_id`, `route_id`, and `stop_id` are the main links between static and realtime concepts.
- `stop_sequence` is essential for understanding the order of stops and matching stop-time updates.
- `vehicle.id` identifies a vehicle, not a route; vehicle assignments and ID formats can change.
- Scheduled time, predicted time, observation timestamp, and delay are different operational concepts.
- Delay can be negative, and realtime fields may be absent or null, so consumers must not infer unsupported facts.
- The current AT static snapshot includes several optional files with no data rows; this observation must not be generalized to all future feeds.
