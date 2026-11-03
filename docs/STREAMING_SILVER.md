# Streaming Silver Layer

## 1. Purpose and Scope

This document describes the Silver transformations for validated and
deduplicated GTFS Realtime events.

The implementation supports three canonical event types:

- vehicle_position
- trip_update
- service_alert

The Silver layer parses canonical JSON payloads into typed Spark columns,
preserves event provenance, and exposes event-specific records for
downstream processing.

This implementation does not replace upstream source validation,
event identity resolution, watermarking, or deduplication.

## 2. Input Requirements

The transformations consume a Spark DataFrame or streaming DataFrame
containing a `message_value` column with a canonical JSON event.

The expected upstream processing sequence is:

1. Read realtime events from Kafka.
2. Validate and normalize source records.
3. Parse event timestamps.
4. Build contract-based deduplication identities.
5. Separate resolved and unresolved identities.
6. Apply the event-time watermark.
7. Deduplicate resolved events.
8. Pass the deduplicated event stream to Silver transformations.

When `identity_status` is present, Silver retains only records whose
status is `resolved`.

The transformations do not independently apply a watermark or
deduplication operation.

## 3. Common Event Fields

All three Silver outputs preserve the following canonical fields:

| Field | Description |
|---|---|
| event_type | Canonical realtime event type |
| source_provider | Provider identity, such as AT or Metlink |
| source_entity_id | Original provider entity identifier when available |
| event_ts | Source event or observation timestamp |
| ingest_ts | Platform ingestion timestamp |

The implementation also preserves available processing provenance:

- dedup_identity
- identity_status
- event_time
- topic
- partition
- offset
- timestamp
- raw_message

Optional provenance columns are retained when present in the input.

## 4. Vehicle Position Silver

Function: `build_vehicle_position_silver`

The output contains:

| Field | Description |
|---|---|
| vehicle_id | Operational vehicle identifier |
| trip_id | Associated scheduled trip |
| route_id | Route context supplied by the event |
| latitude | Reported vehicle latitude |
| longitude | Reported vehicle longitude |
| speed | Reported vehicle speed |
| bearing | Reported vehicle direction |

The Vehicle Position model does not derive `route_id` from `vehicle_id`.
It does not infer `stop_id` from coordinates.

`stop_id` is intentionally excluded from this Silver output.

## 5. Trip Update Silver

Function: `build_trip_update_silver`

The output contains:

| Field | Description |
|---|---|
| trip_id | Scheduled trip being updated |
| route_id | Route context when supplied |
| vehicle_id | Associated vehicle when supplied |
| stop_id | Affected stop when supplied |
| stop_sequence | Stop position within the trip |
| scheduled_arrival_ts | Static scheduled arrival time when available |
| scheduled_departure_ts | Static scheduled departure time when available |
| predicted_arrival_ts | Realtime predicted arrival |
| predicted_departure_ts | Realtime predicted departure |
| delay_seconds | Reported delay in seconds |
| schedule_relationship | Relationship to scheduled service |

Scheduled timestamps are Static reference values defined by
Data Contract v1. The Day34 transformation only parses and normalizes
these fields; it does not perform a Static GTFS join.

Predicted timestamps remain separate from scheduled timestamps.

## 6. Service Alert Silver

Function: `build_service_alert_silver`

The output contains:

| Field | Description |
|---|---|
| alert_id | Canonical alert identifier when supplied |
| active_start_ts | Alert active-period start |
| active_end_ts | Alert active-period end |
| cause | Reported alert cause |
| effect | Reported service effect |
| description | Alert description |
| affected_route_id | Affected route when identified |
| affected_trip_id | Affected trip when identified |
| affected_stop_id | Affected stop when identified |

Data Contract v1 does not define a complete array or child-table
representation for alerts affecting multiple entities.

The current implementation preserves the three canonical affected
entity fields and does not expand multi-entity alert relationships.

## 7. Timestamp and Type Handling

The transformation parses canonical JSON using an explicit Spark schema.

The following fields are converted to Spark `TimestampType`:

- event_ts
- ingest_ts
- scheduled_arrival_ts
- scheduled_departure_ts
- predicted_arrival_ts
- predicted_departure_ts
- active_start_ts
- active_end_ts

Numeric fields are parsed using explicit numeric types:

- latitude, longitude, speed, bearing: double
- stop_sequence, delay_seconds: integer

Missing optional fields remain null.

The Spark session timezone controls timestamp interpretation.
A missing `event_ts` is never replaced with `ingest_ts`.

## 8. Data Quality and Contract Rules

The Silver transformations follow these rules:

1. Only the matching canonical event type enters each event-specific output.
2. Resolved identity records are retained when identity metadata is present.
3. Unresolved identity records are excluded from normal Silver outputs.
4. Missing route, trip, stop, vehicle, and alert identifiers are not invented.
5. `route_id` is never inferred from `vehicle_id`.
6. `stop_id` is never inferred from vehicle coordinates.
7. `event_ts` and `ingest_ts` remain separate.
8. Scheduled and predicted timestamps remain separate.
9. Optional source fields remain nullable.
10. Provider provenance and available Kafka metadata are preserved.

This module assumes upstream validation has already been performed.
It is not a replacement for the source validation or quarantine path.

## 9. Testing

Unit tests are maintained in:

`tests/unit/test_streaming_silver.py`

The test suite verifies:

- Vehicle Position field extraction and data types.
- Trip Update contract field preservation.
- Service Alert field and affected-entity preservation.
- Isolation between the three event-specific outputs.
- Missing event timestamps are not backfilled.
- Missing route identifiers are not inferred.
- Unresolved identity events are excluded.
- Service Alert active-period timestamps are converted.
- The required `message_value` input column is enforced.

The Day34 targeted test suite currently contains nine tests.

## 10. Known Limitations

- The transformation expects canonical JSON events in `message_value`.
- Upstream validation, watermarking, and deduplication are separate stages.
- Static GTFS joins are not performed by these transformations.
- Service Alert multi-entity cardinality is not modeled in v1.
- Invalid or malformed payloads are excluded from normal event-specific
  outputs; a separate quarantine/DLQ implementation is outside this
  module's scope.
- Timestamp interpretation depends on the Spark session timezone.
