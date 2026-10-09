# Data Contract v1

## 1. Purpose and Scope

This document defines the provider-neutral canonical contract for the project's GTFS Static reference data and GTFS Realtime events.

The contract is intentionally limited to facts supported by the Day 2/3/4 project materials:

- Auckland Transport (AT) Static and Realtime validation.
- Metlink / Greater Wellington Static and Realtime validation.
- The GTFS domain relationships recorded in `GTFS_DOMAIN_NOTES.md`.

This is a v1 data contract, not an implementation specification. It does not define Kafka topics, Spark jobs, storage layouts, APIs, SLAs, or provider quotas.

### Evidence labels

- **Verified fact**: observed in a local feed/validation result or explicitly documented in the project evidence.
- **v1 design decision**: a normalization rule needed to provide one canonical model; it is not claimed to be a provider guarantee.
- **Not verified**: intentionally left open because the Day 2/3/4 evidence did not establish it.

## 2. Static Domain and Realtime Boundary

Static GTFS describes planned service and is the reference domain:

```text
Route -> Trip -> Stop Times -> Stop
```

Realtime describes changing operational state. The canonical realtime event types are:

- `vehicle_position`
- `trip_update`
- `service_alert`

Realtime records may reference Static entities, but they do not replace the Static records. A realtime message must not create a missing route, trip, or stop merely because it contains an identifier.

## 3. Canonical Event Envelope

The following fields apply to every canonical realtime event. Fields marked as platform-added are not source payload fields.

| Field              | Type        | Required / nullable | Meaning                                                                                                | Source/provider mapping                                                                                                                                                                 |
| ------------------ | ----------- | ------------------- | ------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `event_type`       | string enum | Required, non-null  | One of `vehicle_position`, `trip_update`, `service_alert`.                                             | **v1 design decision** derived from the GTFS-RT entity type. AT returned all three types; Metlink verified all three endpoint/entity categories.                                        |
| `source_provider`  | string      | Required, non-null  | Provider identity, at minimum `AT` or `Metlink`.                                                       | **v1 design decision**. Required to distinguish the two feeds. Mapped from the fetch configuration, not inferred from route or vehicle IDs.                                             |
| `source_entity_id` | string      | Optional, nullable  | Provider feed entity identifier when present.                                                          | AT validation observed entity `id` for Trip Updates, Vehicle Positions, and Service Alerts. Metlink entity identifier presence was not field-level verified.                            |
| `event_ts`         | timestamp   | Optional, nullable  | Source event/observation time used for event-time interpretation.                                      | AT validation observed timestamps on Vehicle Position and Service Alert entities and `timestamp` where present in Trip Updates. Metlink timestamp mapping was not field-level verified. |
| `ingest_ts`        | timestamp   | Required, non-null  | Time at which the project accepted the source record for ingestion. This is not the source event time. | **v1 design decision**, added by the platform.                                                                                                                                          |
| `is_deleted`       | boolean     | Optional, nullable  | Source deletion/tombstone indicator when supplied.                                                     | AT validation observed `is_deleted` on Trip Update and Vehicle Position entities. Presence for alerts and Metlink was not verified.                                                     |

`event_ts` and `ingest_ts` must remain separate. A missing source timestamp must not be replaced silently with `ingest_ts`.

## 4. Canonical Event Schemas

The canonical grain for `trip_update` is one record for one trip/stop-time update when the source provides stop-time updates. A provider envelope may contain multiple such updates and therefore may produce multiple canonical records. This is a **v1 design decision** to make stop-level timing usable while retaining the source entity identity.

### 4.1 `vehicle_position`

| Field              | Type        | Required / nullable | Meaning                                                                               | Source/provider mapping                                                                                                     |
| ------------------ | ----------- | ------------------- | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `event_type`       | string enum | Required, non-null  | Always `vehicle_position`.                                                            | Canonical envelope.                                                                                                         |
| `source_provider`  | string      | Required, non-null  | AT or Metlink.                                                                        | Canonical envelope.                                                                                                         |
| `source_entity_id` | string      | Optional, nullable  | Feed entity identity.                                                                 | AT entity `id`; Metlink not verified.                                                                                       |
| `event_ts`         | timestamp   | Optional, nullable  | Time associated with the vehicle observation.                                         | AT Vehicle Position entity `timestamp`; Metlink not verified.                                                               |
| `ingest_ts`        | timestamp   | Required, non-null  | Project ingestion time.                                                               | Platform-added.                                                                                                             |
| `vehicle_id`       | string      | Optional, nullable  | Operational vehicle identity. It is not a route identity.                             | AT `vehicle.id` observed and documented; Metlink field-level mapping not verified.                                          |
| `trip_id`          | string      | Optional, nullable  | Scheduled trip associated with the vehicle.                                           | AT vehicle trip context documented; Metlink field-level mapping not verified.                                               |
| `route_id`         | string      | Optional, nullable  | Route context supplied by the realtime message or resolved from a referenced trip.    | AT trip context can include `route_id`; do not infer it from `vehicle_id`. Metlink realtime mapping not verified.           |
| `latitude`         | decimal     | Optional, nullable  | Reported vehicle latitude.                                                            | AT `position.latitude` observed.                                                                                            |
| `longitude`        | decimal     | Optional, nullable  | Reported vehicle longitude.                                                           | AT `position.longitude` observed.                                                                                           |
| `speed`            | decimal     | Optional, nullable  | Reported vehicle speed, with source units preserved/documented by the source adapter. | AT `position.speed` observed in the validation response, but documentation says availability may differ. Treat as optional. |
| `bearing`          | decimal     | Optional, nullable  | Reported direction/bearing when supplied.                                             | AT documentation says it is conditional on AVL data; not required.                                                          |

`stop_id` is intentionally not part of the required Vehicle Position model. AT documentation says Vehicle Position does not provide it, and the validation did not infer it from coordinates.

### 4.2 `trip_update`

| Field                    | Type        | Required / nullable                                       | Meaning                                                        | Source/provider mapping                                                                                                                                                                         |
| ------------------------ | ----------- | --------------------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `event_type`             | string enum | Required, non-null                                        | Always `trip_update`.                                          | Canonical envelope.                                                                                                                                                                             |
| `source_provider`        | string      | Required, non-null                                        | AT or Metlink.                                                 | Canonical envelope.                                                                                                                                                                             |
| `source_entity_id`       | string      | Optional, nullable                                        | Feed entity identity.                                          | AT entity `id` observed; Metlink not verified.                                                                                                                                                  |
| `event_ts`               | timestamp   | Optional, nullable                                        | Time associated with the trip update.                          | AT `trip_update.timestamp` where present; Metlink not verified.                                                                                                                                 |
| `ingest_ts`              | timestamp   | Required, non-null                                        | Project ingestion time.                                        | Platform-added.                                                                                                                                                                                 |
| `trip_id`                | string      | Required for an accepted stop-level Trip Update, non-null | Scheduled trip being updated.                                  | AT `trip_update.trip.trip_id` observed and documented; both providers expose the core Static identifier in the validated domain, but Metlink realtime field-level mapping remains not verified. |
| `route_id`               | string      | Optional, nullable                                        | Route context supplied by the trip update.                     | AT `trip_update.trip.route_id` observed. Never derive it from `vehicle_id`. Metlink not verified.                                                                                               |
| `vehicle_id`             | string      | Optional, nullable                                        | Vehicle associated with the trip update.                       | AT vehicle context is documented; Metlink not verified.                                                                                                                                         |
| `stop_id`                | string      | Optional, nullable                                        | Stop affected by this stop-time update.                        | AT `stop_time_update.stop_id` observed.                                                                                                                                                         |
| `stop_sequence`          | integer     | Optional, nullable                                        | Position of the stop within the trip.                          | AT `stop_time_update.stop_sequence` observed; Static `stop_times` uses the same relationship.                                                                                                   |
| `scheduled_arrival_ts`   | timestamp   | Optional, nullable                                        | Scheduled arrival used as the Static reference.                | **v1 design decision**: obtained by joining `trip_id` and stop context to Static `stop_times`; not necessarily a realtime source field.                                                         |
| `scheduled_departure_ts` | timestamp   | Optional, nullable                                        | Scheduled departure used as the Static reference.              | **v1 design decision** from Static `stop_times`.                                                                                                                                                |
| `predicted_arrival_ts`   | timestamp   | Optional, nullable                                        | Current predicted arrival.                                     | AT `arrival` time concept documented/observed; exact sampled scalar shape was not retained in the validation report.                                                                            |
| `predicted_departure_ts` | timestamp   | Optional, nullable                                        | Current predicted departure.                                   | AT `departure` time concept documented/observed; Metlink field-level mapping not verified.                                                                                                      |
| `delay_seconds`          | integer     | Optional, nullable                                        | Delay associated with the update, in seconds; may be negative. | AT `trip_update.delay` and stop-time delay documented/observed.                                                                                                                                 |
| `schedule_relationship`  | string      | Optional, nullable                                        | Relationship between realtime and scheduled service.           | AT values including scheduled and cancelled were documented/observed. Do not assume a complete cross-provider enum set in v1.                                                                   |

A Trip Update with neither `trip_id` nor an identifiable stop-time context is not an accepted canonical stop-level record; it must be flagged or quarantined until a provider mapping is established.

### 4.3 `service_alert`

| Field               | Type        | Required / nullable | Meaning                                                     | Source/provider mapping                                                                                                     |
| ------------------- | ----------- | ------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `event_type`        | string enum | Required, non-null  | Always `service_alert`.                                     | Canonical envelope.                                                                                                         |
| `source_provider`   | string      | Required, non-null  | AT or Metlink.                                              | Canonical envelope.                                                                                                         |
| `source_entity_id`  | string      | Optional, nullable  | Feed entity identity.                                       | AT entity `id` observed; Metlink not verified.                                                                              |
| `event_ts`          | timestamp   | Optional, nullable  | Source time associated with the alert.                      | AT alert entity `timestamp` observed; Metlink not verified.                                                                 |
| `ingest_ts`         | timestamp   | Required, non-null  | Project ingestion time.                                     | Platform-added.                                                                                                             |
| `alert_id`          | string      | Optional, nullable  | Canonical alert identity when the source supplies one.      | **v1 design decision**: AT entity `id` is the verified source mapping. Metlink alert identity was not field-level verified. |
| `active_start_ts`   | timestamp   | Optional, nullable  | Start of the alert's active period.                         | AT `active_period.start`; may be null.                                                                                      |
| `active_end_ts`     | timestamp   | Optional, nullable  | End of the alert's active period.                           | AT `active_period.end`; may be null.                                                                                        |
| `cause`             | string      | Optional, nullable  | Provider-reported alert cause.                              | AT `alert.cause` observed.                                                                                                  |
| `effect`            | string      | Optional, nullable  | Provider-reported service effect.                           | AT `alert.effect` observed.                                                                                                 |
| `description`       | string      | Optional, nullable  | Passenger-facing or operational description.                | AT `alert.description_text` observed.                                                                                       |
| `affected_route_id` | string      | Optional, nullable  | Route affected by the alert when the source identifies one. | **v1 design decision** from a matching `informed_entity`; exact Metlink mapping not verified.                               |
| `affected_trip_id`  | string      | Optional, nullable  | Trip affected by the alert when the source identifies one.  | **v1 design decision** from a matching `informed_entity`; exact Metlink mapping not verified.                               |
| `affected_stop_id`  | string      | Optional, nullable  | Stop affected by the alert when the source identifies one.  | **v1 design decision** from a matching `informed_entity`; exact Metlink mapping not verified.                               |

An alert may affect more than one entity. v1 does not define an array or child-table representation for all affected entities; additional affected-entity modeling is a known gap.

## 5. Core Business and Entity Identifiers

| Identifier   | Type   | Scope and meaning                                                                                                 | Rules                                                                                         |
| ------------ | ------ | ----------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `route_id`   | string | Identifies a route in a provider's Static feed and provides route context for related trips and realtime records. | Feed/provider scoped. Never infer from `vehicle_id`.                                          |
| `trip_id`    | string | Identifies a scheduled trip and links Static `trips` to Trip Updates.                                             | Feed/provider scoped; interpret with `source_provider` and the relevant Static snapshot.      |
| `stop_id`    | string | Identifies a stop, station, platform, or other scheduled location.                                                | Feed/provider scoped; do not infer for Vehicle Position when absent.                          |
| `vehicle_id` | string | Identifies an operational vehicle in a realtime message.                                                          | Provider/source scoped. Vehicle assignments and ID formats can change; it is not a route key. |

These identifiers are strings even when a provider's values contain only digits. No identifier is assumed to be globally unique across AT and Metlink.

## 6. Primary and Business Key Rules

The canonical business key is composite and begins with `source_provider`. Provider identifiers must not be merged across providers solely because their text values match.

- Static Route: `source_provider + route_id`.
- Static Trip: `source_provider + trip_id`.
- Static Stop: `source_provider + stop_id`.
- Realtime vehicle context: `source_provider + vehicle_id`, only within the source's observed/relevant time range. It is not a permanent vehicle master key.
- A Trip Stop context: `source_provider + trip_id + (stop_sequence when present, otherwise stop_id)`.

These are v1 identity rules for the canonical model, not claims that providers guarantee global uniqueness or immutable identifiers.

## 7. Event Identity and Deduplication

The source evidence does not establish a universal immutable event ID or a complete cross-provider deduplication rule. v1 therefore uses source identity plus event time only when the required components are present, and does not invent a fallback hash as a business identity.

| Event type         | Preferred v1 event identity                                                                                                     | Deduplication rule                                                                                                                  | Limitation                                                                                                                                                               |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vehicle_position` | `source_provider + source_entity_id + event_ts` when all three exist.                                                           | Treat repeated occurrences of that identity as duplicates; retain the first/selected record according to a later processing policy. | AT entity ID and timestamp are verified. Metlink field-level identity mapping is not verified. Without the components, no reliable provider-neutral identity is defined. |
| `trip_update`      | `source_provider + trip_id + stop context + event_ts`, where stop context is `stop_sequence` when present, otherwise `stop_id`. | Deduplicate only when `trip_id`, stop context, and `event_ts` are present.                                                          | A trip update can contain multiple stop-time updates. Missing timestamps or stop context prevent a reliable v1 identity.                                                 |
| `service_alert`    | `source_provider + alert_id + event_ts` when both are present.                                                                  | Repeated occurrences of the same source alert at the same event time are duplicates.                                                | AT entity ID/timestamp are verified. Alert lifecycle changes may legitimately produce different timestamps; active-period-only fallback is intentionally not defined.    |

A record that cannot meet its preferred identity must remain observable as `identity_unresolved` or be quarantined by the future implementation. It must not be silently deduplicated using `vehicle_id`, `trip_id`, or route/stop alone.

## 8. Timestamp Semantics

- `event_ts` is source event/observation time when supplied. For Vehicle Position it represents the vehicle observation timestamp; for Trip Update it represents the update timestamp; for Service Alert it represents the alert record timestamp.
- `ingest_ts` is assigned by the project at acceptance/ingestion time and is always distinct from `event_ts`.
- Static scheduled arrival/departure are planned times from `stop_times.txt`. They are not event timestamps.
- Predicted arrival/departure are realtime predictions from a Trip Update.
- `delay_seconds` is the provider-reported delay or the corresponding canonical value. It may be negative.
- A missing or nullable source timestamp remains null. The contract does not backfill it from ingestion time.
- The exact timezone normalization and precision policy are not fixed by Day 2/3/4 evidence and remain a gap.

## 9. Source and Provenance Requirements

Every canonical realtime record must carry `source_provider` and `ingest_ts`. The ingestion process should also retain the following provenance outside or alongside the canonical event where available:

| Provenance field         | Status and purpose                                                                                                                                                           |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `source_provider`        | Required canonical field. Values distinguish AT from Metlink.                                                                                                                |
| `source_entity_id`       | Optional canonical field. Preserves the provider entity ID when present.                                                                                                     |
| `source_endpoint`        | Optional ingestion metadata. AT combined endpoint and the three validated Metlink endpoint paths were recorded; endpoint is not part of event meaning.                       |
| `source_format`          | Optional ingestion metadata. JSON was observed for both validations; AT documentation also supports Protocol Buffers. Metlink Protocol Buffer behavior was not verified.     |
| `source_fetched_at`      | Optional ingestion metadata. If retained, it is the fetch time and must not replace `event_ts` or `ingest_ts`.                                                               |
| Static feed version/date | Static snapshot provenance, not a realtime event field. The project evidence confirms these values exist in `feed_info.txt`, but does not define a canonical version format. |

API keys, subscription headers, and other secrets are never data-contract fields and must not be stored in events or documentation.

## 10. Provider-Specific Fields

Provider-specific fields may be preserved in Bronze/raw data or a provider extension area, but they are not required canonical fields in v1.

| Provider-specific or source-specific field                                | Evidence                                                                                | v1 treatment                                                                                  |
| ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| AT `vehicle.label`, `vehicle.license_plate`                               | Observed in the AT Vehicle Position field summary.                                      | Preserve only as an optional AT extension; do not require canonically.                        |
| AT `effect_detail`, `header_text`, `severity_level`                       | Observed in the AT Service Alert field summary.                                         | Preserve only as an optional AT extension; do not force into the canonical alert schema.      |
| AT `direction_id`, `start_date`, `start_time` in Trip Update trip context | Observed in AT validation.                                                              | Optional provider/source extension unless a later cross-provider mapping justifies promotion. |
| AT `contract_id`, `trip_short_name`, and additional Static columns        | Observed in the AT Static snapshot.                                                     | Static provider-specific fields; retain in raw/provider-specific structures.                  |
| Metlink `etm_id`, `stop_patterns.txt`, `stop_pattern_trips.txt`           | Observed in the Metlink Static snapshot.                                                | Metlink-specific Static structures; not canonical realtime fields.                            |
| `active_period`, `informed_entity`, and other nested source structures    | AT fields/concepts were observed, but full cross-provider cardinality was not verified. | Normalize only the fields defined above; preserve the complete raw structure separately.      |

## 11. Contract Invariants and Basic Validation

The following checks are in scope for v1:

1. `event_type`, `source_provider`, and `ingest_ts` are present and non-null on every canonical event.
2. `event_type` is exactly one of the three canonical values.
3. `source_provider` identifies the provider; AT and Metlink identifiers are never treated as one shared namespace.
4. All canonical identifiers are strings and are not trimmed into numeric types.
5. `vehicle_position` latitude and longitude, when present, are valid numeric coordinates. A missing coordinate is nullable; it is not inferred from another field.
6. `vehicle_position` must not derive `stop_id` from coordinates or derive `route_id` from `vehicle_id`.
7. `trip_update` must contain `trip_id` for an accepted stop-level canonical record. `stop_id` and `stop_sequence` may be nullable individually, but a stop context is needed for the preferred event identity.
8. `delay_seconds`, when present, is an integer and may be negative.
9. Predicted timestamps and Static scheduled timestamps remain separate fields.
10. `active_start_ts` and `active_end_ts` are nullable. Null alert bounds are valid source data and are not replaced with ingestion time.
11. `ingest_ts` must not be used as a substitute for missing `event_ts`.
12. Records failing parsing, required-field, type, identity, or referential checks are rejected from normal Silver/Gold outputs and retained in a future quarantine/DLQ path with failure reason.
13. When Static referential validation is applied, a supplied `route_id`, `trip_id`, or `stop_id` must match the relevant provider's Static snapshot; the contract does not require a realtime record to contain all three identifiers.

## 12. Known Gaps and Intentionally Undefined Items

The following are deliberately not defined in Data Contract v1:

- A universal provider-independent immutable event ID.
- A reliable fallback deduplication identity when source entity ID, event timestamp, or stop context is missing.
- Metlink realtime field-level mappings for timestamps, IDs, nested trip/vehicle/alert fields, and optional values. Day 4 verified endpoint access, JSON, and entity categories only.
- A complete enum mapping for `cause`, `effect`, and `schedule_relationship` across providers.
- A complete representation of alerts affecting multiple routes, trips, or stops.
- A canonical vehicle master record or vehicle lifecycle/history model.
- A timezone, timestamp precision, or daylight-saving normalization policy.
- A canonical source feed version for realtime payloads.
- Whether `speed` is consistently available across AT requests or modes; the AT documentation and one live response differed.
- API quotas, throttling, refresh cadence measurements, retention permissions, and replay policy.
- Kafka, Spark, S3, PostgreSQL, DLQ implementation details, watermark settings, and window definitions.

These gaps require additional source observations or implementation evidence before they can be promoted into a later contract version.
