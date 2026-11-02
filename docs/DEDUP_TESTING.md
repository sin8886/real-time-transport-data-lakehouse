# Streaming Deduplication Testing

## 1. Objective

Verify contract-based event identity and watermark-aware deduplication
in Spark Structured Streaming using an isolated Kafka topic.

## 2. Event Identity Rules

| Event type | Deduplication identity |
|---|---|
| vehicle_position | source_provider + source_entity_id + event_ts |
| trip_update | source_provider + trip_id + stop context + event_ts |
| service_alert | source_provider + alert_id + event_ts |

For Trip Updates, stop_sequence takes precedence over stop_id.

The event type is included in the technical identity serialization.

Events missing required identity components are marked
identity_unresolved and excluded from the deduplication branch.
No fallback identity is generated from vehicle_id, trip_id, or route_id alone.

## 3. Streaming Implementation

1. Read canonical events from Kafka.
2. Parse the event timestamp.
3. Build a contract-based technical deduplication identity.
4. Separate resolved and unresolved identities.
5. Apply a five-minute event-time watermark.
6. Deduplicate resolved events using dropDuplicatesWithinWatermark.
7. Persist deduplicated and unresolved test results separately.

## 4. Integration Test

Test topic: transport.dedup.test

Test script: scripts/dedup_test_spark_streaming.py

Ten synthetic events are published:
- Vehicle Position: one exact duplicate and one later event timestamp.
- Trip Update: one exact duplicate and a different stop sequence.
- Service Alert: one exact duplicate and a later lifecycle timestamp.
- One Vehicle Position with missing source_entity_id.

Expected results:
- vehicle_position: 2
- trip_update: 2
- service_alert: 2
- identity_unresolved: 1

Expected total: six deduplicated events and one unresolved event.

## 5. Verified Outcome

The Spark 3.5.3 integration test passed:
- Six resolved events remained after deduplication.
- Exact duplicate events were removed.
- Different event timestamps and stop contexts were retained.
- One unresolved event remained observable in a separate branch.
- Both streaming queries stopped cleanly.
- Spark reported the dedupeWithinWatermark state operator.
