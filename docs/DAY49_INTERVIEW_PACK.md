# Day 49 - Interview Pack

## Ten-minute project explanation

### 0:00-1:00 - Problem and scope

This project is a local-first transport data lakehouse for Auckland Transport
and Metlink. GTFS Static supplies planned routes, stops, trips, and schedules;
GTFS Realtime supplies vehicle positions, trip updates, and service alerts.
The goal is a reproducible batch and streaming platform rather than a dashboard.

### 1:00-2:30 - Architecture

Static and realtime adapters preserve raw Bronze data and attach provider
provenance. Static data is parsed into canonical Silver dimensions and Gold
metrics. Realtime events are published to Kafka, consumed by Spark Structured
Streaming, validated, watermarked, deduplicated, and routed to Silver/Gold or a
DLQ. PostgreSQL is a serving layer, not the primary lake store.

### 2:30-4:00 - Kafka design

Raw topics are separated by event family and keyed by the stable entity
identity, such as `vehicle_id` or `trip_id`. Kafka provides the replayable
decoupling boundary. The project uses at-least-once processing and makes
duplicates observable and removable through event identity plus watermark-aware
deduplication.

### 4:00-5:30 - Spark Structured Streaming

Spark reads Kafka as a streaming DataFrame. Validation checks JSON shape,
canonical fields, topic/event consistency, timestamps, coordinates, and event
identity. Event time is parsed from the payload, watermarks bound state, and
windows produce route-level delay metrics. Persistent checkpoints retain source
offset and query progress across restart.

### 5:30-6:30 - Lakehouse layers

Bronze preserves source payloads and ingestion metadata. Silver contains
normalized dimensions and realtime entities. Gold contains route delay, vehicle
activity, and service reliability products. Parquet and date/provider
partitions provide a simple analytical layout, while PostgreSQL supports
query-oriented serving.

### 6:30-7:30 - Reliability and data quality

HTTP failures use bounded retries. Kafka producer acknowledgements are
propagated. Invalid streaming records go to a DLQ with the original payload,
source topic, partition, offset, and validation reason. Checkpoints support
restart recovery, while the documentation explicitly avoids claiming exactly
once where the sink semantics do not provide it.

### 7:30-8:30 - Performance and trade-offs

The Spark benchmark measures shuffle partitions, input partitions, broadcast
joins, and output repartitioning. Results are workload-specific and local, so
they support an engineering decision rather than a universal tuning rule.
For example, the benchmark showed that the tested 200 shuffle-partition case
was slower than lower partition counts for this local workload.

### 8:30-9:20 - Reproducibility

Docker Compose starts Kafka, Spark Master/Worker, and PostgreSQL. The custom
Spark image fixes the Spark Kafka connector instead of downloading it during
submission. Day 47 uses an isolated topic and fresh checkpoint, then verifies
that a second event is consumed after query restart.

### 9:20-10:00 - Limitations and next steps

The platform is an interview-sized MVP. It does not claim production-scale
capacity, managed Kafka, exactly-once end-to-end delivery, centralized alerting,
or a fully mapped Metlink realtime contract. Those boundaries are deliberate
and documented.

## Common questions and concise answers

### Why Kafka and Spark together?

Kafka provides durable decoupling, replay, partitions, and consumer-group
coordination. Spark provides event-time processing, watermarks, stateful
deduplication, windows, and batch/streaming API consistency.

### Why use event time instead of ingestion time?

Transport events describe when a vehicle or trip update occurred. Event time
keeps late and out-of-order data semantically correct; ingestion time would
measure arrival rather than the operational event.

### What does the watermark do?

It bounds how long Spark retains event-time state. Events later than the
configured threshold may no longer update closed state, so the policy is a
correctness/latency trade-off and must be documented.

### How is a duplicate identified?

The canonical identity combines event type, provider, source entity, event
timestamp, and event-specific identifiers such as trip/stop or alert ID.
Unresolved identity is quarantined rather than guessed.

### What happens to malformed messages?

They are routed to `transport.events.dlq` with the raw message and source
metadata plus an explicit validation error.

### Why Parquet?

Parquet is columnar, self-describing, efficient for analytical scans, and
works locally and in S3 without requiring a proprietary table format.

### What does the checkpoint prove?

It proves Spark can retain source progress and resume a query with the same
checkpoint after a graceful restart. It does not prove exactly-once delivery
through every possible sink.
