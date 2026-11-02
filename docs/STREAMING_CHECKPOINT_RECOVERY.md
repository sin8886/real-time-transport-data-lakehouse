# Streaming Checkpoint and Recovery

## Overview

Day 31 implements persistent Spark Structured Streaming checkpoints
and verifies query restart and recovery with Kafka as the source.

Checkpoint files are stored outside the read-only project source mount.

- Host directory: `checkpoints/day31/`
- Spark container directory: `/opt/transport-lakehouse-checkpoints/day31/`
- Recovery test checkpoint:
  `/opt/transport-lakehouse-checkpoints/day31/recovery-test`

Checkpoint directories are excluded from Git.

## Checkpoint Responsibilities

Spark Structured Streaming checkpoints preserve query execution
metadata, including:

- Query metadata and identity.
- Kafka source offsets.
- Micro-batch commit information.
- Source progress required for query recovery.

When a query restarts with the same checkpoint location, Spark
recovers its previous progress instead of applying the initial
Kafka starting offset again.

The `starting_offsets` argument in `read_kafka_stream()` supports
`earliest` and `latest`. It applies when a new checkpoint is created.
An existing checkpoint determines the recovery offsets.

## Recovery Test

The recovery test uses a dedicated Kafka streaming query and checkpoint.

It performs these steps:

1. Starts a Kafka streaming query with a fresh checkpoint.
2. Publishes a synthetic vehicle-position event.
3. Verifies that the first event is consumed.
4. Stops the query gracefully while retaining its checkpoint.
5. Verifies checkpoint offset and commit metadata.
6. Restarts the query using the same checkpoint.
7. Publishes a second event.
8. Verifies that the recovered query consumes the second event.

Expected final result:

`RECOVERY TEST PASSED`

## Delivery Semantics

Checkpoint recovery restores Spark's recorded source progress.
It does not by itself guarantee end-to-end exactly-once delivery
for a Kafka sink.

The Kafka sink may exhibit at-least-once behavior in failure scenarios.
This Day 31 test verifies graceful query stop and restart recovery;
it does not claim exactly-once delivery.

## Scope

Day 31 covers checkpoint persistence and restart/recovery.

Watermarking, event deduplication, streaming Silver transformations,
and window aggregations are implemented in subsequent project days.
