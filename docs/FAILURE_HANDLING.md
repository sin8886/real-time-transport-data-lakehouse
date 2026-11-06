# Failure Handling and Reliability

## 1. Overview

This document describes failure handling across the transport
data ingestion and streaming pipeline.

The reliability strategy includes:

- Bounded HTTP retries with exponential backoff.
- Kafka producer acknowledgement and error propagation.
- Structured Streaming checkpoint recovery.
- Schema validation and Dead Letter Queue (DLQ) routing.

## 2. HTTP Ingestion Failures

RealtimeFetcher uses bounded retries.

Default configuration:

- Maximum attempts: 3
- Initial backoff: 0.5 seconds
- Backoff strategy: exponential
- Request timeout: 10 seconds

Retryable failures:

| Failure | Action |
|---|---|
| HTTP 429 | Retry |
| HTTP 5xx | Retry |
| ConnectionError | Retry |
| Timeout | Retry |

Non-retryable failures:

| Failure | Action |
|---|---|
| HTTP 4xx (except 429) | Fail immediately |
| Other RequestException | Fail immediately |
| Invalid response Content-Type | Fail |
| Invalid response payload | Fail during parsing |

Backoff sequence:

| Attempt | Wait |
|---|---:|
| 1 | 0.5 seconds |
| 2 | 1.0 seconds |
| 3 | No further retry |

The maximum attempt count includes the initial request.

Retry-After response headers are not currently interpreted.
The client uses its configured exponential backoff instead.

Exhausted retries raise RealtimeHTTPError.

## 3. Kafka Producer Failures

Kafka producers use synchronous acknowledgement through
the producer future.

Reliability behavior:

- Wait for message acknowledgement.
- Propagate send/acknowledgement failures.
- Do not silently treat failed messages as published.
- Flush after successful publishing.
- Empty batches do not produce messages.

A Kafka send failure is propagated to the caller.
Automatic producer-level retry is not implemented
by the application producer wrappers.

## 4. Streaming Validation and DLQ

Incoming Kafka messages are validated before downstream
processing.

Validation covers:

- JSON structure.
- Required canonical fields.
- Event type and source topic consistency.
- Data types and numeric constraints.
- Timezone-aware timestamps.
- Geographic coordinate ranges.
- Trip update identity requirements.

Routing behavior:

| Message | Destination |
|---|---|
| Valid event | Validated Kafka topic |
| Invalid event | Dead Letter Queue |

DLQ records preserve:

- Original message.
- Source topic.
- Source partition.
- Source offset.
- Kafka timestamp.
- Error type.
- Error message.

DLQ records allow invalid messages to be investigated
without silently discarding their original payloads.

## 5. Structured Streaming Recovery

Spark Structured Streaming uses persistent checkpoints.

Checkpoints preserve:

- Query metadata.
- Kafka source offsets.
- Micro-batch commit information.
- Recovery progress.

Restarting with the same checkpoint allows Spark to
recover previously recorded source progress.

Checkpoint recovery does not independently guarantee
end-to-end exactly-once delivery.

Kafka sink delivery may exhibit at-least-once behavior
under failure scenarios.

See STREAMING_CHECKPOINT_RECOVERY.md for recovery procedures.

## 6. Failure Matrix

| Component | Failure | Handling |
|---|---|---|
| HTTP ingestion | Timeout | Bounded retry |
| HTTP ingestion | Connection failure | Bounded retry |
| HTTP ingestion | HTTP 429 | Bounded retry |
| HTTP ingestion | HTTP 5xx | Bounded retry |
| HTTP ingestion | HTTP 4xx | Fail immediately |
| Kafka producer | ACK failure | Propagate exception |
| Kafka consumer | Invalid JSON | DLQ |
| Kafka consumer | Schema violation | DLQ |
| Spark streaming | Query restart with existing checkpoint | Recover previously recorded source progress |
| Kafka sink | Delivery failure | Query failure; recovery depends on checkpoint and sink semantics |

## 7. Operational Limitations

The current implementation does not provide:

- HTTP Retry-After header handling.
- Automatic application-level Kafka producer retry.
- Automatic DLQ replay.
- End-to-end exactly-once guarantees.
- A centralized alerting and incident management system.

These limitations should be considered when operating
the pipeline in a production environment.

## 8. Reliability Verification

Reliability is verified through unit and integration tests
covering:

- HTTP retry behavior.
- Exponential backoff.
- Retry exhaustion.
- Non-retryable HTTP failures.
- Kafka producer acknowledgement failures.
- Message validation.
- DLQ routing.
- Checkpoint recovery.

Integration tests requiring external Kafka, S3, or PostgreSQL
services are executed separately from isolated unit tests.
