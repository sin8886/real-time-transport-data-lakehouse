# Event-Time Watermark Testing

## 1. Objective

Validate Spark Structured Streaming event-time processing with a watermark.

The test covers:
- Parsing canonical `event_ts` values into Spark timestamps.
- Applying a five-minute event-time watermark.
- Processing events that arrive out of order but remain within the watermark delay.
- Excluding an event that arrives after the watermark has advanced beyond its event time.
- Finalizing one-minute event-time windows.

This is a focused Day 32 integration test. It does not implement the production streaming deduplication or Silver-layer processing planned for later project stages.

## 2. Implementation

Production functions are implemented in:

`src/transport_lakehouse/streaming/spark_kafka_consumer.py`

### `parse_event_time()`

Extracts `event_ts` from the Kafka message JSON and converts it into a Spark `timestamp` column named `event_time`.

If the timestamp is missing, the resulting `event_time` is null.

### `apply_event_time_watermark()`

Applies a watermark to the `event_time` column.

Default delay threshold:

`5 minutes`

An empty or whitespace-only threshold raises `ValueError`.

## 3. Integration Test Configuration

Test script:

`scripts/watermark_test_spark_streaming.py`

| Setting | Value |
|---|---|
| Kafka topic | `transport.watermark.test` |
| Watermark delay | 5 minutes |
| Window duration | 1 minute |
| Starting offsets | `latest` |
| Checkpoint directory | `/opt/transport-lakehouse-checkpoints/day32/watermark-test` |
| Results file | `/opt/transport-lakehouse-checkpoints/day32/watermark-results.jsonl` |

The test uses a dedicated Kafka topic and checkpoint directory. The script expects a fresh checkpoint and refuses to reuse an existing non-empty test checkpoint.

## 4. Test Scenario

The integration test publishes synthetic events in phases:

1. Publish events at `10:00:10` and `10:00:30`. Both belong to the same one-minute window.
2. Publish an event at `10:07:00` to advance the maximum observed event time.
3. Publish an out-of-order event at `10:03:00`, which remains within the watermark delay.
4. Publish an event at `09:50:00`, which is too late after the watermark has advanced.
5. Publish an event at `10:15:00` to advance event time.
6. Publish a final event at `10:20:00` to advance the watermark and allow eligible windows to be finalized.

The watermark is based on the maximum observed event time minus the configured delay threshold. It is not simply a fixed cutoff based on wall-clock time.

## 5. Expected Results

The integration test verifies:

- The `10:00` window contains 2 events.
- The `10:03` window contains 1 event.
- The `09:50` window is absent from the finalized results.
- The test exits successfully when these assertions pass.

The `10:07` window is also expected in the output because an event was published for that window.

## 6. Validation Results

Day 32 integration test:

- Watermark advanced to `2026-10-29T10:15:00.000Z`.
- Finalized `10:00` window: 2 events.
- Finalized `10:03` window: 1 event.
- Finalized `10:07` window: 1 event.
- The too-late `09:50` event was absent.
- Integration script completed with exit code 0.

Unit tests cover timestamp parsing, missing timestamps, watermark application, and empty-threshold validation.

Full project validation:

- Ruff: all checks passed.
- Pytest: 317 passed, 11 skipped.

The skipped tests require optional S3 integration configuration or PostgreSQL integration environment variables.

## 7. Important Notes

A watermark tracks event-time progress using the maximum event time observed by the streaming query. It helps Spark manage state for stateful streaming operations.

The delay threshold is not a guarantee that every event arriving later than five minutes will always be discarded in every streaming scenario. Actual late-data handling depends on the stateful operation and the watermark at processing time.

This test uses a temporary window aggregation to verify watermark behavior. It does not introduce the production window metrics planned for a later project day.
