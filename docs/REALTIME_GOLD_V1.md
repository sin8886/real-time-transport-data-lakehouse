# Realtime Gold v1 — Route Delay Window Metrics

## Day 35 scope

Realtime Gold v1 aggregates Trip Update Silver events into route-level
event-time windows of 5, 15, and 60 minutes.

## Input

The input is the Trip Update Silver DataFrame produced by
`build_trip_update_silver`.

Required fields:

- `route_id`
- `event_ts`
- `delay_seconds`

`event_ts` is the event-time column. `ingest_ts` is not used as a substitute.

## Metrics

Each window is grouped by `route_id` and contains:

- `window_start`: inclusive window start timestamp
- `window_end`: exclusive window end timestamp
- `route_id`: route identifier
- `avg_delay_seconds`: average reported delay in seconds
- `event_count`: number of valid delay observations

Negative delay values are retained because they represent early running
relative to schedule.

Rows with a missing or blank route ID, missing event timestamp, or missing
delay value are excluded from the aggregation.

## Window configuration

Call `build_route_delay_gold` independently for each supported duration:

- `5 minutes`
- `15 minutes`
- `60 minutes`

The function applies a five-minute event-time watermark to `event_ts` by
default. The watermark delay can be overridden explicitly.

## Output mode and downstream use

This is a stateful Spark Structured Streaming aggregation. For a streaming
input, select an output mode and checkpoint strategy appropriate to the
downstream sink. This module defines the transformation; it does not start a
streaming query or write to Kafka, S3, or PostgreSQL.
