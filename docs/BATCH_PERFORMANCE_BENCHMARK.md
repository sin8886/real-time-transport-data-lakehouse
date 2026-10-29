# Batch Performance Benchmark v1

## Status

The real AT data benchmark has been executed successfully. The authoritative
machine-readable report is
[`reports/benchmarks/batch_performance_v1.json`](../reports/benchmarks/batch_performance_v1.json).
The report status is `completed`; this document summarizes its measured
results and does not replace the raw report.

## Purpose and scope

Day 23 establishes a repeatable local baseline for:

1. Parquet layout: unpartitioned output versus `partitionBy("route_id")`.
2. Parquet file size: `repartition(1)`, `repartition(2)`, and
   `repartition(8)` in temporary output.
3. Join execution: the existing Day 17 four-table Join and the existing
   active-trip filtering Join used by the Gold batch.

The benchmark does not modify production code, Gold output, or Spark
production configuration. It does not use broadcast joins, Join hints, or
production repartition tuning. Those topics remain in scope for Day 40.

## Implementation

The manual runner is [batch_performance.py](../benchmarks/batch_performance.py).
It reuses the existing functions in
[spark_batch.py](../src/transport_lakehouse/processing/spark_batch.py) and
[gold_metrics.py](../src/transport_lakehouse/processing/gold_metrics.py).

The default input is `data/gtfs/at`. Every case writes to a unique temporary
directory created with `tempfile.mkdtemp`. The temporary Parquet directory is
removed after the JSON report is written. Results are written separately to
`reports/benchmarks/batch_performance_v1.json`, which is ignored by Git.

The recorded run used Spark/PySpark 3.5.9, Python 3.12.3, Java 17,
`local[*]`, and a 16-CPU WSL2 Linux environment. The input produced
1,163,934 trip-stop events and used `OUT-202` as the fixed route filter.

## Partition benchmark

The input is the Day 17 trip-stop events DataFrame.

| Case | Layout |
|---|---|
| `baseline_unpartitioned` | ordinary Parquet write |
| `partitioned_by_route_id` | `partitionBy("route_id")` |

The same input lineage, schema, Spark session, and fixed non-empty `route_id`
filter are used for both cases. The filter value is selected from the real
input and recorded in the report.

Day 17 events do not contain `service_date`. The runner does not add or infer
that field. A `service_date` partition case is therefore intentionally absent
from this v1 implementation rather than creating a misleading benchmark.

Each case records:

- write time;
- all-row and filtered-read time;
- returned row counts;
- output partition count;
- Parquet file count and byte-size distribution;
- partition directory counts and per-directory rows for the route layout,
  using the validated single `groupBy("route_id").count()` distribution;
- formatted explain output and observed `PartitionFilters` text.

Directory count is not treated as proof of effective partition pruning.

Recorded median write times from the report:

| Case | Median write time |
|---|---:|
| `baseline_unpartitioned` | 1,149.749 ms |
| `partitioned_by_route_id` | 3,503.125 ms |

## File-size benchmark

The same events DataFrame is written after temporary repartitioning:

```text
repartition(1)
repartition(2)
repartition(8)
```

Each case has an independent output directory. The report records requested
and observed output partitions, write time, all-row read time, fixed-route
filtered read time, file count, total size, min/max/mean/median size, and
the output partition count.

Recorded median write times from the report:

| Case | Median write time |
|---|---:|
| `repartition(1)` | 2,295.215 ms |
| `repartition(2)` | 1,904.678 ms |
| `repartition(8)` | 1,510.905 ms |

PySpark's lazy execution does not provide a reliable isolated shuffle timing
from this runner without changing the execution behavior with caching or an
extra action. Therefore `repartition_shuffle_ms` is recorded as unavailable;
the write timing explicitly includes any repartition shuffle.

## Join benchmark

The two measured Join definitions are the existing implementations:

1. `build_trip_stop_events()` in `spark_batch.py`;
2. `events.join(active_trip_ids, on="trip_id", how="inner")` from
   `scripts/run_gold_batch.py`.

The active-trip context uses the existing `calendar`, `calendar_dates`, and
`service_date="2026-10-22"` semantics. No business filtering rule is changed.

For each Join, the report records:

- input row counts and schema summaries;
- lazy DataFrame construction time;
- `count()` action time;
- output rows, schema, and partition count;
- formatted physical plan;
- whether the plan text contains `Exchange`, `BroadcastHashJoin`,
  `SortMergeJoin`, `HashAggregate`, or `FileScan`.

The runner does not use `collect()` for large-data timing.

File metadata collection does not execute Spark actions per Parquet file or
per partition directory. File statistics use filesystem metadata only. Route
distribution validation and route partition row counts share one
`groupBy("route_id").count()` action per validation.

## Measurement protocol

Each case runs one warm-up followed by three measured runs by default. Raw
measurements are preserved in the JSON report, and medians are reported for
write and read timings. Results are compared only after row count and schema
validation succeeds.

The runner does not claim to clear OS file caches. The first run may include
JVM and Spark initialization costs. Local `local[*]` measurements, one AT
snapshot, current machine resources, and filesystem cache behavior are
limitations and must not be generalized to a production cluster.

## Manual command

After implementation validation, run the real benchmark explicitly:

```bash
.venv/bin/python -m benchmarks.batch_performance
```

Optional arguments:

```bash
.venv/bin/python -m benchmarks.batch_performance \
  --input-dir data/gtfs/at \
  --results-dir reports/benchmarks \
  --runs 3
```

The normal pytest run only covers the lightweight tests in
`tests/unit/test_batch_performance.py`; it does not load the full GTFS input
or execute this command.

## Validation and limitations

The lightweight tests cover file recognition, size statistics, partition
directory recognition, schema serialization, result validation, and route
filter validation. The completed report also records all warm-up and measured
runs, read timings, output file metadata, execution-plan summaries, and both
Join measurements. The benchmark was run in local mode against one AT
snapshot; it is not a production-cluster performance conclusion.

Known limitations recorded in the report:

- OS file caches were not cleared.
- `local[*]` results do not represent a production cluster.
- Repartition shuffle time is unavailable as a separate reliable measure.
- `service_date` was not added to Day 17 events.
