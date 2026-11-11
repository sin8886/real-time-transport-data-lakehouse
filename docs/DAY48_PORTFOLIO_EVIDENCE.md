# Day 48 - Portfolio Evidence

## Purpose

This page is the evidence index for the Real-Time Transport Data Lakehouse.
It links the architecture, data contract, operational tests, quality checks,
streaming recovery, and performance measurements into one reviewable story.

## Evidence matrix

| Area | Evidence | What it proves |
|---|---|---|
| Architecture | [ARCHITECTURE.md](ARCHITECTURE.md), [architecture.png](architecture.png) | Provider-neutral batch and streaming design, lake layers, serving, DLQ, and recovery boundaries |
| Data contract | [DATA_CONTRACT_V1.md](DATA_CONTRACT_V1.md) | Canonical event fields, identity rules, timestamps, and provider compatibility |
| Kafka topics | `transport.vehicle_positions.raw`, `transport.trip_updates.raw`, `transport.service_alerts.raw`, `transport.events.validated`, `transport.events.dlq` | Raw, validated, and failure-stream boundaries |
| Day 47 reproducibility | [Dockerfile.spark](../Dockerfile.spark), [docker-compose.yml](../docker-compose.yml), [STREAMING_CHECKPOINT_RECOVERY.md](STREAMING_CHECKPOINT_RECOVERY.md) | Fixed Spark/Kafka dependencies and checkpoint recovery |
| Spark UI | `http://localhost:8080` and `http://localhost:8081` while Compose is running | Cluster health, worker registration, resources, and application execution |
| Dashboard | [README.md](../README.md#public-demo), [dashboard/app.py](../dashboard/app.py) | Streamlit serving view for live vehicles, route activity, observed Trip Update delay windows, and platform health |
| Public demo | [AWS deployment and operations](AWS_DEPLOYMENT.md) | On-demand EC2 demonstration with Nginx HTTP proxy; the public IP is not permanent and no domain/HTTPS is configured |
| S3 serving boundary | [S3 storage](S3_STORAGE.md), [dashboard/data.py](../dashboard/data.py) | Private S3 remains lake storage; Dashboard performs only a bounded read-only health probe and queries vehicle details from PostgreSQL |
| Data quality | [FAILURE_HANDLING.md](FAILURE_HANDLING.md), [DATA_CONTRACT_V1.md](DATA_CONTRACT_V1.md), `tests/unit/test_data_quality.py`, `tests/unit/test_stream_validation.py` | Schema, nullability, identity, geographic, and DLQ behavior |
| Tests | CI workflow and `pytest` result | Automated regression coverage across batch, streaming, DQ, storage, and serving |
| Benchmark | [BATCH_PERFORMANCE_BENCHMARK.md](BATCH_PERFORMANCE_BENCHMARK.md), [spark_tuning_v2.md](../reports/benchmarks/spark_tuning_v2.md) | Measured partition, join, broadcast, and output-layout trade-offs |
| Cross-city compatibility | [DAY46_CROSS_CITY_COMPATIBILITY.md](DAY46_CROSS_CITY_COMPATIBILITY.md) | AT and Metlink Static feeds fit the canonical Silver dimensions |

## Reproducible evidence commands

Run from the repository root:

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/python -m build --no-isolation
docker compose up -d --wait
.venv/bin/python scripts/docker_smoke_test.py
```

For the optional realtime dashboard demonstration, create the production
Trip Update topic before starting the serving job:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic transport.trip_updates.raw \
  --partitions 1 --replication-factor 1
```

Start `run_realtime_serving.py` with a fresh checkpoint, then run
`scripts/publish_at_realtime.py --interval 30` with `AT_API_KEY` set only in
the shell environment. The publisher sends both Vehicle Position and Trip
Update events; the dashboard shows an empty delay trend when no valid Trip
Update events have reached serving.

For the streaming evidence, create or reuse the Day 47 test topic and use a
fresh checkpoint directory:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic transport.day47.reproducibility.vehicle_positions \
  --partitions 1 --replication-factor 1

docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/transport-lakehouse/scripts/recovery_test_spark_streaming.py \
  --bootstrap-servers kafka:29092 \
  --topic transport.day47.reproducibility.vehicle_positions \
  --checkpoint-location \
  "/opt/transport-lakehouse-checkpoints/day47/evidence-$(date +%Y%m%d-%H%M%S)"
```

## Evidence interpretation

- The project is local-first; AWS is not required for the core demonstration.
- The Spark tuning measurements are local `local[*]` measurements, not a
  production SLA or distributed-cluster capacity claim.
- Checkpoint recovery demonstrates restart progress, not end-to-end exactly-once
  delivery.
- Synthetic Kafka events are explicitly test data and are not business source
  data.
