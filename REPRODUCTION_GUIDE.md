# Reproduction Guide

This guide reproduces the local-first portfolio MVP from a clean clone. The
commands below assume Linux, macOS, or WSL2 and are run from the repository
root. The project is tested with Python 3.11+ and Docker Compose v2.

## 1. Prerequisites

- Git
- Python 3.11 or newer
- Docker Engine/Desktop with the `docker compose` command
- Enough disk and memory for the Spark, Kafka, and PostgreSQL containers

## 2. Clone and install

```bash
git clone https://github.com/sin8886/real-time-transport-data-lakehouse.git
cd real-time-transport-data-lakehouse

python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python -m pip install -e ".[dashboard]"
```

The dashboard extra is separate because the core package does not require
Streamlit, Plotly, pandas, or PyArrow.

## 3. Download GTFS Static data

GTFS files are not stored in GitHub. Download each official ZIP and extract
the `.txt` files into the matching directory:

```bash
mkdir -p data/gtfs/at data/gtfs/metlink
curl -L https://gtfs.at.govt.nz/gtfs.zip -o /tmp/at-gtfs.zip
unzip -o /tmp/at-gtfs.zip -d data/gtfs/at

curl -L https://static.opendata.metlink.org.nz/v1/gtfs/full.zip \
  -o /tmp/metlink-gtfs.zip
unzip -o /tmp/metlink-gtfs.zip -d data/gtfs/metlink
```

The Auckland feed is used by the runnable batch and dashboard examples.
Metlink is available for provider-compatibility work and must be passed as
`--provider metlink`; it must not be labelled as AT. Feed contents and
service dates change over time.

## 4. Configure `.env`

```bash
cp .env.example .env
```

Edit `.env` locally. Never commit it:

```dotenv
AT_API_KEY=your-auckland-transport-api-key
AT_REALTIME_ENDPOINT=https://api.at.govt.nz/realtime/legacy/
METLINK_API_KEY=
POSTGRES_DB=transport_lakehouse
POSTGRES_USER=transport
POSTGRES_PORT=5433
POSTGRES_PASSWORD=change_me_locally
```

Request an Auckland Transport API key through the official AT developer/API
portal at <https://api.at.govt.nz/>. The realtime publisher sends the key in
the `Ocp-Apim-Subscription-Key` header; do not put it in source code, Git,
Docker images, or committed service files.

Without an AT key, the following still work:

- GTFS Static parsing and the local Gold batch pipeline;
- PostgreSQL serving loads from local Gold Parquet;
- the dashboard's static Overview, Route Performance, Pipeline, and platform
  health views (subject to the local database/data being populated);
- unit tests and synthetic Kafka/Spark tests.

Only the live AT fetch/publish path requires `AT_API_KEY`. `validate_at_realtime.py`
also reports that the key is not configured instead of making a request.

## 5. Start the local platform

```bash
docker compose up -d --wait
docker compose ps
.venv/bin/python scripts/docker_smoke_test.py
```

This starts Kafka, Spark Master/Worker, and PostgreSQL. PostgreSQL is exposed
locally on port `5433`; Kafka is exposed on `9092`.

## 6. Run the Static Gold batch

The checked-in example uses the service date `2026-10-22` and
`data/gtfs/at`:

```bash
.venv/bin/python scripts/run_gold_batch.py
```

Successful output is written under:

```text
data/gold/at/service_date=2026-10-22/
  daily_metrics/
  route_metrics/
  stop_metrics/
```

Load that snapshot into PostgreSQL serving tables:

```bash
.venv/bin/python scripts/load_gold_serving.py \
  --provider at \
  --service-date 2026-10-22 \
  --ingest-date 2026-10-22
```

## 7. Run Kafka + Spark Structured Streaming

For a synthetic, credential-free recovery test, use a fresh checkpoint:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic transport.day47.reproducibility.vehicle_positions \
  --partitions 1 --replication-factor 1

CHECKPOINT="day47/manual-$(date +%Y%m%d-%H%M%S)"
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  /opt/transport-lakehouse/scripts/recovery_test_spark_streaming.py \
  --bootstrap-servers kafka:29092 \
  --topic transport.day47.reproducibility.vehicle_positions \
  --checkpoint-location "/opt/transport-lakehouse-checkpoints/${CHECKPOINT}"
```

This synthetic test validates Kafka/Spark checkpoint recovery. It is not AT
realtime data and must not be presented as live vehicles.

For the actual dashboard realtime path, start the Spark serving job:

```bash
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  /opt/transport-lakehouse/scripts/run_realtime_serving.py
```

In another terminal, with a real AT key configured:

```bash
export AT_API_KEY='your-real-key'
.venv/bin/python scripts/publish_at_realtime.py --interval 30
```

This polls the authenticated AT GTFS-Realtime feed and publishes vehicle
positions and Trip Updates to Kafka. Stop both processes with `Ctrl+C`.

## 8. Start the Dashboard

With Docker running and the Gold snapshot loaded:

```bash
.venv/bin/streamlit run dashboard/app.py
```

Open <http://localhost:8501>. Live Vehicles requires the realtime producer and
Spark serving job; without them it correctly shows no fresh vehicle events.
Trip Update delay charts are empty until Trip Update events reach serving.

The dashboard's congestion value is a transparent current-speed proxy, not a
measured delay or machine-learning prediction. Its S3 health probe is
read-only and is `Not configured` when no AWS identity is available.

## 9. Tests and verification

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
docker compose config >/dev/null
```

The S3/PostgreSQL integration tests require their external services and
credentials. They may skip in a clean local environment; this does not block
the core local reproduction.

## 10. Optional AWS demo

The portfolio deployment is an on-demand EC2 demonstration. It uses Docker
Compose, systemd, Spark serving, Streamlit, and Nginx; PostgreSQL, Kafka, and
Spark administrative ports should remain private. Read
[docs/AWS_DEPLOYMENT.md](docs/AWS_DEPLOYMENT.md) before deploying.

Check the documented instance without starting it:

```bash
aws ec2 describe-instances \
  --region ap-southeast-2 \
  --instance-ids i-019630fd6c36b3e84
```

On the host, verify:

```bash
sudo systemctl status transport-compose.service
sudo systemctl status transport-spark-serving.service
sudo systemctl status transport-dashboard.service
curl http://127.0.0.1/_stcore/health
```

Do not copy `.env`, API keys, database data, S3 data, or Spark checkpoints
from one environment into Git or a container image.

## 11. Common problems

| Symptom | Resolution |
|---|---|
| `No GTFS text files found` | Extract the official ZIP into exactly `data/gtfs/at` or `data/gtfs/metlink`. |
| Dashboard says the serving database is unavailable | Run `docker compose up -d --wait`, then run `scripts/load_gold_serving.py`. |
| No Live Vehicles appear | Set a real `AT_API_KEY`, run the publisher and Spark serving job, and wait for a polling cycle. |
| AT publisher rejects the key | Remove example/placeholder text; the key must be ASCII and supplied through the environment. |
| Kafka/Spark recovery test behaves inconsistently | Use a new checkpoint directory when the topic has been recreated; do not overwrite existing checkpoints. |
| Port already in use | Stop the process using ports `5433`, `8501`, `9092`, `7077`, `8080`, or `8081`, or use the documented configuration boundaries. |
| Windows Spark Parquet write fails with `winutils.exe` | Run under WSL/Linux or follow the Windows prerequisite in `README.md`; the project does not download executables automatically. |
| S3 health shows `Not configured` | Configure an AWS profile or an EC2 IAM role. This probe is optional and does not prevent local batch/dashboard operation. |

When finished, stop containers without deleting their volumes:

```bash
docker compose down
```

Do not use `docker compose down -v` unless you intentionally want to delete
the PostgreSQL volume.
