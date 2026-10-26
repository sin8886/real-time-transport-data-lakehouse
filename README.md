# Real-Time Transport Data Lakehouse

A local-first project skeleton for combining GTFS Static reference data with GTFS Realtime events from Auckland Transport and Metlink.

## Repository Structure

```text
src/transport_lakehouse/  Python package areas for future implementation
tests/                    Automated tests
data/                     Validated Static data and future local inputs
docs/                     Project charter, evidence, contract, and architecture
scripts/                  Validation and operational helper scripts
```

The current skeleton intentionally does not implement Kafka, Spark, API clients, or ingestion pipelines. See [docs/PROJECT_CHARTER.md](docs/PROJECT_CHARTER.md), [docs/DATA_CONTRACT_V1.md](docs/DATA_CONTRACT_V1.md), and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the project boundaries and design.

## Local Setup

Create a virtual environment and install the development tools:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Copy `.env.example` to `.env` and provide local credentials only when a future implementation requires them. Do not commit `.env` or expose API keys.

Run the checks from the repository root:

```powershell
pytest
ruff check .
ruff format --check .
```

### Incremental Static Batch (Day 20)

`IncrementalBatchPipeline` compares a deterministic SHA-256 fingerprint of the
local GTFS `.txt` feed with the provider watermark in
`data/state/static_gtfs_watermarks.json`. A changed feed is uploaded to the
existing Bronze `ingest_date=YYYY-MM-DD` partition and processed through the
existing snapshot pipeline. The watermark is written atomically only after
processing reports success; unchanged feeds are skipped. Day 20 v1 keeps the
existing one-snapshot-per-ingest-date behavior; multiple changes on the same
day reuse that partition and are outside this version's history-retention
scope.

### Pipeline Audit (Day 21)

`IncrementalBatchPipeline` writes one independent PostgreSQL audit record per
invocation to `pipeline_runs`. The record includes a UUID `run_id`, pipeline
status (`success`, `skipped`, or `failed`), duration in milliseconds, parsed
input records, Silver output records, failed quality records, and any error
message. Audit writes use their own PostgreSQL connection and transaction so a
failed business run can still be recorded. Audit errors are surfaced rather
than silently ignored.

### Prefect Static Ingestion Flow (Day 22)

Prefect is used only as an orchestration layer around the existing
`IncrementalBatchPipeline`; fingerprinting, Bronze ingestion, snapshot
processing, watermarking, and PostgreSQL audit remain in that pipeline. The
retryable task performs at most three attempts (`retries=2`), and each attempt
keeps the existing Day 21 audit behavior.

Run one parameterized flow locally:

```bash
.venv/bin/python -m transport_lakehouse.flows.static_ingestion_flow \
  --source-dir data/gtfs/at \
  --provider at \
  --ingest-date 2026-10-26
```

Start a local daily schedule at 02:00:

```bash
.venv/bin/python -m transport_lakehouse.flows.static_ingestion_flow \
  --serve \
  --source-dir data/gtfs/at \
  --provider at
```

The `--serve` command uses Prefect's local `.serve()` mechanism with
`cron="0 2 * * *"` and `webserver=False`. Prefect 3 starts a temporary local
API service for orchestration; this process must remain running, and it is not
a replacement for a durable Prefect Server or Prefect Cloud deployment.

### Windows Parquet prerequisite

The Day 10 Spark lab keeps real Parquet writes and `partitionBy("route_id")`.
On native Windows, Hadoop's local filesystem calls `winutils.exe` when Spark
creates the output directories; Spark configuration alone does not reliably
remove that requirement. If Parquet writing reports
`HADOOP_HOME and hadoop.home.dir are unset`, use one of these supported
options:

1. Build the Windows native tools matching the Hadoop version bundled with
   this PySpark environment from the official Apache Hadoop source. Put the
   resulting `winutils.exe` under a local `bin` directory and set
   `HADOOP_HOME` to its parent before starting Python:

   ```powershell
   $env:HADOOP_HOME = "D:\path\to\hadoop"
   .\.venv\Scripts\python.exe .\scripts\spark_lab.py
   ```

2. Run the lab under WSL/Linux, where Hadoop's Windows native helper is not
   needed.

The lab does not download executables or assume a machine-specific path. Its
Parquet test writes to pytest's temporary directory and reads the files back
to verify the partitions and schema.
