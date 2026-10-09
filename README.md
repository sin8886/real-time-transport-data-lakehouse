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
