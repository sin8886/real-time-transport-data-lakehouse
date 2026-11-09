# Day 50 - Final Acceptance and v1.0 Release

## Final acceptance checklist

### Functional

- [x] GTFS Static input is parsed into canonical dimensions.
- [x] Batch Gold metrics are generated from Static data.
- [x] GTFS Realtime event families have canonical producers and validation.
- [x] Kafka raw, validated, and DLQ boundaries are defined.
- [x] Spark Structured Streaming supports event-time processing, watermarking,
      deduplication, and checkpoint recovery.
- [x] Docker Compose starts Kafka, Spark Master/Worker, and PostgreSQL.
- [x] Day 47 clean-room recovery passes with a fixed Kafka connector image.
- [x] AT and Metlink Static compatibility is documented and tested.

### Quality and operations

- [x] Unit and integration test suite is green in the local environment.
- [x] Ruff lint is green.
- [x] Python package build succeeds.
- [x] Data-quality and DLQ behavior are tested.
- [x] Failure handling and checkpoint limitations are documented.
- [x] Performance benchmark evidence is stored in `reports/benchmarks/`.
- [x] Secrets and generated runtime data are excluded from Git.

### Portfolio

- [x] Architecture diagram and architecture narrative are available.
- [x] Kafka topics and streaming boundaries are documented.
- [x] Spark UI endpoints are documented for local demonstration.
- [x] Interview explanation and technical Q&A are available.
- [x] Reproducible commands are available in the Day 48 evidence page.

## Final verification

```bash
git status --short
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check \
  scripts/recovery_test_spark_streaming.py \
  scripts/run_gold_batch.py \
  benchmarks/batch_performance.py
.venv/bin/python -m build --no-isolation
docker compose config >/dev/null
docker compose build spark-master spark-worker
docker compose up -d --wait
.venv/bin/python scripts/docker_smoke_test.py
```

Run the Day 47 streaming recovery command from
[DAY48_PORTFOLIO_EVIDENCE.md](DAY48_PORTFOLIO_EVIDENCE.md) with a fresh
checkpoint directory before release.

## Release boundary

The v1.0 release is a reproducible portfolio MVP. It does not claim:

- production-scale throughput or availability;
- managed Kafka, Spark, or S3 operations;
- end-to-end exactly-once delivery;
- complete field-level Metlink realtime normalization;
- automated production alerting or incident response.

## Release procedure

```bash
git status --short
git diff --check
git add .
git commit -m "Complete Day 48-50 final project release" \
  -m "Add portfolio evidence, interview material, and final acceptance documentation." \
  -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
git tag -a v1.0.0 -m "Real-Time Transport Data Lakehouse v1.0.0"
git push origin main
git push origin v1.0.0
```
