# Real-Time Transport Data Lakehouse Platform

## Business Problem

Urban public transport is a continuously changing operational environment. The state of the network does not remain fixed after a timetable is published: vehicles move, trips progress, arrival and departure predictions change, and service conditions can produce alerts such as delays, cancellations, diversions, or other disruptions. Auckland Transport's GTFS Static data describes the planned network and timetable, while its GTFS Realtime feeds provide changing operational information through Vehicle Positions, Trip Updates, and Service Alerts.

Using static data alone provides only a planned view of transport services. It can describe routes, stops, trips, and scheduled stop times, but it cannot reliably show what is happening during service operation. Static data therefore cannot by itself support questions such as which vehicles are active, how much a trip is delayed, whether delays are concentrated on particular routes or time windows, or how service reliability changes in practice.

The operational problem requires both batch and streaming data processing. GTFS Static data is comparatively stable and is naturally ingested, versioned, validated, and transformed as batch data. GTFS Realtime data changes continuously and must be captured as a sequence of events, where duplicate, late, out-of-order, malformed, or temporarily unavailable data can occur. Treating both forms of data as one simple periodic download would lose the distinction between the planned network and its changing operational state.

A reliable data platform is needed because the objective is not merely to produce a one-off analysis or dashboard. The platform must preserve source data, support repeatable processing, make batch and real-time data available through consistent data layers, and provide a basis for recovering from failures and verifying data quality. It must also make processing behavior observable and reproducible so that results can be regenerated, tested, and explained.

The platform will therefore support practical transport operations and analysis questions, including:

- Which vehicles are active on the network and how does their activity change over time?
- Which routes and trips are experiencing delays, and how large are those delays across defined time windows?
- How do observed trips, delayed trips, cancelled trips, and service alerts affect route-level service reliability?
- Can planned service data and real-time operational events be combined into trustworthy, repeatable analytical outputs?

This project addresses these questions by building a small but genuine modern data platform around Auckland Transport's GTFS Static and GTFS Realtime data. Its value is in solving the engineering problem of turning stable schedules and continuously changing operational events into reliable, reusable data products, rather than in presenting transport data through visualization alone.

## Project Goals

The goal is to build a reproducible Real-Time Transport Data Lakehouse Platform for urban mobility data. The platform will combine batch processing for GTFS Static data with streaming processing for GTFS Realtime events, while preserving a consistent path from source data to analytical outputs.

The platform will:

1. Ingest and preserve Auckland Transport GTFS Static snapshots as planned-network source data.
2. Capture GTFS Realtime Vehicle Positions, Trip Updates, and Service Alerts as continuously changing operational events.
3. Reliably transport real-time events through Kafka so that event ingestion and downstream consumption are decoupled and repeatable.
4. Process real-time events with Spark Structured Streaming and process static snapshots with Spark batch workloads.
5. Organize source and processed data into Bronze, Silver, and Gold data lake layers, using Parquet as the primary analytical storage format.
6. Establish normalized Silver data for transport entities and events, including routes, stops, trips, vehicle positions, trip updates, and service alerts.
7. Produce business-meaningful Gold data products for route delay metrics, vehicle activity, and service reliability.
8. Demonstrate that the platform can handle the operational realities of event-based data, including duplicate, late, out-of-order, malformed, and unavailable source data, with appropriate validation and recovery behavior.
9. Make the core Batch and Streaming demonstration reproducible in a clean local environment, so the project can be examined, tested, and rerun rather than treated as a one-time analysis.

This project is intended to demonstrate modern Data Engineering capability through a coherent end-to-end platform: integrating batch and streaming sources, handling event-time data, separating raw and refined data layers, producing reliable analytical products, and designing for data quality, observability, reproducibility, and recovery. The technology choices serve those engineering requirements; the project is not defined as a dashboard or visualization project.

## Non-Functional Requirements

The following requirements define the engineering qualities that the project must demonstrate. They are intentionally scoped to a reproducible portfolio data platform and do not claim enterprise production guarantees, fixed service-level objectives, or large-scale operational capacity.

| NFR                              | Requirement                                                                                                                                           | Evidence                                                                                                                     |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Reliability                      | Handle duplicate, late, out-of-order, malformed, and temporarily unavailable GTFS Realtime data. Invalid events must be isolated.                     | Tests and recorded results for deduplication, late-event handling, source outage, retry, and DLQ/quarantine behavior.        |
| Data Quality                     | Apply schema, nullability, uniqueness, referential-integrity, and applicable geographic checks. Rejected records must not contaminate Silver or Gold. | Schema and DQ tests, quarantine/DLQ output, and accepted Silver/Gold output inspection.                                      |
| Reproducibility                  | Rerun the core Batch and Streaming demonstration from a clean environment without requiring continuously running AWS compute.                         | Docker Compose startup, documented configuration, clean-environment run, and repeatable controlled or replayed inputs.       |
| Recoverability                   | Verify checkpoint- and offset-based recovery after Kafka or Spark restart. This is not an enterprise disaster-recovery claim.                         | Restart tests showing that the pipeline resumes and records the resulting processing behavior.                               |
| Observability                    | Record pipeline runs, records in/out, duration, status, processing latency, and relevant streaming indicators such as consumer lag.                   | Logs, audit records, metrics, and captured Kafka/Spark evidence.                                                             |
| Performance                      | Measure the effect of partitioning, file size, joins, shuffle, broadcast, and repartition/coalesce without assuming an unmeasured target.             | Reproducible benchmark configurations, measured results, and comparisons. No fixed TPS, latency, or SLA is assumed.          |
| Maintainability and Code Quality | Maintain Python quality checks, tests, CI, configuration separation, and protection of credentials, API keys, and AWS secrets.                        | Passing pytest, ruff, and GitHub Actions checks, plus repository and secret-handling review.                                 |
| Scalability and Extensibility    | Support both Batch and Streaming workloads through basic Kafka and Spark partitioning, while remaining a small demonstrable platform.                 | Kafka partition and consumer-group experiments, Spark partition benchmarks, and canonical-model evidence.                    |
| Cost and Portability             | Use S3 as the cloud data lake while keeping core computation and the main demonstration runnable locally after AWS free-tier availability ends.       | Local Batch + Streaming run, S3 integration check, and documented separation of optional cloud storage from local execution. |
| Security                         | Use least-privilege IAM and secure environment-based configuration. Credentials and API keys must not be committed to GitHub.                         | IAM review, environment-based secret handling, and repository/CI checks for exposed secrets.                                 |

### NFR Verification Status

These requirements are defined on Day 1; defining them does not mean that they have already been satisfied. Each requirement has a corresponding verification path in the 50-day plan through code, tests, benchmarks, logs, Docker runs, CI results, or portfolio evidence. The project can prove the scoped behaviors listed above, but it cannot honestly prove enterprise-grade availability, disaster recovery, security certification, or unrestricted large-scale performance from this plan. Those claims are explicitly outside the project scope.

## Success Criteria

At the end of the 50-day project, the platform will be considered complete only when the following conditions are demonstrated with working outputs and recorded evidence.

| Area                      | Success Criterion                                                                                                                                                                                     | Evidence                                                                                                                 |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Real Data Sources         | Auckland Transport GTFS Static and GTFS Realtime data can be retrieved successfully. The Realtime flow includes Vehicle Positions, Trip Updates, and Service Alerts.                                  | Successful source-ingestion records and captured source or replay evidence.                                              |
| Batch Pipeline            | GTFS Static data completes the Bronze -> Silver -> Gold path, including parsing, validation, transformation, and batch processing.                                                                    | Working batch outputs in each layer and an end-to-end batch run.                                                         |
| Streaming Pipeline        | GTFS Realtime events enter Kafka and are consumed by Spark Structured Streaming. The stream performs schema validation, watermarking, deduplication, and event-time/window processing.                | Kafka topic records, Spark streaming output, checkpoint data, and processing evidence.                                   |
| Data Lake                 | S3 contains Bronze, Silver, and Gold data layers, with Parquet as the primary analytical format and data organized using documented partitioning.                                                     | S3 prefixes, Parquet files, partition layout, and storage inspection evidence.                                           |
| Gold Data Products        | The platform produces working Route Delay Metrics, Vehicle Activity, and Service Reliability datasets.                                                                                                | Actual generated Gold datasets with representative records and documented schemas.                                       |
| Reliability and Recovery  | Duplicate, late, out-of-order, and malformed events are handled as designed. Kafka/Spark restart behavior verifies checkpoint/offset recovery, and unprocessable data reaches DLQ or quarantine.      | Failure and recovery test results, restart evidence, and DLQ/quarantine records.                                         |
| Data Quality              | Schema validation and the defined null, uniqueness, referential-integrity, and applicable geographic checks run successfully. Invalid records do not appear in normal Silver or Gold outputs.         | DQ test results, rejected-record outputs, and validated Silver/Gold samples.                                             |
| Performance               | Spark performance benchmarks are completed for the planned partition, file-size, join, shuffle, broadcast, and repartition/coalesce experiments, with results recorded.                               | Benchmark configurations, measurements, and comparison notes. No unmeasured performance target is required.              |
| Engineering Understanding | The project can explain Kafka partitions, offsets, consumer groups, and at-least-once processing, as well as Spark shuffle, joins, windows, and partitioning decisions used in the platform.          | Interview/demo notes supported by the implemented pipeline, configuration, and benchmark evidence.                       |
| Engineering Quality       | pytest, ruff, and GitHub Actions checks run successfully, and credentials or other secrets are absent from the repository.                                                                            | Test output, lint output, passing CI run, and secret-handling evidence.                                                  |
| Reproducibility           | Docker Compose starts the core local environment, and a clean environment can rerun the core Batch + Streaming demonstration. The core project remains runnable locally without ongoing AWS services. | Docker startup evidence, clean-room run, documented commands, and local execution evidence.                              |
| Portfolio Evidence        | The final project provides clear evidence of its architecture, Kafka topics, Spark processing, Data Lake layers, data quality, recovery, tests, and performance benchmark.                            | Architecture diagram, topic/output samples, Spark evidence, DQ and recovery results, test results, and benchmark record. |

### Definition of Done

The project may be marked as a `v1.0 Release` when every Success Criterion above has working evidence, the core Batch + Streaming demonstration passes in a clean local environment, the required Gold products are generated, quality and recovery checks are recorded, and the final README and portfolio evidence accurately describe the demonstrated scope. This definition does not imply production readiness, enterprise SLA compliance, or enterprise disaster-recovery capability.
