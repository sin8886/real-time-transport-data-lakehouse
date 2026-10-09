第二项目：Real-Time Transport Data Lakehouse Platform
50天现代数据工程项目最终规划 V2.0
一、项目最终定位
本项目不再与第一个 Job Data ETL 项目使用同一业务域。项目一证明可靠 ETL、增量加载、PostgreSQL、Prefect、测试与 AWS 基础能力；项目二证明现代 Data Platform 能力：Batch + Streaming、Kafka、Spark、Data Lake、Parquet、实时计算、Docker、数据质量与可恢复性。
业务域确定为：城市公共交通 / Urban Mobility。核心目标：把公开 GTFS Static 与 GTFS Realtime 数据建设成一个可复现的小型现代数据平台。
二、最终数据源选择
① Auckland Transport（主数据源，NZ）：优先作为核心城市数据源。AT 官方 Realtime API 覆盖 Auckland 公交、火车、渡轮；官方说明目前 feed 至少每 30 秒更新一次，并提供 Trip Updates、Vehicle Positions、Service Alerts。AT 还提供 GTFS Static API。推荐度：★★★★★。
② Metlink / Greater Wellington（第二 NZ 数据源）：提供 Wellington 区域 GTFS Static，可作为跨城市批数据扩展与 schema compatibility 测试；是否纳入实时流由后续 API 实测决定。推荐度：★★★★☆。
③ GTFS 官方标准 / 公共 GTFS Realtime Feed（备用兼容源）：GTFS 是开放标准，Realtime 明确支持车辆位置、到站/延误更新和服务告警；用于验证 pipeline 不绑定某一家运营商。推荐度：★★★★☆。
④ Synthetic Event Replay（受控测试数据）：把历史真实 GTFS-RT 快照转换/回放为 Kafka events，用于重复事件、乱序、late event、故障恢复、负载测试。推荐度：★★★★★（测试必需，不作为业务主数据源）。
结论：主数据源先锁定 Auckland Transport；Metlink 作为第二城市/兼容性扩展。开发前再做一次实际 API 可用性、历史留存和条款核验；若某实时接口不可稳定回放，则使用已保存的真实 GTFS-RT 快照进行 replay，而不是伪造业务数据。
三、为什么这个数据源最贴合项目目标
天然同时存在静态 Batch 数据与动态 Streaming 数据，不需要为了 Kafka 人为创造业务理由。
车辆位置包含 latitude、longitude、timestamp、speed、bearing、congestion/occupancy 等潜在字段；Trip Update 包含预测到站/离站和 delay；Service Alert 描述取消、延误、改道、施工等事件。
GTFS Realtime 的推荐刷新周期至少为 30 秒，因此实时数据工程场景真实、可解释。
数据结构稳定、公开标准化，适合 schema validation、数据契约、跨城市兼容测试。
新西兰本地数据源让项目叙事与 NZ 求职市场直接相关，但技术本身具有国际通用性。
交通数据比招聘数据更能自然解释 Kafka、watermark、window、late events、stateful streaming。
四、项目需求文档 PRD
REQ-01：获取 GTFS Static，保存原始 Bronze 快照。
REQ-02：周期性获取 GTFS Realtime，捕获 vehicle positions、trip updates、service alerts。
REQ-03：把实时 feed 转换为 canonical events 并发送到 Kafka。
REQ-04：Kafka 支持 partition、consumer group、offset、at-least-once 语义。
REQ-05：Spark Structured Streaming 消费 Kafka，执行 schema validation、dedup、watermark、window aggregation。
REQ-06：S3 Data Lake 采用 Bronze/Silver/Gold；Parquet 为主存储格式。
REQ-07：Batch Spark 处理历史 GTFS Static/快照；Streaming Spark 处理实时事件。
REQ-08：输出 route performance、delay、vehicle activity、service reliability 等 Gold 数据集。
REQ-09：处理 duplicate、late event、malformed message、source outage，并支持 DLQ/quarantine。
REQ-10：记录 pipeline run、records in/out、processing latency、failed records。
REQ-11：Docker Compose 可启动 Kafka、Spark、PostgreSQL 等核心本地组件。
REQ-12：pytest、ruff、GitHub Actions；AWS secrets 不进仓库。
REQ-13：S3/IAM 作为云端 Lake；核心计算可本地运行，避免 AWS 免费额度成为项目运行前提。
REQ-14：最终项目必须能从干净环境复现核心 Batch + Streaming Demo。
五、最终技术栈
Python、SQL、Bash；GTFS Static、GTFS Realtime、Protocol Buffers/JSON；Python HTTP client、GTFS parser、protobuf parser；Apache Kafka；Apache Spark、PySpark、Spark SQL、Structured Streaming；AWS S3、Parquet；PostgreSQL；Prefect；Docker、Docker Compose；pytest、ruff、schema validation、data quality checks；GitHub Actions；Python logging、pipeline metrics、Spark UI、Kafka consumer metrics。
六、Data Lake Schema
Bronze/static：原始 GTFS zip/text 快照 + source_url + fetched_at + feed_version + ingest_date。
Bronze/realtime：原始 GTFS-RT payload/snapshot + feed_timestamp + fetched_at + source + ingest_date。
Silver/stops：stop_id, stop_name, latitude, longitude, zone_id, wheelchair_boarding。
Silver/routes：route_id, agency_id, route_short_name, route_long_name, route_type。
Silver/trips：trip_id, route_id, service_id, trip_headsign, direction_id, shape_id。
Silver/vehicle_positions：vehicle_id, trip_id, route_id, latitude, longitude, bearing, speed, occupancy_status, event_ts, ingest_ts。
Silver/trip_updates：trip_id, route_id, stop_id, stop_sequence, scheduled_ts, predicted_ts, delay_seconds, event_ts。
Silver/service_alerts：alert_id, cause, effect, route_id, stop_id, active_start, active_end, description。
Gold/route_delay_metrics：window_start, window_end, route_id, trip_count, avg_delay_seconds, p95_delay_seconds。
Gold/vehicle_activity：window_start, route_id, active_vehicle_count, avg_speed, congestion_rate。
Gold/service_reliability：date, route_id, scheduled_trips, observed_trips, delayed_trips, cancelled_trips, reliability_rate。
PostgreSQL：dim_route, dim_stop, dim_vehicle, fact_trip_update, fact_vehicle_position, fact_service_alert, pipeline_runs。
七、Kafka Topic 设计
transport.vehicle_positions.raw：原始车辆位置事件；key=vehicle_id。
transport.trip_updates.raw：实时班次/站点预测事件；key=trip_id。
transport.service_alerts.raw：服务告警；key=route_id/stop_id。
transport.events.validated：通过 schema 与基础 DQ 的 canonical events。
transport.events.dlq：解析失败、schema 错误、业务校验失败消息。
transport.metrics.optional：实时聚合指标；后期增强。
默认语义：at-least-once；通过 event identity + watermark + dedup 处理重复。Kafka partition 数量以本地 benchmark 决定。
八、Spark Pipeline 设计
Batch：GTFS Static/历史快照 → Bronze → Spark Read → schema normalize → dimension building → validation → Silver → daily/route aggregates → Gold → PostgreSQL（可选）。
Streaming：GTFS-RT fetcher → canonical event → Kafka → Spark Structured Streaming → parse → validate → watermark → dedup → event-time windows → Silver/Gold → checkpoint。
关键实验：late event、out-of-order event、duplicate event、Kafka restart、Spark restart、checkpoint recovery、partition/shuffle、broadcast join、repartition/coalesce。
九、Docker 架构
核心本地环境：Kafka + Kafka broker dependency + Spark master + Spark worker + PostgreSQL；Python ingestion/producer 与 PySpark jobs 作为应用代码运行。S3 使用 AWS SDK/IAM，不容器化。
目标不是模拟大型 Kubernetes 集群，而是让个人开发环境具备可复现的分布式数据平台骨架。
十、50天精确开发计划
Day 1 Kickoff：冻结业务问题、目标、NFR、成功标准 → Charter/README
Day 2 GTFS 标准：Static/Realtime、trip/route/stop/vehicle/alert → Domain Notes
Day 3 AT 数据源验证：注册 API、验证 Static/Realtime、刷新频率与限额 → Source Validation
Day 4 Metlink 验证：下载 Static、验证 schema、记录实时接口 → Secondary Source Note
Day 5 数据合同：canonical schema、字段类型、主键、event identity → Data Contract v1
Day 6 架构：Batch/Streaming/Data Lake/Serving/Failure path → Architecture Diagram
Day 7 仓库：项目结构、pyproject、ruff、pytest、env template → Repo Skeleton
Day 8 Docker：Kafka/Spark/Postgres health checks → Docker Smoke Test
Day 9 Kafka：topic、partition、producer、consumer、group → Kafka Lab
Day 10 Spark：DataFrame、SQL、Parquet、partition → Spark Lab
Day 11 S3/IAM：Lake bucket/prefix、最小权限、读写 → S3 Test
Day 12 Static ingestion：AT Static → Bronze → Bronze Static v1
Day 13 Static parsing：agency/routes/stops/trips/stop_times → Parsed Tables
Day 14 Silver dimensions：route/stop/trip 标准化 → Silver v1
Day 15 Batch DQ：schema/null/unique/geo/referential checks → DQ v1
Day 16 Historical snapshots：按 ingest_date 保存快照 → Snapshot Pipeline
Day 17 Spark transformations：join routes/trips/stops/stop_times → Spark Batch v1
Day 18 Gold batch metrics：每日班次、路线、站点指标 → Gold v1
Day 19 PostgreSQL serving：dim/fact/metrics → Serving v1
Day 20 Incremental batch：变化 feed + watermark → Incremental v1
Day 21 Pipeline audit：run_id、records、duration、status → Audit Table
Day 22 Prefect：每日/参数化 Static ingestion + retry → Flow v1
Day 23 Batch performance：partition、file size、join benchmark → Benchmark v1
Day 24 Batch milestone：端到端 Batch 验收 → Milestone 1
Day 25 Realtime fetcher：GTFS-RT、protobuf/JSON、canonical event → Fetcher v1
Day 26 Kafka vehicle topic：vehicle_positions producer → Topic + Producer
Day 27 Kafka trip topic：trip_updates producer → Topic + Producer
Day 28 Kafka alert topic：service_alerts producer → Topic + Producer
Day 29 Streaming Spark：Structured Streaming 消费 Kafka → Streaming Smoke Test
Day 30 Schema/DLQ：bad message、schema error、DLQ → Validated Stream
Day 31 Checkpoint：restart/recovery → Recovery Test
Day 32 Watermark：event-time、late/out-of-order → Watermark Test
Day 33 Dedup：event_id/vehicle_id/trip_id → Dedup Test
Day 34 Streaming Silver：车辆/Trip Update/Alert → Streaming Silver
Day 35 Window metrics：5/15/60 min route delay → Realtime Gold v1
Day 36 Vehicle analytics：active vehicles/speed/congestion → Realtime Gold v2
Day 37 Reliability：delay/cancel/service reliability → Gold Reliability
Day 38 Batch+stream reconciliation：统一 canonical model → Unified Model
Day 39 Failure engineering：Kafka/source outage、malformed data、retry → Failure Matrix
Day 40 Spark tuning：shuffle、partition、broadcast、repartition → Benchmark v2
Day 41 Testing：parser、transform、DQ、incremental、stream logic → Test Suite
Day 42 CI/CD：ruff + pytest + build checks → CI Green
Day 43 Observability：logging、metrics、consumer lag、latency → Monitoring v1
Day 44 Cloud integration：Bronze/Silver/Gold S3 → Cloud Pipeline
Day 45 Security/cost：IAM、secret handling、S3 lifecycle/cost → Checklist
Day 46 Cross-city：Metlink Static 验证 canonical schema → Compatibility Test
Day 47 Clean-room：空环境启动 Docker，Batch + Streaming → Reproducibility Test
Day 48 Evidence：架构图、Kafka topics、Spark UI、DQ、测试、benchmark → Portfolio Evidence
Day 49 Interview：Kafka/Spark/Streaming/Data Lake/SQL/Python + 10分钟讲解 → Interview Pack
Day 50 Final acceptance：Demo、代码冻结、tag、README、复盘 → v1.0 Release
十一、最终验收标准
真实获取 Auckland Transport GTFS Static 与 GTFS Realtime。
Static 完成 Bronze → Silver → Gold Batch pipeline。
Realtime 进入 Kafka，并被 Spark Structured Streaming 消费。
实现 watermark、dedup、checkpoint、DLQ/recovery。
S3 有 Bronze/Silver/Gold 与 Parquet partition。
完成 route delay、vehicle activity、service reliability 三类 Gold 数据产品。
能解释 Kafka partition/offset/consumer group 与 Spark shuffle/join/window。
完成 Spark 性能 benchmark，有实际测量结果。
DQ、pytest、ruff、CI 可运行；Docker 可复现核心环境。
AWS 免费额度结束后核心项目仍可本地运行。
十二、最终技术取舍与求职定位
MVP 不强制 EMR、MSK、Kubernetes、Terraform、Databricks、Flink。每个组件必须有工程理由：Kafka 处理事件流；Spark 做分布式计算；S3 做 Data Lake；Parquet/partition 做分析存储；Prefect 做 orchestration；Docker 保证复现。Delta Lake、Airflow、云端 Spark/EMR 可作为后续增强。
项目一：Reliable Job Data ETL Pipeline——Python/SQL/ETL、增量、PostgreSQL、数据质量、Prefect、CI/CD、AWS。
项目二：Real-Time Transport Data Lakehouse Platform——Batch + Streaming、Kafka、PySpark、Structured Streaming、S3 Data Lake、Parquet、实时指标、数据质量、Docker、故障恢复。两个项目业务域不同、技术能力互补。
十三、数据源核验依据
GTFS 官方文档说明 Static 提供 routes、stops、trips、stop_times 等计划数据；Realtime 提供 Trip Updates、Service Alerts、Vehicle Positions。官方最佳实践建议 Realtime feed 至少每30秒刷新一次。Auckland Transport 官方开发者门户说明其 Realtime API 覆盖 Auckland 的 bus、train、ferry，并聚合 Trip Update、Vehicle Position、Service Alert；当前 feed 至少每30秒更新。AT 另提供 GTFS Static API。Metlink 官方提供 GTFS Schedule 下载。
