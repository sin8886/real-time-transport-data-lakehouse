# Real-Time Transport Data Lakehouse 操作手册

这是一个 local-first 的小型城市交通数据平台。它可以读取 GTFS Static
计划数据，并通过 Kafka 和 Spark Structured Streaming 处理 GTFS Realtime
事件。项目支持 Auckland Transport（AT）和 Metlink 的数据模型；仓库中的
`data/gtfs/at` 是 Auckland 数据，`data/gtfs/metlink` 是 Wellington 数据。

## 1. 环境要求

- Linux/WSL2
- Docker Engine 或 Docker Desktop
- Docker Compose v2
- Python 3.11+
- 仓库自带的 `.venv`

进入项目目录：

```bash
cd ~/projects/real-time-transport-data-lakehouse
```

首次安装 Python 依赖：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
```

## 2. 启动和停止平台

首次启动或修改 [Dockerfile.spark](../Dockerfile.spark) 后，构建 Spark 镜像：

```bash
docker compose build spark-master spark-worker
```

启动 Kafka、Spark Master、Spark Worker 和 PostgreSQL：

```bash
docker compose up -d --wait
docker compose ps
```

服务端口：

| Service | Address |
|---|---|
| Kafka external listener | `localhost:9092` |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| PostgreSQL | `localhost:5433` |

运行 Docker 连通性检查：

```bash
.venv/bin/python scripts/docker_smoke_test.py
```

停止容器但保留 PostgreSQL Volume、Kafka 数据和 checkpoint：

```bash
docker compose down
```

不要使用 `docker compose down -v`，除非明确要删除 PostgreSQL Volume。

## 3. 运行 Static Batch 分析

当前批处理示例使用 Auckland GTFS Static 数据和固定服务日期：

```bash
.venv/bin/python scripts/run_gold_batch.py
```

输出目录：

```text
data/gold/at/service_date=2026-10-22/
├── daily_metrics/
├── route_metrics/
└── stop_metrics/
```

批处理会生成：

- 每日指标
- 路线指标
- 站点指标

如果要分析 Wellington 数据，应先使用现有解析和兼容性模块，以
`data/gtfs/metlink` 作为输入；不要把 Metlink 数据标记为 AT。

## 4. 运行 Day 47 Streaming Recovery

创建 Day 47 专用 Topic（重复执行不会重复创建）：

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic transport.day47.reproducibility.vehicle_positions \
  --partitions 1 \
  --replication-factor 1
```

每次测试都使用新的 checkpoint：

```bash
CHECKPOINT="day47/manual-$(date +%Y%m%d-%H%M%S)"

docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/transport-lakehouse/scripts/recovery_test_spark_streaming.py \
  --bootstrap-servers kafka:29092 \
  --topic transport.day47.reproducibility.vehicle_positions \
  --checkpoint-location "/opt/transport-lakehouse-checkpoints/${CHECKPOINT}"
```

成功标志：

```text
RECOVERY TEST PASSED
The query restarted with the same checkpoint.
The second Kafka event was consumed successfully.
Checkpoint progress was retained across query restart.
```

检查 checkpoint：

```bash
find checkpoints/day47 -maxdepth 4 -type f | sort
```

不要覆盖 Day 31–33 的 checkpoint，也不要复用已经有内容的目录。

## 5. 查看 Kafka Topic

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

查看 Topic 分区：

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic transport.day47.reproducibility.vehicle_positions
```

项目规划中的生产 Topic 为：

- `transport.vehicle_positions.raw`
- `transport.trip_updates.raw`
- `transport.service_alerts.raw`
- `transport.events.validated`
- `transport.events.dlq`

Day 47 的 synthetic recovery Topic 是测试 Topic，不是业务数据源。

## 6. 最终质量检查

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check \
  scripts/recovery_test_spark_streaming.py \
  scripts/run_gold_batch.py \
  benchmarks/batch_performance.py
.venv/bin/python -m build --no-isolation
docker compose config >/dev/null
.venv/bin/python scripts/docker_smoke_test.py
```

S3 和 PostgreSQL integration tests 默认会 skip，因为它们需要额外的
外部服务或环境变量。它们不属于本地核心演示的必需条件。

## 7. 本地 Dashboard

Dashboard 是 v1.0.3 数据平台之上的展示层。首次安装 Dashboard 依赖：

```bash
.venv/bin/python -m pip install -e ".[dashboard]"
```

确保 PostgreSQL 正在运行，并先把本地 Auckland Gold 快照加载到 Serving
表：

```bash
POSTGRES_DB=transport_lakehouse \
POSTGRES_USER=transport \
POSTGRES_PASSWORD=transport_local_dev \
POSTGRES_HOST=localhost \
POSTGRES_PORT=5433 \
.venv/bin/python scripts/load_gold_serving.py \
  --provider at \
  --service-date 2026-10-22
```

启动网页：

```bash
.venv/bin/streamlit run dashboard/app.py
```

然后打开 http://localhost:8501。网页展示真实的 Batch Gold 数据和
Pipeline audit 状态；Live Vehicles 页面在实时 Producer 和 Spark Serving
运行后展示真实车辆事件，没有实时数据时显示空状态，不会把 Day 47 的
synthetic recovery 事件冒充成实时车辆。

## 8. 按需运行实时 Auckland 车辆

实时模式需要 AT API key。只在运行 Producer 时请求 Auckland 的
GTFS-Realtime feed；停止 Producer 后不会继续请求。

终端一：启动 Spark 到 PostgreSQL 的实时 Serving：

```bash
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/transport-lakehouse/scripts/run_realtime_serving.py
```

终端二：设置 key 并按 30 秒轮询发布车辆事件：

```bash
export AT_API_KEY='只在本地终端设置，不要提交到 Git'
.venv/bin/python scripts/publish_at_realtime.py --interval 30
```

The publisher sends both vehicle positions and canonical Trip Update events.
Route delay windows appear after Trip Update events have been received and
processed by the Spark serving job.

然后刷新网页的 Live vehicles 页面。地图只显示最近 10 分钟内有真实
事件的车辆；没有数据时显示空状态，不显示虚构车辆。

停止实时分析：

```text
终端二：Ctrl+C
终端一：Ctrl+C
```

实时 checkpoint 位于 `checkpoints/dashboard/realtime-vehicles/`。如果 Kafka
topic 被删除并重新创建，必须使用新的 `REALTIME_CHECKPOINT` 路径，避免旧
offset 与新 topic 不一致：

```bash
CHECKPOINT="dashboard/realtime-vehicles-$(date +%Y%m%d-%H%M%S)"
docker compose exec -T \
  -e REALTIME_CHECKPOINT="/opt/transport-lakehouse-checkpoints/${CHECKPOINT}" \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  /opt/transport-lakehouse/scripts/run_realtime_serving.py
```

### Dashboard metric boundary

The current dashboard serving table stores the latest vehicle position per
vehicle. The Overview congestion score is a transparent proxy:

```text
score = 100 * fresh observations with speed <= 2 m/s / fresh observations with speed
```

It is an estimate based on current vehicle positions, not measured delay and
not machine learning. The realtime serving job also consumes
`transport.trip_updates.raw` and persists five-minute route windows to
`dashboard_route_delay_trends`. The Route Performance page renders those
observed windows when data is available; an empty result means no Trip Update
events have reached serving yet. Realtime stop arrivals remain unavailable.
Static route and stop rankings continue to use the verified Gold snapshot.

The Platform Health page performs a bounded, read-only S3 object listing when
the configured AWS identity is available. S3 remains the private lakehouse
storage layer; PostgreSQL remains the dashboard's vehicle-detail query source.

## 9. 数据层说明

| Layer | 内容 |
|---|---|
| Bronze | 原始 Static 快照和 Realtime payload |
| Silver | 标准化 routes、stops、trips、vehicle positions、trip updates、alerts |
| Gold | route delay、vehicle activity、service reliability 等指标 |
| PostgreSQL | 面向查询的 serving 层，不是主数据湖存储 |

## 10. 重要边界

- 这是可复现的 portfolio MVP，不是生产级托管平台。
- Benchmark 是本地测量，不代表生产 SLA。
- Checkpoint 证明 Spark 查询可恢复，不代表所有 sink 都是 exactly-once。
- Synthetic Kafka 事件仅用于测试，不代表真实业务数据。
- Metlink Static 已完成兼容性验证；Metlink Realtime 的完整字段映射仍有限制。
