# S3 存储配置与实现

## 1. 项目概述

本项目使用 Amazon S3 作为公共交通数据湖仓的对象存储层。

存储架构采用 Bronze–Silver–Gold 三层结构：

- **Bronze（原始层）**：存储从数据源获取的原始数据。
- **Silver（清洗层）**：存储经过清洗和标准化的数据。
- **Gold（分析层）**：存储整理后的分析数据和业务指标。

## 2. AWS 配置

| 配置项 | 当前配置 |
|---|---|
| AWS 区域 | `ap-southeast-2`（悉尼） |
| S3 Bucket 名称 | `sin8886-transport-lakehouse` |
| 存储服务 | Amazon S3 |
| 默认加密方式 | SSE-S3（AES-256） |
| 阻止公共访问 | 已启用 |
| Bucket 版本控制 | 已关闭 |

当前 Bucket 为私有存储，不需要开放公共访问。

## 3. S3 存储目录结构

```text
s3://sin8886-transport-lakehouse/
├── bronze/
│   └── integration-tests/
├── silver/
└── gold/
```

其中：

- `bronze/`：原始数据存储层。
- `silver/`：清洗和标准化数据存储层。
- `gold/`：分析数据存储层。
- `bronze/integration-tests/`：专门用于存放临时集成测试文件。

## 4. IAM 权限配置

项目创建了独立的 IAM 用户：

`transport-lakehouse`

该用户遵循最小权限原则，只拥有项目所需的 S3 权限。

| IAM 权限 | 资源范围 | 用途 |
|---|---|---|
| `s3:ListBucket` | 项目 Bucket | 查看 Bucket 中的对象 |
| `s3:GetBucketLocation` | 项目 Bucket | 获取 Bucket 所在区域 |
| `s3:GetObject` | 项目 Bucket 中的对象 | 下载对象 |
| `s3:PutObject` | 项目 Bucket 中的对象 | 上传对象 |
| `s3:DeleteObject` | `bronze/integration-tests/*` | 清理集成测试对象 |

应用 IAM 用户没有 S3 管理员权限，也不能随意删除项目中的其他对象。

AWS 凭证通过本地 AWS CLI 配置管理，不应提交到 GitHub。

## 5. Python S3 存储模块

实现文件：

`src/transport_lakehouse/storage/s3.py`

项目使用 `boto3` 实现 S3 文件操作，并封装为 `S3Storage` 类。

目前支持以下功能：

- `build_key()`：检查数据分层名称，并构建 S3 对象路径。
- `upload_file()`：将本地文件上传到指定数据层。
- `download_file()`：将 S3 对象下载到本地路径。

支持的数据层：

- `bronze`
- `silver`
- `gold`

该模块支持注入 S3 Client，因此可以使用 Mock 对象进行单元测试，而不必每次都连接 AWS。

## 6. 环境配置

存储模块支持以下环境变量：

| 环境变量 | 默认值 |
|---|---|
| `S3_BUCKET_NAME` | `sin8886-transport-lakehouse` |
| `AWS_DEFAULT_REGION` | `ap-southeast-2` |

本地开发环境通过 AWS CLI 配置凭证：

```bash
aws configure
```

不要将 AWS Access Key 或 Secret Key 写入 `.env`、`.env.example`、源代码或 Git 仓库。

## 7. 测试方法

### 7.1 单元测试

单元测试使用 Mock S3 Client，不需要连接真实 AWS：

```bash
pytest -v tests/unit/test_s3.py
```

### 7.2 真实 S3 集成测试

集成测试会执行以下操作：

1. 创建临时测试文件。
2. 将文件上传到 S3。
3. 从 S3 下载文件。
4. 检查下载内容是否与原文件一致。
5. 删除临时测试对象。

运行前需要启用集成测试：

```bash
export RUN_S3_INTEGRATION=1
export S3_BUCKET_NAME="sin8886-transport-lakehouse"
export AWS_DEFAULT_REGION="ap-southeast-2"

pytest -v -m integration tests/integration/test_s3_integration.py
```

运行该测试需要有效的 AWS 凭证，以及前面列出的 IAM 权限。

### 7.3 完整测试

运行项目全部测试：

```bash
pytest -v
```

只有设置 `RUN_S3_INTEGRATION=1` 时，才会启用真实 S3 集成测试。

## 8. 安全注意事项

- 保持 S3 阻止公共访问功能开启。
- 不要将 AWS 凭证或 SSH 私钥提交到 GitHub。
- 不要为应用 IAM 用户授予不必要的管理员权限。
- 将删除权限限制在集成测试专用目录。
- 开发过程中定期检查 AWS 资源使用情况和费用。