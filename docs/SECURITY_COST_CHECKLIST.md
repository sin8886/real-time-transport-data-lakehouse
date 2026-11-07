# Day 45 - Security and Cost Checklist

## 1. IAM Least Privilege

IAM user: transport-lakehouse

Managed policy: TransportLakehouseS3Access

Verified policy permissions:

- s3:ListBucket on the project bucket.
- s3:GetBucketLocation on the project bucket.
- s3:GetObject on project bucket objects.
- s3:PutObject on project bucket objects.
- s3:DeleteObject only on bronze/integration-tests/*.

No S3 administrator permissions are granted to the application user.
No IAM management permissions are granted to the application user.

IAM policy uses explicit bucket and object ARNs instead of Resource "*".

## 2. Secret Handling

- .env is excluded through .gitignore.
- .env is not tracked by Git.
- .env.example is retained as a configuration template.
- AT_API_KEY and METLINK_API_KEY are read from environment variables.
- AWS credentials are managed through the AWS CLI credential chain.
- AWS credentials must never be committed to source control.

Local development only:
- Docker Compose has a fallback PostgreSQL password.
- The fallback password must not be used in production.
- Production deployments must inject strong secrets through a secure
  secret-management mechanism.

## 3. GitHub Actions Security

- CI workflow permissions are restricted to contents: read.
- CI runs lint, tests and package build.
- No repository write permission is required by the current workflow.

## 4. S3 Bucket Security

Bucket: sin8886-transport-lakehouse
Region: ap-southeast-2

Verified through AWS Console:

- Block Public Access: all four settings enabled.
- Default encryption: SSE-S3 (AES-256).
- Bucket Versioning: Suspended.
- No custom bucket policy is configured.

Application IAM user cannot read administrative bucket configuration.
AccessDenied from GetBucketLifecycleConfiguration is expected and
does not indicate a failed Lifecycle configuration.

## 5. S3 Lifecycle Management

Three enabled Lifecycle rules were confirmed through AWS Console:

| Rule | Prefix | Expiration |
|---|---|---|
| expire-bronze-integration-tests | bronze/integration-tests/ | 7 days |
| expire-silver-integration-tests | silver/integration-tests/ | 7 days |
| expire-gold-integration-tests | gold/integration-tests/ | 7 days |

Production Bronze, Silver and Gold data are not included in these
expiration rules.

No automatic transition of production data to archive storage is
configured.

## 6. Cost Optimization

- Keep local Kafka, Spark and PostgreSQL workloads on Docker.
- Use S3 for cloud object storage.
- Retain SSE-S3 instead of introducing unnecessary KMS costs.
- Automatically expire temporary integration-test objects after 7 days.
- Avoid automatic expiration of production lake data.
- Review S3 storage usage and AWS billing periodically.
- Review incomplete multipart uploads and configure cleanup if needed.
- Avoid unnecessary cross-region data transfer.

## 7. Final Verification

- [x] IAM policy reviewed.
- [x] Application IAM permissions follow least privilege.
- [x] .env excluded from Git.
- [x] .env.example available.
- [x] CI permissions restricted.
- [x] S3 Block Public Access verified.
- [x] S3 SSE-S3 encryption verified.
- [x] Bucket Versioning status verified.
- [x] Three S3 Lifecycle rules enabled.
- [x] Full pytest suite passed (459 passed, 14 skipped).
- [x] Ruff check passed.
- [x] Git diff check passed.
- [ ] Changes committed and pushed.

