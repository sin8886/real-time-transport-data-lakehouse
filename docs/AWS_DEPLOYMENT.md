# AWS deployment and operations

## Current status

An on-demand EC2 demonstration deployment has been completed for the portfolio
environment. The deployment is not always running; verify the instance state
and current public IP before each demonstration.

| Item | Current demonstration configuration |
|---|---|
| Region | `ap-southeast-2` |
| Instance | `i-019630fd6c36b3e84` |
| Instance type | `c7i-flex.large` |
| Application | Docker Compose, Spark serving, Streamlit, Nginx |
| Public entry point | HTTP port 80 through Nginx |
| Domain / HTTPS | Not configured |

The last known public endpoint was `http://3.107.2.91`; it is not permanent.
Stopping and starting the instance can assign a different public IP because an
Elastic IP is not attached. The local Docker environment remains the default
reproducible development environment.

An always-on public demo requires a continuously running host. The recommended
first deployment is a single private-by-default EC2 host running Docker Compose:

```text
Internet -> HTTP reverse proxy -> Streamlit
                          |
                    Kafka / Spark / PostgreSQL
                          |
                    AT realtime producer
```

This is a portfolio demonstration topology, not a production availability
claim. It keeps PostgreSQL, Kafka, Spark, and their administrative ports off
the public internet.

## Safe deployment sequence

1. Create a dedicated security group. Allow TCP 22 only from the operator's
   fixed IP and TCP 80/443 from the internet. Do not expose 5433, 7077, 8080,
   8081, or 9092.
2. Launch an appropriately sized instance in `ap-southeast-2` with encrypted
   EBS. Add a billing budget before starting the instance.
3. Attach an instance profile with least-privilege S3 read access to the
   project bucket. Do not copy long-lived AWS access keys to the host.
4. Store `AT_API_KEY`, database credentials, and TLS configuration in a secret
   manager or protected deployment environment. Never put them in the image,
   Git repository, or Streamlit source.
5. Install Docker, clone the repository, inject secrets, and run the same
   Compose health checks used locally.
6. Put Caddy or Nginx in front of Streamlit and issue an HTTPS certificate for
   a DNS name. Streamlit and internal services remain bound to localhost or the
   private Docker network.
7. Verify the dashboard, Kafka-to-Spark-to-PostgreSQL freshness, and the
   read-only S3 probe from the instance before sharing the URL. The S3 probe
   reports `Not configured` when no instance IAM role or AWS profile is
   available; that is an explicit configuration status, not fabricated health.
8. Stop the instance when the demo is finished. Review EBS and public IPv4
   resources separately because stopping compute does not necessarily remove
   all charges.

## Cost model

| Mode | Realtime processing | Public access | Main cost |
|---|---|---|---|
| Local | Only while local jobs run | No | Local machine |
| On-demand EC2 | Only while EC2/jobs run | During deployment | EC2, EBS, IPv4, S3 |
| Always-on EC2 | Continuous | Yes | Continuous EC2 plus storage/network |

The deployment is suitable for an on-demand portfolio demonstration, not a
production availability claim. PostgreSQL, Kafka, Spark, and their management
ports remain private. The current public proxy is HTTP only because no domain
or TLS certificate has been configured.

## Existing demonstration controls

The EC2 host uses systemd services:

```bash
sudo systemctl status transport-compose.service
sudo systemctl status transport-spark-serving.service
sudo systemctl status transport-dashboard.service
```

The authenticated producer is started separately and must not store the API
key in Git, a Docker image, or a committed systemd unit. Stop compute charges
after a demonstration with:

```bash
aws ec2 stop-instances \
  --region ap-southeast-2 \
  --instance-ids i-019630fd6c36b3e84
```
