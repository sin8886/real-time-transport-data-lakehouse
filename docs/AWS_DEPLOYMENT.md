# AWS deployment preparation

## Current status

The project has not created or deployed an EC2 instance from this repository.
Local Docker is the default development and demonstration environment, so
there is no EC2 compute charge while the local platform is running.

An always-on public demo requires a continuously running host. The recommended
first deployment is a single private-by-default EC2 host running Docker Compose:

```text
Internet -> HTTPS reverse proxy -> Streamlit
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
7. Verify the dashboard, Kafka-to-Spark-to-PostgreSQL freshness, and S3 access
   from the instance before sharing the URL.
8. Stop the instance when the demo is finished. Review EBS and public IPv4
   resources separately because stopping compute does not necessarily remove
   all charges.

## Cost model

| Mode | Realtime processing | Public access | Main cost |
|---|---|---|---|
| Local | Only while local jobs run | No | Local machine |
| On-demand EC2 | Only while EC2/jobs run | During deployment | EC2, EBS, IPv4, S3 |
| Always-on EC2 | Continuous | Yes | Continuous EC2 plus storage/network |

This document is an implementation runbook, not evidence that AWS deployment
has already been completed.
