"""Check that the local Docker infrastructure is reachable."""

from __future__ import annotations

import os
from pathlib import Path
import socket
import subprocess
import sys
from typing import Callable
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]


def load_local_value(name: str, default: str) -> str:
    """Read one non-secret local setting without printing its value."""
    value = os.getenv(name)
    if value is not None:
        return value

    env_file = ROOT / ".env"
    if not env_file.exists():
        return default

    for line in env_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() == name:
            return value.strip().strip('"').strip("'")
    return default


def check_tcp(host: str, port: int) -> None:
    with socket.create_connection((host, port), timeout=3):
        pass


def check_http(url: str) -> None:
    with urlopen(url, timeout=3) as response:
        if response.status < 200 or response.status >= 400:
            raise RuntimeError(f"HTTP status {response.status}")


def check_postgres() -> None:
    user = load_local_value("POSTGRES_USER", "transport")
    database = load_local_value("POSTGRES_DB", "transport_lakehouse")
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "pg_isready",
            "-U",
            user,
            "-d",
            database,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(detail or "pg_isready failed")
    postgres_port = int(load_local_value("POSTGRES_PORT", "5433"))
    check_tcp("localhost", postgres_port)


def run_check(name: str, check: Callable[[], None]) -> bool:
    try:
        check()
    except (OSError, RuntimeError, URLError) as error:
        print(f"FAIL {name}: {error}")
        return False
    print(f"PASS {name}")
    return True


def main() -> int:
    checks = [
        ("PostgreSQL connectivity", check_postgres),
        ("Kafka availability", lambda: check_tcp("localhost", 9092)),
        ("Spark Master availability", lambda: check_http("http://localhost:8080")),
    ]
    results = [run_check(name, check) for name, check in checks]
    passed = sum(results)
    print(f"Result: {passed}/{len(results)} checks passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
