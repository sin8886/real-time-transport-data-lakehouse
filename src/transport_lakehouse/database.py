"""PostgreSQL connection utilities."""

from __future__ import annotations

import os
from contextlib import contextmanager
from collections.abc import Iterator

import psycopg


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    """Yield a PostgreSQL connection and close it when the context exits."""
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    if not database or not user or not password:
        raise ValueError(
            "POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD must be set"
        )

    try:
        port = int(os.getenv("POSTGRES_PORT", "5433"))
    except ValueError as exc:
        raise ValueError("POSTGRES_PORT must be a valid integer") from exc

    try:
        connection = psycopg.connect(
            dbname=database,
            user=user,
            password=password,
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=port,
        )
    except psycopg.Error as exc:
        raise ConnectionError("Unable to connect to PostgreSQL") from exc

    try:
        yield connection
    finally:
        connection.close()
