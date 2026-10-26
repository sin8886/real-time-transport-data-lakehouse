"""Persistent watermark storage for batch pipelines."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from re import fullmatch
from typing import Any


class WatermarkStore:
    """Read and atomically update provider-specific JSON watermarks."""

    VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @staticmethod
    def _validate_watermark(provider: str, watermark: Any) -> dict[str, Any]:
        if not isinstance(watermark, dict):
            raise ValueError(f"Invalid watermark for provider: {provider}")

        fingerprint = watermark.get("fingerprint")
        if (
            not isinstance(fingerprint, str)
            or fullmatch(r"[0-9a-fA-F]{64}", fingerprint) is None
        ):
            raise ValueError(f"Invalid watermark for provider: {provider}")

        ingest_date = watermark.get("ingest_date")
        if not isinstance(ingest_date, str):
            raise ValueError(f"Invalid watermark for provider: {provider}")
        try:
            parsed_date = date.fromisoformat(ingest_date)
        except ValueError as exc:
            raise ValueError(f"Invalid watermark for provider: {provider}") from exc
        if parsed_date.isoformat() != ingest_date:
            raise ValueError(f"Invalid watermark for provider: {provider}")

        processed_at = watermark.get("processed_at")
        if not isinstance(processed_at, str):
            raise ValueError(f"Invalid watermark for provider: {provider}")
        try:
            parsed_timestamp = datetime.fromisoformat(processed_at)
        except ValueError as exc:
            raise ValueError(f"Invalid watermark for provider: {provider}") from exc
        if parsed_timestamp.tzinfo is None:
            raise ValueError(f"Invalid watermark for provider: {provider}")

        return watermark

    @classmethod
    def _validate_document(cls, document: Any) -> dict[str, Any]:
        if (
            not isinstance(document, dict)
            or document.get("version") != cls.VERSION
            or not isinstance(document.get("feeds"), dict)
        ):
            raise ValueError("Invalid watermark document")

        for provider, watermark in document["feeds"].items():
            cls._validate_watermark(provider, watermark)

        return document

    def read(self, provider: str) -> dict[str, Any] | None:
        """Return a provider watermark, or None when no watermark exists."""
        if not self.path.exists():
            return None

        try:
            with self.path.open(encoding="utf-8") as watermark_file:
                document = json.load(watermark_file)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Invalid watermark file: {self.path}") from exc

        try:
            document = self._validate_document(document)
        except ValueError as exc:
            raise ValueError(f"Invalid watermark file: {self.path}") from exc

        watermark = document["feeds"].get(provider)
        if watermark is None:
            return None

        return watermark

    def write(self, provider: str, watermark: dict[str, Any]) -> None:
        """Persist a provider watermark using an atomic same-directory replace."""
        self._validate_watermark(provider, watermark)

        document: dict[str, Any] = {"version": self.VERSION, "feeds": {}}
        if self.path.exists():
            try:
                with self.path.open(encoding="utf-8") as watermark_file:
                    document = json.load(watermark_file)
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError(f"Invalid watermark file: {self.path}") from exc

        try:
            document = self._validate_document(document)
        except ValueError as exc:
            raise ValueError(f"Invalid watermark file: {self.path}") from exc

        document["feeds"][provider] = watermark
        self.path.parent.mkdir(parents=True, exist_ok=True)

        temporary_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                delete=False,
            ) as temporary_file:
                temporary_path = temporary_file.name
                json.dump(document, temporary_file, indent=2, sort_keys=True)
                temporary_file.write("\n")
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_path, self.path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                Path(temporary_path).unlink(missing_ok=True)
