"""Validate the Auckland Transport Combined GTFS Realtime Feed."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_ENDPOINT = "https://api.at.govt.nz/realtime/legacy/"


def load_dotenv(path: Path) -> None:
    """Load simple KEY=VALUE entries without printing their values."""
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        name = name.strip()
        value = value.strip().strip('"').strip("'")
        if name and name not in os.environ:
            os.environ[name] = value


def summarize_entity(entity: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {"keys": sorted(entity)}
    for key in ("id", "trip_update", "vehicle", "alert"):
        value = entity.get(key)
        if isinstance(value, dict):
            summary[key] = {"keys": sorted(value)}
            for nested_key in (
                "trip",
                "vehicle",
                "position",
                "stop_time_update",
                "active_period",
                "informed_entity",
            ):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, dict):
                    summary[key][nested_key] = {"keys": sorted(nested_value)}
                elif isinstance(nested_value, list):
                    summary[key][nested_key] = {
                        "type": "list",
                        "count": len(nested_value),
                        "item_keys": (
                            sorted(nested_value[0])
                            if nested_value and isinstance(nested_value[0], dict)
                            else []
                        ),
                    }
    return summary


def validate_combined_feed(endpoint: str, api_key: str, timeout: int) -> dict[str, Any]:
    request = Request(
        endpoint,
        headers={
            "Accept": "application/json",
            "Ocp-Apim-Subscription-Key": api_key,
            "User-Agent": "real-time-transport-data-lakehouse-source-validation/1.0",
        },
    )
    result: dict[str, Any] = {"feed": "combined", "endpoint": endpoint}
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.status
            content_type = response.headers.get("Content-Type", "")
            body = response.read()
        result.update(
            {
                "http_status": status,
                "content_type": content_type,
                "body_bytes": len(body),
            }
        )
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            result.update({"json": False, "error": "response was not valid JSON"})
            return result

        response_payload = (
            payload.get("response", {}) if isinstance(payload, dict) else {}
        )
        entities = (
            response_payload.get("entity", [])
            if isinstance(response_payload, dict)
            else []
        )
        entity_types = (
            Counter(
                key
                for entity in entities
                if isinstance(entity, dict)
                for key in ("trip_update", "vehicle", "alert")
                if key in entity
            )
            if isinstance(entities, list)
            else Counter()
        )
        entity_type_summaries: dict[str, dict[str, Any]] = {}
        if isinstance(entities, list):
            for entity_type in sorted(entity_types):
                sample = next(
                    entity
                    for entity in entities
                    if isinstance(entity, dict) and entity_type in entity
                )
                entity_type_summaries[entity_type] = summarize_entity(sample)
        result.update(
            {
                "json": True,
                "top_level_keys": sorted(payload) if isinstance(payload, dict) else [],
                "status": payload.get("status") if isinstance(payload, dict) else None,
                "response_status": (
                    response_payload.get("status")
                    if isinstance(response_payload, dict)
                    else None
                ),
                "response_keys": (
                    sorted(response_payload)
                    if isinstance(response_payload, dict)
                    else []
                ),
                "entity_count": len(entities) if isinstance(entities, list) else 0,
                "entity_types": dict(sorted(entity_types.items())),
                "entity_summaries": [
                    summarize_entity(entity)
                    for entity in entities[:3]
                    if isinstance(entity, dict)
                ],
                "entity_type_summaries": entity_type_summaries,
            }
        )
        return result
    except HTTPError as error:
        result.update({"http_status": error.code, "error": "HTTP request failed"})
    except (TimeoutError, URLError) as error:
        result.update({"error": f"request failed: {type(error).__name__}"})
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--endpoint", default=os.getenv("AT_REALTIME_ENDPOINT", DEFAULT_ENDPOINT)
    )
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    api_key = os.getenv("AT_API_KEY")
    if not api_key:
        print("AT_API_KEY is not configured.", file=sys.stderr)
        return 2

    result = validate_combined_feed(args.endpoint, api_key, args.timeout)
    print(json.dumps(result, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
