from pathlib import Path

import pytest

from transport_lakehouse.processing import watermark as watermark_module
from transport_lakehouse.processing.watermark import WatermarkStore


def build_watermark(fingerprint: str, ingest_date: str) -> dict[str, str]:
    return {
        "fingerprint": fingerprint * 64,
        "ingest_date": ingest_date,
        "processed_at": "2026-10-26T20:00:00+00:00",
    }


def test_read_returns_none_when_watermark_file_does_not_exist(
    tmp_path: Path,
) -> None:
    store = WatermarkStore(tmp_path / "state" / "watermarks.json")

    assert store.read("at") is None


def test_watermarks_are_isolated_between_providers(tmp_path: Path) -> None:
    store = WatermarkStore(tmp_path / "watermarks.json")

    at_watermark = build_watermark("a", "2026-10-26")
    metlink_watermark = build_watermark("b", "2026-10-27")

    store.write("at", at_watermark)
    store.write("metlink", metlink_watermark)

    assert store.read("at") == at_watermark
    assert store.read("metlink") == metlink_watermark


def test_failed_atomic_replace_preserves_existing_watermark(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "watermarks.json"
    store = WatermarkStore(path)

    original = build_watermark("a", "2026-10-26")
    updated = build_watermark("b", "2026-10-27")

    store.write("at", original)
    original_content = path.read_text(encoding="utf-8")

    def fail_replace(source: str, destination: str | Path) -> None:
        raise OSError("simulated atomic replace failure")

    monkeypatch.setattr(watermark_module.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated atomic replace failure"):
        store.write("at", updated)

    assert path.read_text(encoding="utf-8") == original_content
    assert store.read("at") == original


def test_failed_atomic_replace_cleans_up_temporary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "watermarks.json"
    store = WatermarkStore(path)

    store.write("at", build_watermark("a", "2026-10-26"))

    def fail_replace(source: str, destination: str | Path) -> None:
        raise OSError("simulated atomic replace failure")

    monkeypatch.setattr(watermark_module.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated atomic replace failure"):
        store.write("at", build_watermark("b", "2026-10-27"))

    temporary_files = list(tmp_path.glob(".watermarks.json.*"))
    assert temporary_files == []
