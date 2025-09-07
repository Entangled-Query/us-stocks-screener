from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional


DEFAULT_CACHE_DIR = Path("data/cache")
DEFAULT_OUTPUT_DIR = Path("data/outputs")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_text_cached(url: str, cache_file: Path, refresh: bool = False, fetch_fn=None) -> str:
    """Read text content from cache or fetch via provided function.

    fetch_fn should be a function `() -> str` that returns text.
    """
    if cache_file.exists() and not refresh:
        return cache_file.read_text(encoding="utf-8")
    if fetch_fn is None:
        raise ValueError("fetch_fn must be provided when cache miss")
    ensure_dir(cache_file.parent)
    text = fetch_fn()
    cache_file.write_text(text, encoding="utf-8")
    return text


def read_json_cached(url: str, cache_file: Path, refresh: bool = False, fetch_fn=None) -> Any:
    if cache_file.exists() and not refresh:
        return json.loads(cache_file.read_text(encoding="utf-8"))
    if fetch_fn is None:
        raise ValueError("fetch_fn must be provided when cache miss")
    ensure_dir(cache_file.parent)
    data = fetch_fn()
    if isinstance(data, str):
        cache_file.write_text(data, encoding="utf-8")
        return json.loads(data)
    else:
        cache_file.write_text(json.dumps(data), encoding="utf-8")
        return data


def throttle(seconds: float) -> None:
    if seconds and seconds > 0:
        time.sleep(seconds)


def safe_int(s: Optional[str]) -> Optional[int]:
    try:
        return int(s) if s is not None and s != "" else None
    except Exception:
        return None

