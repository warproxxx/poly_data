from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from poly_data.io.platform import atomic_write


def load(path: Path) -> dict[str, Any] | None:
    """Return cursor dict, or None if file missing/empty/corrupt."""
    p = Path(path)
    if not p.is_file():
        return None
    try:
        text = p.read_text(encoding="utf-8")
    except OSError:
        return None
    text = text.strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    return obj


def save(path: Path, state: dict[str, Any]) -> None:
    """Atomically persist `state` as JSON."""
    payload = json.dumps(state, sort_keys=True).encode("utf-8")
    atomic_write(Path(path), payload)
