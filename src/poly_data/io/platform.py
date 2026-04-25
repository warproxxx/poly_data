from __future__ import annotations

import os
import sys
from pathlib import Path

_CHUNK = 4096


def last_line(path: Path) -> str | None:
    """Return the last non-empty line of `path`, or None if missing/empty.

    Reverse-seeks in CHUNK-sized blocks; cross-platform; handles \\n, \\r\\n,
    no-trailing-newline, UTF-8 BOM. Returns the decoded string with trailing
    line terminators stripped.
    """
    p = Path(path)
    if not p.is_file():
        return None
    size = p.stat().st_size
    if size == 0:
        return None

    with p.open("rb") as f:
        end = size
        buf = b""
        while end > 0:
            read_size = min(_CHUNK, end)
            end -= read_size
            f.seek(end)
            buf = f.read(read_size) + buf
            # Strip trailing whitespace (including newlines) before searching.
            stripped = buf.rstrip(b"\r\n")
            if not stripped:
                # only whitespace so far, keep reading earlier
                continue
            nl = stripped.rfind(b"\n")
            if nl != -1:
                line_bytes = stripped[nl + 1 :]
                return _decode(line_bytes)
            if end == 0:
                return _decode(stripped)
    return None


def _decode(b: bytes) -> str:
    # Strip leading UTF-8 BOM if present.
    if b.startswith(b"\xef\xbb\xbf"):
        b = b[3:]
    return b.rstrip(b"\r\n").decode("utf-8", errors="replace")


def atomic_write(path: Path, data: bytes | str) -> None:
    """Atomically write data to path via a tmp file in the same dir + os.replace."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        data = data.encode("utf-8")
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)


def setup_utf8_console() -> None:
    """Force UTF-8 on stdout/stderr (Windows cp1252 default crashes on emoji)."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass
