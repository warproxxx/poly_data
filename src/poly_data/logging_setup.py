from __future__ import annotations

import logging

from poly_data.io.platform import setup_utf8_console

_CONFIGURED = False


def configure_logging(level: int = logging.INFO, *, rich: bool = True) -> None:
    """Idempotent logging config. Uses RichHandler when available."""
    global _CONFIGURED
    setup_utf8_console()

    root = logging.getLogger()
    if _CONFIGURED:
        root.setLevel(level)
        return

    handler: logging.Handler
    if rich:
        try:
            from rich.logging import RichHandler
            handler = RichHandler(rich_tracebacks=True, show_path=False)
            fmt = "%(message)s"
        except ImportError:
            handler = logging.StreamHandler()
            fmt = "%(asctime)s %(levelname)-7s %(name)s — %(message)s"
    else:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s %(levelname)-7s %(name)s — %(message)s"

    handler.setFormatter(logging.Formatter(fmt))
    root.handlers = [handler]
    root.setLevel(level)
    _CONFIGURED = True
