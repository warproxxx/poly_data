from __future__ import annotations

import logging

from poly_data.logging_setup import configure_logging


def test_configure_logging_idempotent(caplog) -> None:
    configure_logging(level=logging.INFO)
    handlers_first = list(logging.getLogger().handlers)
    configure_logging(level=logging.INFO)
    handlers_second = list(logging.getLogger().handlers)
    assert len(handlers_second) == len(handlers_first)


def test_configure_logging_respects_level(caplog) -> None:
    configure_logging(level=logging.WARNING)
    logger = logging.getLogger("test")
    with caplog.at_level(logging.WARNING):
        logger.info("hidden")
        logger.warning("shown")
    assert any("shown" in r.getMessage() for r in caplog.records)
    assert not any("hidden" in r.getMessage() for r in caplog.records)
