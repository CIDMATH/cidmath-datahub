"""Structured logger for production pipeline code.

Use this in `src/cidmath_datahub/` rather than `print()`. Output is JSON-line
formatted so it can be parsed by Databricks' log aggregation and the
observability dashboards (ADR 0010).

Usage:

    from cidmath_datahub.common.logging import get_logger
    log = get_logger(__name__)
    log.info("Ingested CDC NWSS batch", extra={"rows": 12_345, "source_file": uri})

The structured `extra` keys end up as top-level JSON fields, making them
queryable in log-based dashboards.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any


class _JsonFormatter(logging.Formatter):
    """Format LogRecord as a single-line JSON object."""

    _RESERVED = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Surface any `extra=` keys passed by the caller.
        for key, value in record.__dict__.items():
            if key in self._RESERVED or key.startswith("_"):
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, sort_keys=True)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a configured logger that emits JSON lines to stdout.

    Idempotent — calling `get_logger` repeatedly with the same name returns
    the same logger and does not stack handlers.
    """
    logger = logging.getLogger(name)
    if getattr(logger, "_cidmath_configured", False):
        return logger

    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    logger._cidmath_configured = True  # type: ignore[attr-defined]
    return logger
