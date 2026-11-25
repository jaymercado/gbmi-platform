"""Structured JSON logs that carry a run identifier (blueprint 5.10)."""

from __future__ import annotations

import logging
import sys
from typing import Any, TextIO

import structlog

from gbmi import __version__


def configure_logging(
    *,
    run_id: str | None = None,
    level: str = "INFO",
    stream: TextIO | None = None,
) -> None:
    """Configure structlog to write one JSON object per line.

    Logs go to standard error so that a command's standard output stays free for its
    result. Airflow captures both streams into the task log.
    """
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _name_logger,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelNamesMapping()[level]),
        logger_factory=(
            structlog.PrintLoggerFactory(file=stream) if stream is not None else _stderr_logger
        ),
        cache_logger_on_first_use=False,
    )
    structlog.contextvars.bind_contextvars(pipeline_version=__version__)
    if run_id is not None:
        bind_run(run_id)


class _StderrLogger:
    """Writes each line to whatever ``sys.stderr`` is when the line is logged.

    Capturing the stream once, at configuration time, would keep writing to a stream that a
    test runner or a task wrapper has since replaced or closed.
    """

    def msg(self, message: str) -> None:
        print(message, file=sys.stderr, flush=True)  # noqa: T201 - this is the log sink

    log = debug = info = warn = warning = msg
    fatal = failure = err = error = critical = exception = msg


def _stderr_logger(*_args: Any) -> _StderrLogger:
    return _StderrLogger()


def bind_run(run_id: str, **fields: Any) -> None:
    """Attach the orchestrator run identifier, and any other fields, to every later log line."""
    structlog.contextvars.bind_contextvars(run_id=run_id, **fields)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a logger; ``name`` is recorded as the ``logger`` field.

    The logger is a lazy proxy: it takes the configuration in force when a line is logged, so
    a module-level logger created at import still follows a later ``configure_logging``.
    """
    logger: structlog.stdlib.BoundLogger = (
        structlog.get_logger(logger_name=name) if name else structlog.get_logger()
    )
    return logger


def _name_logger(
    _logger: Any, _method: str, event: structlog.typing.EventDict
) -> structlog.typing.EventDict:
    """Report the logger's name as ``logger`` (structlog reserves that keyword for itself)."""
    name = event.pop("logger_name", None)
    if name is not None:
        event["logger"] = name
    return event
