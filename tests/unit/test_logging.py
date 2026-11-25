"""Log lines are JSON and carry the run identifier."""

from __future__ import annotations

import io
import json

from gbmi.logging import bind_run, configure_logging, get_logger


def _lines(stream: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_log_line_is_json_with_run_identifier() -> None:
    stream = io.StringIO()
    configure_logging(run_id="manual__2026-11-25T08:00:00+00:00", stream=stream)

    get_logger("gbmi.test").info("slot_landed", feed="export", bytes=76194)

    (line,) = _lines(stream)
    assert line["event"] == "slot_landed"
    assert line["run_id"] == "manual__2026-11-25T08:00:00+00:00"
    assert line["logger"] == "gbmi.test"
    assert line["level"] == "info"
    assert line["feed"] == "export"
    assert line["bytes"] == 76194
    assert str(line["timestamp"]).endswith("Z")
    assert "pipeline_version" in line


def test_level_filter_and_late_binding() -> None:
    stream = io.StringIO()
    configure_logging(level="WARNING", stream=stream)
    logger = get_logger()

    logger.info("ignored")
    bind_run("run-2", dag_id="tracer_gdelt_events")
    logger.warning("kept")

    (line,) = _lines(stream)
    assert line["event"] == "kept"
    assert line["run_id"] == "run-2"
    assert line["dag_id"] == "tracer_gdelt_events"
