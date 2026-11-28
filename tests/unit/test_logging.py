"""Log lines are JSON and carry the run identifier."""

from __future__ import annotations

import io
import json

import pytest

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


def test_logger_created_before_configuration_follows_it() -> None:
    """Modules create their logger at import, before any command configures logging."""
    early = get_logger("gbmi.early")
    stream = io.StringIO()

    configure_logging(run_id="run-3", level="WARNING", stream=stream)
    early.info("filtered_out")
    early.warning("kept", slot="20261125084500")

    (line,) = _lines(stream)
    assert (line["event"], line["logger"], line["run_id"]) == ("kept", "gbmi.early", "run-3")


def test_a_logged_exception_never_renders_local_variables() -> None:
    """A local variable can hold a secret, for example a user agent with a contact address."""
    stream = io.StringIO()
    configure_logging(stream=stream)

    def send(user_agent: str) -> None:
        headers = {"User-Agent": user_agent}
        raise RuntimeError(f"the request with {len(headers)} header failed")

    try:
        send("Sample Company Name contact@example.org")
    except RuntimeError:
        get_logger("gbmi.test").exception("request_failed")

    (line,) = _lines(stream)
    assert line["event"] == "request_failed"
    (error,) = line["exception"]  # type: ignore[misc]
    assert error["exc_type"] == "RuntimeError"
    assert error["exc_value"] == "the request with 1 header failed"
    assert [frame["name"] for frame in error["frames"]][-1] == "send"
    assert not [frame for frame in error["frames"] if frame.get("locals")]
    assert "contact@example.org" not in stream.getvalue()


def test_a_log_line_carries_the_pipeline_version_of_the_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Week 3 plan, finding F19 i: in an image the version is the Git commit, and a log line
    # names the same version as the rows and the run records of the run.
    from gbmi.config import get_settings

    commit = "0123456789abcdef0123456789abcdef01234567"
    monkeypatch.setenv("GBMI_PIPELINE_VERSION", commit)
    get_settings.cache_clear()
    stream = io.StringIO()
    try:
        configure_logging(stream=stream)
        get_logger("gbmi.test").info("built")
    finally:
        get_settings.cache_clear()

    (line,) = _lines(stream)
    assert line["pipeline_version"] == commit


def test_settings_which_cannot_be_read_do_not_stop_the_logging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gbmi import __version__
    from gbmi.config import get_settings

    # The backend and the address of the lake do not fit: the settings refuse to load.
    monkeypatch.setenv("GBMI_LAKE_BACKEND", "aws")
    monkeypatch.setenv("GBMI_LAKE_URI", "file:///lake")
    get_settings.cache_clear()
    stream = io.StringIO()
    try:
        configure_logging(stream=stream)
        get_logger("gbmi.test").info("still logging")
    finally:
        get_settings.cache_clear()

    (line,) = _lines(stream)
    assert (line["event"], line["pipeline_version"]) == ("still logging", __version__)
