"""The command line starts and reports version and configuration."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from gbmi import __version__
from gbmi.cli import app
from gbmi.config import get_settings

runner = CliRunner()


def test_version_command() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_config_show_masks_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBMI_CH_URL", "http://dbt_rw:hunter2@clickhouse:8123")
    get_settings.cache_clear()

    result = runner.invoke(app, ["config", "show"])
    get_settings.cache_clear()

    assert result.exit_code == 0
    shown = json.loads(result.stdout)
    assert shown["lake_backend"] == "local"
    assert "hunter2" not in result.stdout


def test_config_show_masks_the_user_agent_for_the_sec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBMI_SEC_USER_AGENT", "Sample Company Name contact@example.org")
    monkeypatch.setenv("GBMI_SEC_SOURCE", "live")
    get_settings.cache_clear()

    result = runner.invoke(app, ["config", "show"])
    get_settings.cache_clear()

    assert result.exit_code == 0
    shown = json.loads(result.stdout)
    assert shown["sec_source"] == "live"
    assert shown["sec_user_agent"] == "**********"
    assert "contact@example.org" not in result.stdout and "Sample Company" not in result.stdout
