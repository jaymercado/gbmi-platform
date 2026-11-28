"""Settings load from the environment and reject inconsistent combinations."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from gbmi.config import Environment, LakeBackend, SecSource, Settings, get_settings


@pytest.fixture(autouse=True)
def _clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "GBMI_ENV",
        "GBMI_LAKE_BACKEND",
        "GBMI_LAKE_URI",
        "GBMI_CATALOG_URI",
        "GBMI_PG_DSN",
        "GBMI_MIGRATIONS_DIR",
        "GBMI_CH_URL",
        "GBMI_SEC_USER_AGENT",
        "GBMI_SEC_SOURCE",
        "GBMI_SEC_MAX_RPS",
        "GBMI_SEC_FIXTURES_DIR",
        "GBMI_WATCHLIST_SIZE",
        "GBMI_ALERT_WEBHOOK",
        "GBMI_PIPELINE_VERSION",
    ):
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()


def test_defaults_describe_the_local_backend() -> None:
    settings = Settings()

    assert settings.env is Environment.LOCAL
    assert settings.lake_backend is LakeBackend.LOCAL
    assert settings.bronze_uri == "file:///lake/raw"
    assert settings.quarantine_uri == "file:///lake/quarantine"
    assert settings.warehouse_uri == "file:///lake/warehouse"
    assert settings.catalog_uri == "sqlite:////lake/catalog/catalog.db"
    assert settings.silver_namespace == "silver"
    assert settings.watchlist_size == 50


def test_environment_variables_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBMI_ENV", "prod")
    monkeypatch.setenv("GBMI_LAKE_BACKEND", "aws")
    monkeypatch.setenv("GBMI_LAKE_URI", "s3://gbmi-prod-lake-000000000000/")
    monkeypatch.setenv("GBMI_CATALOG_URI", "glue://us-east-1")
    monkeypatch.setenv("GBMI_WATCHLIST_SIZE", "500")
    monkeypatch.setenv("GBMI_PIPELINE_VERSION", "abc1234")

    settings = Settings()

    assert settings.env is Environment.PROD
    assert settings.warehouse_uri == "s3://gbmi-prod-lake-000000000000/warehouse"
    assert settings.silver_namespace == "gbmi_prod_silver"
    assert settings.watchlist_size == 500
    assert settings.pipeline_version == "abc1234"


def test_ci_namespace_on_aws_is_separate_from_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBMI_ENV", "ci")
    monkeypatch.setenv("GBMI_LAKE_BACKEND", "aws")
    monkeypatch.setenv("GBMI_LAKE_URI", "s3://gbmi-prod-lake-000000000000/ci")
    monkeypatch.setenv("GBMI_CATALOG_URI", "glue://us-east-1")

    settings = Settings()

    assert settings.silver_namespace == "gbmi_ci_silver"
    assert settings.warehouse_uri == "s3://gbmi-prod-lake-000000000000/ci/warehouse"


@pytest.mark.parametrize(
    ("backend", "lake_uri", "catalog_uri"),
    [
        ("local", "s3://bucket", "sqlite:////lake/catalog/catalog.db"),
        ("local", "file:///lake", "glue://us-east-1"),
        ("aws", "file:///lake", "glue://us-east-1"),
        ("aws", "s3://bucket", "sqlite:////lake/catalog/catalog.db"),
    ],
)
def test_backend_and_uris_must_agree(
    monkeypatch: pytest.MonkeyPatch, backend: str, lake_uri: str, catalog_uri: str
) -> None:
    monkeypatch.setenv("GBMI_LAKE_BACKEND", backend)
    monkeypatch.setenv("GBMI_LAKE_URI", lake_uri)
    monkeypatch.setenv("GBMI_CATALOG_URI", catalog_uri)

    with pytest.raises(ValidationError):
        Settings()


def test_unknown_environment_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBMI_ENV", "staging")

    with pytest.raises(ValidationError):
        Settings()


def test_secrets_are_masked_when_printed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBMI_CH_URL", "http://dbt_rw:hunter2@clickhouse:8123")
    monkeypatch.setenv("GBMI_PG_DSN", "postgresql://gbmi:hunter2@postgres/gbmi")

    settings = Settings()

    assert "hunter2" not in repr(settings)
    assert "hunter2" not in settings.model_dump_json()
    assert settings.ch_url is not None
    assert settings.ch_url.get_secret_value().endswith("@clickhouse:8123")


def test_get_settings_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    first = get_settings()
    monkeypatch.setenv("GBMI_WATCHLIST_SIZE", "7")

    assert get_settings() is first


def test_without_settings_no_client_of_the_sec_can_start() -> None:
    settings = Settings()

    assert settings.sec_source is SecSource.OFF
    assert settings.sec_user_agent is None
    assert settings.sec_max_rps == 8
    assert settings.sec_fixtures_dir == "/fixtures/sec"


def test_the_sec_settings_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBMI_SEC_SOURCE", "fixture")
    monkeypatch.setenv("GBMI_SEC_MAX_RPS", "4")
    monkeypatch.setenv("GBMI_SEC_FIXTURES_DIR", "/tmp/sec")
    monkeypatch.setenv("GBMI_SEC_USER_AGENT", "Sample Company Name contact@example.org")

    settings = Settings()

    assert (settings.sec_source, settings.sec_max_rps) == (SecSource.FIXTURE, 4)
    assert settings.sec_fixtures_dir == "/tmp/sec"
    assert settings.sec_user_agent is not None
    assert settings.sec_user_agent.get_secret_value() == "Sample Company Name contact@example.org"
    # The setting is a secret: no text form of the settings shows the value.
    for text in (repr(settings), str(settings), settings.model_dump_json()):
        assert "contact@example.org" not in text


@pytest.mark.parametrize("value", ["", "   "])
def test_an_empty_user_agent_is_no_user_agent(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    # Compose passes an unset variable as an empty text.
    monkeypatch.setenv("GBMI_SEC_USER_AGENT", value)

    assert Settings().sec_user_agent is None


@pytest.mark.parametrize("value", ["0", "9", "10", "16"])
def test_the_ceiling_of_eight_requests_cannot_be_raised(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("GBMI_SEC_MAX_RPS", value)

    with pytest.raises(ValidationError, match="sec_max_rps"):
        Settings()


def test_a_validation_error_never_shows_the_value_which_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GBMI_SEC_SOURCE", "Sample-Company-Name-contact@example.org")

    with pytest.raises(ValidationError) as raised:
        Settings()

    assert "sec_source" in str(raised.value)
    assert "contact@example.org" not in str(raised.value)
    assert "input_value" not in str(raised.value)
