"""The checks of the lake backend which the smoke test of a deployment runs."""

from __future__ import annotations

from pathlib import Path

import pytest

from gbmi.config import Environment, LakeBackend, Settings
from gbmi.lake.check import check_object, check_version, run_checks

COMMIT = "0123456789abcdef0123456789abcdef01234567"


def host(version: str) -> Settings:
    return Settings(
        env=Environment.PROD,
        lake_backend=LakeBackend.AWS,
        lake_uri="s3://gbmi-prod-lake-000000000000",
        catalog_uri="glue://us-east-1",
        pipeline_version=version,
    )


def test_one_object_is_written_read_and_deleted(tmp_path: Path) -> None:
    result = check_object(f"file://{tmp_path}/tmp/smoke/")

    assert result["deleted"] is True
    assert result["bytes"] > 0
    # Nothing stays behind: neither the object nor a temporary file.
    assert [path for path in tmp_path.rglob("*") if path.is_file()] == []


def test_the_host_has_to_run_the_commit_of_the_release() -> None:
    assert check_version(host(COMMIT), COMMIT) == {"pipeline_version": COMMIT, "env": "prod"}
    with pytest.raises(ValueError, match="has to be a Git commit"):
        check_version(host("0.0.1"), None)
    with pytest.raises(ValueError, match="and the release is"):
        check_version(host(COMMIT), "f" * 40)
    # The laptop runs the package version.
    assert check_version(Settings(), None)["env"] == "local"


def test_a_failed_check_is_a_result_and_the_other_checks_still_run(tmp_path: Path) -> None:
    # No catalog and no ClickHouse here: the two checks fail, and the two others pass.
    settings = Settings(
        lake_uri=f"file://{tmp_path}/lake",
        catalog_uri=f"sqlite:///{tmp_path}/lake/catalog/catalog.db",
    )

    checks = {check.name: check for check in run_checks(settings)}

    assert list(checks) == ["catalog", "object", "pin", "version"]
    assert checks["object"].passed and checks["version"].passed
    assert not checks["catalog"].passed
    assert "no namespace silver" in checks["catalog"].detail["error"]
    assert not checks["pin"].passed
    assert "GBMI_CH_URL is not set" in checks["pin"].detail["error"]
