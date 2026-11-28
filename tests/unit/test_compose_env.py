"""What a container receives from the env file through Compose (week 3 plan, finding RF8).

The owner writes the user agent for the SEC into the git-ignored ``.env``, and the value can
stand in quotes there. The test renders ``compose/compose.yaml`` with an env file of made-up
values and reads the environment of the services: the quotes are gone, and every service of
the stack receives the three SEC variables.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
IDENTITY = "Sample Company Name contact@example.org"
SERVICES = ("airflow-scheduler", "airflow-apiserver", "airflow-dag-processor", "toolbox")

_STACK = ("GBMI_", "COMPOSE_", "AIRFLOW_", "DBT_", "MEM_", "CH_", "METABASE_", "POSTGRES_")


def _compose_is_installed() -> bool:
    docker = shutil.which("docker")
    if docker is None:
        return False
    return subprocess.run([docker, "compose", "version"], capture_output=True).returncode == 0


pytestmark = pytest.mark.skipif(
    not _compose_is_installed(), reason="Docker with the Compose plugin is not installed"
)


def render(tmp_path: Path, *lines: str, **environment: str) -> dict[str, dict[str, str]]:
    """The environment of each service, as Compose renders the file."""
    template = (REPO / ".env.example").read_text(encoding="utf-8")
    made_up = template.replace("__GENERATE_FERNET__", "fernet").replace("__GENERATE__", "made-up")
    env_file = tmp_path / "env"
    env_file.write_text(made_up + "\n".join(lines) + "\n", encoding="utf-8")
    command = [
        shutil.which("docker") or "docker",
        "compose",
        "--project-name",
        "gbmi-render",
        "--project-directory",
        str(REPO),
        "--env-file",
        str(env_file),
        "-f",
        str(REPO / "compose" / "compose.yaml"),
        "--profile",
        "tools",
        "config",
        "--format",
        "json",
    ]
    # A variable of the shell wins over the env file in Compose, so the stack variables of the
    # caller (the Makefile exports some) are removed first.
    base = {name: value for name, value in os.environ.items() if not name.startswith(_STACK)}
    result = subprocess.run(
        command, capture_output=True, text=True, check=False, env={**base, **environment}
    )
    assert result.returncode == 0, result.stderr
    services = json.loads(result.stdout)["services"]
    return {name: services[name]["environment"] for name in SERVICES}


@pytest.mark.parametrize(
    "line", [f'GBMI_SEC_USER_AGENT="{IDENTITY}"', f"GBMI_SEC_USER_AGENT={IDENTITY}"]
)
def test_a_quoted_value_of_the_env_file_arrives_without_the_quotes(
    tmp_path: Path, line: str
) -> None:
    rendered = render(tmp_path, line, GBMI_SEC_SOURCE="live")

    for service in SERVICES:
        environment = rendered[service]
        assert environment["GBMI_SEC_USER_AGENT"] == IDENTITY
        assert environment["GBMI_SEC_SOURCE"] == "live"
        assert environment["GBMI_SEC_FIXTURES_DIR"] == "/fixtures/sec"
        assert environment["GBMI_WATCHLIST_FILE"] == "/opt/gbmi/dbt/seeds/watchlist.csv"
        assert environment["GBMI_WATCHLIST_SIZE"] == "50"


def test_without_the_makefile_the_source_is_off_and_the_identity_is_empty(tmp_path: Path) -> None:
    rendered = render(tmp_path)

    for service in SERVICES:
        assert rendered[service]["GBMI_SEC_SOURCE"] == "off"
        assert rendered[service]["GBMI_SEC_USER_AGENT"] == ""
