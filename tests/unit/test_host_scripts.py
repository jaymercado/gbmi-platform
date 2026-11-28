"""The scripts of the AWS host, run with stand-in commands (week 3 plan, WP-10 step 5).

``compose/host/`` holds the scripts which write the environment file from Parameter Store,
start the stack and deploy a release. The tests run the real scripts with made-up values. A
stand-in ``aws`` answers with the parameters, and a stand-in ``docker`` records its calls. The
tests check what the scripts write and in which order they call, and search every line of
their output for the made-up values: a script of the host prints no value.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[2]
HOST = REPO / "compose" / "host"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
PREVIOUS = "f" * 40
ACCOUNT = "000000000000"
REQUIRED = (
    "POSTGRES_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "GBMI_DB_PASSWORD",
    "GBMI_RO_DB_PASSWORD",
    "METABASE_DB_PASSWORD",
    "CLICKHOUSE_ADMIN_PASSWORD",
    "CH_DBT_PASSWORD",
    "CH_BI_PASSWORD",
    "CH_GBMI_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_JWT_SECRET",
    "METABASE_ADMIN_PASSWORD",
    "METABASE_ENCRYPTION_KEY",
)
SECRETS = {name: f"madeup{index:02d}{'c0ffee' * 4}" for index, name in enumerate(REQUIRED)}
USER_AGENT = "Sample Company Name contact@example.org"

AWS_STUB = """#!/bin/bash
printf 'aws %s\\n' "$*" >> "$STUB_CALLS"
case "$1 $2" in
  "sts get-caller-identity") printf '%s\\n' "$STUB_ACCOUNT" ;;
  "ssm get-parameters-by-path") cat "$STUB_PARAMETERS" ;;
  *) echo "unexpected call of aws: $*" >&2; exit 64 ;;
esac
"""
DOCKER_STUB = """#!/bin/bash
printf 'docker %s\\n' "$*" >> "$STUB_CALLS"
case " $* " in
  *" up "*) exit "${STUB_UP_EXIT:-0}" ;;
esac
exit 0
"""
SMOKE_STUB = """#!/bin/bash
line="smoke mode=$GBMI_SMOKE_MODE release=$GBMI_RELEASE cwd=$PWD"
printf '%s\\n' "$line" >> "$STUB_CALLS"
exit "${STUB_SMOKE_EXIT:-0}"
"""


def parameters(**extra: str) -> dict[str, Any]:
    values = {**SECRETS, **extra}
    return {
        "Parameters": [
            {"Name": f"/gbmi/prod/{name}", "Type": "SecureString", "Value": value}
            for name, value in values.items()
        ]
    }


class Host:
    """A made-up host in a temporary folder: the folders, the stand-ins and one release."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.home = root / "opt" / "gbmi"
        self.env_file = root / "etc" / "gbmi" / "env"
        self.calls = root / "calls.log"
        self.parameters = root / "parameters.json"
        self.bin = root / "bin"
        self.bin.mkdir()
        for name, text in (("aws", AWS_STUB), ("docker", DOCKER_STUB)):
            stub = self.bin / name
            stub.write_text(text, encoding="utf-8")
            stub.chmod(0o755)
        self.calls.write_text("", encoding="utf-8")
        self.set_parameters(parameters())
        self.release(COMMIT)

    def set_parameters(self, answer: dict[str, Any]) -> None:
        self.parameters.write_text(json.dumps(answer), encoding="utf-8")

    def release(self, commit: str) -> Path:
        """The release folder of a commit, with the real host scripts and a stand-in smoke test."""
        folder = self.home / "releases" / commit
        shutil.copytree(HOST, folder / "compose" / "host")
        smoke = folder / "compose" / "smoke.sh"
        smoke.write_text(SMOKE_STUB, encoding="utf-8")
        return folder

    def run(
        self, script: Path, *arguments: str, **environment: str
    ) -> subprocess.CompletedProcess[str]:
        # The Python of the test stands in for the Python of the host.
        python = self.bin / "python3"
        if not python.exists():
            python.symlink_to(sys.executable)
        return subprocess.run(
            [shutil.which("bash") or "/bin/bash", str(script), *arguments],
            capture_output=True,
            text=True,
            check=False,
            env={
                "PATH": f"{self.bin}:/usr/bin:/bin",
                "GBMI_HOME": str(self.home),
                "GBMI_ENV_FILE": str(self.env_file),
                "STUB_CALLS": str(self.calls),
                "STUB_PARAMETERS": str(self.parameters),
                "STUB_ACCOUNT": ACCOUNT,
                **environment,
            },
        )

    def script(self, name: str, commit: str = COMMIT) -> Path:
        return self.home / "releases" / commit / "compose" / "host" / name

    def called(self) -> list[str]:
        return self.calls.read_text(encoding="utf-8").splitlines()

    def variables(self) -> dict[str, str]:
        """The names of the environment file with the text after the equals sign."""
        lines = self.env_file.read_text(encoding="utf-8").splitlines()
        return dict(line.split("=", 1) for line in lines if not line.startswith("#"))


@pytest.fixture
def host(tmp_path: Path) -> Host:
    return Host(tmp_path)


def no_value_in(result: subprocess.CompletedProcess[str], *more: str) -> None:
    output = result.stdout + result.stderr
    for value in (*SECRETS.values(), *more):
        assert value not in output


def test_the_environment_file_holds_the_parameters_and_the_values_of_the_host(host: Host) -> None:
    result = host.run(host.script("render_env.sh"), COMMIT)

    assert result.returncode == 0, result.stderr
    no_value_in(result)
    assert result.stdout.strip() == (
        f"render_env: wrote 21 variables to {host.env_file} (source of the SEC client: off)"
    )
    variables = host.variables()
    assert {name: variables[name] for name in REQUIRED} == {
        name: f"'{value}'" for name, value in SECRETS.items()
    }
    # What the host derives: the account number comes from the role of the host, not from a
    # file of the repository. Without a parameter the host sends no request to the SEC.
    assert {name: variables[name] for name in set(variables) - set(REQUIRED)} == {
        "GBMI_SEC_SOURCE": "'off'",
        "GBMI_RELEASE": f"'{COMMIT}'",
        "AWS_DEFAULT_REGION": "'us-east-1'",
        "GBMI_LAKE_BUCKET": f"'gbmi-prod-lake-{ACCOUNT}'",
        "GBMI_OPS_BUCKET": f"'gbmi-prod-ops-{ACCOUNT}'",
        "GBMI_CH_LAKE_ROLE_ARN": f"'arn:aws:iam::{ACCOUNT}:role/gbmi-clickhouse-lake-ro'",
        "GBMI_LOG_GROUP": "'/gbmi/prod/containers'",
    }
    # Readable by its owner only, and the secrets are asked for with decryption.
    assert stat.S_IMODE(host.env_file.stat().st_mode) == 0o600
    assert host.called() == [
        "aws sts get-caller-identity --query Account --output text",
        "aws ssm get-parameters-by-path --path /gbmi/prod/ --recursive --with-decryption "
        "--output json",
    ]


def test_the_source_of_the_sec_client_is_live_only_with_the_two_parameters(host: Host) -> None:
    host.set_parameters(parameters(GBMI_SEC_SOURCE="live", GBMI_SEC_USER_AGENT=USER_AGENT))

    result = host.run(host.script("render_env.sh"), COMMIT)

    assert result.returncode == 0, result.stderr
    no_value_in(result, USER_AGENT, "contact@example.org")
    assert "source of the SEC client: live" in result.stdout
    variables = host.variables()
    assert variables["GBMI_SEC_SOURCE"] == "'live'"
    assert variables["GBMI_SEC_USER_AGENT"] == f"'{USER_AGENT}'"

    # The source live without a declared identity is refused, and the old file stays.
    before = host.env_file.read_text(encoding="utf-8")
    host.set_parameters(parameters(GBMI_SEC_SOURCE="live"))
    refused = host.run(host.script("render_env.sh"), COMMIT)

    assert refused.returncode == 1
    assert "the parameter GBMI_SEC_USER_AGENT is missing" in refused.stderr
    assert host.env_file.read_text(encoding="utf-8") == before


@pytest.mark.parametrize(
    ("answer", "message"),
    [
        (
            {"Parameters": parameters()["Parameters"][:-2]},
            "parameters are missing: METABASE_ADMIN_PASSWORD, METABASE_ENCRYPTION_KEY",
        ),
        (parameters(GBMI_LAKE_BUCKET="another"), "the host derives GBMI_LAKE_BUCKET itself"),
        (parameters(SOMETHING_ELSE="x"), "parameters which no service reads: SOMETHING_ELSE"),
        (parameters(GBMI_SEC_SOURCE="fixture"), "GBMI_SEC_SOURCE has to be off or live"),
        (
            parameters(GBMI_SEC_SOURCE="live", GBMI_SEC_USER_AGENT="Two\nLines a@example.org"),
            "the parameter GBMI_SEC_USER_AGENT holds a line break",
        ),
        (
            {"Parameters": [{"Name": "/gbmi/prod/lower-case", "Value": "x"}]},
            "no name of a variable",
        ),
        ({"Parameters": "broken"}, "the answer of Parameter Store cannot be read"),
    ],
)
def test_a_wrong_set_of_parameters_is_refused_and_no_file_is_written(
    host: Host, answer: dict[str, Any], message: str
) -> None:
    host.set_parameters(answer)

    result = host.run(host.script("render_env.sh"), COMMIT)

    assert result.returncode == 1
    assert message in result.stderr
    # The message names the parameter and never a value.
    no_value_in(result, "Two", "a@example.org", "another")
    assert not host.env_file.exists()
    assert list(host.env_file.parent.glob("*")) == [] if host.env_file.parent.exists() else True


def test_a_text_which_is_no_commit_is_refused_before_any_call(host: Host) -> None:
    for wrong in ("", "main", COMMIT[:39], COMMIT.upper(), f"{COMMIT[:39]};"):
        result = host.run(host.script("render_env.sh"), wrong)

        assert result.returncode == 1, wrong
        assert "a commit is 40 hexadecimal characters" in result.stderr
    assert host.called() == []


def test_compose_reads_the_rendered_file_and_receives_each_value(
    host: Host, tmp_path: Path
) -> None:
    # The file feeds the overlay of the host: Compose finds every variable which the two files
    # require, and a value arrives in a container as Parameter Store holds it, with a quote,
    # a dollar sign and a backslash.
    docker = shutil.which("docker")
    if (
        docker is None
        or subprocess.run([docker, "compose", "version"], capture_output=True).returncode
    ):
        pytest.skip("Docker with the Compose plugin is not installed")
    awkward = 'O\'Brien & Sons $HOME \\ "quoted" contact@example.org'
    host.set_parameters(parameters(GBMI_SEC_SOURCE="live", GBMI_SEC_USER_AGENT=awkward))
    assert host.run(host.script("render_env.sh"), COMMIT).returncode == 0

    base = {name: value for name, value in os.environ.items() if name in ("PATH", "HOME")}
    command = [docker, "compose", "--project-name", "gbmi", "--project-directory", str(REPO)]
    command += ["--env-file", str(host.env_file)]
    for name in ("compose.yaml", "compose.prod.yaml"):
        command += ["-f", str(REPO / "compose" / name)]
    command += ["config", "--format", "json"]
    interpolated = subprocess.run(command, capture_output=True, text=True, check=False, env=base)
    assert interpolated.returncode == 0, interpolated.stderr
    services = json.loads(interpolated.stdout)["services"]
    scheduler = services["airflow-scheduler"]["environment"]
    # The rendered configuration writes a dollar sign twice; a container receives one.
    assert scheduler["GBMI_SEC_USER_AGENT"].replace("$$", "$") == awkward
    assert scheduler["GBMI_SEC_SOURCE"] == "live"
    assert scheduler["GBMI_LAKE_URI"] == f"s3://gbmi-prod-lake-{ACCOUNT}"
    assert services["postgres"]["environment"]["POSTGRES_PASSWORD"] == SECRETS["POSTGRES_PASSWORD"]
    assert services["airflow-scheduler"]["image"].endswith(f":{COMMIT}")


def deployed(host: Host) -> str | None:
    marker = host.home / "DEPLOYED"
    return marker.read_text(encoding="utf-8").strip() if marker.exists() else None


def test_a_deployment_records_the_release_only_after_a_passed_smoke_test(host: Host) -> None:
    host.release(PREVIOUS)
    host.release("a" * 40)  # a release which is older than the one before
    (host.home / "DEPLOYED").write_text(PREVIOUS + "\n", encoding="utf-8")

    result = host.run(host.script("deploy.sh"), COMMIT)

    assert result.returncode == 0, result.stdout + result.stderr
    no_value_in(result)
    assert deployed(host) == COMMIT
    assert result.stdout.strip().endswith(f"DEPLOYED {COMMIT}")
    release = host.home / "releases" / COMMIT
    compose = (
        f"docker compose --project-name gbmi --project-directory {release} "
        f"--env-file {host.env_file} -f {release}/compose/compose.yaml "
        f"-f {release}/compose/compose.prod.yaml"
    )
    # The order of the steps: secrets, images, start (migrations and initialisation run as
    # one-shot services), smoke test in the host mode, and the clean-up last.
    assert host.called() == [
        "aws sts get-caller-identity --query Account --output text",
        "aws ssm get-parameters-by-path --path /gbmi/prod/ --recursive --with-decryption "
        "--output json",
        f"{compose} pull --quiet",
        f"{compose} up --detach --wait --remove-orphans",
        f"smoke mode=host release={COMMIT} cwd={release}",
        "docker image prune --all --force",
    ]
    # The release before stays for a way back; an older one is removed.
    assert sorted(path.name for path in (host.home / "releases").iterdir()) == sorted(
        [COMMIT, PREVIOUS]
    )


@pytest.mark.parametrize("failing", [{"STUB_SMOKE_EXIT": "1"}, {"STUB_UP_EXIT": "1"}])
def test_a_failed_deployment_leaves_the_release_before_as_the_deployed_one(
    host: Host, failing: dict[str, str]
) -> None:
    host.release(PREVIOUS)
    (host.home / "DEPLOYED").write_text(PREVIOUS + "\n", encoding="utf-8")

    result = host.run(host.script("deploy.sh"), COMMIT, **failing)

    assert result.returncode != 0
    no_value_in(result)
    assert deployed(host) == PREVIOUS
    assert "DEPLOYED" not in result.stdout
    # Nothing was cleaned up: both release folders are still there.
    assert sorted(path.name for path in (host.home / "releases").iterdir()) == sorted(
        [COMMIT, PREVIOUS]
    )


def test_deploy_runs_only_from_the_release_folder_of_its_commit(host: Host) -> None:
    result = host.run(HOST / "deploy.sh", COMMIT)

    assert result.returncode == 1
    assert "deploy.sh has to run from the release folder of its commit" in result.stderr
    assert host.called() == []
    assert deployed(host) is None


def test_the_start_at_boot_brings_up_the_deployed_release(host: Host) -> None:
    missing = host.run(host.script("stack-up"))
    assert missing.returncode == 1
    assert "no release is deployed" in missing.stderr

    (host.home / "DEPLOYED").write_text(COMMIT + "\n", encoding="utf-8")
    result = host.run(host.script("stack-up"))

    assert result.returncode == 0, result.stderr
    no_value_in(result)
    release = host.home / "releases" / COMMIT
    assert host.called()[-1] == (
        f"docker compose --project-name gbmi --project-directory {release} "
        f"--env-file {host.env_file} -f {release}/compose/compose.yaml "
        f"-f {release}/compose/compose.prod.yaml up --detach --wait --remove-orphans --quiet-pull"
    )
    # The environment file is written again at every start: a changed parameter takes effect.
    assert host.variables()["GBMI_RELEASE"] == f"'{COMMIT}'"


def test_no_host_script_traces_its_commands() -> None:
    # A trace would print each command with its values into the output of a deployment.
    for script in sorted(path for path in HOST.iterdir() if path.is_file()):
        text = script.read_text(encoding="utf-8")
        assert "set -x" not in text and "xtrace" not in text, script.name
        assert "set -euo pipefail" in text or script.suffix == ".py" or script.name == "common.sh"
