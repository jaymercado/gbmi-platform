"""The Makefile keeps the working stack and the demo stack apart (week 2 plan 3.3).

Every check is a dry run: ``make --dry-run`` prints the commands and runs none of them, except
the calls of make itself, which are dry runs too.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
_MAKE_STATE = {
    "STACK",
    "COMPOSE_PROJECT_NAME",
    "GBMI_LAKE_DIR",
    "GBMI_DAG_SCHEDULES",
    "GBMI_TASK_RETRIES",
    "GBMI_SEC_SOURCE",
    "CONFIRM",
    "MAKEFLAGS",
    "MFLAGS",
}

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="GNU Make is not installed")


def dry_run(*arguments: str, **environment: str) -> subprocess.CompletedProcess[str]:
    env = {name: value for name, value in os.environ.items() if name not in _MAKE_STATE}
    make = shutil.which("make") or "make"
    return subprocess.run(
        [make, "--dry-run", "--no-print-directory", *arguments],
        cwd=REPO,
        env={**env, **environment},
        capture_output=True,
        text=True,
        check=False,
    )


def compose_commands(output: str) -> list[str]:
    return [line for line in output.splitlines() if line.lstrip().startswith("docker compose")]


@pytest.mark.parametrize("target", ["demo", "e2e"])
@pytest.mark.parametrize("given", [{}, {"arguments": ("STACK=gbmi",)}, {"environment": "gbmi"}])
def test_the_demo_targets_name_only_the_demo_stack(target: str, given: dict[str, object]) -> None:
    arguments = given.get("arguments", ())
    environment = {"STACK": str(given["environment"])} if "environment" in given else {}

    result = dry_run(target, *arguments, **environment)  # type: ignore[misc]

    assert result.returncode == 0, result.stderr
    commands = compose_commands(result.stdout)
    assert commands, result.stdout
    assert all("--project-name gbmi-demo " in command for command in commands), commands
    assert "mkdir -p data/demo/lake" in result.stdout
    # The demo stops while the working stack runs.
    assert "label=com.docker.compose.project=gbmi)" in result.stdout


def test_other_targets_use_the_working_stack_unless_told_otherwise() -> None:
    working = dry_run("up")
    demo = dry_run("up", "STACK=gbmi-demo")

    assert working.returncode == 0 and demo.returncode == 0
    assert all("--project-name gbmi " in command for command in compose_commands(working.stdout))
    assert "mkdir -p data/lake" in working.stdout
    assert "label=com.docker.compose.project=gbmi-demo)" in working.stdout
    assert all("--project-name gbmi-demo " in command for command in compose_commands(demo.stdout))
    assert "mkdir -p data/demo/lake" in demo.stdout


def test_the_demo_stack_runs_its_dags_by_hand_and_the_working_stack_by_the_clock() -> None:
    # The value reaches the containers through compose/compose.yaml; dags/common reads the value.
    working = dry_run("--print-data-base", "up")
    demo = dry_run("--print-data-base", "up", "STACK=gbmi-demo")
    inherited = dry_run("--print-data-base", "up", GBMI_DAG_SCHEDULES="off")

    assert "GBMI_DAG_SCHEDULES := on" in working.stdout
    assert "GBMI_DAG_SCHEDULES := off" in demo.stdout
    # A value from the shell does not take the schedules from the working stack.
    assert "GBMI_DAG_SCHEDULES := on" in inherited.stdout
    compose = (REPO / "compose" / "compose.yaml").read_text(encoding="utf-8")
    assert "GBMI_DAG_SCHEDULES: ${GBMI_DAG_SCHEDULES:-on}" in compose


def test_the_working_stack_retries_a_failed_task_and_the_demo_stack_does_not() -> None:
    working = dry_run("--print-data-base", "up")
    demo = dry_run("--print-data-base", "up", "STACK=gbmi-demo")

    # Three retries on live data (blueprint 8.1); on the fixtures a failure shows at once.
    assert "GBMI_TASK_RETRIES := 3" in working.stdout
    assert "GBMI_TASK_RETRIES := 0" in demo.stdout


def test_the_stack_decides_where_the_sec_client_reads() -> None:
    # The value reaches the containers through compose/compose.yaml; gbmi.config reads the value.
    working = dry_run("--print-data-base", "up")
    demo = dry_run("--print-data-base", "up", "STACK=gbmi-demo")
    inherited = dry_run("--print-data-base", "up", "STACK=gbmi-demo", GBMI_SEC_SOURCE="live")
    one_command = dry_run("--print-data-base", "up", "STACK=gbmi-demo", "GBMI_SEC_SOURCE=live")

    # The working stack asks the SEC; the demo stack reads the recorded files.
    assert "GBMI_SEC_SOURCE := live" in working.stdout
    assert "GBMI_SEC_SOURCE := fixture" in demo.stdout
    # A value from the shell does not make the demo stack ask the SEC.
    assert "GBMI_SEC_SOURCE := fixture" in inherited.stdout
    # A value on the command line of make does, for the one command (week 3 plan, change C12).
    assert "GBMI_SEC_SOURCE = live" in one_command.stdout
    assert "GBMI_SEC_SOURCE := fixture" not in one_command.stdout
    compose = (REPO / "compose" / "compose.yaml").read_text(encoding="utf-8")
    assert "GBMI_SEC_SOURCE: ${GBMI_SEC_SOURCE:-off}" in compose


@pytest.mark.parametrize("target", ["demo", "e2e"])
def test_the_demo_never_asks_the_sec(target: str) -> None:
    result = dry_run("--print-data-base", target, "GBMI_SEC_SOURCE=live")

    assert result.returncode == 0, result.stderr
    # make demo starts make again for the demo stack, with the source of the recorded files.
    assert f"STACK=gbmi-demo GBMI_SEC_SOURCE=fixture _{target}" in result.stdout
    assert "GBMI_SEC_SOURCE = fixture" in result.stdout


def test_an_unknown_stack_is_refused() -> None:
    result = dry_run("up", "STACK=production")

    assert result.returncode != 0
    assert 'STACK is "production"' in result.stderr


def test_a_reset_deletes_one_stack_and_the_working_stack_asks_first() -> None:
    working = dry_run("reset")
    confirmed = dry_run("reset", "CONFIRM=yes")
    demo = dry_run("reset", "STACK=gbmi-demo")

    # The first command of the recipe is the question; without CONFIRM=yes the recipe stops there.
    assert working.stdout.lstrip().startswith('if [ "" != "yes" ]')
    assert confirmed.stdout.lstrip().startswith('if [ "yes" != "yes" ]')
    assert "rm -rf data/lake\n" in working.stdout
    # The demo stack has nothing which cannot be built again, so nothing is asked.
    assert demo.stdout.lstrip().startswith("docker compose --project-name gbmi-demo ")
    assert "rm -rf data/demo/lake\n" in demo.stdout
    for result in (working, confirmed, demo):
        assert "rm -rf data\n" not in result.stdout
        assert "rm -rf data " not in result.stdout
