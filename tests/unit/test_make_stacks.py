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
_MAKE_STATE = {"STACK", "COMPOSE_PROJECT_NAME", "GBMI_LAKE_DIR", "CONFIRM", "MAKEFLAGS", "MFLAGS"}

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
