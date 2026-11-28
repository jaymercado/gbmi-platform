"""The job plan of the workflow terraform shows counts and error messages, never a plan."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("ci_plan", REPO / "tools" / "ci_plan.py")
assert SPEC is not None and SPEC.loader is not None
ci_plan = importlib.util.module_from_spec(SPEC)
sys.modules["ci_plan"] = ci_plan
SPEC.loader.exec_module(ci_plan)

ACCOUNT = "123456789012"
#: Text which only the plan of Terraform holds; no line of the job may show it.
PLAN_ONLY = "module.ec2_host.aws_instance.host"

#: A stand-in for Terraform: it records its arguments and answers from a scenario file.
STAND_IN = """#!/usr/bin/env python3
import json, os, sys
scenario = json.load(open(os.environ["FAKE_TERRAFORM"]))
root = sys.argv[1].rsplit("/infra/terraform/", 1)[-1]
command = sys.argv[2]
with open(scenario["calls"], "a") as calls:
    calls.write(json.dumps([root, *sys.argv[2:]]) + "\\n")
answer = scenario["answers"].get(f"{root}:{command}", {"code": 0, "lines": []})
for line in answer["lines"]:
    print(line if isinstance(line, str) else json.dumps(line))
print("a line of standard error which is never shown", file=sys.stderr)
sys.exit(answer["code"])
"""


def summary(add: int = 0, change: int = 0, remove: int = 0, imported: int = 0) -> dict[str, Any]:
    return {
        "@level": "info",
        "@message": "Plan: ...",
        "type": "change_summary",
        "changes": {
            "add": add,
            "change": change,
            "import": imported,
            "remove": remove,
            "operation": "plan",
        },
    }


def planned(address: str = PLAN_ONLY) -> dict[str, Any]:
    return {
        "@message": f"{address}: Plan to create",
        "type": "planned_change",
        "change": {"resource": {"addr": address}, "action": "create"},
    }


def diagnostic(severity: str, text: str, detail: str = "", address: str = "") -> dict[str, Any]:
    body = {"severity": severity, "summary": text, "detail": detail}
    if address:
        body["address"] = address
    return {"@level": severity, "@message": text, "type": "diagnostic", "diagnostic": body}


class Job:
    """One run of the script with the stand-in for Terraform."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.calls = tmp_path / "calls.jsonl"
        self.scenario = tmp_path / "scenario.json"
        self.summary = tmp_path / "summary.md"
        folder = tmp_path / "bin"
        folder.mkdir()
        stand_in = folder / "terraform"
        stand_in.write_text(STAND_IN, "utf-8")
        stand_in.chmod(0o755)
        monkeypatch.setenv("PATH", f"{folder}:{sys.exec_prefix}/bin:/usr/bin:/bin")
        monkeypatch.setenv("FAKE_TERRAFORM", str(self.scenario))
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(self.summary))
        monkeypatch.setenv("AWS_ACCOUNT_ID", ACCOUNT)

    def run(self, answers: dict[str, Any]) -> int:
        self.scenario.write_text(
            json.dumps({"calls": str(self.calls), "answers": answers}), "utf-8"
        )
        code: int = ci_plan.main()
        return code

    def commands(self) -> list[list[str]]:
        if not self.calls.exists():
            return []
        return [json.loads(line) for line in self.calls.read_text("utf-8").splitlines()]


@pytest.fixture
def job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Job:
    return Job(tmp_path, monkeypatch)


def test_the_job_shows_one_line_for_each_configuration_and_no_line_of_a_plan(
    job: Job, capsys: pytest.CaptureFixture[str]
) -> None:
    code = job.run(
        {
            "bootstrap:plan": {"code": 0, "lines": [planned("aws_iam_role.x"), summary()]},
            "envs/prod:plan": {
                "code": 0,
                "lines": ["not JSON", planned(), summary(add=31, change=2, remove=1)],
            },
        }
    )
    shown = capsys.readouterr()
    assert code == 0
    assert shown.out.splitlines() == [
        "bootstrap: No changes",
        "envs/prod: 31 to add, 2 to change, 1 to destroy",
    ]
    assert shown.err == ""
    table = job.summary.read_text("utf-8")
    assert "| `bootstrap` | No changes |" in table
    assert "| `envs/prod` | 31 to add, 2 to change, 1 to destroy |" in table
    for text in (shown.out, table):
        assert PLAN_ONLY not in text
        assert "aws_iam_role.x" not in text
        assert "never shown" not in text
        assert ACCOUNT not in text


def test_the_state_is_read_from_the_bucket_of_the_account_without_a_lock(job: Job) -> None:
    assert job.run({"bootstrap:plan": {"code": 0, "lines": [summary()]}}) == 1
    assert job.commands() == [
        [
            "bootstrap",
            "init",
            "-input=false",
            "-json",
            f"-backend-config=bucket=gbmi-tfstate-{ACCOUNT}",
            "-backend-config=key=bootstrap/terraform.tfstate",
            "-backend-config=region=us-east-1",
        ],
        ["bootstrap", "plan", "-input=false", "-json", "-lock=false"],
        [
            "envs/prod",
            "init",
            "-input=false",
            "-json",
            f"-backend-config=bucket=gbmi-tfstate-{ACCOUNT}",
            "-backend-config=key=envs/prod/terraform.tfstate",
            "-backend-config=region=us-east-1",
        ],
        ["envs/prod", "plan", "-input=false", "-json", "-lock=false"],
    ]


def test_a_failed_plan_shows_the_error_messages_and_the_other_configuration_still_runs(
    job: Job, capsys: pytest.CaptureFixture[str]
) -> None:
    denied = diagnostic(
        "error",
        "reading SSM Parameter",
        "api error AccessDeniedException: not authorized\nto perform: ssm:GetParameter",
        "module.ec2_host.data.aws_ssm_parameter.al2023_arm64",
    )
    code = job.run(
        {
            "bootstrap:plan": {"code": 0, "lines": [summary(add=1, change=2, imported=1)]},
            "envs/prod:plan": {
                "code": 1,
                "lines": [planned(), diagnostic("warning", "Deprecated attribute"), denied],
            },
        }
    )
    shown = capsys.readouterr().out
    assert code == 1
    assert shown.splitlines() == [
        "warning (envs/prod): Deprecated attribute",
        "error (envs/prod, terraform plan): reading SSM Parameter",
        "    api error AccessDeniedException: not authorized",
        "    to perform: ssm:GetParameter",
        "    at module.ec2_host.data.aws_ssm_parameter.al2023_arm64",
        "bootstrap: 1 to add, 2 to change, 0 to destroy, 1 to import",
        "envs/prod: terraform plan failed",
    ]
    assert "| `envs/prod` | terraform plan failed |" in job.summary.read_text("utf-8")


def test_a_failed_init_stops_the_configuration_before_its_plan(
    job: Job, capsys: pytest.CaptureFixture[str]
) -> None:
    code = job.run(
        {
            "bootstrap:init": {"code": 1, "lines": [diagnostic("error", "Failed to get state")]},
            "envs/prod:init": {"code": 1, "lines": ["plain text of a crash"]},
        }
    )
    shown = capsys.readouterr().out
    assert code == 1
    assert [call[1] for call in job.commands()] == ["init", "init"]
    assert "error (bootstrap, terraform init): Failed to get state" in shown
    assert (
        "error (envs/prod): terraform init ended with the exit code 1 and without an error "
        "message in its JSON output" in shown
    )
    assert "plain text of a crash" not in shown
    assert shown.splitlines()[-2:] == [
        "bootstrap: terraform init failed",
        "envs/prod: terraform init failed",
    ]


def test_a_plan_without_counts_is_a_failure_and_changed_outputs_are_named(
    job: Job, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs = {"type": "outputs", "outputs": {"a": {"action": "noop"}, "b": {"action": "create"}}}
    code = job.run(
        {
            "bootstrap:plan": {"code": 0, "lines": [planned()]},
            "envs/prod:plan": {"code": 0, "lines": [summary(), outputs]},
        }
    )
    shown = capsys.readouterr().out
    assert code == 1
    assert shown.splitlines()[-2:] == [
        "bootstrap: terraform plan failed",
        "envs/prod: No changes; the values of outputs change",
    ]


@pytest.mark.parametrize("value", ["", "12345678901", "1234567890123", "12345678901a"])
def test_without_the_account_number_terraform_does_not_run(
    job: Job, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], value: str
) -> None:
    monkeypatch.setenv("AWS_ACCOUNT_ID", value)
    assert job.run({}) == 2
    shown = capsys.readouterr()
    assert job.commands() == []
    assert shown.out == ""
    assert "AWS_ACCOUNT_ID has to hold the 12 digits" in shown.err
    assert not value or value not in shown.err
    assert not job.summary.exists()
