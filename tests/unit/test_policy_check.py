"""make host-policy-check: the table of requests and the reading of the answers of IAM."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("policy_check", REPO / "tools" / "policy_check.py")
assert SPEC is not None and SPEC.loader is not None
policy_check = importlib.util.module_from_spec(SPEC)
sys.modules["policy_check"] = policy_check
SPEC.loader.exec_module(policy_check)

ACCOUNT = "123456789012"
ROLES = {
    "gbmi-host",
    "gbmi-clickhouse-lake-ro",
    "gbmi-host-scheduler",
    "gbmi-ci-deploy",
    "gbmi-ci-plan",
}


class Sts:
    def get_caller_identity(self) -> dict[str, str]:
        return {"Account": ACCOUNT}


class Iam:
    """Answers as IAM does: one decision for each resource of a request."""

    def __init__(self, refuse: set[str] | None = None, flip: Any = None) -> None:
        self.refuse = refuse or set()
        self.flip = flip
        self.requests: list[dict[str, Any]] = []

    def simulate_principal_policy(self, **request: Any) -> dict[str, Any]:
        self.requests.append(request)
        results = [
            {
                "EvalResourceName": arn,
                "EvalResourceDecision": "implicitDeny" if arn in self.refuse else "allowed",
            }
            for arn in request["ResourceArns"]
        ]
        decisions = {item["EvalResourceDecision"] for item in results}
        overall = "allowed" if decisions == {"allowed"} else "implicitDeny"
        answer: dict[str, Any] = {"EvalActionName": request["ActionNames"][0]}
        answer["EvalDecision"] = overall
        if len(results) > 1:
            answer["ResourceSpecificResults"] = results
        return {"EvaluationResults": [answer]}


def test_the_table_names_the_five_roles_with_allowed_and_refused_requests() -> None:
    table = policy_check.cases()

    assert {case.role for case in table} == ROLES
    for role in ROLES:
        expected = {case.expected for case in table if case.role == role}
        assert expected == {policy_check.ALLOWED, policy_check.DENIED}, role
    assert len(set(table_keys(table))) == len(table)
    # The check which the plan names: the host role cannot delete a raw file.
    assert any(
        case.role == "gbmi-host"
        and case.expected == policy_check.DENIED
        and case.action == "s3:DeleteObject"
        and "/raw/" in case.resources[0]
        for case in table
    )


def table_keys(table: list[Any]) -> list[tuple[Any, ...]]:
    return [(case.role, case.action, case.resources, tuple(case.context.items())) for case in table]


def test_a_request_with_two_resources_is_allowed_only_when_each_one_is() -> None:
    document = "arn:aws:ssm:us-east-1:{a}:document/gbmi-prod-deploy"
    instance = "arn:aws:ec2:us-east-1:{a}:instance/i-0123456789abcdef0"
    case = policy_check.Case(
        "gbmi-ci-deploy",
        policy_check.ALLOWED,
        "ssm:SendCommand",
        (document, instance),
        {"ssm:resourceTag/project": "gbmi"},
    )

    iam = Iam()
    assert policy_check.decision(iam, ACCOUNT, case) == policy_check.ALLOWED
    assert iam.requests == [
        {
            "PolicySourceArn": f"arn:aws:iam::{ACCOUNT}:role/gbmi-ci-deploy",
            "ActionNames": ["ssm:SendCommand"],
            "ResourceArns": [document.format(a=ACCOUNT), instance.format(a=ACCOUNT)],
            "ContextEntries": [
                {
                    "ContextKeyName": "ssm:resourceTag/project",
                    "ContextKeyValues": ["gbmi"],
                    "ContextKeyType": "string",
                }
            ],
        }
    ]

    refused = Iam(refuse={document.format(a=ACCOUNT)})
    assert policy_check.decision(refused, ACCOUNT, case) == policy_check.DENIED


def test_the_run_fails_on_a_wrong_decision_and_never_shows_the_account(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # An IAM which allows everything: every request which has to be refused is wrong.
    code = policy_check.main([], iam=Iam(), sts=Sts())

    shown = capsys.readouterr().out
    table = policy_check.cases()
    refused = sum(case.expected == policy_check.DENIED for case in table)
    assert code == 1
    assert shown.count("WRONG ") == refused
    assert f"{len(table) - refused} of {len(table)} requests were decided as expected" in shown
    assert ACCOUNT not in shown
    assert "<account>" in shown


def test_the_list_needs_no_access_to_aws(capsys: pytest.CaptureFixture[str]) -> None:
    assert policy_check.main(["--list"]) == 0

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == len(policy_check.cases())
    assert all(line.startswith(("expect allowed", "expect denied")) for line in lines)
