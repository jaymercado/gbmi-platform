"""make host-policy-check: the policies of the five roles against a table of requests.

    python tools/policy_check.py            ask IAM for each request of the table
    python tools/policy_check.py --list     print the table; no request to AWS

Week 3 plan, WP-11 step 8: after the apply, each action which the code needs is allowed for
the role of the host, and a delete below raw/ is denied. The script asks IAM to evaluate each
request of the table for its role (iam:SimulatePrincipalPolicy) and compares the decision
with the expected one. A simulation changes nothing in the account, and it sends no request
to S3, Glue or the other services.

The table also holds requests which have to be refused: the trust boundaries of the design.
A request which names two resources (a command document and an instance, or a catalog, a
database and a table) is allowed only when IAM allows it for every one of them.

The script prints the role, the action, the resource and the decision. The account number is
shown as <account>. The credentials come from the environment (the profile gbmi of "aws
login").
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import Any

REGION = "us-east-1"
ALLOWED = "allowed"
DENIED = "denied"
#: A made-up instance id: the policies name every instance and decide by a tag.
INSTANCE = "i-0123456789abcdef0"


@dataclass(frozen=True)
class Case:
    role: str
    expected: str
    action: str
    resources: tuple[str, ...]
    #: Values of condition keys which the request would carry.
    context: dict[str, str] = field(default_factory=dict)


def cases() -> list[Case]:
    """The table. {a} stands for the account number, {r} for the region."""
    lake = "arn:aws:s3:::gbmi-prod-lake-{a}"
    ops = "arn:aws:s3:::gbmi-prod-ops-{a}"
    state = "arn:aws:s3:::gbmi-tfstate-{a}"
    glue = "arn:aws:glue:{r}:{a}"
    silver = (
        f"{glue}:catalog",
        f"{glue}:database/gbmi_prod_silver",
        f"{glue}:table/gbmi_prod_silver/gdelt_event",
    )
    parameter = "arn:aws:ssm:{r}:{a}:parameter"
    groups = "arn:aws:logs:{r}:{a}:log-group"
    instance = f"arn:aws:ec2:{{r}}:{{a}}:instance/{INSTANCE}"
    deploy_document = "arn:aws:ssm:{r}:{a}:document/gbmi-prod-deploy"
    shell_document = "arn:aws:ssm:{r}::document/AWS-RunShellScript"
    lake_role = "arn:aws:iam::{a}:role/gbmi-clickhouse-lake-ro"
    host_tag = {"aws:ResourceTag/Name": "gbmi-prod-host"}
    project_tag = {"ssm:resourceTag/project": "gbmi"}
    host, lake_ro, scheduler = "gbmi-host", "gbmi-clickhouse-lake-ro", "gbmi-host-scheduler"
    deploy, plan = "gbmi-ci-deploy", "gbmi-ci-plan"

    table = [
        # --- gbmi-host: what the pipelines, Compose and the agents need -------------------------
        Case(host, ALLOWED, "s3:ListBucket", (lake,)),
        Case(host, ALLOWED, "s3:GetObject", (f"{lake}/raw/gdelt/x.zip",)),
        Case(host, ALLOWED, "s3:PutObject", (f"{lake}/raw/gdelt/x.zip",)),
        Case(host, ALLOWED, "s3:GetObject", (f"{lake}/warehouse/t/data/x.parquet",)),
        Case(host, ALLOWED, "s3:PutObject", (f"{lake}/warehouse/t/data/x.parquet",)),
        Case(host, ALLOWED, "s3:DeleteObject", (f"{lake}/warehouse/t/data/x.parquet",)),
        Case(host, ALLOWED, "s3:PutObject", (f"{lake}/quarantine/x.json",)),
        Case(host, ALLOWED, "s3:DeleteObject", (f"{lake}/quarantine/x.json",)),
        Case(host, ALLOWED, "glue:GetDatabase", silver[:2]),
        Case(host, ALLOWED, "glue:GetTable", silver),
        Case(host, ALLOWED, "glue:CreateTable", silver),
        Case(host, ALLOWED, "glue:UpdateTable", silver),
        Case(host, ALLOWED, "s3:ListBucket", (ops,)),
        Case(host, ALLOWED, "s3:PutObject", (f"{ops}/airflow-logs/dag_id=x/attempt=1.log",)),
        Case(host, ALLOWED, "s3:GetObject", (f"{ops}/airflow-logs/dag_id=x/attempt=1.log",)),
        Case(host, ALLOWED, "s3:PutObject", (f"{ops}/artifacts/x",)),
        Case(host, ALLOWED, "s3:PutObject", (f"{ops}/backups/x.dump",)),
        # The prefix tmp/ of the ops bucket, not a folder of a machine.
        Case(host, ALLOWED, "s3:PutObject", (f"{ops}/tmp/x",)),  # noqa: S108
        Case(host, ALLOWED, "s3:DeleteObject", (f"{ops}/tmp/x",)),  # noqa: S108
        Case(host, ALLOWED, "ssm:GetParametersByPath", (f"{parameter}/gbmi/prod/",)),
        Case(host, ALLOWED, "ssm:GetParametersByPath", (f"{parameter}/gbmi/prod",)),
        Case(
            host, ALLOWED, "logs:CreateLogStream", (f"{groups}:/gbmi/prod/containers:log-stream:x",)
        ),
        Case(host, ALLOWED, "logs:PutLogEvents", (f"{groups}:/gbmi/prod/containers:log-stream:x",)),
        Case(host, ALLOWED, "logs:PutLogEvents", (f"{groups}:/gbmi/prod/host:log-stream:x",)),
        Case(host, ALLOWED, "sts:AssumeRole", (lake_role,)),
        # --- gbmi-host: what the role must not do -------------------------------------------------
        Case(host, DENIED, "s3:DeleteObject", (f"{lake}/raw/gdelt/x.zip",)),
        Case(host, DENIED, "s3:DeleteObjectVersion", (f"{lake}/raw/gdelt/x.zip",)),
        Case(host, DENIED, "s3:DeleteObjectVersion", (f"{lake}/warehouse/t/data/x.parquet",)),
        Case(host, DENIED, "s3:PutObject", (f"{lake}/elsewhere/x",)),
        Case(host, DENIED, "s3:DeleteBucket", (lake,)),
        Case(host, DENIED, "s3:PutBucketPolicy", (lake,)),
        Case(host, DENIED, "s3:DeleteObject", (f"{ops}/backups/x.dump",)),
        Case(host, DENIED, "s3:DeleteObject", (f"{ops}/airflow-logs/dag_id=x/attempt=1.log",)),
        Case(host, DENIED, "s3:GetObject", (f"{state}/envs/prod/terraform.tfstate",)),
        Case(host, DENIED, "glue:DeleteTable", silver),
        Case(host, DENIED, "glue:CreateDatabase", silver[:2]),
        Case(host, DENIED, "glue:DeleteDatabase", silver[:2]),
        Case(host, DENIED, "ssm:PutParameter", (f"{parameter}/gbmi/prod/POSTGRES_PASSWORD",)),
        Case(host, DENIED, "ssm:SendCommand", (shell_document, instance), project_tag),
        Case(host, DENIED, "ec2:TerminateInstances", (instance,), host_tag),
        Case(host, DENIED, "ec2:StopInstances", (instance,), host_tag),
        Case(host, DENIED, "iam:PassRole", ("arn:aws:iam::{a}:role/gbmi-host",)),
        Case(host, DENIED, "iam:CreateAccessKey", ("arn:aws:iam::{a}:user/x",)),
        Case(host, DENIED, "sts:AssumeRole", ("arn:aws:iam::{a}:role/gbmi-ci-deploy",)),
        # --- gbmi-clickhouse-lake-ro: read the Iceberg tables, nothing else ---------------------
        Case(lake_ro, ALLOWED, "s3:ListBucket", (lake,), {"s3:prefix": "warehouse/t/metadata/"}),
        Case(lake_ro, ALLOWED, "s3:GetObject", (f"{lake}/warehouse/t/data/x.parquet",)),
        Case(lake_ro, DENIED, "s3:ListBucket", (lake,), {"s3:prefix": "raw/"}),
        Case(lake_ro, DENIED, "s3:GetObject", (f"{lake}/raw/gdelt/x.zip",)),
        Case(lake_ro, DENIED, "s3:GetObject", (f"{lake}/quarantine/x.json",)),
        Case(lake_ro, DENIED, "s3:PutObject", (f"{lake}/warehouse/t/data/x.parquet",)),
        Case(lake_ro, DENIED, "s3:DeleteObject", (f"{lake}/warehouse/t/data/x.parquet",)),
        Case(lake_ro, DENIED, "s3:GetObject", (f"{ops}/backups/x.dump",)),
        # --- gbmi-host-scheduler: start and stop the one host -----------------------------------
        Case(scheduler, ALLOWED, "ec2:StartInstances", (instance,), host_tag),
        Case(scheduler, ALLOWED, "ec2:StopInstances", (instance,), host_tag),
        Case(
            scheduler, DENIED, "ec2:StopInstances", (instance,), {"aws:ResourceTag/Name": "another"}
        ),
        Case(scheduler, DENIED, "ec2:TerminateInstances", (instance,), host_tag),
        Case(scheduler, DENIED, "ec2:RunInstances", (instance,), host_tag),
        # --- gbmi-ci-deploy: send the one command document ---------------------------------------
        Case(deploy, ALLOWED, "ssm:SendCommand", (deploy_document, instance), project_tag),
        Case(deploy, ALLOWED, "ssm:GetCommandInvocation", ("*",)),
        Case(deploy, ALLOWED, "ec2:DescribeInstances", ("*",)),
        Case(deploy, DENIED, "ssm:SendCommand", (shell_document, instance), project_tag),
        Case(
            deploy,
            DENIED,
            "ssm:SendCommand",
            (deploy_document, instance),
            {"ssm:resourceTag/project": "x"},
        ),
        Case(deploy, DENIED, "ssm:StartSession", (instance,), project_tag),
        Case(deploy, DENIED, "ssm:GetParameter", (f"{parameter}/gbmi/prod/POSTGRES_PASSWORD",)),
        Case(deploy, DENIED, "s3:GetObject", (f"{state}/envs/prod/terraform.tfstate",)),
        Case(deploy, DENIED, "s3:PutObject", (f"{state}/envs/prod/terraform.tfstate",)),
        Case(deploy, DENIED, "ec2:StopInstances", (instance,), host_tag),
        # --- gbmi-ci-plan: read for terraform plan, and no write ---------------------------------
        Case(plan, ALLOWED, "s3:GetObject", (f"{state}/envs/prod/terraform.tfstate",)),
        Case(plan, ALLOWED, "s3:ListBucket", (state,)),
        Case(plan, ALLOWED, "ec2:DescribeInstances", ("*",)),
        Case(plan, ALLOWED, "iam:GetRole", ("arn:aws:iam::{a}:role/gbmi-host",)),
        Case(
            plan,
            ALLOWED,
            "iam:GetInstanceProfile",
            ("arn:aws:iam::{a}:instance-profile/gbmi-host",),
        ),
        Case(plan, ALLOWED, "ssm:GetDocument", (deploy_document,)),
        Case(
            plan,
            ALLOWED,
            "ssm:GetParameter",
            (
                "arn:aws:ssm:{r}::parameter/aws/service/ami-amazon-linux-latest/"
                "al2023-ami-kernel-default-arm64",
            ),
        ),
        Case(plan, ALLOWED, "logs:DescribeLogGroups", ("*",)),
        Case(
            plan,
            ALLOWED,
            "cloudwatch:DescribeAlarms",
            ("arn:aws:cloudwatch:{r}:{a}:alarm:gbmi-prod-host-cpu-high",),
        ),
        Case(plan, ALLOWED, "sns:GetTopicAttributes", ("arn:aws:sns:{r}:{a}:gbmi-prod-alerts",)),
        Case(
            plan,
            ALLOWED,
            "scheduler:GetSchedule",
            ("arn:aws:scheduler:{r}:{a}:schedule/gbmi-prod-host/gbmi-prod-host-stop",),
        ),
        Case(plan, DENIED, "ssm:GetParameter", (f"{parameter}/gbmi/prod/POSTGRES_PASSWORD",)),
        Case(plan, DENIED, "ssm:GetParametersByPath", (f"{parameter}/gbmi/prod/",)),
        Case(plan, DENIED, "s3:PutObject", (f"{state}/envs/prod/terraform.tfstate.tflock",)),
        Case(plan, DENIED, "s3:PutObject", (f"{state}/envs/prod/terraform.tfstate",)),
        Case(plan, DENIED, "s3:GetObject", (f"{lake}/raw/gdelt/x.zip",)),
        Case(plan, DENIED, "ssm:SendCommand", (deploy_document, instance), project_tag),
        Case(plan, DENIED, "ec2:StopInstances", (instance,), host_tag),
        Case(plan, DENIED, "iam:PutRolePolicy", ("arn:aws:iam::{a}:role/gbmi-ci-plan",)),
    ]
    return table


def decision(client: Any, account: str, case: Case) -> str:
    """Ask IAM for one case: "allowed" only when every resource of the request is allowed."""
    resources = [text.format(a=account, r=REGION) for text in case.resources]
    answer = client.simulate_principal_policy(
        PolicySourceArn=f"arn:aws:iam::{account}:role/{case.role}",
        ActionNames=[case.action],
        ResourceArns=resources,
        ContextEntries=[
            {"ContextKeyName": name, "ContextKeyValues": [value], "ContextKeyType": "string"}
            for name, value in case.context.items()
        ],
    )
    decisions: list[str] = []
    for result in answer["EvaluationResults"]:
        specific = result.get("ResourceSpecificResults") or []
        decisions += [item["EvalResourceDecision"] for item in specific] or [result["EvalDecision"]]
    return ALLOWED if decisions and all(item == ALLOWED for item in decisions) else DENIED


def shown(case: Case) -> str:
    resources = ", ".join(text.format(a="<account>", r=REGION) for text in case.resources)
    context = "".join(f" [{name}={value}]" for name, value in case.context.items())
    return f"{case.role:<24} {case.action:<28} {resources}{context}"


def main(arguments: list[str] | None = None, *, iam: Any = None, sts: Any = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--list", action="store_true", help="print the table; no request to AWS")
    options = parser.parse_args(arguments)
    table = cases()
    if options.list:
        for case in table:
            sys.stdout.write(f"expect {case.expected:<8} {shown(case)}\n")
        return 0
    if iam is None or sts is None:
        import boto3

        iam = iam or boto3.client("iam")
        sts = sts or boto3.client("sts", region_name=REGION)
    account = sts.get_caller_identity()["Account"]
    wrong = 0
    for case in table:
        found = decision(iam, account, case)
        mark = "ok    " if found == case.expected else "WRONG "
        wrong += found != case.expected
        sys.stdout.write(f"{mark} {found:<8} {shown(case)}\n")
    sys.stdout.write(
        f"policy-check: {len(table) - wrong} of {len(table)} requests were decided as expected\n"
    )
    return 1 if wrong else 0


if __name__ == "__main__":
    sys.exit(main())
