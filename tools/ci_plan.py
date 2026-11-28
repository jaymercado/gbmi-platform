"""The job plan of the workflow terraform: terraform plan for both configurations.

    python3 tools/ci_plan.py

The job assumes the read-only role gbmi-ci-plan (week 3 plan, WP-11 step 4). The log of a
workflow of the repository is public, and the text of a plan names the AWS account in every
ARN. The script therefore asks Terraform for its machine-readable output, keeps the output in
memory and prints three kinds of lines only:

* one result line for each configuration, built from the counts of Terraform;
* after a failure, the summary and the detail of each error message of Terraform;
* the summary of each warning.

The account number and the alert address are secrets of the repository, which GitHub masks in
the log: an error message of AWS can name the account.

The script takes no state lock (``-lock=false``). A job which GitHub cancels halfway would
leave the lock file in the state bucket, and the next ``terraform apply`` on the laptop would
stop. A plan reads one whole state file and writes nothing, so the role has no write access
to the state bucket.

The standard library only: the script runs with the Python of the runner.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
#: The two configurations. The key of each state file is "<configuration>/terraform.tfstate".
ROOTS = ("bootstrap", "envs/prod")
REGION = "us-east-1"
_ACCOUNT = re.compile(r"^[0-9]{12}$")
_COUNTS = ("add", "change", "remove", "import")


def out(text: str) -> None:
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


def terraform(root: str, *arguments: str) -> tuple[int, list[dict[str, Any]]]:
    """Run Terraform in one configuration: the exit code and the messages of the JSON output."""
    process = subprocess.run(
        ["terraform", f"-chdir={REPO / 'infra' / 'terraform' / root}", *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    messages: list[dict[str, Any]] = []
    for line in process.stdout.splitlines():
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if isinstance(message, dict):
            messages.append(message)
    return process.returncode, messages


def diagnostics(messages: list[dict[str, Any]], severity: str) -> list[dict[str, Any]]:
    """The error messages or the warnings of Terraform."""
    found = []
    for message in messages:
        diagnostic = message.get("diagnostic")
        if (
            message.get("type") == "diagnostic"
            and isinstance(diagnostic, dict)
            and diagnostic.get("severity") == severity
        ):
            found.append(diagnostic)
    return found


def result_line(messages: list[dict[str, Any]]) -> str | None:
    """The result of a plan from the counts of Terraform; None when Terraform gave no counts."""
    counts: dict[str, Any] | None = None
    outputs_change = False
    for message in messages:
        changes = message.get("changes")
        if message.get("type") == "change_summary" and isinstance(changes, dict):
            counts = changes
        outputs = message.get("outputs")
        if message.get("type") == "outputs" and isinstance(outputs, dict):
            outputs_change = any(
                isinstance(output, dict) and output.get("action", "noop") != "noop"
                for output in outputs.values()
            )
    if counts is None or counts.get("operation") != "plan":
        return None
    numbers = {name: counts.get(name, 0) for name in _COUNTS}
    if not all(isinstance(number, int) for number in numbers.values()):
        return None
    if not any(numbers.values()):
        return "No changes; the values of outputs change" if outputs_change else "No changes"
    line = f"{numbers['add']} to add, {numbers['change']} to change, {numbers['remove']} to destroy"
    if numbers["import"]:
        line += f", {numbers['import']} to import"
    return line


def plan(root: str, account: str) -> tuple[bool, str]:
    """Plan one configuration: whether the plan passed, and the result line."""
    step = "init"
    code, messages = terraform(
        root,
        "init",
        "-input=false",
        "-json",
        f"-backend-config=bucket=gbmi-tfstate-{account}",
        f"-backend-config=key={root}/terraform.tfstate",
        f"-backend-config=region={REGION}",
    )
    if code == 0:
        step = "plan"
        code, messages = terraform(root, "plan", "-input=false", "-json", "-lock=false")
    for warning in diagnostics(messages, "warning"):
        out(f"warning ({root}): {warning.get('summary', '')}")
    line = result_line(messages) if code == 0 and step == "plan" else None
    if line is not None:
        return True, line
    errors = diagnostics(messages, "error")
    for error in errors:
        out(f"error ({root}, terraform {step}): {error.get('summary', '')}")
        for detail in str(error.get("detail") or "").splitlines():
            out(f"    {detail}")
        if error.get("address"):
            out(f"    at {error['address']}")
    if not errors:
        out(
            f"error ({root}): terraform {step} ended with the exit code {code} and without an "
            "error message in its JSON output"
        )
    return False, f"terraform {step} failed"


def main() -> int:
    account = os.environ.get("AWS_ACCOUNT_ID", "")
    if not _ACCOUNT.match(account):
        sys.stderr.write(
            "ci_plan: AWS_ACCOUNT_ID has to hold the 12 digits of the account "
            "(a secret of the repository)\n"
        )
        return 2
    results = [(root, *plan(root, account)) for root in ROOTS]
    table = ["### terraform plan with the role gbmi-ci-plan", "", "| Configuration | Result |"]
    table += ["|---|---|", *(f"| `{root}` | {line} |" for root, _, line in results)]
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as stream:
            stream.write("\n".join(table) + "\n")
    for root, _, line in results:
        out(f"{root}: {line}")
    return 0 if all(passed for _, passed, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main())
