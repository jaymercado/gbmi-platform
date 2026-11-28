"""Write the environment file of Compose on the AWS host from Parameter Store (week 3 plan 3.3).

    aws ssm get-parameters-by-path --path /gbmi/prod/ --with-decryption --recursive \\
        --output json | python3 render_env.py <target file>

Called by render_env.sh, which also passes the account, the region and the commit of the
release in the environment. The script reads the answer of the AWS CLI on standard input,
writes the file with the mode 600 and prints names and counts only: never a value.

The standard library only, and nothing newer than Python 3.9 at run time: the script runs
with the Python of the host, outside any container.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

#: The generated secrets of .env.example: every one has to be a parameter (make host-secrets).
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
#: Parameters which may exist: the declared identity for the SEC and the source of the SEC
#: client. Without the second one the source is "off": the host sends no request to the SEC.
OPTIONAL = ("GBMI_SEC_USER_AGENT", "GBMI_SEC_SOURCE")
#: What the host derives itself; a parameter with one of the names is refused.
DERIVED = (
    "GBMI_RELEASE",
    "GBMI_LAKE_BUCKET",
    "GBMI_OPS_BUCKET",
    "GBMI_CH_LAKE_ROLE_ARN",
    "GBMI_LOG_GROUP",
    "AWS_DEFAULT_REGION",
)
NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
ACCOUNT = re.compile(r"^[0-9]{12}$")
LINE_BREAKS = ("\n", "\r", "\x00")


class RenderError(Exception):
    """The environment file cannot be written; the text names no value."""


def quoted(name: str, value: str) -> str:
    """One value in the syntax of a Compose env file.

    Single quotes keep every character as it is. A value with a single quote takes double
    quotes, in which a backslash protects the backslash, the double quote and the dollar sign.
    """
    if any(character in value for character in LINE_BREAKS):
        raise RenderError(f"the parameter {name} holds a line break; an env file cannot hold one")
    if "'" not in value:
        return f"'{value}'"
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$")
    return f'"{escaped}"'


def variables(answer: Mapping[str, Any], environment: Mapping[str, str]) -> list[tuple[str, str]]:
    """The variables of the env file, in the order of the file."""
    commit = environment.get("GBMI_RELEASE", "")
    account = environment.get("GBMI_ACCOUNT", "")
    region = environment.get("AWS_DEFAULT_REGION", "")
    if not COMMIT.match(commit):
        raise RenderError("GBMI_RELEASE has to be the 40 characters of a commit")
    if not ACCOUNT.match(account):
        raise RenderError("GBMI_ACCOUNT has to be the 12 digits of the account of the host")
    if not region:
        raise RenderError("AWS_DEFAULT_REGION is not set")

    found: dict[str, str] = {}
    for parameter in answer.get("Parameters", []):
        name = parameter["Name"].rsplit("/", 1)[-1]
        if not NAME.match(name):
            raise RenderError("a parameter has a name which is no name of a variable")
        if name in DERIVED:
            raise RenderError(f"the host derives {name} itself; remove the parameter")
        if name in found:
            raise RenderError(f"two parameters end with the name {name}")
        found[name] = parameter["Value"]
    missing = [name for name in REQUIRED if not found.get(name)]
    if missing:
        raise RenderError(f"parameters are missing: {', '.join(missing)}")
    unknown = sorted(set(found) - set(REQUIRED) - set(OPTIONAL))
    if unknown:
        raise RenderError(f"parameters which no service reads: {', '.join(unknown)}")
    source = found.get("GBMI_SEC_SOURCE", "off")
    if source not in ("off", "live"):
        raise RenderError("the parameter GBMI_SEC_SOURCE has to be off or live")
    if source == "live" and not found.get("GBMI_SEC_USER_AGENT"):
        raise RenderError(
            "GBMI_SEC_SOURCE is live, and the parameter GBMI_SEC_USER_AGENT is missing"
        )

    result = [(name, found[name]) for name in REQUIRED]
    result.append(("GBMI_SEC_SOURCE", source))
    if found.get("GBMI_SEC_USER_AGENT"):
        result.append(("GBMI_SEC_USER_AGENT", found["GBMI_SEC_USER_AGENT"]))
    result += [
        ("GBMI_RELEASE", commit),
        ("AWS_DEFAULT_REGION", region),
        ("GBMI_LAKE_BUCKET", f"gbmi-prod-lake-{account}"),
        ("GBMI_OPS_BUCKET", f"gbmi-prod-ops-{account}"),
        ("GBMI_CH_LAKE_ROLE_ARN", f"arn:aws:iam::{account}:role/gbmi-clickhouse-lake-ro"),
        ("GBMI_LOG_GROUP", "/gbmi/prod/containers"),
    ]
    return result


def write(target: Path, pairs: list[tuple[str, str]]) -> None:
    """Write the file whole, readable by its owner only."""
    lines = ["# Written by compose/host/render_env.sh from Parameter Store. Do not edit."]
    lines += [f"{name}={quoted(name, value)}" for name, value in pairs]
    directory = target.resolve().parent
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(dir=directory, prefix=".env.")
    try:
        os.fchmod(handle, 0o600)
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write("\n".join(lines) + "\n")
        Path(temporary).replace(target)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def main(arguments: list[str]) -> int:
    if len(arguments) != 2:
        sys.stderr.write("usage: render_env.py <target file>\n")
        return 2
    try:
        answer = json.load(sys.stdin)
        pairs = variables(answer, os.environ)
        write(Path(arguments[1]), pairs)
    except RenderError as error:
        sys.stderr.write(f"render_env: {error}\n")
        return 1
    except (ValueError, KeyError, TypeError, AttributeError):
        # The text of such an error can quote the input, and the input holds the values.
        sys.stderr.write("render_env: the answer of Parameter Store cannot be read\n")
        return 1
    source = dict(pairs)["GBMI_SEC_SOURCE"]
    sys.stdout.write(
        f"render_env: wrote {len(pairs)} variables to {arguments[1]} "
        f"(source of the SEC client: {source})\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
