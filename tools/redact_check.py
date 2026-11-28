"""make redact-check: no AWS account number and no address of a person in the repository.

The repository is public (week 3 plan 3.3, with change C18). Three values of the owner live
only in git-ignored local files:

* the AWS account number, in the ``backend.hcl`` files of the Terraform configurations;
* the alert address, as ``TF_VAR_alert_email`` in ``infra/terraform/local.env``;
* the contact address for the SEC, inside ``GBMI_SEC_USER_AGENT`` in ``.env``.

The script reads the three values, searches every file which Git tracks or would add, and
prints the name of each file with a hit and the kind of the value. The script never prints a
value. An address is also searched without the part between ``+`` and ``@``. Without a local
file (CI, a fresh clone) the script has nothing to search for and passes.

Only the standard library is used, so the script runs as a pre-commit hook with any Python 3.

Usage:
    python3 tools/redact_check.py [--root <repository>]
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ACCOUNT = "AWS account number"
ALERT = "alert address"
CONTACT = "contact address for the SEC"
BACKEND_FILES = ("infra/terraform/bootstrap/backend.hcl", "infra/terraform/envs/prod/backend.hcl")
ALERT_FILE = ("infra/terraform/local.env", "TF_VAR_alert_email")
CONTACT_FILE = (".env", "GBMI_SEC_USER_AGENT")
_ACCOUNT = re.compile(r"(?<![0-9])[0-9]{12}(?![0-9])")
_ADDRESS = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
#: Addresses which are no address of a person.
_PLACEHOLDER_DOMAINS = ("example.com", "example.org", "example.net", ".invalid", ".example")
_MAX_BYTES = 50 * 1024 * 1024


def unquote(value: str) -> str:
    """A value of an env file without the quotes around it."""
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def env_value(path: Path, name: str) -> str | None:
    """The value of ``NAME=value`` (or ``export NAME=value``) in an env file; None when absent."""
    if not path.is_file():
        return None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip().removeprefix("export ").strip()
        key, separator, value = stripped.partition("=")
        if separator and key.strip() == name:
            return unquote(value)
    return None


def addresses(text: str | None) -> set[str]:
    """Every address in a text, and each address without the part between + and @."""
    found: set[str] = set()
    for address in _ADDRESS.findall(text or ""):
        lowered = address.lower()
        if lowered.endswith(_PLACEHOLDER_DOMAINS):
            continue
        found.add(lowered)
        local, _, domain = lowered.partition("@")
        if "+" in local:
            found.add(f"{local.split('+', 1)[0]}@{domain}")
    return found


def local_values(root: Path) -> dict[str, set[str]]:
    """The values to search for, by kind; a kind without a local file has no value."""
    accounts: set[str] = set()
    for name in BACKEND_FILES:
        path = root / name
        if path.is_file():
            accounts.update(_ACCOUNT.findall(path.read_text(encoding="utf-8", errors="replace")))
    return {
        ACCOUNT: accounts,
        ALERT: addresses(env_value(root / ALERT_FILE[0], ALERT_FILE[1])),
        CONTACT: addresses(env_value(root / CONTACT_FILE[0], CONTACT_FILE[1])),
    }


def candidate_files(root: Path) -> list[Path]:
    """The files which Git tracks, and the new files which Git would add."""
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root,
        capture_output=True,
        check=True,
    ).stdout
    return [root / name.decode("utf-8", "surrogateescape") for name in listed.split(b"\0") if name]


def search(files: list[Path], values: dict[str, set[str]]) -> list[tuple[Path, str]]:
    """Each file which holds a value, with the kind of the value."""
    needles = [
        (kind, value.lower().encode("utf-8")) for kind, group in values.items() for value in group
    ]
    hits: list[tuple[Path, str]] = []
    for path in files:
        if not path.is_file() or path.stat().st_size > _MAX_BYTES:
            continue
        content = path.read_bytes().lower()
        for kind in sorted({kind for kind, needle in needles if needle in content}):
            hits.append((path, kind))
    return hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    root = parser.parse_args(argv).root.resolve()

    values = local_values(root)
    kinds = [kind for kind, group in values.items() if group]
    if not kinds:
        print("redact-check: no local file holds a value to search for (CI or a fresh clone)")
        return 0
    files = candidate_files(root)
    hits = search(files, values)
    for path, kind in hits:
        print(f"{path.relative_to(root)}: holds the {kind}")
    searched = f"{len(files)} files searched for the {', the '.join(kinds)}"
    if hits:
        print(f"redact-check: FAILED, {len(hits)} hits; {searched}. No value is printed.")
        return 1
    print(f"redact-check: passed; {searched}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
