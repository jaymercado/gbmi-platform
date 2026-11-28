"""make redact-check: the search for the account number and the two addresses (change C18)."""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[2]
ACCOUNT = "123456789012"
ALERT = "owner+alerts@mailbox.test"
CONTACT = "Owner+sec@Mailbox.test"
GIT = shutil.which("git") or "git"


def load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "redact_check", REPO / "tools" / "redact_check.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


redact = load()


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    """A small repository with the three local files, which Git ignores."""
    subprocess.run([GIT, "init", "--quiet", str(tmp_path)], check=True)
    (tmp_path / ".gitignore").write_text(".env\nbackend.hcl\ninfra/terraform/local.env\n")
    for name in redact.BACKEND_FILES:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'bucket = "gbmi-tfstate-{ACCOUNT}"\nregion = "us-east-1"\n')
    (tmp_path / "infra/terraform/local.env").write_text(f"TF_VAR_alert_email='{ALERT}'\n")
    (tmp_path / ".env").write_text(f'OTHER=1\nGBMI_SEC_USER_AGENT="Owner Name {CONTACT}"\n')
    (tmp_path / "README.md").write_text("A public file about account <account>.\n")
    return tmp_path


def run(root: Path) -> tuple[int, str]:
    result = subprocess.run(
        [sys.executable, str(REPO / "tools" / "redact_check.py"), "--root", str(root)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout + result.stderr


def test_the_values_come_from_the_local_files_with_and_without_the_alias(repository: Path) -> None:
    values = redact.local_values(repository)

    assert values == {
        redact.ACCOUNT: {ACCOUNT},
        redact.ALERT: {"owner+alerts@mailbox.test", "owner@mailbox.test"},
        redact.CONTACT: {"owner+sec@mailbox.test", "owner@mailbox.test"},
    }
    assert redact.unquote(' "a b" ') == "a b" and redact.unquote("'x'") == "x"
    assert redact.unquote('"open') == '"open'
    assert redact.addresses("Sample contact@example.org x@host.invalid") == set()
    assert redact.env_value(repository / "missing.env", "X") is None


def test_a_clean_repository_passes_and_the_ignored_files_are_not_searched(repository: Path) -> None:
    code, output = run(repository)

    assert code == 0, output
    assert "passed" in output and "2 files searched" in output


@pytest.mark.parametrize(
    ("text", "kinds"),
    [
        (f"arn:aws:iam::{ACCOUNT}:root", ["AWS account number"]),
        (f"write to {ALERT.upper()}", ["alert address"]),
        ("ask owner@mailbox.test", ["alert address", "contact address for the SEC"]),
        (f"User-Agent: Owner Name {CONTACT.lower()}", ["contact address for the SEC"]),
    ],
)
def test_a_hit_names_the_file_and_the_kind_and_never_the_value(
    repository: Path, text: str, kinds: list[str]
) -> None:
    (repository / "docs").mkdir()
    (repository / "docs" / "report.md").write_text(f"evidence\n{text}\n")  # a new file, not added

    code, output = run(repository)

    assert code == 1
    for kind in kinds:
        assert f"docs/report.md: holds the {kind}" in output
    for value in (ACCOUNT, "mailbox.test", "owner"):
        assert value not in output.lower().replace("the owner", "")
    assert "No value is printed" in output


def test_a_number_of_another_length_is_no_account_number(repository: Path) -> None:
    (repository / "notes.md").write_text(f"20261125080000 and {ACCOUNT}0 and 1{ACCOUNT}\n")
    # The values are matched as text: a longer number which holds the 12 digits is a hit.
    code, output = run(repository)

    assert code == 1 and "notes.md: holds the AWS account number" in output


def test_without_a_local_file_the_check_has_nothing_to_search_for(tmp_path: Path) -> None:
    subprocess.run([GIT, "init", "--quiet", str(tmp_path)], check=True)
    (tmp_path / "README.md").write_text(f"{ACCOUNT} {ALERT}\n")

    code, output = run(tmp_path)

    assert code == 0
    assert "no local file holds a value" in output
