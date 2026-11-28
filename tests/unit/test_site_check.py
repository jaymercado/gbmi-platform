"""make docs: the search of the documentation site for secrets of the stack."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("site_check", REPO / "tools" / "site_check.py")
assert SPEC is not None and SPEC.loader is not None
site_check = importlib.util.module_from_spec(SPEC)
sys.modules["site_check"] = site_check
SPEC.loader.exec_module(site_check)

PASSWORD = "madeup-password-0123456789"
IDENTITY = "Sample Company Name contact@example.org"


@pytest.fixture
def stack(tmp_path: Path) -> dict[str, Path]:
    env = tmp_path / ".env"
    env.write_text(
        f"POSTGRES_PASSWORD={PASSWORD}\n"
        f'GBMI_SEC_USER_AGENT="{IDENTITY}"\n'
        "AIRFLOW_ADMIN_PASSWORD=short\n"
        "GBMI_LAKE_BACKEND=local\n",
        "utf-8",
    )
    site = tmp_path / "site"
    site.mkdir()
    (site / "index.html").write_text("<html>the models of the project</html>", "utf-8")
    return {"env": env, "site": site}


def run(stack: dict[str, Path]) -> int:
    code: int = site_check.main([str(stack["site"]), "--env", str(stack["env"])])
    return code


def test_a_site_without_a_secret_passes(
    stack: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(stack) == 0
    shown = capsys.readouterr().out
    # The generated secret and the declared identity; the short value is left out.
    assert shown == "site-check: passed; 1 files searched for the values of 2 variables\n"


@pytest.mark.parametrize(
    ("value", "variable"), [(PASSWORD, "POSTGRES_PASSWORD"), (IDENTITY, "GBMI_SEC_USER_AGENT")]
)
def test_a_value_in_a_site_file_fails_and_only_names_are_shown(
    stack: dict[str, Path], capsys: pytest.CaptureFixture[str], value: str, variable: str
) -> None:
    folder = stack["site"] / "assets"
    folder.mkdir()
    (folder / "manifest.json").write_text(f'{{"compiled": "password = {value}"}}', "utf-8")

    assert run(stack) == 1
    shown = capsys.readouterr()
    assert shown.out == f"site-check: the file assets/manifest.json holds the value of {variable}\n"
    assert value not in shown.out + shown.err


def test_an_empty_site_or_a_missing_environment_file_fails(
    stack: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    (stack["site"] / "index.html").unlink()
    assert run(stack) == 1
    assert "holds no file" in capsys.readouterr().err

    (stack["site"] / "index.html").write_text("x", "utf-8")
    stack["env"].unlink()
    assert run(stack) == 1
    assert "the environment file is missing" in capsys.readouterr().err


def test_the_template_names_the_generated_secrets() -> None:
    env = REPO / ".env.example"
    values = site_check.secrets(env, env)
    # The template holds placeholders, which are longer than the limit: every generated
    # secret of the template is searched for.
    assert "POSTGRES_PASSWORD" in values
    assert "AIRFLOW_FERNET_KEY" in values
    assert len(values) >= 14
