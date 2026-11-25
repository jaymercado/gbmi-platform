"""Source attributions required by the licence and compliance register (blueprint 4.7)."""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

GDELT_REQUIRED = ("The GDELT Project", "https://www.gdeltproject.org/")
SEC_REQUIRED = ("U.S. Securities and Exchange Commission", "EDGAR", "https://www.sec.gov/")


@pytest.mark.parametrize("filename", ["README.md", "NOTICE"])
@pytest.mark.parametrize("required", [*GDELT_REQUIRED, *SEC_REQUIRED])
def test_attribution_is_present(filename: str, required: str) -> None:
    # Collapse whitespace so that a line wrap inside a name does not hide it.
    text = " ".join((REPO_ROOT / filename).read_text(encoding="utf-8").split())

    assert required in text, f"{filename} must contain {required!r} (blueprint 4.7)"


def test_licence_is_apache_2() -> None:
    text = (REPO_ROOT / "LICENSE").read_text(encoding="utf-8")

    assert "Apache License" in text
    assert "Version 2.0, January 2004" in text
