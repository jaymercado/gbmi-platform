"""Rules for every test of the repository."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _no_inherited_sec_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """No test inherits the source or the identity of the SEC client from its environment.

    The Makefile exports ``GBMI_SEC_SOURCE`` for the stack, and the value is ``live`` for the
    working stack. A test which needs a source builds its settings by name, so no test can
    open a connection to the SEC by accident (week 3 plan 3.3).
    """
    monkeypatch.delenv("GBMI_SEC_SOURCE", raising=False)
    monkeypatch.delenv("GBMI_SEC_USER_AGENT", raising=False)
