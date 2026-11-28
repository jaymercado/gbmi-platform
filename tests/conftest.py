"""Rules for every test of the repository."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from gbmi.config import get_settings


def _no_network(*_arguments: object, **_options: object) -> httpx.HTTPTransport:
    raise AssertionError(
        "a test tried to build a network transport for the SEC client: pass a mock transport"
    )


@pytest.fixture(autouse=True)
def _no_live_sec(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """No test can ask the SEC (week 3 plan 3.3; review 1, weak tests).

    The Makefile exports ``GBMI_SEC_SOURCE`` for the stack, and the value is ``live`` for the
    working stack. A test never inherits the source or the identity from its environment: a
    test which needs a source builds its settings by name. And the one place of the package
    which builds a network transport for the SEC, ``httpx.HTTPTransport()`` in the client,
    fails in every test.
    """
    monkeypatch.delenv("GBMI_SEC_SOURCE", raising=False)
    monkeypatch.delenv("GBMI_SEC_USER_AGENT", raising=False)
    monkeypatch.setattr(httpx, "HTTPTransport", _no_network)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
