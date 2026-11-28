"""The transport of the recorded SEC files (week 3 plan 3.3 and WP-04)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
import pytest

from gbmi.sources.sec.circuit import BODY_START, Verdict, classify
from gbmi.sources.sec.errors import SecConfigurationError
from gbmi.sources.sec.transport import (
    ACCESS_DENIED,
    LAST_MODIFIED,
    NO_SUCH_KEY,
    REFUSALS,
    FixtureHandler,
    fixture_transport,
)
from gbmi.sources.sec.urls import (
    DATA_HOST,
    SUBMISSIONS_ARCHIVE_URL,
    TICKER_FILE_URL,
    WWW_HOST,
    companyfacts_url,
    index_file_url,
    submissions_url,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "sec"


@pytest.fixture
def http() -> Iterator[httpx.Client]:
    with httpx.Client(transport=fixture_transport(FIXTURES)) as client:
        yield client


def verdict(response: httpx.Response) -> Verdict:
    request = response.request
    return classify(
        response.status_code, request.url.host, request.url.path, response.content[:BODY_START]
    )


def test_each_recorded_file_answers_at_the_address_of_the_sec(http: httpx.Client) -> None:
    files = [
        path
        for host in (WWW_HOST, DATA_HOST)
        for path in (FIXTURES / host).rglob("*")
        if path.is_file()
    ]

    assert len(files) == 17
    for path in files:
        host, _, rest = str(path.relative_to(FIXTURES)).partition("/")
        response = http.get(f"https://{host}/{rest}")

        assert response.status_code == 200
        assert response.content == path.read_bytes()
        assert parsedate_to_datetime(response.headers["Last-Modified"]) == LAST_MODIFIED
        assert verdict(response) is Verdict.OK
    assert http.get(TICKER_FILE_URL).json()["fields"] == ["cik", "name", "ticker", "exchange"]
    assert http.get(SUBMISSIONS_ARCHIVE_URL).content[:2] == b"PK"


def test_a_company_without_a_recorded_document_answers_404_with_the_body_of_the_sec(
    http: httpx.Client,
) -> None:
    # Finding F8: easyJet has a submissions document and no facts document.
    assert http.get(submissions_url(1470419)).status_code == 200
    for url in (companyfacts_url(1470419), submissions_url(1018724), companyfacts_url(999)):
        response = http.get(url)

        assert response.status_code == 404
        assert response.content == (FIXTURES / REFUSALS / NO_SUCH_KEY).read_bytes()
        assert b"<Code>NoSuchKey</Code>" in response.content
        assert verdict(response) is Verdict.MISSING


def test_an_index_file_which_is_not_recorded_answers_403_with_the_body_of_the_sec(
    http: httpx.Client,
) -> None:
    # Finding F9: below /Archives/ a missing file answers 403, not 404.
    for url in (
        index_file_url(date(2026, 11, 26)),
        f"https://{WWW_HOST}/Archives/edgar/daily-index/2026/QTR3/index.json",
    ):
        response = http.get(url)

        assert response.status_code == 403
        assert response.content == (FIXTURES / REFUSALS / ACCESS_DENIED).read_bytes()
        assert verdict(response) is Verdict.MISSING
    # Outside /Archives/ the SEC was not asked for a missing file: the answer is no "missing".
    other = http.get(f"https://{WWW_HOST}/files/other.json")
    assert other.status_code == 404
    assert verdict(other) is Verdict.ERROR


def test_the_transport_answers_get_for_the_two_hosts_of_the_sec_only(http: httpx.Client) -> None:
    assert http.post(TICKER_FILE_URL).status_code == 405
    assert http.head(TICKER_FILE_URL).status_code == 405
    assert http.get("https://example.org/files/company_tickers_exchange.json").status_code == 405


def test_an_address_cannot_leave_the_folder_of_its_host() -> None:
    handler = FixtureHandler(FIXTURES)
    # A dot segment which arrives encoded is not removed by the URL parser.
    inside = httpx.Request(
        "GET", f"https://{WWW_HOST}/files/%2e%2e/files/company_tickers_exchange.json"
    )
    outside = httpx.Request(
        "GET", f"https://{WWW_HOST}/Archives/%2e%2e/%2e%2e/{REFUSALS}/{NO_SUCH_KEY}"
    )
    other_host = httpx.Request(
        "GET", f"https://{WWW_HOST}/%2e%2e/{DATA_HOST}/submissions/CIK0001326801.json"
    )

    assert ".." in outside.url.path
    assert handler(inside).status_code == 200
    assert handler(outside).status_code == 403
    assert handler(other_host).status_code == 404


def test_a_folder_without_the_recorded_files_is_refused(tmp_path: Path) -> None:
    with pytest.raises(SecConfigurationError, match="holds no recorded SEC files") as raised:
        fixture_transport(tmp_path)
    assert "GBMI_SEC_FIXTURES_DIR" in str(raised.value)
    (tmp_path / REFUSALS).mkdir()
    (tmp_path / REFUSALS / NO_SUCH_KEY).write_bytes(b"<Error/>")
    with pytest.raises(SecConfigurationError, match="holds no recorded SEC files"):
        fixture_transport(str(tmp_path))
