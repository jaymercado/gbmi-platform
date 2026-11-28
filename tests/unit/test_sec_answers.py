"""What an answer of the SEC means: one test for each row of the table (week 3 plan, WP-03)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from gbmi.sources.sec.circuit import BLOCK, Verdict, block_length, classify
from gbmi.sources.sec.urls import (
    DATA_HOST,
    SUBMISSIONS_ARCHIVE_URL,
    TICKER_FILE_URL,
    WWW_HOST,
    cik_file_name,
    companyfacts_url,
    index_file_name,
    index_file_url,
    index_listing_url,
    quarter_of,
    split,
    submissions_url,
)

ACCESS_DENIED = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>AccessDenied</Code>'
    b"<Message>Access Denied</Message></Error>"
)
RATE_PAGE = b"<html><head><title>SEC.gov | Request Rate Threshold Exceeded</title></head></html>"
INDEX = "/Archives/edgar/daily-index/2026/QTR4/form.20261126.idx"
FACTS = "/api/xbrl/companyfacts/CIK0001740332.json"


@pytest.mark.parametrize(
    ("status", "host", "path", "body", "verdict"),
    [
        (200, DATA_HOST, FACTS, b"{}", Verdict.OK),
        (200, WWW_HOST, INDEX, b"Description", Verdict.OK),
        # Only the status 200 is the object: the client asks for whole objects.
        (206, WWW_HOST, INDEX, b"", Verdict.ERROR),
        (204, DATA_HOST, FACTS, b"", Verdict.ERROR),
        (201, DATA_HOST, FACTS, b"{}", Verdict.ERROR),
        # Finding F8: the SEC has no facts document for the company.
        (404, DATA_HOST, FACTS, b"<Error><Code>NoSuchKey</Code></Error>", Verdict.MISSING),
        (404, DATA_HOST, FACTS, b"", Verdict.MISSING),
        # Finding F9: a missing file below /Archives/ answers 403 with an XML body.
        (403, WWW_HOST, INDEX, ACCESS_DENIED, Verdict.MISSING),
        # A 403 with another body is a refusal.
        (403, WWW_HOST, INDEX, RATE_PAGE, Verdict.BLOCKED),
        (403, WWW_HOST, INDEX, b"", Verdict.BLOCKED),
        # The XML body means "missing" below /Archives/ on www.sec.gov only.
        (403, WWW_HOST, "/files/company_tickers_exchange.json", ACCESS_DENIED, Verdict.BLOCKED),
        (403, DATA_HOST, FACTS, ACCESS_DENIED, Verdict.BLOCKED),
        (429, DATA_HOST, FACTS, RATE_PAGE, Verdict.BLOCKED),
        (429, WWW_HOST, INDEX, b"", Verdict.BLOCKED),
        (500, DATA_HOST, FACTS, b"", Verdict.RETRY),
        (503, WWW_HOST, INDEX, b"Service Unavailable", Verdict.RETRY),
        # No redirect is followed, and a 404 outside data.sec.gov is no known answer.
        (301, WWW_HOST, INDEX, b"", Verdict.ERROR),
        (404, WWW_HOST, "/files/company_tickers_exchange.json", b"", Verdict.ERROR),
        (400, DATA_HOST, FACTS, b"", Verdict.ERROR),
    ],
)
def test_an_answer_is_read_from_status_host_path_and_body(
    status: int, host: str, path: str, body: bytes, verdict: Verdict
) -> None:
    assert classify(status, host, path, body) is verdict


def test_the_verdicts_are_the_outcomes_of_the_request_log() -> None:
    assert {verdict.value for verdict in Verdict} == {"ok", "missing", "blocked", "retry", "error"}


@pytest.mark.parametrize(
    ("retry_after", "minutes"),
    [
        (None, 15),
        (0, 15),
        (-5, 15),
        (30, 15),
        (900, 15),
        (901, 15.016667),
        (3600, 60),
        # A header cannot make the block endless or break the arithmetic.
        (9_999_999, 7 * 24 * 60),
        (float("inf"), 15),
        (float("nan"), 15),
    ],
)
def test_a_block_lasts_15_minutes_or_the_longer_time_of_the_header(
    retry_after: float | None, minutes: float
) -> None:
    assert block_length(retry_after) >= BLOCK
    assert block_length(retry_after) == pytest.approx(
        timedelta(minutes=minutes), abs=timedelta(seconds=1)
    )


def test_the_addresses_of_the_sec_files() -> None:
    assert cik_file_name(1326801) == "CIK0001326801.json"
    assert submissions_url(1326801) == "https://data.sec.gov/submissions/CIK0001326801.json"
    assert (
        companyfacts_url(63276) == "https://data.sec.gov/api/xbrl/companyfacts/CIK0000063276.json"
    )
    assert TICKER_FILE_URL == "https://www.sec.gov/files/company_tickers_exchange.json"
    assert SUBMISSIONS_ARCHIVE_URL.endswith("/Archives/edgar/daily-index/bulkdata/submissions.zip")
    day = date(2026, 11, 25)
    assert quarter_of(day) == (2026, 4)
    assert [quarter_of(date(2026, month, 1))[1] for month in (1, 3, 4, 6, 7, 9, 10, 12)] == [
        1,
        1,
        2,
        2,
        3,
        3,
        4,
        4,
    ]
    assert index_file_name(day) == "form.20261125.idx"
    assert (
        index_file_url(day)
        == "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/form.20261125.idx"
    )
    assert (
        index_listing_url(2026, 4)
        == "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/index.json"
    )
    for bad in (0, -1, 10**10):
        with pytest.raises(ValueError, match="CIK"):
            cik_file_name(bad)
    with pytest.raises(ValueError, match="quarter"):
        index_listing_url(2026, 5)


def test_only_an_https_address_of_the_sec_without_a_query_is_split() -> None:
    assert split(submissions_url(1326801)) == (DATA_HOST, "/submissions/CIK0001326801.json")
    assert split("https://www.sec.gov") == (WWW_HOST, "/")
    for bad in (
        "http://www.sec.gov/files/company_tickers_exchange.json",
        "https://example.org/files/company_tickers_exchange.json",
        "https://www.sec.gov.example.org/x",
        "https://data.sec.gov/submissions/CIK0001326801.json?x=1",
        "https://data.sec.gov/submissions/CIK0001326801.json#top",
        "ftp://www.sec.gov/x",
    ):
        with pytest.raises(ValueError, match="not an HTTPS address of the SEC"):
            split(bad)
