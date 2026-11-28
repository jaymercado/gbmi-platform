"""The addresses of the SEC files which the platform asks for (blueprint 4.3; week 3 plan, F1-F12).

The SEC serves the files from two hosts. The fair-access limit counts the requests to both hosts
together.
"""

from __future__ import annotations

from datetime import date
from urllib.parse import urlsplit

#: Reference files, the daily index and the bulk archives.
WWW_HOST = "www.sec.gov"
#: The submissions documents and the facts documents.
DATA_HOST = "data.sec.gov"
HOSTS = frozenset({WWW_HOST, DATA_HOST})
#: Below the prefix a missing file answers HTTP 403 with the XML body AccessDenied (finding F9).
ARCHIVES_PREFIX = "/Archives/"

TICKER_FILE_NAME = "company_tickers_exchange.json"
TICKER_FILE_URL = f"https://{WWW_HOST}/files/{TICKER_FILE_NAME}"
SUBMISSIONS_ARCHIVE_NAME = "submissions.zip"
SUBMISSIONS_ARCHIVE_URL = (
    f"https://{WWW_HOST}/Archives/edgar/daily-index/bulkdata/{SUBMISSIONS_ARCHIVE_NAME}"
)


def cik_file_name(cik: int) -> str:
    """The name of the document of a company: the CIK with ten digits."""
    if not 0 < cik < 10**10:
        raise ValueError(f"a CIK is a positive number of at most ten digits, got {cik}")
    return f"CIK{cik:010d}.json"


def submissions_url(cik: int) -> str:
    return f"https://{DATA_HOST}/submissions/{cik_file_name(cik)}"


def companyfacts_url(cik: int) -> str:
    return f"https://{DATA_HOST}/api/xbrl/companyfacts/{cik_file_name(cik)}"


def quarter_of(day: date) -> tuple[int, int]:
    """The year and the quarter (1 to 4) of a day."""
    return day.year, (day.month - 1) // 3 + 1


def index_listing_url(year: int, quarter: int) -> str:
    """The listing of the daily index files of one quarter (finding F10)."""
    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"a quarter is 1, 2, 3 or 4, got {quarter}")
    return f"https://{WWW_HOST}/Archives/edgar/daily-index/{year}/QTR{quarter}/index.json"


def index_file_name(day: date) -> str:
    return f"form.{day:%Y%m%d}.idx"


def index_file_url(day: date) -> str:
    """The form index of one day (finding F11)."""
    year, quarter = quarter_of(day)
    return (
        f"https://{WWW_HOST}/Archives/edgar/daily-index/{year}/QTR{quarter}/{index_file_name(day)}"
    )


def split(url: str) -> tuple[str, str]:
    """The host and the path of an address of the SEC; any other address is refused."""
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname not in HOSTS or parts.query or parts.fragment:
        raise ValueError(f"not an HTTPS address of the SEC without a query: {url!r}")
    return parts.hostname, parts.path or "/"
