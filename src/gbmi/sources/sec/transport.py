"""The recorded SEC files as a transport (week 3 plan 3.3 and WP-04).

`make demo`, the tests and CI run the real client, with its limiter, its circuit and its
request log, against the files of `tests/fixtures/sec`. No request leaves the machine. The
folder holds one file for each recorded address, under the host name and the path of the
address, and the bodies of the two "no such object" answers of the SEC:

* an address of `data.sec.gov` without a file answers HTTP 404 with the recorded body
  (finding F8: a company without a facts document);
* an address below `www.sec.gov/Archives/` without a file answers HTTP 403 with the recorded
  body (finding F9: no index file for a day).
"""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path

import httpx

from gbmi.sources.sec.errors import SecConfigurationError
from gbmi.sources.sec.urls import ARCHIVES_PREFIX, DATA_HOST, HOSTS

REFUSALS = "refusals"
NO_SUCH_KEY = "data.sec.gov-404.xml"
ACCESS_DENIED = "www.sec.gov-archives-403.xml"
#: The time of the last change which the transport reports for every recorded file. The time
#: lies before the fixed clock of the demo.
LAST_MODIFIED = datetime(2026, 11, 26, 5, 6, 36, tzinfo=UTC)
_XML = {"Content-Type": "application/xml"}


class FixtureHandler:
    """Answers a request with the recorded file of the address."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        refusals = self.directory / REFUSALS
        try:
            self._no_such_key = (refusals / NO_SUCH_KEY).read_bytes()
            self._access_denied = (refusals / ACCESS_DENIED).read_bytes()
        except OSError:
            raise SecConfigurationError(
                f"{directory} holds no recorded SEC files: the two bodies under {REFUSALS}/ "
                "are missing. GBMI_SEC_FIXTURES_DIR names the folder"
            ) from None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        if request.method != "GET" or host not in HOSTS:
            return httpx.Response(405, content=b"the recorded files answer GET for the SEC only")
        root = self.directory / host
        target = (root / path.lstrip("/")).resolve()
        if target.is_relative_to(root) and target.is_file():
            modified = format_datetime(LAST_MODIFIED, usegmt=True)
            return httpx.Response(
                200, content=target.read_bytes(), headers={"Last-Modified": modified}
            )
        if host == DATA_HOST:
            return httpx.Response(404, content=self._no_such_key, headers=_XML)
        if path.startswith(ARCHIVES_PREFIX):
            return httpx.Response(403, content=self._access_denied, headers=_XML)
        return httpx.Response(404, content=b"no recorded file for the address")


def fixture_transport(directory: Path | str) -> httpx.MockTransport:
    """The transport of the source `fixture`: the files below `directory`."""
    return httpx.MockTransport(FixtureHandler(Path(directory)))
