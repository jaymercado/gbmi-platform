"""The errors of the SEC source. No message holds a request header.

The user agent of a request holds a contact address (blueprint 4.3). An error message names the
host and the path of an address and never a header, so the address cannot reach a log line or a
run record through an error.
"""

from __future__ import annotations


class SecError(Exception):
    """A problem with a request to the SEC."""


class SecConfigurationError(SecError):
    """The settings do not allow a client: the source is off, or the user agent is missing."""


class ObjectMissingError(SecError):
    """The SEC has no such object (week 3 plan, findings F8 and F9). No block, no retry.

    ``status`` and ``body`` are the status and the first bytes of the answer: the recording of
    the test files keeps both.
    """

    def __init__(self, message: str, *, status: int, body: bytes = b"") -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class SourceBlockedError(SecError):
    """The circuit of the source is open or locked, and the run may not wait."""


class SecResponseError(SecError):
    """The SEC answered with a status which the client cannot use."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class SecTransportError(SecError):
    """No answer arrived: the connection failed or a time limit ended."""
