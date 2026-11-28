"""Land one object of the SEC: from ``expected`` to ``verified`` (week 3 plan 3.3, WP-06 step 1).

One object is one file which the platform asks from the SEC. The object ledger holds its
state, and bronze holds its bytes under ``raw/sec/<dataset>/dt=<date>/<name>`` (blueprint 7.1).

* Bronze already holds the bytes of the object, and the bytes open: no request.
* The SEC has no such object: ``not_published`` with the status of the answer. No retry.
* Gate G1 fails: the format of the bytes does not open. The object is asked again; after the
  third failed try the bytes go to the quarantine area and the object is ``quarantined``.
* A block, a transport error or an unusable status ends the run: the object stays
  ``expected``, and the next run asks again.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from gbmi.ledger.object_states import ObjectState
from gbmi.ledger.objects import (
    MAX_ATTEMPTS,
    Download,
    Expected,
    ObjectKey,
    ObjectLedger,
    ObjectRecord,
)
from gbmi.logging import get_logger
from gbmi.quality.quarantine import write_quarantined_file
from gbmi.quality.results import FAILED, G1, CheckResult, record_results
from gbmi.sources.gdelt.land import Bronze
from gbmi.sources.sec.client import SecClient
from gbmi.sources.sec.errors import ObjectMissingError
from gbmi.sources.sec.index import IndexFormatError, parse_index

log = get_logger(__name__)

#: Gate G1 for one dataset: raises GateError unless the format of the bytes opens.
Check = Callable[[bytes], None]


class GateError(Exception):
    """The bytes of an object do not open as the format of the dataset (gate G1)."""


class BronzeError(Exception):
    """Bronze does not hold the bytes which the ledger records for an object."""


def bronze_key(key: ObjectKey) -> str:
    """The key of an object in the lake, relative to the lake root (blueprint 7.1)."""
    return f"raw/sec/{key.dataset.value}/dt={key.object_date.isoformat()}/{key.name}"


def md5_hex(content: bytes) -> str:
    # The SEC publishes no hash for a file. The MD5 names the stored bytes; no secret.
    return hashlib.md5(content, usedforsecurity=False).hexdigest()


def json_opens(content: bytes) -> None:
    """Gate G1 for a JSON document: the bytes parse, and the document is an object."""
    try:
        document = json.loads(content)
    except ValueError as error:
        raise GateError(f"the bytes are no JSON document: {error}") from None
    if not isinstance(document, Mapping):
        raise GateError("the JSON document is no object")


def index_opens(content: bytes) -> None:
    """Gate G1 for an index file: the header ends with the dashed line, and rows follow."""
    try:
        parsed = parse_index(content)
    except IndexFormatError as error:
        raise GateError(str(error)) from None
    if parsed.raw_records == 0:
        raise GateError("the index file has no row after its header")


@dataclass
class LandingCounts:
    """What the landings of one run did."""

    requests: int = 0
    bytes: int = 0
    verified: int = 0
    from_bronze: int = 0
    not_published: int = 0
    failed_tries: int = 0
    quarantined: int = 0
    kept: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "bytes": self.bytes,
            "verified": self.verified,
            "from_bronze": self.from_bronze,
            "not_published": self.not_published,
            "failed_tries": self.failed_tries,
            "quarantined": self.quarantined,
        }


class Lander:
    """Lands the objects of one run: the client, the ledger and bronze together."""

    def __init__(
        self,
        *,
        client: SecClient,
        ledger: ObjectLedger,
        bronze: Bronze,
        lake_uri: str,
        as_of: datetime,
    ) -> None:
        self.client = client
        self.ledger = ledger
        self.bronze = bronze
        self.lake_uri = lake_uri
        self.as_of = as_of
        self.counts = LandingCounts()

    def land(self, expected: Expected, *, check: Check, listed: bool) -> ObjectRecord:
        """Bring one object to ``verified`` when the object is ``expected``; return its row.

        An object in any other state is returned as the ledger holds it: the day has its
        answer already. ``listed`` says that a listing of the SEC names the object.
        """
        key = expected.key
        self.ledger.ensure_expected([expected])
        record = self._row(key)
        if record.state is not ObjectState.EXPECTED:
            return record
        stored = self.bronze.read(bronze_key(key))
        if stored is not None and _opens(check, stored):
            # A run before stored the bytes and stopped before the ledger knew.
            self._verified(key, stored, status=None, modified=None)
            self.counts.from_bronze += 1
            return self._row(key)
        while True:
            try:
                response = self.client.get(expected.url, listed=listed)
            except ObjectMissingError as error:
                self.counts.requests += 1
                self.ledger.mark_not_published(
                    key,
                    reason=f"the SEC answered HTTP {error.status}: no such object",
                    http_status=error.status,
                    as_of=self.as_of,
                )
                self.counts.not_published += 1
                log.info("sec_object_not_published", object=key.label, status=error.status)
                return self._row(key)
            self.counts.requests += 1
            self.counts.bytes += len(response.content)
            try:
                check(response.content)
            except GateError as error:
                if self._failed_try(key, response.content, str(error), response.status):
                    return self._row(key)
                continue
            self.bronze.write(bronze_key(key), response.content)
            self._verified(
                key, response.content, status=response.status, modified=response.last_modified
            )
            return self._row(key)

    def store(
        self, expected: Expected, content: bytes, *, check: Check, detail: Mapping[str, Any]
    ) -> ObjectRecord | None:
        """Store bytes which arrived without a request of their own: a member of an archive.

        Returns None, and stores nothing, when the bytes fail gate G1: the member is not an
        object of the day then, and the company keeps its last attributes.
        """
        key = expected.key
        self.ledger.ensure_expected([expected])
        record = self._row(key)
        if record.state is not ObjectState.EXPECTED:
            return record
        try:
            check(content)
        except GateError as error:
            self.ledger.record_failed_attempt(key, error=str(error))
            self.counts.failed_tries += 1
            return None
        self.bronze.write(bronze_key(key), content)
        self._verified(key, content, status=None, modified=None)
        self.ledger.set_detail(key, dict(detail))
        return self._row(key)

    def read(self, record: ObjectRecord) -> bytes:
        """The bytes of a verified or loaded object. Raises BronzeError when bronze differs."""
        content = self.bronze.read(bronze_key(record.key))
        if content is None:
            raise BronzeError(f"bronze does not hold {record.key.label}")
        if md5_hex(content) != record.md5:
            raise BronzeError(f"the bytes of {record.key.label} in bronze differ from the ledger")
        return content

    def quarantine_verified(
        self, record: ObjectRecord, *, gate: str, check: str, reason: str, threshold: str
    ) -> str | None:
        """``verified → quarantined``: gate G2 or G3 refused the object. Returns the kept key."""
        key = record.key
        content = self.bronze.read(bronze_key(key))
        kept = self._keep(key, content) if content is not None else None
        self.ledger.quarantine(
            key, from_state=ObjectState.VERIFIED, reason=reason, as_of=self.as_of
        )
        self.counts.quarantined += 1
        log.error("sec_object_quarantined", object=key.label, gate=gate, reason=reason)
        self._record_failure(key, gate, check, reason, threshold, kept)
        return kept

    # --- parts ----------------------------------------------------------------------------

    def _row(self, key: ObjectKey) -> ObjectRecord:
        record = self.ledger.get(key)
        if record is None:  # ensure_expected ran before
            raise BronzeError(f"the ledger has no object {key.label}")
        return record

    def _verified(
        self, key: ObjectKey, content: bytes, *, status: int | None, modified: datetime | None
    ) -> None:
        self.ledger.record_download(
            key,
            Download(
                bronze_uri=self.bronze.uri(bronze_key(key)),
                byte_count=len(content),
                md5=md5_hex(content),
                http_status=status,
                last_modified=modified,
            ),
            as_of=self.as_of,
        )
        self.counts.verified += 1

    def _failed_try(self, key: ObjectKey, content: bytes, error: str, status: int) -> bool:
        """Count one failed try of gate G1; True when the object is quarantined now."""
        attempts = self.ledger.record_failed_attempt(key, error=error, http_status=status)
        self.counts.failed_tries += 1
        log.warning("sec_object_failed_gate_g1", object=key.label, attempts=attempts, error=error)
        if attempts < MAX_ATTEMPTS:
            return False
        reason = f"gate G1 failed {attempts} times: {error}"
        kept = self._keep(key, content)
        self.ledger.quarantine(
            key, from_state=ObjectState.EXPECTED, reason=reason, as_of=self.as_of
        )
        self.counts.quarantined += 1
        self._record_failure(
            key, G1, "format_opens", reason, f"the format opens within {MAX_ATTEMPTS} tries", kept
        )
        return True

    def _keep(self, key: ObjectKey, content: bytes) -> str:
        kept = write_quarantined_file(
            self.lake_uri,
            self.ledger.source,
            key.dataset.value,
            key.object_date,
            key.name,
            content,
        )
        self.counts.kept.append(kept)
        return kept

    def _record_failure(
        self, key: ObjectKey, gate: str, check: str, reason: str, threshold: str, kept: str | None
    ) -> None:
        result = CheckResult(
            gate=gate,
            check_name=check,
            status=FAILED,
            threshold=threshold,
            feed=key.dataset.value,
            unit=key.label,
            detail={"reason": reason, "kept": kept},
        )
        with self.ledger.connection.transaction():
            record_results(
                self.ledger.connection,
                [result],
                source=self.ledger.source,
                run_id=self.ledger.run_id,
            )


def _opens(check: Check, content: bytes) -> bool:
    try:
        check(content)
    except GateError:
        return False
    return True
