"""The bulk archive of the submissions documents (week 3 plan 3.3, difference D37).

One request loads ``submissions.zip``, about 1.6 GB, into a temporary file on the disk of the
task. The job copies the member ``CIK##########.json`` of each wanted company to bronze as the
submissions object of the day, writes a manifest and deletes the temporary file. The archive is
not stored in the lake (blueprint 7.1) and is never copied to the quarantine area. A run tries
the download once. A run without the archive goes on: the companies keep the attributes which
the platform knows (week 3 plan, WP-06, "When the work fails").

The request is expensive, so the job protects the downloaded file (review 2, finding R2-1):

* a member which cannot be stored, for any reason, counts as refused and does not stop the
  copy of the other members;
* the download and the use of the archive are recorded in one transaction;
* after three failed tries of one day the archive of the day is quarantined, and no run of
  the day asks again;
* no download from the live source starts between 04:30 and 05:30 UTC, when the SEC builds
  the archive (week 3 plan, finding F12 and change C19).

A process which is killed during the copy loses the temporary file: the next run of the day
asks for the archive again.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
import zipfile
import zlib
from collections.abc import Collection
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

from gbmi.config import SecSource
from gbmi.core.clock import utc_now
from gbmi.ledger.object_states import ObjectState
from gbmi.ledger.objects import (
    MAX_ATTEMPTS,
    Dataset,
    Download,
    Expected,
    ObjectAccounting,
    ObjectKey,
)
from gbmi.logging import get_logger
from gbmi.sources.sec.errors import ObjectMissingError, SecResponseError, SecTransportError
from gbmi.sources.sec.land import json_opens
from gbmi.sources.sec.silver import SecRun
from gbmi.sources.sec.urls import (
    SUBMISSIONS_ARCHIVE_NAME,
    SUBMISSIONS_ARCHIVE_URL,
    cik_file_name,
    submissions_url,
)

log = get_logger(__name__)

MANIFEST_NAME = "submissions.manifest.json"
#: The SEC builds the archive in the night: the build of 2026-11-27 ended at 05:06 UTC.
BUILD_WINDOW = (time(4, 30), time(5, 30))
_CHUNK = 1 << 20
_UNREADABLE = (zipfile.BadZipFile, zlib.error, EOFError, NotImplementedError, RuntimeError)


def manifest_key(run: SecRun) -> str:
    return f"raw/sec/{Dataset.BULK.value}/dt={run.day.isoformat()}/{MANIFEST_NAME}"


def in_build_window(moment: datetime) -> bool:
    """True from 04:30 to 05:30 UTC, the end not counted."""
    clock = moment.astimezone(UTC).time()
    return BUILD_WINDOW[0] <= clock < BUILD_WINDOW[1]


def waits_for_the_build(run: SecRun, moment: datetime) -> bool:
    """True when a download from the live SEC would start while the SEC builds the archive.

    The recorded files have no build: the demo and CI run at any time of the day.
    """
    return run.settings.sec_source is SecSource.LIVE and in_build_window(moment)


def load_archive(run: SecRun, ciks: Collection[int]) -> dict[str, Any]:
    """Copy the documents of ``ciks`` from the archive of the day to bronze.

    ``ciks`` are the companies without a document of the day. Returns a summary of built-in
    types; ``state`` says what became of the archive of the day.
    """
    key = ObjectKey(Dataset.BULK, run.day, SUBMISSIONS_ARCHIVE_NAME)
    summary: dict[str, Any] = {"wanted": len(ciks), "copied": 0, "requested": False}
    if not ciks:
        return {**summary, "state": "not needed"}
    if waits_for_the_build(run, utc_now()):
        # No object is created: a later run of the day asks for the archive.
        return {**summary, "state": "not now", "reason": "the SEC builds the archive at this time"}
    run.ledger.ensure_expected([Expected(key, SUBMISSIONS_ARCHIVE_URL)])
    item = run.ledger.get(key)
    if item is None or item.state is not ObjectState.EXPECTED:
        # The archive of the day has its answer: a run of the same day asks once.
        return {**summary, "state": item.state.value if item else "unknown"}
    with tempfile.TemporaryDirectory(prefix="gbmi-sec-bulk-") as folder:
        path = Path(folder) / SUBMISSIONS_ARCHIVE_NAME
        summary["requested"] = True
        run.lander.counts.requests += 1
        try:
            response = run.lander.client.download(SUBMISSIONS_ARCHIVE_URL, path)
        except ObjectMissingError as error:
            run.ledger.mark_not_published(
                key,
                reason=f"the SEC answered HTTP {error.status}: no such object",
                http_status=error.status,
                as_of=run.as_of,
            )
            return {**summary, "state": ObjectState.NOT_PUBLISHED.value}
        except (SecTransportError, SecResponseError) as error:
            # One try in a run. The next run of the day asks again, three times at most.
            attempts = run.ledger.record_failed_attempt(key, error=str(error))
            _give_up(run, key, attempts, f"the download failed: {error}")
            log.warning("sec_archive_not_loaded", error=str(error), attempts=attempts)
            return {**summary, "state": "failed", "error": str(error), "attempts": attempts}
        try:
            members, copied, refused = _copy_members(run, path, ciks)
        except _UNREADABLE as error:
            problem = f"the archive does not open: {type(error).__name__}: {error}"
            attempts = run.ledger.record_failed_attempt(
                key, error=problem, http_status=response.status
            )
            _give_up(run, key, attempts, problem)
            log.error("sec_archive_unreadable", error=problem, attempts=attempts)
            return {**summary, "state": "failed", "error": problem, "attempts": attempts}
        size = path.stat().st_size
        digest = _file_md5(path)
    missing = len(ciks) - len(copied) - len(refused)
    manifest = {
        "archive": SUBMISSIONS_ARCHIVE_NAME,
        "url": SUBMISSIONS_ARCHIVE_URL,
        "bytes": size,
        "md5": digest,
        "last_modified": response.last_modified.isoformat() if response.last_modified else None,
        "members": members,
        "wanted": len(ciks),
        "copied": copied,
        "refused": refused,
        "not_in_archive": missing,
    }
    content = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
    run.lander.bronze.write(manifest_key(run), content)
    # One transaction: an archive which was used up is never left open for another request.
    with run.ledger.connection.transaction():
        run.ledger.record_download(
            key,
            Download(
                bronze_uri=run.lander.bronze.uri(manifest_key(run)),
                byte_count=size,
                md5=digest,
                http_status=response.status,
                last_modified=response.last_modified,
            ),
            as_of=run.as_of,
        )
        run.ledger.mark_loaded(
            [ObjectAccounting(key, len(ciks), len(copied), len(ciks) - len(copied))],
            as_of=run.as_of,
            reason="the members of the archive were copied to bronze",
        )
    run.ledger.supersede_older(key, as_of=run.as_of)
    run.lander.counts.bytes += size
    log.info("sec_archive_loaded", bytes=size, members=members, copied=len(copied))
    return {
        **summary,
        "state": ObjectState.LOADED.value,
        "bytes": size,
        "members": members,
        "copied": len(copied),
        "refused": len(refused),
        "not_in_archive": missing,
        "manifest": manifest_key(run),
    }


def _copy_members(
    run: SecRun, path: Path, ciks: Collection[int]
) -> tuple[int, list[str], list[str]]:
    """Copy the wanted members; returns the member count, the copied and the refused names."""
    copied: list[str] = []
    refused: list[str] = []
    with zipfile.ZipFile(path) as archive:
        members = len(archive.infolist())
        wanted = []
        for cik in sorted(ciks):
            name = cik_file_name(cik)
            try:
                archive.getinfo(name)
            except KeyError:
                continue
            key = ObjectKey(Dataset.SUBMISSIONS, run.day, name)
            wanted.append(Expected(key, submissions_url(cik), cik=cik))
        run.ledger.ensure_expected(wanted)
        for expected in wanted:
            name = expected.key.name
            try:
                stored = run.lander.store(
                    expected,
                    archive.read(name),
                    check=json_opens,
                    detail={"archive": SUBMISSIONS_ARCHIVE_NAME},
                )
            except Exception as error:
                # Whatever stops one member, the other members and the record of the archive
                # go on: the request for 1.6 GB must not be lost to one document.
                log.warning("sec_archive_member_not_stored", member=name, error=repr(error))
                refused.append(name)
                continue
            (copied if stored is not None else refused).append(name)
    return members, copied, refused


def _give_up(run: SecRun, key: ObjectKey, attempts: int, problem: str) -> None:
    """After three failed tries of one day the archive of the day is quarantined."""
    if attempts >= MAX_ATTEMPTS:
        run.ledger.quarantine(
            key,
            from_state=ObjectState.EXPECTED,
            reason=f"{attempts} tries failed; the last one: {problem}",
            as_of=run.as_of,
        )


def _file_md5(path: Path) -> str:
    # The MD5 names the bytes of the archive in the manifest; no secret.
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as stream:
        while chunk := stream.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()
