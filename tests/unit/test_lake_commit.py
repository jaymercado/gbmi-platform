"""The retry around a commit, and the hash of a file set (week 2 plan WP-06)."""

from __future__ import annotations

import hashlib
from typing import Any, cast

import pytest
from pyiceberg.exceptions import CommitFailedException, ValidationException

from gbmi.lake.commit import COMMIT_RETRIES, commit_with_retry
from gbmi.ledger.repository import fileset_hash


class Attempts:
    """A commit which fails a number of times with the given error and then succeeds."""

    def __init__(self, failures: int, error: Exception) -> None:
        self.failures = failures
        self.error = error
        self.loaded: list[object] = []
        self.committed_on: list[object] = []

    def load(self) -> Any:
        self.loaded.append(object())
        return self.loaded[-1]

    def commit(self, table: Any) -> Any:
        self.committed_on.append(table)
        if len(self.committed_on) <= self.failures:
            raise self.error
        return "committed"


@pytest.mark.parametrize("error", [CommitFailedException("lost"), ValidationException("same")])
def test_a_commit_conflict_is_tried_again_on_a_freshly_loaded_table(error: Exception) -> None:
    attempts = Attempts(failures=2, error=error)

    result = commit_with_retry(attempts.load, attempts.commit)

    assert cast(str, result) == "committed"
    # Each attempt got a table of its own from the catalog.
    assert attempts.committed_on == attempts.loaded
    assert len(attempts.loaded) == 3


def test_a_commit_is_tried_again_three_times_and_then_the_conflict_is_raised() -> None:
    lands_at_last = Attempts(failures=COMMIT_RETRIES, error=CommitFailedException("lost"))
    never_lands = Attempts(failures=COMMIT_RETRIES + 1, error=CommitFailedException("lost"))

    assert cast(str, commit_with_retry(lands_at_last.load, lands_at_last.commit)) == "committed"
    with pytest.raises(CommitFailedException, match="lost"):
        commit_with_retry(never_lands.load, never_lands.commit)

    # One attempt and three more (blueprint 8.4).
    assert len(lands_at_last.committed_on) == len(never_lands.committed_on) == 4
    assert COMMIT_RETRIES == 3


def test_another_error_is_not_tried_again() -> None:
    attempts = Attempts(failures=1, error=RuntimeError("the disk is full"))

    with pytest.raises(RuntimeError, match="disk"):
        commit_with_retry(attempts.load, attempts.commit)

    assert len(attempts.committed_on) == 1


def test_the_hash_of_a_file_set_is_the_sha256_of_its_lines_in_slot_order() -> None:
    files = [("20261125081500", "b" * 32), ("20261125080000", "a" * 32)]
    text = f"20261125080000:{'a' * 32}\n20261125081500:{'b' * 32}\n"

    assert fileset_hash(files) == hashlib.sha256(text.encode("ascii")).hexdigest()
    assert fileset_hash(reversed(files)) == fileset_hash(files)


def test_another_file_or_another_md5_gives_another_hash() -> None:
    files = [("20261125080000", "a" * 32), ("20261125081500", "b" * 32)]

    assert fileset_hash(files[:1]) != fileset_hash(files)
    assert fileset_hash([files[0], ("20261125081500", "c" * 32)]) != fileset_hash(files)
    assert fileset_hash([]) == hashlib.sha256(b"").hexdigest()
