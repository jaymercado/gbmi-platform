"""Snapshot expiry that counts commits, not snapshots (blueprint 7.9; week 1 plan, D5 and D8).

The blueprint's rule is "expire snapshots older than 7 days, keeping at least the last 5".
PyIceberg 0.12 has no "keep the last N" and one of our commits creates up to two snapshots,
so the rule is implemented here by snapshot id:

* the newest ``keep_last_commits`` commits on the main branch are always kept, whole;
* a snapshot outside them is expired once it is older than ``older_than``;
* the head of a branch and a tagged snapshot are never expired.

Expiry removes snapshot metadata only. Data and manifest files that are no longer referenced
stay on storage; removing them is a separate clean-up (Athena VACUUM on AWS, blueprint 7.9).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyiceberg.table import Table
from pyiceberg.table.snapshots import Snapshot

from gbmi.lake.commit import COMMIT_ID_PROPERTY


@dataclass(frozen=True)
class Commit:
    """The snapshots one catalog commit created on the main branch, newest first."""

    commit_id: str | None
    snapshot_ids: tuple[int, ...]
    timestamp_ms: int


@dataclass(frozen=True)
class ExpiryResult:
    expired_snapshot_ids: tuple[int, ...]
    kept_commits: tuple[Commit, ...]
    remaining_snapshots: int


def commits_on_main(table: Table) -> list[Commit]:
    """Group the main branch's lineage into commits, newest first.

    Snapshots that share a ``gbmi.commit-id`` form one commit; a snapshot without one (written
    by another engine) is a commit of its own.
    """
    commits: list[Commit] = []
    snapshot = table.current_snapshot()
    while snapshot is not None:
        commit_id = _commit_id(snapshot)
        if commits and commit_id is not None and commits[-1].commit_id == commit_id:
            last = commits[-1]
            commits[-1] = Commit(
                commit_id, (*last.snapshot_ids, snapshot.snapshot_id), last.timestamp_ms
            )
        else:
            commits.append(Commit(commit_id, (snapshot.snapshot_id,), snapshot.timestamp_ms))
        parent = snapshot.parent_snapshot_id
        snapshot = table.snapshot_by_id(parent) if parent is not None else None
    return commits


def expire_snapshots(
    table: Table,
    *,
    older_than: timedelta = timedelta(days=7),
    keep_last_commits: int = 5,
    now: datetime | None = None,
) -> ExpiryResult:
    """Expire old snapshots while keeping the newest commits whole. Returns what was done."""
    if keep_last_commits < 1:
        raise ValueError("keep_last_commits must be at least 1")
    cutoff_ms = int(((now or datetime.now(UTC)) - older_than).timestamp() * 1000)

    kept = tuple(commits_on_main(table)[:keep_last_commits])
    keep_ids = {snapshot_id for commit in kept for snapshot_id in commit.snapshot_ids}
    protected = {ref.snapshot_id for ref in table.metadata.refs.values()}
    expire = [
        snapshot.snapshot_id
        for snapshot in table.metadata.snapshots
        if snapshot.snapshot_id not in keep_ids
        and snapshot.snapshot_id not in protected
        and snapshot.timestamp_ms < cutoff_ms
    ]
    if expire:
        table.maintenance.expire_snapshots().by_ids(expire).commit()
    return ExpiryResult(
        expired_snapshot_ids=tuple(expire),
        kept_commits=kept,
        remaining_snapshots=len(table.metadata.snapshots),
    )


def _commit_id(snapshot: Snapshot) -> str | None:
    if snapshot.summary is None:
        return None
    value = snapshot.summary[COMMIT_ID_PROPERTY]
    return str(value) if value is not None else None
