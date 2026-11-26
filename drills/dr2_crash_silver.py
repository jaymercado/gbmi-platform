"""Drill DR2, silver part: a crash during the silver commit (blueprint 9.8; week 2 plan WP-11).

Fault: the process which builds an hour is killed with SIGKILL, once before the Iceberg commit
and once between the Iceberg commit and the ledger update.

Recovery: the next run builds the same unit again (blueprint 8.4, failure recovery).

Checked for each of the three feeds:

1. Killed before the commit: nothing is visible. The table has no snapshot, the ledger has no
   partition version, and the slots are still verified.
2. Killed after the commit: silver holds the new rows, and the slots are still verified.
3. The rerun ends with loaded slots, a content checksum equal to an undisturbed build, and no
   repeated key.
4. A rebuild which is killed before the commit leaves the table as it is: the same snapshot and
   the same content checksum.

The mart half of DR2 is work of phase P3.
"""

from __future__ import annotations

import signal

from drills.common import AS_OF, HOUR, Drill, digests, world
from gbmi.core.failpoint import SILVER_AFTER_COMMIT, SILVER_BEFORE_COMMIT
from gbmi.core.gdelt import Feed
from gbmi.lake.tables import FEED_TABLES

KILLED = -signal.SIGKILL


def main() -> int:
    drill = Drill("DR2", "a crash during the silver commit is repaired by the next run")
    facts: dict[str, object] = {}
    with world("dr2-undisturbed") as undisturbed, world("dr2-crashed") as crashed:
        drill.step("an undisturbed build of the fixture hour, as the reference")
        undisturbed.land()
        expected = {}
        for feed in Feed:
            expected.update(digests(undisturbed.build(feed, HOUR), FEED_TABLES[feed]))
        facts["undisturbed"] = expected

        crashed.land()
        for feed in Feed:
            tables = FEED_TABLES[feed]
            build = (
                "silver", "build-hour", "--hour", HOUR.label, "--feed", feed.value,
                "--as-of", AS_OF.isoformat(),
            )  # fmt: skip

            drill.step(f"{feed.value}: the build is killed before the Iceberg commit")
            killed = crashed.command(*build, failpoint=SILVER_BEFORE_COMMIT)
            drill.check(
                "the process was killed with SIGKILL",
                killed.returncode == KILLED,
                killed.returncode,
            )
            for name in tables:
                drill.check(
                    f"{name} has no snapshot and no partition version",
                    crashed.snapshot_id(name) is None and crashed.versions(name) == [],
                )
            drill.check(
                "the slots are still verified",
                crashed.states(feed) == {"verified": 4},
                crashed.states(feed),
            )

            drill.step(
                f"{feed.value}: the build is killed between the commit and the ledger update"
            )
            killed = crashed.command(*build, failpoint=SILVER_AFTER_COMMIT)
            drill.check(
                "the process was killed with SIGKILL",
                killed.returncode == KILLED,
                killed.returncode,
            )
            for name in tables:
                held = crashed.checksum(name, HOUR)
                drill.check(
                    f"{name} holds the new rows",
                    {"rows": held.rows, "digest": held.digest} == expected[name],
                    str(held),
                )
                drill.check(f"{name} has no partition version yet", crashed.versions(name) == [])
            drill.check(
                "the slots are still verified",
                crashed.states(feed) == {"verified": 4},
                crashed.states(feed),
            )

            drill.step(f"{feed.value}: the next run builds the same unit again")
            rerun = crashed.command(*build)
            drill.check(
                "the rerun ended with exit code 0", rerun.returncode == 0, rerun.stderr[-400:]
            )
            drill.check(
                "the slots are loaded", crashed.states(feed) == {"loaded": 4}, crashed.states(feed)
            )
            for name in tables:
                held = crashed.checksum(name, HOUR)
                drill.check(
                    f"{name} has the checksum of the undisturbed build",
                    {"rows": held.rows, "digest": held.digest} == expected[name],
                    str(held),
                )
                drill.check(f"{name} has no repeated key", crashed.repeated_keys(name, HOUR) == 0)
                recorded = crashed.versions(name)
                drill.check(
                    f"{name} has one partition version with the same checksum",
                    [version["content_digest"] for version in recorded]
                    == [expected[name]["digest"]],
                    recorded,
                )

            drill.step(f"{feed.value}: a rebuild is killed before the commit")
            before = {
                name: (crashed.snapshot_id(name), crashed.checksum(name, HOUR)) for name in tables
            }
            killed = crashed.command(*build, failpoint=SILVER_BEFORE_COMMIT)
            drill.check(
                "the process was killed with SIGKILL",
                killed.returncode == KILLED,
                killed.returncode,
            )
            for name in tables:
                drill.check(
                    f"{name} keeps the snapshot and the content checksum",
                    (crashed.snapshot_id(name), crashed.checksum(name, HOUR)) == before[name],
                )
            drill.check(
                "the slots are still loaded",
                crashed.states(feed) == {"loaded": 4},
                crashed.states(feed),
            )
    return drill.finish(facts)


if __name__ == "__main__":
    raise SystemExit(main())
