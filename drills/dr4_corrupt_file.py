"""Drill DR4: a corrupt file from the source (blueprint 9.8; week 2 plan WP-11).

Fault: the fetcher changes one byte of one zip file, on every download of the file.

Recovery: an operator sends the quarantined slot back to the download with
``gbmi ledger requeue``, and the next ingest run gets the good file.

Checked:

1. Gate G1 rejects the file three times; the other eleven files of the hour are verified.
2. The slot is quarantined, the bad bytes lie in the quarantine area, bronze holds no file of
   the slot, and ``ops.dq_result`` holds the three failures.
3. After the requeue and one run with the good file the slot is verified, and bronze holds
   the bytes with the MD5 of the list.
4. A build of the hour loads the slot. The recorded history of the slot shows every step.
"""

from __future__ import annotations

import dataclasses
import hashlib

from drills.common import DAY, HOUR, Drill, World, fixtures_dir, parse_json, world
from gbmi.core.gdelt import Feed, Slot
from gbmi.ledger.repository import SlotKey, SlotRecord
from gbmi.ledger.states import State
from gbmi.sources.gdelt.index import IndexLine, IndexTail
from gbmi.sources.gdelt.land import Fetched, Fetcher, FixtureFetcher

#: The file which arrives damaged.
DAMAGED = SlotKey(Feed.EXPORT, Slot.parse("20261125083000"))


def md5(content: bytes) -> str:
    return hashlib.md5(content, usedforsecurity=False).hexdigest()


class OneByteChanged:
    """A fetcher which hands out one file with one changed byte."""

    def __init__(self, inner: Fetcher, file_name: str) -> None:
        self.inner = inner
        self.file_name = file_name
        self.served: list[str] = []

    def last_update(self) -> str:
        return self.inner.last_update()

    def master_tail(self, tail_bytes: int) -> IndexTail:
        return self.inner.master_tail(tail_bytes)

    def fetch(self, line: IndexLine) -> Fetched:
        fetched = self.inner.fetch(line)
        if line.file_name != self.file_name:
            return fetched
        content = bytearray(fetched.content)
        content[len(content) // 2] ^= 0xFF
        self.served.append(md5(bytes(content)))
        return dataclasses.replace(fetched, content=bytes(content))


def damaged_slot(stack: World) -> SlotRecord:
    record = stack.ledger.get(DAMAGED)
    if record is None:
        raise RuntimeError(f"the ledger has no row for the slot {DAMAGED.label}")
    return record


def main() -> int:
    drill = Drill("DR4", "a corrupt file is quarantined, and a requeue lands the good file")
    file_name = DAMAGED.slot.file_name(DAMAGED.feed)
    bronze_key = DAMAGED.slot.bronze_key(DAMAGED.feed)
    quarantine_key = f"quarantine/gdelt/export/dt={DAY.isoformat()}/{file_name}"
    facts: dict[str, object] = {"file": file_name}
    with world("dr4") as stack:
        drill.step("the source serves one file with one changed byte")
        fetcher = OneByteChanged(FixtureFetcher(fixtures_dir()), file_name)
        first = stack.land(fetcher)
        record = damaged_slot(stack)
        facts["first_run"] = {name: first[name] for name in ("verified", "rejected", "quarantined")}
        drill.check(
            "gate G1 rejected the file three times", len(fetcher.served) == 3, len(fetcher.served)
        )
        drill.check(
            "the run verified the other eleven files",
            (first["verified"], first["rejected"], first["quarantined"]) == (11, 3, 1),
            facts["first_run"],
        )
        drill.check(
            "the slot is quarantined after three attempts",
            (record.state, record.attempts) == (State.QUARANTINED, 3),
            (record.state.value, record.attempts),
        )
        kept = stack.lake_path / quarantine_key
        drill.check(
            "the bad bytes lie in the quarantine area",
            kept.is_file() and md5(kept.read_bytes()) == fetcher.served[-1] != record.expected_md5,
            quarantine_key,
        )
        drill.check("bronze holds no file of the slot", not (stack.lake_path / bronze_key).exists())
        failures = stack.ledger.connection.execute(
            "SELECT count(*) FROM ops.dq_result WHERE source = %s AND gate = 'G1' "
            "AND status = 'failed' AND unit = %s",
            (stack.ledger.source, DAMAGED.slot.stamp),
        ).fetchone()
        drill.check("ops.dq_result holds the three failures of gate G1", failures == (3,), failures)

        drill.step("the operator sends the slot back to the download")
        requeue = stack.command(
            "ledger", "requeue", "--from", DAY.isoformat(), "--to", DAY.isoformat(),
            "--feed", "export", "--decided-by", "drill",
            "--note", "drill DR4: the source serves the good file again",
        )  # fmt: skip
        drill.check(
            "gbmi ledger requeue ended with exit code 0",
            requeue.returncode == 0,
            requeue.stderr[-400:],
        )
        facts["requeue"] = parse_json(requeue.stdout) if requeue.returncode == 0 else None
        record = damaged_slot(stack)
        drill.check("the slot is listed again", record.state is State.LISTED, record.state.value)

        drill.step("one ingest run with the good file")
        second = stack.land()
        record = damaged_slot(stack)
        drill.check(
            "the run downloaded and verified one file",
            (second["selected"], second["verified"], second["written"]) == (1, 1, 1),
            {name: second[name] for name in ("selected", "verified", "written")},
        )
        drill.check("the slot is verified", record.state is State.VERIFIED, record.state.value)
        landed = stack.lake_path / bronze_key
        drill.check(
            "bronze holds the bytes with the MD5 of the list",
            landed.is_file() and md5(landed.read_bytes()) == record.expected_md5,
        )

        drill.step("the hour is built")
        built = stack.build(Feed.EXPORT, HOUR)
        record = damaged_slot(stack)
        drill.check(
            "the build loaded the four slots of the hour", built["loaded"] == 4, built["loaded"]
        )
        drill.check("the slot is loaded", record.state is State.LOADED, record.state.value)
        drill.check(
            "silver holds the 4,023 events of the hour",
            stack.checksum("gdelt_event", HOUR).rows == 4023,
        )
        history = [
            f"{before.value} -> {after.value}"
            for before, after in stack.ledger.transitions(DAMAGED)
        ]
        facts["history"] = history
        drill.check(
            "the history of the slot ends: quarantined, listed, downloaded, verified, loaded",
            history[-5:]
            == [
                "listed -> quarantined",
                "quarantined -> listed",
                "listed -> downloaded",
                "downloaded -> verified",
                "verified -> loaded",
            ],
            history[-5:],
        )
    return drill.finish(facts)


if __name__ == "__main__":
    raise SystemExit(main())
