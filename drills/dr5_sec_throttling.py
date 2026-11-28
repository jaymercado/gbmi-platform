"""Drill DR5: the SEC throttles the client (blueprint 9.8; week 3 plan WP-09).

Fault: the source answers one request with HTTP 429 and the header Retry-After, and a later
request with HTTP 403 and a body which is not the answer for a missing file. In a third part
the source refuses the probe after a block, too.

Recovery: none by hand for a block which ends. The run waits once for the circuit and sends
one probe; a second block in the same run ends the run, and the retry of the task goes on
where the run stopped. A refused probe locks the circuit, and an operator ends the lock with
``gbmi sec circuit close --note`` (runbook ``docs/runbooks/sec-circuit-open.md``).

The drill runs the filings pipeline on the recorded SEC files, with a clock which the drill
moves: a wait of 15 minutes takes no real time. No request leaves the machine.

Checked:

1. Run 1: the 429 opens the circuit, the client waits by the clock, sends one probe and goes
   on; the 403 opens the circuit a second time, and the run ends with SourceBlockedError.
2. Run 2, the retry of the task: the run waits until the end of the block, the probe passes,
   and the run completes.
3. No window of one second in the arrival list of the server holds more than 8 requests, and
   the server received no request while the circuit was open.
4. The circuit shows two openings and is closed; every object is in a final state; the tables
   hold no repeated key; the content checksums equal an undisturbed run.
5. A refused probe locks the circuit, a new run sends no request, and the command of the
   operator ends the lock.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx

from drills.common import (
    SEC_AS_OF,
    SEC_WATCHLIST,
    Drill,
    World,
    parse_json,
    sec_fixtures_dir,
    world,
)
from gbmi.lake.tables import SEC_FILING_DETAIL, SEC_FILING_INDEX
from gbmi.ledger.objects import open_object_ledger
from gbmi.ops.circuit import circuit_events, read_circuit
from gbmi.sources.sec.client import SOURCE, SecClient
from gbmi.sources.sec.errors import SourceBlockedError
from gbmi.sources.sec.filings import run_filings
from gbmi.sources.sec.silver import new_run
from gbmi.sources.sec.transport import FixtureHandler
from gbmi.sources.sec.urls import index_file_url, split, submissions_url

TABLES = (SEC_FILING_INDEX, SEC_FILING_DETAIL)
#: The request which the source throttles, and the request which the source refuses later.
THROTTLED = split(index_file_url(SEC_AS_OF.date() - timedelta(days=1)))[1]
REFUSED = split(submissions_url(SEC_WATCHLIST[2]))[1]
RATE_PAGE = b"<html><body>Request Rate Threshold Exceeded</body></html>"
BLOCK_PAGE = b"<html><body>Your Request Originates from an Undeclared Automated Tool</body></html>"
CEILING = 8


class Clock:
    """A clock which only the drill moves: a wait of the client takes no real time."""

    def __init__(self, start: datetime) -> None:
        self._start = start
        self.seconds = 0.0

    def monotonic(self) -> float:
        return 1_000.0 + self.seconds

    def now(self) -> datetime:
        return self._start + timedelta(seconds=self.seconds)

    def sleep(self, seconds: float) -> None:
        self.seconds += max(0.0, seconds)


@dataclass(frozen=True)
class Arrival:
    at: datetime
    monotonic: float
    path: str
    status: int


class ThrottlingSource:
    """The recorded SEC files as a source which refuses requests on demand."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._files = FixtureHandler(sec_fixtures_dir())
        #: Path -> the answers which come before the recorded file, in order.
        self.once: dict[str, list[Callable[[], httpx.Response]]] = {}
        #: When set, every request gets the answer.
        self.always: Callable[[], httpx.Response] | None = None
        self.arrivals: list[Arrival] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if self.always is not None:
            response = self.always()
        elif self.once.get(path):
            response = self.once[path].pop(0)()
        else:
            response = self._files(request)
        self.arrivals.append(
            Arrival(self._clock.now(), self._clock.monotonic(), path, response.status_code)
        )
        return response

    def most_in_one_second(self) -> int:
        times = sorted(arrival.monotonic for arrival in self.arrivals)
        return max(
            (sum(1 for other in times if start <= other < start + 1.0) for start in times),
            default=0,
        )


def too_many() -> httpx.Response:
    return httpx.Response(429, headers={"Retry-After": "120"}, content=RATE_PAGE)


def forbidden() -> httpx.Response:
    return httpx.Response(403, content=BLOCK_PAGE)


def run_once(stack: World, source: ThrottlingSource, clock: Clock, run_id: str) -> dict[str, Any]:
    """One run of the filings pipeline in the world, with the clock of the drill."""
    settings = stack.sec_settings()
    with (
        open_object_ledger(stack.dsn, run_id=run_id) as ledger,
        SecClient(
            settings,
            dsn=stack.dsn,
            run_id=run_id,
            transport=httpx.MockTransport(source),
            clock=clock.monotonic,
            sleep=clock.sleep,
            now=clock.now,
        ) as client,
    ):
        run = new_run(as_of=SEC_AS_OF, settings=settings, ledger=ledger, client=client)
        return run_filings(run, watchlist=SEC_WATCHLIST)


def blocked(stack: World, source: ThrottlingSource, clock: Clock, run_id: str) -> str | None:
    """Run once; the text of SourceBlockedError, or None when the run completed."""
    try:
        run_once(stack, source, clock, run_id)
    except SourceBlockedError as error:
        return str(error)
    return None


def open_objects(stack: World) -> list[str]:
    with open_object_ledger(stack.dsn) as ledger:
        return [f"{item.key.label}: {item.state.value}" for item in ledger.pending()]


def requests_while_open(stack: World, source: ThrottlingSource) -> list[str]:
    """The arrivals which lie inside a block: after the refusal and before the end of the block."""
    events = circuit_events(stack.ledger.connection, SOURCE, limit=50)
    blocks = [
        (event["at"], event["blocked_until"]) for event in events if event["event"] == "opened"
    ]
    return [
        f"{arrival.path} at {arrival.at.isoformat()}"
        for arrival in source.arrivals
        for opened, until in blocks
        if opened < arrival.at < until
    ]


def main() -> int:
    drill = Drill(
        "DR5", "the SEC throttles the client: wait, one probe, and a lock after a refusal"
    )
    facts: dict[str, Any] = {}

    drill.step("an undisturbed run in a world of its own gives the content to compare with")
    with world("dr5-plain") as plain:
        clock = Clock(SEC_AS_OF)
        calm = ThrottlingSource(clock)
        summary = run_once(plain, calm, clock, "drill-dr5-plain")
        expected = {name: plain.table_checksum(name) for name in TABLES}
        facts["undisturbed"] = {
            "requests": len(calm.arrivals),
            "index_rows": summary["index"]["rows"],
            "detail_rows": summary["detail"]["rows"],
        }
        drill.check(
            "the undisturbed run sends 7 requests", len(calm.arrivals) == 7, len(calm.arrivals)
        )

    with world("dr5") as stack:
        clock = Clock(SEC_AS_OF)
        source = ThrottlingSource(clock)
        source.once[THROTTLED] = [too_many]
        source.once[REFUSED] = [forbidden]

        drill.step("run 1: a 429 for the index file, later a 403 for a submissions document")
        error = blocked(stack, source, clock, "drill-dr5-run-1")
        index = [arrival for arrival in source.arrivals if arrival.path == THROTTLED]
        drill.check(
            "the index file was asked twice: the 429 and one probe which passed",
            [arrival.status for arrival in index] == [429, 200],
            [arrival.status for arrival in index],
        )
        waited = (index[1].at - index[0].at).total_seconds() if len(index) == 2 else None
        facts["wait_before_the_probe_seconds"] = waited
        drill.check(
            "the probe left 15 minutes after the 429, by the clock", waited == 900.0, waited
        )
        drill.check(
            "the run ended with SourceBlockedError at the second block",
            error is not None and "has waited for a block before" in error,
            error,
        )
        first_run = len(source.arrivals)
        circuit = read_circuit(stack.ledger.connection, SOURCE)
        drill.check(
            "the circuit is open and counts two openings",
            circuit.recorded and not circuit.locked and circuit.open_count == 2,
            (circuit.open_count, circuit.locked),
        )
        facts["open_after_run_1"] = open_objects(stack)

        drill.step("run 2, the retry of the task, starts 5 minutes later")
        clock.sleep(300)
        before = clock.now()
        error = blocked(stack, source, clock, "drill-dr5-run-2")
        drill.check("run 2 completed", error is None, error)
        drill.check(
            "run 2 waited for the rest of the block: 10 minutes by the clock",
            600.0 <= (clock.now() - before).total_seconds() < 660.0,
            (clock.now() - before).total_seconds(),
        )
        refused = [arrival.status for arrival in source.arrivals if arrival.path == REFUSED]
        drill.check(
            "the refused document was asked twice: the 403 and the probe",
            refused == [403, 200],
            refused,
        )
        facts["requests"] = {"run_1": first_run, "run_2": len(source.arrivals) - first_run}
        drill.check(
            "the two runs sent 9 requests: the 7 of an undisturbed run and the 2 refused ones",
            len(source.arrivals) == 9,
            len(source.arrivals),
        )

        drill.step("the evidence")
        most = source.most_in_one_second()
        facts["most_requests_in_one_second"] = most
        drill.check(
            f"no second of the arrival list holds more than {CEILING} requests",
            most <= CEILING,
            most,
        )
        inside = requests_while_open(stack, source)
        drill.check("the server received no request while the circuit was open", not inside, inside)
        circuit = read_circuit(stack.ledger.connection, SOURCE)
        drill.check(
            "the circuit shows two openings and is closed",
            circuit.open_count == 2 and not circuit.recorded and not circuit.locked,
            (circuit.open_count, circuit.recorded, circuit.locked),
        )
        events = [event["event"] for event in circuit_events(stack.ledger.connection, SOURCE)]
        facts["circuit_events_newest_first"] = events
        drill.check(
            "the history of the circuit: opened, closed, opened, closed",
            events == ["closed", "opened", "closed", "opened"],
            events,
        )
        still_open = open_objects(stack)
        drill.check("every object is in a final state", not still_open, still_open)
        for name in TABLES:
            drill.check(f"{name} holds no repeated key", stack.repeated_table_keys(name) == 0)
            found = stack.table_checksum(name)
            drill.check(
                f"{name} has the content of the undisturbed run",
                (found.rows, found.digest) == (expected[name].rows, expected[name].digest),
                (found.rows, found.digest),
            )

    drill.step("third part: the source refuses the probe, too")
    with world("dr5-lock") as stack:
        clock = Clock(SEC_AS_OF)
        source = ThrottlingSource(clock)
        source.always = too_many
        error = blocked(stack, source, clock, "drill-dr5-lock-1")
        circuit = read_circuit(stack.ledger.connection, SOURCE)
        drill.check(
            "the first request and the probe were refused: two requests",
            [arrival.status for arrival in source.arrivals] == [429, 429],
            [arrival.status for arrival in source.arrivals],
        )
        drill.check("the circuit is locked", circuit.locked, error)
        drill.check(
            "the error names the command of the operator",
            error is not None and "gbmi sec circuit close --note" in error,
            error,
        )

        source.always = None
        clock.sleep(24 * 3600)
        error = blocked(stack, source, clock, "drill-dr5-lock-2")
        drill.check(
            "a new run one day later sends no request and is refused",
            len(source.arrivals) == 2 and error is not None and "is locked" in error,
            (len(source.arrivals), error),
        )

        drill.step("the operator ends the lock")
        closed = stack.command(
            "sec",
            "circuit",
            "close",
            "--note",
            "drill DR5: the cause is known",
            "--decided-by",
            "drill",
        )
        answer = parse_json(closed.stdout) if closed.returncode == 0 else {}
        drill.check(
            "gbmi sec circuit close ends the lock",
            closed.returncode == 0
            and answer.get("closed") is True
            and answer.get("was_locked") is True,
            closed.stderr[-300:] or answer,
        )
        error = blocked(stack, source, clock, "drill-dr5-lock-3")
        drill.check("the next run completes", error is None, error)
        still_open = open_objects(stack)
        drill.check("every object is in a final state", not still_open, still_open)
        events = [event["event"] for event in circuit_events(stack.ledger.connection, SOURCE)]
        drill.check(
            "the history of the circuit: opened, locked, closed",
            events == ["closed", "locked", "opened"],
            events,
        )
    return drill.finish(facts)


if __name__ == "__main__":
    raise SystemExit(main())
