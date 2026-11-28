# ADR-015 — Fair access to the SEC: one client, a sliding window, a circuit and a request log

| | |
|---|---|
| **Date** | 2026-11-28 |
| **Status** | **Provisional**: the evidence of the laptop is complete; the request log of the AWS host follows with work package WP-13 |
| **Sources** | Blueprint §4.3 and §8.1; week 3 plan §3.3, differences D30, D31, D46 and D47, changes C14 and C22; findings F1 and F9 of the week 3 plan |
| **Related** | Drill DR5; the runbook `docs/runbooks/sec-circuit-open.md`; review 1 of the status report of phase P2; risk "the SEC blocks the address of the developer" |

## Context

The SEC publishes filings and company data on two hosts, `www.sec.gov` and `data.sec.gov`, without a sign-up and without a key. The SEC sets two conditions for automated access. A client declares who is asking: every request carries a user agent with a name and a contact address. A client sends at most 10 requests in each second. The SEC counts the requests of one user over every machine of the user, and it blocks a user who breaks a condition. A block arrives as the status 429 or 403 and can last from minutes to days.

The platform has one user, the developer, and the contact address in the user agent is the developer's own address. Three points follow.

- A mistake costs more than a failed run. A block stops every SEC pipeline, on the laptop and on the AWS host, and nobody on the side of the platform can lift it.
- The limit is easy to break without noticing. Three pipelines, a retry, a command of an operator beside a scheduled task, and two machines can each be correct alone and break the limit together.
- A block can look like a missing file. The storage behind `/Archives/` answers with the status 403 for a file which does not exist, and the SEC answers with the status 403 for a client which it refuses (finding F9).

Four points need a decision: where the limit is enforced; how the limit is counted; what the platform does after a refusal; and how the platform proves afterwards what it sent.

## Decision

**1. One client is the only way to the SEC.** Every request of the platform goes through the class `SecClient` (`src/gbmi/sources/sec/client.py`). No pipeline, test or script opens a connection of its own. The client cannot exceed the ceiling, cannot hide its identity and cannot ignore a block.

**2. One process at a time, on one machine at a time.** A client holds a PostgreSQL advisory lock for the source as long as the client lives. A second client on the same stack waits for the lock. Inside a process, one request runs at a time. Across machines there is no technical lock: exactly one place sends live requests. The stack decides the source of the client (`off`, `fixture` or `live`); neither the shell nor the command line of `make` changes it. Before the source of the AWS host is turned on, the working stack of the laptop gets the source `off` (change C22).

**3. At most 8 requests in any second, counted by a sliding window.** The SEC allows 10; the platform takes 8. Request n leaves at the earliest 1.050 seconds after request n − 8 **was answered**. The 50 milliseconds are the margin between the clock of the limiter and the clocks which write the evidence. Every attempt passes the limiter, a retry too, and the window covers both hosts together.

**4. A refusal opens a circuit, and the circuit lives in the database of the stack.** The table `ops.source_circuit` holds one row for the source, and every client reads the row before every request.

| Answer | What the client does |
|---|---|
| Status 429, or a status 403 which is not the answer for a missing file | The circuit opens for 15 minutes, or for the time of the header `Retry-After` when the header gives more; a block lasts at most seven days |
| Status 404 on `data.sec.gov`, or the status 403 with the body `AccessDenied` below `/Archives/` | The object is missing: no block and no retry |
| Three "missing" answers in a row for objects which a listing of the SEC names | The circuit opens: a block can look like a missing file |
| Status 500 or above, or an error of the transport | Up to five attempts with a growing pause |

A run waits for an open circuit once, when the wait is at most 16 minutes; a second block in the same run fails the run. The first request after a block is one probe. The object (status 200) closes the circuit. A refusal of the probe **locks** the circuit: no request leaves until an operator has read the situation and run `gbmi sec circuit close --note`. The append-only table `ops.circuit_event` keeps each opening, lock and close.

**5. The identity is declared and never stored.** The user agent comes from the settings as a secret value. A live client refuses a missing value, a value without an address and an address of a domain which is reserved for examples. The value stands in no log line, in no table and in no error message.

**6. Every attempt is recorded before it leaves.** The table `ops.source_request` gets one row for each attempt, written before the request is sent and completed with one statement when the answer has arrived: the host, the path, the time of the client, the time of the database, the identifier of the client session, the grant time on the monotonic clock of the process, the status, the bytes and the outcome. A trigger refuses a delete and a change of the fixed columns. The rule "no second holds more than 8 requests" is checked from the table, not from the code.

## Alternatives considered

| Alternative | Why it lost |
|---|---|
| A token bucket of 8 tokens a second | After a pause, a bucket allows 16 requests in one second: 8 saved tokens and 8 new ones. The rule of the SEC is about every window of one second |
| A sliding window over the moments of permission | Review 1 found the defect before the first live request. After the permission the client still writes a log row and may set up a connection, so request n + 8 could reach the SEC less than one second behind request n. The window now counts a request from the moment its answer has arrived |
| A fixed pause of 125 milliseconds between requests | Simple, and safe only while every request is fast. A slow answer followed by fast ones can still put 9 requests into one second at the SEC, and a retry path can forget the pause |
| The limit of the SEC itself, 10 requests a second | No margin for a clock which is off or for a second process. A full load of the watchlist took 81 requests, so the speed has no value |
| A limit in each pipeline, or in the scheduler (a pool with one slot) | A pool limits tasks, not requests, and a command of an operator does not pass the pool. One client with one lock covers every caller |
| A retry with a growing pause after a 429 or a 403 | A retry against a block is a further offence. The circuit stops every caller, and a second refusal needs a person |
| The state of a block in memory, or in a throwaway database of a test run | Review 1, finding B2: a block has to outlive the process, the run and the container. The circuit lives in the database of the stack, a client with a network connection refuses a throwaway database, and `make reset` of the working stack refuses while a block is recorded |
| Bulk files only (`submissions.zip`, `companyfacts.zip`) and no request for single objects | The archives cover the reference data and the facts of every company in one download each; the platform uses the first one. The daily form index and the filing details of the watchlist still need single requests, and each download is a request which has to follow the same rules |

## Consequences

- **A load is slow on purpose.** At most 8 requests a second, one process at a time. The live load of 2026-11-28 sent 81 requests in 8 minutes; one of them was the download of the archive of 1.58 GB, which took 106 seconds.
- **A block costs at least 15 minutes, and a second block costs a person.** A run which meets an open circuit waits once or fails; nothing works around a block. The runbook has the steps.
- **One place sends live requests.** Moving the SEC pipelines from the laptop to the host is two deliberate steps: the laptop off, the host on. No lock enforces the rule across machines.
- **A missing file and a block are told apart by the first bytes of a body**, which is a property of the storage of the SEC today (finding F9). The guard of three missing listed objects in a row covers a change of that property.
- **The request log grows by one row for each attempt** and is never cleaned by the platform. The two live runs of 2026-11-28 wrote 99 rows.
- **Tests never reach the SEC.** The source `fixture` answers from recorded files, and a client with a network connection refuses the identity of the recorded files.

## Evidence

| Claim | Evidence |
|---|---|
| No window of one second holds more than 8 requests | A property test of the limiter with made-up connection times; a client test with a slow first request; a real-clock test against a local server which records the arrival of each request. With defect B1 put back, five tests fail (review 1) |
| The rule holds for real requests | The request log of the working stack of the laptop after the two live runs of 2026-11-28: 99 requests (18 for the recorded files, 81 for the live load), at most 4 in any second, no status 429 or 403, and the circuit never opened |
| A block is respected | Drill DR5 (`make drill DR=5`): a source which throttles opens the circuit, the run waits once, a refused probe locks the circuit, an operator closes it; 26 checks, none failed, on the laptop and in the workflow `e2e` on every pull request |
| The identity is never stored | Three tests search every log line, the request log and the error text of a run record for the value; `make redact-check` searches the repository at each commit |
| The rule holds on the AWS host | **Open.** Exit-gate row 1 (b) of week 3: the request log of the host after the scheduled runs of the three SEC DAGs, read in work package WP-13 |

## Status

**Provisional.** The decision is in force since the first live request on 2026-11-28. The record becomes *accepted* when the request log of the AWS host shows the same result (work package WP-13).
