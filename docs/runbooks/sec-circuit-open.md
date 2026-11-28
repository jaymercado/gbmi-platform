# Runbook: the circuit of the SEC is open or locked

| | |
|---|---|
| **Pipelines** | PL-3, PL-4 and PL-5: the DAGs `sec_reference_daily`, `sec_filings` and `sec_xbrl_facts`, and the commands `gbmi sec reference`, `gbmi sec filings` and `gbmi sec facts` (blueprint §4.3 and §8.5 to §8.7) |
| **Written** | 2026-11-28, phase P2, work package WP-09 |
| **Proved by** | Drill DR5 (`make drill DR=5`) and the tests of `tests/ledger/test_sec_circuit.py` and `tests/ledger/test_sec_client.py` |

The SEC allows 10 requests in one second for one user. The platform sends at most 8. When the SEC refuses a request, the platform stops asking for a time. The words of the runbook:

- The **circuit** is one row of the table `ops.source_circuit` for the source `sec`. The row records a block: the reason and the time `blocked_until`. Every client reads the row before every request.
- The circuit is **open** while the time `blocked_until` lies in the future. No request leaves.
- The **probe** is the first request after the end of a block. A probe which gets the file closes the circuit.
- The circuit is **locked** when the SEC refused the probe. No client sends a request until a person closes the circuit by command.

Every command of the runbook runs in the repository folder. `make toolbox CMD="…"` runs a `gbmi` command inside the stack, and `make psql SQL="…"` runs one SQL statement on the operational database. Both commands act on the working stack; add `STACK=gbmi-demo` for the demo stack.

## The rule

**Never work around a block: no other address, no other user agent, no second machine.** A block is the answer of the SEC to the behaviour of the client. The only correct reactions are to wait and to find the cause. The user agent of the platform names a contact address of the owner; a request with another identity would hide the client from the SEC.

## Symptom

One of the following is true.

- A task of an SEC DAG, or a `gbmi sec` command, ended with `SourceBlockedError`. The text says "the circuit of sec is open until …" or "the circuit of sec is locked".
- A log line of level warning has the event `sec_request_refused`, `sec_circuit_opened` or `sec_circuit_wait`.
- A log line of level error has the event `sec_circuit_locked`.

## Detection

```bash
# The state of the circuit, and the newest events of its history.
make toolbox CMD="gbmi sec circuit status"

# The largest number of requests in any second, for a range of UTC days (exit-gate row 1).
make toolbox CMD="gbmi sec requests max-burst --from 2026-11-28 --to 2026-11-28"

# The refused requests, newest first.
make psql SQL="SELECT request_id, granted_at, host, path, attempt, probe, http_status, run_id FROM ops.source_request WHERE source = 'sec' AND outcome = 'blocked' ORDER BY request_id DESC LIMIT 20"

# The files of a range of days which are not in a final state.
make toolbox CMD="gbmi sec objects --from 2026-11-28 --to 2026-11-28"
```

`gbmi sec circuit status` prints one of four states.

| State | Meaning |
|---|---|
| `closed` | No block is recorded. Requests leave |
| `open` | A block is recorded, and `blocked_until` lies in the future. No request leaves |
| `probe` | A block is recorded, and `blocked_until` has passed. The next request is the probe |
| `locked` | The SEC refused a probe. No request leaves until the command `gbmi sec circuit close` |

## Diagnosis

Read the state, the column `reason` and the number `open_count`, then find the row of the table.

| State | What the status says | What happened | Way out |
|---|---|---|---|
| `open` or `probe` | `reason` is "HTTP 429" or "HTTP 403"; the run which met the block went on | The SEC refused one request. The run waited until `blocked_until` (15 minutes, or the time of the header `Retry-After` when the header gave more), sent the probe and went on | Recovery A: nothing to do |
| `open` | The task failed with "…and the run has waited for a block before" | The SEC refused a second request in the same run. A run waits once | Recovery A: the retry of the task, 20 minutes later, waits for the rest of the block and sends the probe |
| `open` | The task failed with "…the wait is longer than 16 minutes" | The header `Retry-After` named a long time | Recovery A, and read the time `blocked_until`: no run can succeed before the time |
| `open` | `reason` names missing objects | Three files in a row which a listing of the SEC names were answered with "no such object". A block can look like a missing file | Recovery A. When the block repeats on the next day: open one of the addresses in a web browser by hand and compare |
| `locked` | The newest event is `locked` | The SEC refused the probe: the cause of the block is still there | Recovery B: a person has to look |

Then read the result of `gbmi sec requests max-burst`.

- `max_requests_in_one_second` is 8 or less: the platform kept its ceiling. The block has another cause: a second client of the same user (see Prevention), or a limit which the SEC applied for its own reasons.
- `max_requests_in_one_second` is above 8: the limiter failed, or two clients ran at once. Pause the three SEC DAGs and treat the case as a defect of the platform before any further request.

## Recovery steps

### A. A block which ends: wait

Do nothing. The circuit ends the block by itself: the next run after `blocked_until` sends one probe, and a probe which gets the file closes the circuit. Every file which the stopped run did not load is still `expected` or `verified` in the object ledger, and the next run goes on with exactly the files.

Do not close an open circuit by command to save the wait. The wait is the reaction which the SEC asks for.

### B. A locked circuit: find the cause, then close by command

1. Pause the three SEC DAGs in the Airflow web page (the switch to the left of each DAG name), so that no retry sends a probe while the cause is unknown. A locked circuit already stops every client; the pause keeps the logs quiet.
2. Find the cause. Read the refused requests (Detection, third command) and the result of `max-burst`. Check the two settings of the stack: `make toolbox CMD="gbmi config show"` shows `sec_source` and a mask for `sec_user_agent`. Check that no second machine sends requests with the same identity: after the first deployment the AWS host is the only place with `sec_source` = `live`, and the working stack of the laptop has `off`.
3. Wait. The blueprint records the statement of the SEC: an address which sent too many requests may access the site again after the rate of requests has stayed below the limit for 10 minutes (blueprint §4.3, source [21]: the page "Privacy and security information" of the SEC, <https://www.sec.gov/about/privacy-information>). The wording was read for the blueprint and was not read again for the runbook: read the page in a web browser before you rely on the 10 minutes.
4. Close the circuit, with a note which says what was found:

   ```bash
   make toolbox CMD="gbmi sec circuit close --decided-by jay --note 'two clients ran at once on 2026-11-28; the laptop source is off now'"
   ```

5. Run one pipeline by hand and watch the first requests:

   ```bash
   make toolbox CMD="gbmi sec filings"
   ```

6. Unpause the three DAGs.

When the SEC refuses the first request after step 4 again: stop for the day. Leave the circuit as the client recorded the block, and tell the owner of the contact address in the same hour.

## Verification

```bash
# The state is closed, and the newest event is "closed" with the note.
make toolbox CMD="gbmi sec circuit status"

# The day has no file which waits: the list "open" is empty.
make toolbox CMD="gbmi sec objects --from 2026-11-28 --to 2026-11-28"

# No second holds more than 8 requests.
make toolbox CMD="gbmi sec requests max-burst --from 2026-11-28 --to 2026-11-28"
```

The history of the circuit stays in the table `ops.circuit_event`: one row for each opening, each lock and each close. No command deletes a row.

## Prevention

- **One client at a time.** Every SEC task uses the Airflow pool `sec_api` with one slot. Every client holds one lock in PostgreSQL for the source as long as the client lives, so a command of an operator waits for a running task.
- **The ceiling.** The limiter lets request n leave at the earliest 1.050 seconds after request n − 8, counted from the arrival of the answer. The setting `GBMI_SEC_MAX_RPS` refuses a value above 8.
- **One place with the live source.** The limit of the SEC counts for one user, not for one machine. The Makefile sets `sec_source` for each stack, and no command line changes the value.
- **The database remembers.** `make reset` refuses to delete the working database while a block is recorded.
- **The drill.** `make drill DR=5` runs in the workflow `e2e` on every pull request: a 429, a wait, a probe, a second block, a retry, a refused probe, a lock and the close by command.
