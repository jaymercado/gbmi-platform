"""Record the SEC test files (week 3 plan 3.3 and WP-04, with the changes C13 and C22).

Three steps:

    fetch     Sends 18 requests to the live SEC through the fair-access client and stores the
              raw answers under data/sec-raw/, which Git ignores. The step runs in the toolbox
              of the working stack: only the working stack has the source "live", and the
              database of the working stack remembers a block by the SEC (change C22). The
              rows of the run in the request log are saved beside the raw answers. The same
              command continues a run which stopped early: a stored answer is not asked again.
    shorten   Reads the raw answers and writes the shortened test files to tests/fixtures/sec/.
              Needs no network and no database: a change of the shortening costs no further
              request.
    rehearse  Runs the step fetch against a made-up SEC inside the process, with a throwaway
              database and a temporary folder. No request leaves the machine.

Usage:

    RECORD="python -m benchmarks.source_profile.record_sec_fixtures"
    make toolbox STACK=gbmi-demo CMD="$RECORD rehearse"
    make toolbox CMD="$RECORD fetch"
    uv run $RECORD shorten

The 18 requests: the ticker file; the submissions documents of eight companies; the facts
documents of the five demo companies; the listing of one quarter; one daily index file; and
two requests whose answer is "no such object" (findings F8 and F9).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import tempfile
import zipfile
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from gbmi.sources.sec.urls import (
    DATA_HOST,
    SUBMISSIONS_ARCHIVE_URL,
    TICKER_FILE_URL,
    cik_file_name,
    companyfacts_url,
    index_file_name,
    index_file_url,
    index_listing_url,
    quarter_of,
    split,
    submissions_url,
)

if TYPE_CHECKING:
    import httpx

REPO = Path(__file__).resolve().parents[2]
RAW = REPO / "data" / "sec-raw"
FIXTURES = REPO / "tests" / "fixtures" / "sec"
SEED = REPO / "dbt" / "seeds" / "metric_concept_map.csv"
MANIFEST = "manifest.json"
REQUEST_LOG = "requests.json"
MISSING_SUFFIX = ".missing"
RUN_PREFIX = "record-sec-fixtures"
REQUESTS = 18

#: The five demo companies (week 3 plan 3.3): submissions and facts.
DEMO = {
    1326801: "Meta Platforms",
    1045810: "Nvidia",
    789019: "Microsoft",
    63276: "Mattel",
    1075124: "Thomson Reuters",
}
#: Three more universe companies with a submissions document. easyJet has no facts document.
MORE = {1470419: "easyJet", 1652044: "Alphabet", 46080: "Hasbro"}
#: Two companies which stay in the shortened ticker file without any document.
BARE = {1018724: "Amazon", 1065280: "Netflix"}
NO_FACTS = 1470419
INDEX_DAY = date(2026, 11, 25)
#: A US holiday: the SEC has no index file for the day (finding F9).
HOLIDAY = date(2026, 11, 26)
#: Forms of which a shortened submissions document keeps every filing of the last two years.
KEPT_FORMS = {"10-K", "10-Q", "40-F", "8-K"}
NEWEST_FILINGS = 40
MORE_CONCEPTS = 5
INDEX_ROWS = 300
CURRENCIES = {"USD", "CAD", "EUR", "GBP"}
#: One row of an index file, read from the right end (finding F11).
_ROW = re.compile(
    r"^(?P<head>.*?)\s+(?P<cik>\d{1,10})\s+(?P<date>\d{8})\s+"
    r"(?P<file>edgar/data/\d+/(?P<accession>\d{10}-\d{2}-\d{6})\.txt)\s*$"
)
_REQUEST_IDS = re.compile(rb"(<(?:RequestId|HostId)>)([^<]*)(</)")


def plan() -> list[tuple[str, bool, bool]]:
    """The 18 requests: address, whether a listing names the object, whether it is missing."""
    year, quarter = quarter_of(INDEX_DAY)
    documented = [*DEMO, *MORE]
    return [
        (TICKER_FILE_URL, True, False),
        *((submissions_url(cik), True, False) for cik in documented),
        *((companyfacts_url(cik), False, False) for cik in DEMO),
        (index_listing_url(year, quarter), True, False),
        (index_file_url(INDEX_DAY), True, False),
        (companyfacts_url(NO_FACTS), False, True),
        (index_file_url(HOLIDAY), False, True),
    ]


def raw_path(url: str, raw: Path = RAW) -> Path:
    host, path = split(url)
    return raw / host / path.lstrip("/")


def md5(content: bytes) -> str:
    return hashlib.md5(content, usedforsecurity=False).hexdigest()


def read_state(raw: Path = RAW) -> dict[str, Any]:
    """The manifest of a recording: the run, the answers so far and whether the run is whole."""
    path = raw / MANIFEST
    if not path.exists():
        return {}
    state: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return state


def write_state(state: dict[str, Any], raw: Path = RAW) -> None:
    """Replace the manifest in one step, so a killed run leaves a whole file."""
    raw.mkdir(parents=True, exist_ok=True)
    part = raw / (MANIFEST + ".part")
    part.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    part.replace(raw / MANIFEST)


def surprises(state: dict[str, Any]) -> list[str]:
    """What differs from the plan: an answer which is missing, or of the wrong kind."""
    answered = {entry["url"]: entry for entry in state.get("entries", [])}
    found = []
    for url, _, expect_missing in plan():
        if url not in answered:
            found.append(f"no answer is recorded for {url}")
        elif answered[url]["missing"] != expect_missing:
            kind = "an object" if expect_missing else "no object"
            found.append(f"the SEC has {kind} at {url}: the plan expected the opposite")
    return found


# --- fetch --------------------------------------------------------------------------------------


def fetch(
    *,
    again: bool,
    raw: Path = RAW,
    dsn: str | None = None,
    source: str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> int:
    """Send the requests of the plan which have no stored answer yet.

    Without the last three arguments the client is the live client of the stack: the database
    of the stack, the source "sec" and a network connection.
    """
    from gbmi.config import get_settings
    from gbmi.sources.sec.client import SOURCE, SecClient
    from gbmi.sources.sec.errors import ObjectMissingError

    requests = plan()
    if len(requests) != REQUESTS:
        raise RuntimeError(f"the plan holds {len(requests)} requests, not {REQUESTS}")
    state = read_state(raw)
    if state.get("complete") and not again:
        print(f"{raw / MANIFEST} is complete: the raw answers are recorded. Nothing was requested.")
        print(f"A new recording sends {REQUESTS} more requests to the SEC: pass --again to do so.")
        return 2
    if again or not state:
        run_id = f"{RUN_PREFIX}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
        state = {"run_id": run_id, "complete": False, "entries": []}
    done = {entry["url"] for entry in state["entries"]}
    todo = [request for request in requests if request[0] not in done]
    print(f"run {state['run_id']}: {len(todo)} of {len(requests)} requests to send")
    source = source or SOURCE
    with SecClient(
        get_settings(), dsn=dsn, run_id=state["run_id"], source=source, transport=transport
    ) as client:
        for url, listed, expect_missing in todo:
            target = raw_path(url, raw)
            target.parent.mkdir(parents=True, exist_ok=True)
            modified = None
            try:
                response = client.get(url, listed=listed)
            except ObjectMissingError as error:
                missing, status, content = True, error.status, bytes(error.body)
                target = target.with_name(target.name + MISSING_SUFFIX)
            else:
                missing, status, content = False, response.status, response.content
                modified = response.last_modified
            target.write_bytes(content)
            state["entries"].append(
                {
                    "url": url,
                    "status": status,
                    "missing": missing,
                    "expected_missing": expect_missing,
                    "bytes": len(content),
                    "md5": md5(content),
                    "last_modified": modified.isoformat() if modified else None,
                }
            )
            write_state(state, raw)
            kind = "missing" if missing else "object"
            note = "" if missing == expect_missing else "   NOT AS PLANNED"
            print(f"{kind:8} {status}  {len(content):>9}  {url}{note}")
    state["complete"] = True
    write_state(state, raw)
    evidence = request_evidence(state["run_id"], dsn=dsn, source=source)
    (raw / REQUEST_LOG).write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in evidence.items() if key != "rows"}, indent=2))
    found = surprises(state)
    for line in found:
        print(line, file=sys.stderr)
    return 1 if found else 0


def request_evidence(run_id: str, *, dsn: str | None, source: str) -> dict[str, Any]:
    """The rows of a run in the request log, and the largest number of requests in one second."""
    from gbmi.ledger.db import connect
    from gbmi.ops.requests import max_requests_in_session_window, max_requests_in_window

    names = (
        "host",
        "path",
        "method",
        "attempt",
        "probe",
        "outcome",
        "http_status",
        "granted_at",
        "waited_ms",
        "duration_ms",
        "byte_count",
    )
    with connect(dsn, autocommit=True) as connection:
        found = connection.execute(
            "SELECT host, path, method, attempt, probe, outcome, http_status, granted_at, "
            "waited_ms, duration_ms, byte_count FROM ops.source_request "
            "WHERE source = %s AND run_id = %s ORDER BY request_id",
            (source, run_id),
        ).fetchall()
        if not found:
            raise RuntimeError(f"the request log holds no row of the run {run_id}")
        rows = [dict(zip(names, row, strict=True)) for row in found]
        first, last = rows[0]["granted_at"], rows[-1]["granted_at"]
        # The last request of the run is counted: a window does not count its end.
        end = last + timedelta(seconds=1)
        whole = connection.execute(
            "SELECT count(*) FROM ops.source_request WHERE source = %s", (source,)
        ).fetchone()
        return {
            "run_id": run_id,
            "requests": len(rows),
            "outcomes": dict(sorted(Counter(row["outcome"] or "open" for row in rows).items())),
            "max_requests_in_one_second": max_requests_in_window(
                connection, source=source, start=first, end=end
            ),
            "max_requests_in_one_second_monotonic": max_requests_in_session_window(
                connection, source=source, start=first, end=end
            ),
            "requests_of_the_source_in_the_database": whole[0] if whole else None,
            "max_requests_in_one_second_of_the_whole_log": max_requests_in_window(
                connection, source=source
            ),
            "first_request_at": first.isoformat(),
            "last_request_at": last.isoformat(),
            "rows": [{**row, "granted_at": row["granted_at"].isoformat()} for row in rows],
        }


def rehearse() -> int:
    """The step fetch against a made-up SEC inside the process: no request leaves the machine."""
    import httpx

    from gbmi.ledger.scratch import scratch_database

    missing = {url for url, _, expect_missing in plan() if expect_missing}

    def answer(request: httpx.Request) -> httpx.Response:
        if str(request.url) not in missing:
            return httpx.Response(200, content=b'{"made-up": true}')
        if request.url.host == DATA_HOST:
            return httpx.Response(404, content=b"<Error><Code>NoSuchKey</Code></Error>")
        return httpx.Response(403, content=b"<Error><Code>AccessDenied</Code></Error>")

    with tempfile.TemporaryDirectory() as folder, scratch_database() as dsn:
        code = fetch(
            again=False,
            raw=Path(folder),
            dsn=dsn,
            source="sec-rehearsal",
            transport=httpx.MockTransport(answer),
        )
        stored = sorted(
            str(path.relative_to(folder)) for path in Path(folder).rglob("*") if path.is_file()
        )
    print(f"rehearsal: {len(stored)} files were stored and removed again; exit code {code}")
    return code


# --- shorten ------------------------------------------------------------------------------------


def load(url: str) -> Any:
    return json.loads(raw_path(url).read_bytes())


def compact(document: Any) -> bytes:
    return json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def shorten_tickers() -> bytes:
    document = load(TICKER_FILE_URL)
    wanted = {*DEMO, *MORE, *BARE}
    position = document["fields"].index("cik")
    document["data"] = [row for row in document["data"] if row[position] in wanted]
    found = {row[position] for row in document["data"]}
    if found != wanted:
        raise RuntimeError(f"the ticker file lacks the CIKs {sorted(wanted - found)}")
    return compact(document)


def shorten_submissions(cik: int, newest_day: date, named: set[str]) -> bytes:
    """The newest filings, the periodic reports of two years and the filings of the index file."""
    document = load(submissions_url(cik))
    recent = document["filings"]["recent"]
    count = len(recent["accessionNumber"])
    since = (newest_day - timedelta(days=730)).isoformat()
    keep = [
        index
        for index in range(count)
        if index < NEWEST_FILINGS
        or (recent["form"][index] in KEPT_FORMS and recent["filingDate"][index] >= since)
        or recent["accessionNumber"][index] in named
    ]
    for name, values in recent.items():
        if len(values) != count:
            raise RuntimeError(f"CIK {cik}: the list {name} has {len(values)} of {count} values")
        recent[name] = [values[index] for index in keep]
    return compact(document)


def seed_concepts() -> set[tuple[str, str]]:
    with SEED.open(encoding="utf-8", newline="") as stream:
        return {(row["taxonomy"], row["concept"]) for row in csv.DictReader(stream)}


def shorten_facts(cik: int, seed: set[tuple[str, str]]) -> tuple[bytes, list[str]]:
    """The concepts of the metric seed, and five more concepts which keep the special cases."""
    document = load(companyfacts_url(cik))
    sizes: Counter[tuple[str, str]] = Counter()
    no_period: Counter[tuple[str, str]] = Counter()
    other_unit: Counter[tuple[str, str]] = Counter()
    for taxonomy, concepts in document["facts"].items():
        for concept, body in concepts.items():
            key = (taxonomy, concept)
            for unit, facts in body["units"].items():
                sizes[key] += len(facts)
                no_period[key] += sum(1 for fact in facts if fact.get("fp") is None)
                if unit not in CURRENCIES:
                    other_unit[key] += len(facts)
    kept = {key for key in sizes if key in seed}
    more: list[tuple[str, str]] = []

    def take(ranking: Counter[tuple[str, str]], count: int) -> None:
        for key, size in ranking.most_common():
            if count == 0 or len(more) >= MORE_CONCEPTS:
                return
            if size and key not in kept and key not in more:
                more.append(key)
                count -= 1

    # One concept with facts whose fiscal period is null, one concept with a unit which is no
    # currency, then the largest concepts.
    take(no_period, 1)
    take(other_unit, 1)
    take(sizes, MORE_CONCEPTS)
    wanted = kept | set(more)
    document["facts"] = {
        taxonomy: {name: body for name, body in concepts.items() if (taxonomy, name) in wanted}
        for taxonomy, concepts in document["facts"].items()
        if any((taxonomy, name) in wanted for name in concepts)
    }
    return compact(document), sorted(f"{taxonomy}:{concept}" for taxonomy, concept in more)


def shorten_listing() -> bytes:
    """The entries of the index day: the form index and the other files of the day."""
    year, quarter = quarter_of(INDEX_DAY)
    document = load(index_listing_url(year, quarter))
    day = f".{INDEX_DAY:%Y%m%d}."
    items = [item for item in document["directory"]["item"] if day in item["name"]]
    name = index_file_name(INDEX_DAY)
    if [item["name"] for item in items].count(name) != 1:
        raise RuntimeError(f"the listing does not name {name} exactly once")
    document["directory"]["item"] = items
    return compact(document)


def without_request_ids(body: bytes) -> bytes:
    """The body of a "no such object" answer without the two identifiers of the one request.

    The storage service of the SEC writes a request identifier and a host identifier into
    the body. The two values name one request of the recording and nothing else, so the
    test files hold the letter X in their place, in the same length.
    """
    return _REQUEST_IDS.sub(lambda found: found[1] + b"X" * len(found[2]) + found[3], body)


def index_lines() -> tuple[list[str], list[tuple[int, str, re.Match[str] | None]]]:
    """The header of the raw index file, and each later line with its number and its match.

    The file is cut at line feeds only, so every other byte of a line stays as recorded.
    """
    lines = raw_path(index_file_url(INDEX_DAY)).read_text(encoding="latin-1").split("\n")
    dashes = next(number for number, line in enumerate(lines) if line.startswith("-----"))
    body = lines[dashes + 1 :]
    return lines[: dashes + 1], [
        (number, line, _ROW.match(line)) for number, line in enumerate(body)
    ]


def index_accessions() -> dict[int, set[str]]:
    """The accession numbers which the raw index file names for each CIK."""
    found: dict[int, set[str]] = {}
    for _, _, match in index_lines()[1]:
        if match is not None:
            found.setdefault(int(match["cik"]), set()).add(match["accession"])
    return found


def shorten_index() -> bytes:
    """About 300 rows of the index file which keep the cases of finding F11."""
    header, parsed = index_lines()
    rows = [(number, line, match) for number, line, match in parsed if match is not None]
    filers = Counter(match["accession"] for _, _, match in rows)
    pairs = Counter((match["accession"], match["cik"]) for _, _, match in rows)
    companies = {str(cik) for cik in (*DEMO, *MORE, *BARE)}
    day = f"{INDEX_DAY:%Y%m%d}"
    keep: set[int] = set()
    # The rows of the ten companies of the ticker file.
    keep |= {number for number, _, match in rows if match["cik"] in companies}
    # Three filings with several filers.
    several = [accession for accession, count in filers.items() if 2 <= count <= 10][:3]
    keep |= {number for number, _, match in rows if match["accession"] in several}
    # Every row with an earlier date.
    keep |= {number for number, _, match in rows if match["date"] != day}
    # Two repeated pairs of accession number and CIK.
    repeated = [pair for pair, count in pairs.items() if count > 1][:2]
    keep |= {number for number, _, match in rows if (match["accession"], match["cik"]) in repeated}
    # The five rows with the longest form names, and every line which is no row.
    by_form = sorted(rows, key=lambda row: len(row[1][:17].rstrip()), reverse=True)
    keep |= {number for number, _, _ in by_form[:5]}
    keep |= {number for number, line, match in parsed if match is None and line.strip()}
    # Fill up in file order.
    for number, _, _ in rows:
        if len(keep) >= INDEX_ROWS:
            break
        keep.add(number)
    chosen = [line for number, line, _ in parsed if number in keep]
    return ("\n".join([*header, *chosen]) + "\n").encode("latin-1")


def archive(members: dict[str, bytes]) -> bytes:
    """A small submissions.zip with a fixed time, so the bytes are the same on every run."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as bundle:
        for name in sorted(members):
            info = zipfile.ZipInfo(name, date_time=(2026, 11, 27, 5, 6, 36))
            info.compress_type = zipfile.ZIP_DEFLATED
            bundle.writestr(info, members[name])
    return buffer.getvalue()


def shorten() -> int:
    state = read_state()
    found = surprises(state) if state.get("complete") else ["the recording is not complete"]
    if found:
        for line in found:
            print(line, file=sys.stderr)
        print("Run the step fetch first, or change the plan of the script.", file=sys.stderr)
        return 2
    evidence = json.loads((RAW / REQUEST_LOG).read_text(encoding="utf-8"))
    seed = seed_concepts()
    named = index_accessions()
    year, quarter = quarter_of(INDEX_DAY)
    files: dict[str, bytes] = {TICKER_FILE_URL: shorten_tickers()}
    members: dict[str, bytes] = {}
    for cik in (*DEMO, *MORE):
        document = shorten_submissions(cik, INDEX_DAY, named.get(cik, set()))
        files[submissions_url(cik)] = members[cik_file_name(cik)] = document
    notes: dict[str, list[str]] = {}
    for cik in DEMO:
        files[companyfacts_url(cik)], notes[cik_file_name(cik)] = shorten_facts(cik, seed)
    files[index_listing_url(year, quarter)] = shorten_listing()
    files[index_file_url(INDEX_DAY)] = shorten_index()
    files[SUBMISSIONS_ARCHIVE_URL] = archive(members)
    refusals = {
        "data.sec.gov-404.xml": raw_path(companyfacts_url(NO_FACTS)),
        "www.sec.gov-archives-403.xml": raw_path(index_file_url(HOLIDAY)),
    }

    written: list[tuple[str, int, str]] = []
    for url, content in files.items():
        host, path = split(url)
        target = FIXTURES / host / path.lstrip("/")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        written.append((str(target.relative_to(FIXTURES)), len(content), md5(content)))
    for name, source in refusals.items():
        content = without_request_ids(source.with_name(source.name + MISSING_SUFFIX).read_bytes())
        target = FIXTURES / "refusals" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        written.append((str(target.relative_to(FIXTURES)), len(content), md5(content)))
    text = attribution(state["entries"], evidence, written, notes)
    (FIXTURES / "ATTRIBUTION.md").write_text(text, encoding="utf-8")
    total = sum(size for _, size, _ in written)
    print(f"{len(written)} files, {total} bytes, in {FIXTURES.relative_to(REPO)}")
    return 0


def attribution(
    entries: list[dict[str, Any]],
    evidence: dict[str, Any],
    written: list[tuple[str, int, str]],
    notes: dict[str, list[str]],
) -> str:
    first = datetime.fromisoformat(evidence["first_request_at"])
    last = datetime.fromisoformat(evidence["last_request_at"])
    recorded = "\n".join(
        f"| `{entry['url']}` | {entry['status']} | {entry['bytes']:,} | `{entry['md5']}` |"
        for entry in entries
    )
    fixtures = "\n".join(f"| `{name}` | {size:,} | `{digest}` |" for name, size, digest in written)
    more = "\n".join(
        f"| `{name}` | {', '.join(f'`{concept}`' for concept in concepts)} |"
        for name, concepts in notes.items()
    )
    outcomes = ", ".join(f"{count} `{name}`" for name, count in evidence["outcomes"].items())
    by_wall, by_mono = (
        evidence["max_requests_in_one_second"],
        evidence["max_requests_in_one_second_monotonic"],
    )
    demo, others, bare = (", ".join(group.values()) for group in (DEMO, MORE, BARE))
    forms = ", ".join(sorted(KEPT_FORMS))
    index_name = index_file_name(INDEX_DAY)
    return f"""# Recorded SEC files

The files in the folder are shortened copies of public files of the EDGAR system of the U.S.
Securities and Exchange Commission (https://www.sec.gov/). The project is not affiliated
with, endorsed by or sponsored by the SEC. `make demo`, the tests and CI read the files in
place of the live source (week 3 plan, section 3.3), so no test sends a request to the SEC.

## How the files were recorded

The step `fetch` of `benchmarks/source_profile/record_sec_fixtures.py` sent
{evidence["requests"]} requests through the fair-access client
`gbmi.sources.sec.client.SecClient` on {first:%Y-%m-%d}, from {first:%H:%M:%S} to
{last:%H:%M:%S} UTC, with the declared user agent of the owner of the project. The request
log of the run (`ops.source_request`, run `{evidence["run_id"]}`) shows at most {by_wall}
requests in any window of one second by the wall clock, and at most {by_mono} by the monotonic
clock of the client. The outcomes of the requests: {outcomes}.

The raw answers are not part of the repository. The step `shorten` of the same script wrote
the files of the folder from the raw answers, without a network.

| Requested address | Status | Bytes of the raw answer | MD5 of the raw answer |
|---|---|---|---|
{recorded}

## How the files were shortened

- **Ticker file:** the rows of ten CIKs: the five demo companies ({demo}), three more
  companies with a submissions document ({others}), and two companies without any recorded
  document ({bare}).
- **Submissions documents** (eight): every top-level field as recorded. Of `filings.recent`:
  the {NEWEST_FILINGS} newest filings, every filing of the forms {forms} of the two years
  before {INDEX_DAY.isoformat()}, and every filing which the recorded index file names.
- **Facts documents** (five): the concepts of `dbt/seeds/metric_concept_map.csv` and
  {MORE_CONCEPTS} more concepts for each company (table below), each with every fact.
- **Listing of the quarter:** the entries of {INDEX_DAY.isoformat()}: the recorded index file
  and the other files of the day, which the platform does not read.
- **Index file `{index_name}`:** the header and about {INDEX_ROWS} rows: the rows of the ten
  companies, three filings with several filers, every row with an earlier date, two repeated
  pairs of accession number and CIK, the five rows with the longest form names, and rows in
  file order up to the total.
- **`submissions.zip`:** built from the eight shortened submissions documents.
- **`refusals/`:** the bodies of the two "no such object" answers: HTTP 404 on
  `data.sec.gov` (no facts document) and HTTP 403 below `/Archives/` (no index file for a US
  holiday). The request identifier and the host identifier of each body are replaced by the
  letter X in the same length: the two values name one request of the recording.

| Facts document | The {MORE_CONCEPTS} concepts beside the concepts of the metric seed |
|---|---|
{more}

## The files of the folder

| File | Bytes | MD5 |
|---|---|---|
{fixtures}
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("step", choices=["plan", "rehearse", "fetch", "shorten"])
    parser.add_argument(
        "--again", action="store_true", help="fetch although the raw answers are complete"
    )
    args = parser.parse_args()
    if args.step == "plan":
        for url, listed, missing in plan():
            kind = "missing" if missing else "object"
            print(f"{kind:8} {'listed' if listed else '':6} {url}")
        return 0
    if args.step == "rehearse":
        return rehearse()
    return fetch(again=args.again) if args.step == "fetch" else shorten()


if __name__ == "__main__":
    sys.exit(main())
