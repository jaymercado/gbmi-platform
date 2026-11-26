"""Bronze GKG files to the silver tables ``gdelt_gkg_doc`` and ``gdelt_gkg_org`` (pipeline PL-2).

One pass over the files gives both tables. A document row keeps the source, the URL, the title,
the story identifier, the tone values, the distinct themes and countries, and the number of its
organisation strings. An organisation row is one normalised organisation string of one
document.

Derivations (blueprint 8.4; week 2 plan 3.3 and WP-05):

* ``themes``: the distinct codes of ``V1THEMES``, sorted. ``countries``: the distinct two-letter
  codes of ``V1LOCATIONS``, sorted; the code is the fifth field from the right end of a location
  block, because a place name can hold the separator ``#`` (F6). Both lists are never NULL.
* ``page_title``: the tag ``<PAGE_TITLE>`` of the extras, cleaned. ``story_id``: the hash of the
  normalised title, or of the URL when the title is not usable.
* Organisations come from ``V2ENHANCEDORGANIZATIONS`` (name and character offset); a document
  which fills only ``V1ORGANIZATIONS`` uses the V1 names, with no offset and a count of 1 (D20).

Normalisation and hashing are the pure Python functions of ``gbmi.core.text``: DuckDB selects
the distinct raw strings, Python maps each string once, and DuckDB joins the result back.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

import duckdb
import pyarrow as pa

from gbmi.core.gdelt import Feed
from gbmi.core.gdelt_gkg import DOC_KEY, GKG, RECORD_ID_PATTERN, TONE_FIELDS
from gbmi.core.text import clean_title, normalise_text, org_string_id, story_id
from gbmi.lake.tables import (
    GDELT_GKG_DOC,
    GDELT_GKG_DOC_SCHEMA,
    GDELT_GKG_ORG,
    GDELT_GKG_ORG_SCHEMA,
    business_names,
)
from gbmi.transforms.duckdb.positional import (
    DOUBLE,
    INTEGER,
    BronzeFile,
    Check,
    Parsed,
    arrow,
    check_files,
    duplicate_rows,
    line_counts,
    quarantine_rows,
    typed_columns,
)
from gbmi.transforms.duckdb.runner import connect

_RECORD_ID = f"f[{GKG.position('GKGRECORDID')}]"
_TONE = f"f[{GKG.position('V1.5TONE')}]"
_THEMES = f"f[{GKG.position('V1THEMES')}]"
_LOCATIONS = f"f[{GKG.position('V1LOCATIONS')}]"
_V1_ORGS = f"f[{GKG.position('V1ORGANIZATIONS')}]"
_V2_ORGS = f"f[{GKG.position('V2ENHANCEDORGANIZATIONS')}]"
_EXTRAS = f"f[{GKG.position('V2EXTRASXML')}]"
_WORD_COUNT = f"string_split({_TONE}, ',')[{len(TONE_FIELDS)}]"

#: The checks of gate G2 which a single typed field cannot express.
EXTRA_CHECKS: tuple[Check, ...] = (
    (
        f"not regexp_full_match({_RECORD_ID}, '{RECORD_ID_PATTERN}')",
        "gkg_record_id (GKGRECORDID): not 14 digits, a hyphen and a number",
    ),
    # A text can have the form of a number and still be too large for the type: try_cast gives
    # NULL for an integer and infinity for a double. With both checks the cast of the typed
    # step can never fail, and no infinity reaches silver.
    (
        f"len(string_split({_TONE}, ',')) <> {len(TONE_FIELDS)} "
        f"or len(list_filter(string_split({_TONE}, ','), "
        f"lambda x: not regexp_full_match(x, '{DOUBLE}') or try_cast(x as double) is null "
        f"or not isfinite(try_cast(x as double)))) > 0",
        "tone (V1.5TONE): not seven numbers",
    ),
    (
        f"not regexp_full_match({_WORD_COUNT}, '{INTEGER}') "
        f"or try_cast({_WORD_COUNT} as integer) is null",
        "word_count (V1.5TONE): not an integer",
    ),
)


def typed_sql() -> str:
    """One row per line that passed, typed, with the raw material of the derived columns."""
    tone = ",\n        ".join(
        f"cast(tone_parts[{index}] as {'integer' if name == 'word_count' else 'double'}) as {name}"
        for index, name in enumerate(TONE_FIELDS, start=1)
    )
    return f"""
    create or replace temp table doc_typed as
    select
        {typed_columns(GKG)},
        {tone},
        list_sort(list_distinct(list_filter(string_split({_THEMES}, ';'), lambda x: x <> '')))
            as themes,
        list_sort(list_distinct(list_filter(
            list_transform(
                list_filter(string_split({_LOCATIONS}, ';'), lambda block: block <> ''),
                lambda block: string_split(block, '#')[-5]),
            lambda code: code is not null and regexp_full_match(code, '[A-Z]{{2}}'))))
            as countries,
        contains({_RECORD_ID}, '-T') as is_translated,
        nullif(regexp_extract({_EXTRAS}, '<PAGE_TITLE>(.*?)</PAGE_TITLE>', 1, 's'), '')
            as title_raw,
        {_V2_ORGS} as v2_orgs,
        {_V1_ORGS} as v1_orgs,
        slot_date,
        slot_hour,
        slot_ts,
        source_file as _source_file,
        source_md5 as _source_md5,
        slot_ts as _slot_ts,
        row_number() over (partition by {_RECORD_ID} order by slot_ts, line_number) as occurrence
    from (select *, string_split({_TONE}, ',') as tone_parts from checked where reason is null)
    """  # noqa: S608


#: The organisation entries of the documents to commit: one row for each name in a document.
#: An entry of the V2 column is "name,offset"; the offset follows the last comma.
ORG_ENTRIES_SQL = """
    create or replace temp table org_entries as
    with docs as (
        select gkg_record_id, v2_orgs, v1_orgs from doc_typed where occurrence = 1
    ),
    v2 as (
        select
            gkg_record_id,
            unnest(string_split(v2_orgs, ';')) as entry,
            generate_subscripts(string_split(v2_orgs, ';'), 1) as position
        from docs where v2_orgs <> ''
    ),
    v1 as (
        select
            gkg_record_id,
            unnest(string_split(v1_orgs, ';')) as entry,
            generate_subscripts(string_split(v1_orgs, ';'), 1) as position
        from docs where v2_orgs = '' and v1_orgs <> ''
    ),
    entries as (
        select
            gkg_record_id,
            position,
            trim(case when regexp_matches(entry, ',[0-9]+$')
                 then regexp_replace(entry, ',[0-9]+$', '') else entry end) as org_raw,
            try_cast(regexp_extract(entry, ',([0-9]+)$', 1) as integer) as char_offset,
            true as enhanced
        from v2
        union all
        select gkg_record_id, position, trim(entry), cast(null as integer), false from v1
    )
    select * from entries where org_raw <> ''
"""

#: One row per document and normalised string. The raw form is the form with the smallest
#: offset, then the first one in the list.
ORG_ROWS_SQL = """
    create or replace temp table org_rows as
    select
        e.gkg_record_id,
        m.org_string_id,
        arg_min(e.org_raw, coalesce(e.char_offset, 2147483647)::bigint * 1000000 + e.position)
            as org_string_raw,
        m.org_string_norm,
        min(e.char_offset) as first_offset,
        cast(case when bool_or(e.enhanced) then count(*) else 1 end as integer) as mention_count
    from org_entries e
    join org_map m on m.org_raw = e.org_raw
    where m.org_string_norm <> ''
    group by e.gkg_record_id, m.org_string_id, m.org_string_norm
"""


def _column(connection: duckdb.DuckDBPyConnection, query: str) -> list[str]:
    return [str(row[0]) for row in connection.execute(query).fetchall()]


def _register(
    connection: duckdb.DuckDBPyConnection, name: str, columns: Mapping[str, Sequence[str | None]]
) -> None:
    """Hand a table of strings from Python to DuckDB."""
    connection.register(
        name, pa.table({key: pa.array(values, pa.string()) for key, values in columns.items()})
    )


def _map_titles(connection: duckdb.DuckDBPyConnection) -> None:
    """``title_map``: each distinct raw title with its clean form and its story identifier."""
    raw = _column(
        connection,
        "select distinct title_raw from doc_typed where occurrence = 1 and title_raw is not null",
    )
    clean = [clean_title(title) for title in raw]
    # A title without a letter or a digit is not usable: the URL gives the story then.
    stories = [story_id(title, "") if title and normalise_text(title) else None for title in clean]
    _register(connection, "title_map", {"title_raw": raw, "page_title": clean, "story": stories})
    alone = _column(
        connection,
        "select distinct d.document_url from doc_typed d "
        "left join title_map t on t.title_raw = d.title_raw "
        "where d.occurrence = 1 and t.story is null",
    )
    _register(
        connection,
        "url_map",
        {"document_url": alone, "story": [story_id(None, url) for url in alone]},
    )


def _map_organisations(connection: duckdb.DuckDBPyConnection) -> int:
    """``org_map``: each distinct raw name with its normal form; returns the names without one."""
    raw = _column(connection, "select distinct org_raw from org_entries")
    normalised = [normalise_text(name) for name in raw]
    _register(
        connection,
        "org_map",
        {
            "org_raw": raw,
            "org_string_norm": normalised,
            "org_string_id": [org_string_id(name) for name in normalised],
        },
    )
    row = connection.execute(
        "select count(*) from org_entries e join org_map m on m.org_raw = e.org_raw "
        "where m.org_string_norm = ''"
    ).fetchone()
    return int(row[0]) if row is not None else 0


def parse_gkg(
    files: Sequence[BronzeFile],
    *,
    run_id: str | None,
    pipeline_version: str,
    ingested_at: datetime | None = None,
    file_threshold: float | None = None,
) -> Parsed:
    """Parse GKG files into document rows, organisation rows and quarantined rows."""
    ingested_at = ingested_at or datetime.now(UTC)
    run = {"ingested_at": ingested_at, "run_id": run_id, "pipeline_version": pipeline_version}
    run_columns = (
        "$ingested_at as _ingested_at, $run_id as _run_id, $pipeline_version as _pipeline_version"
    )
    connection = connect()
    try:
        accepted, rejected = check_files(
            connection, files, GKG, extra=EXTRA_CHECKS, file_threshold=file_threshold
        )
        connection.execute(typed_sql())
        _map_titles(connection)
        connection.execute(ORG_ENTRIES_SQL)
        dropped = _map_organisations(connection)
        connection.execute(ORG_ROWS_SQL)
        connection.execute(
            """
            create or replace temp table doc_rows as
            select
                d.* exclude (title_raw, v2_orgs, v1_orgs, occurrence),
                t.page_title,
                coalesce(t.story, u.story) as story_id,
                cast(coalesce(o.org_count, 0) as integer) as org_count
            from doc_typed d
            left join title_map t on t.title_raw = d.title_raw
            left join url_map u on u.document_url = d.document_url
            left join (
                select gkg_record_id, count(*) as org_count from org_rows group by gkg_record_id
            ) o on o.gkg_record_id = d.gkg_record_id
            where d.occurrence = 1
            """
        )
        docs = arrow(
            connection.sql(
                f"select {', '.join(business_names(GDELT_GKG_DOC_SCHEMA))}, {run_columns} "  # noqa: S608
                f"from doc_rows order by {DOC_KEY}",
                params=run,
            )
        )
        org_names = ", ".join(
            f"d.{name}" if name.startswith(("slot_", "_")) else f"o.{name}"
            for name in business_names(GDELT_GKG_ORG_SCHEMA)
        )
        orgs = arrow(
            connection.sql(
                f"select {org_names}, {run_columns} from org_rows o "  # noqa: S608
                f"join doc_rows d on d.gkg_record_id = o.gkg_record_id "
                f"order by o.gkg_record_id, o.org_string_id",
                params=run,
            )
        )
        quarantine = quarantine_rows(connection, run_id=run_id, ingested_at=ingested_at)
        duplicates = duplicate_rows(connection, "doc_typed")
    finally:
        connection.close()
    docs = docs.select([field.name for field in GDELT_GKG_DOC_SCHEMA.fields]).cast(
        GDELT_GKG_DOC_SCHEMA.as_arrow()
    )
    orgs = orgs.select([field.name for field in GDELT_GKG_ORG_SCHEMA.fields]).cast(
        GDELT_GKG_ORG_SCHEMA.as_arrow()
    )
    return Parsed(
        feed=Feed.GKG,
        tables={GDELT_GKG_DOC: docs, GDELT_GKG_ORG: orgs},
        quarantine=quarantine,
        counts=line_counts(accepted, docs.num_rows, duplicates),
        files=accepted,
        rejected=rejected,
        notes={"organisation_names_without_a_normal_form": dropped},
    )
