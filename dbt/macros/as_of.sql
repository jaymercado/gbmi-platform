{#-
    Bitemporal questions on the financial facts (blueprint 7.10).

    A fact has two times: the period which the fact describes, and the moment at which the
    fact became known (known_at). A restatement is a new row with a later known_at; nothing is
    overwritten. A question "as of" a time reads only the rows which were known at the time, so
    an analysis of a past date cannot use a number which was published later (look-ahead
    error).
-#}

{#-
    The time of the variable facts_as_of as a ClickHouse expression, or an empty text when the
    variable is not set. The value is a UTC time, 'YYYY-MM-DD hh:mm:ss'. ClickHouse refuses a
    value which is no time, so a wrong value stops the build: a silent fallback to "everything"
    would be the look-ahead error.
-#}
{% macro facts_as_of_time() -%}
    {%- set moment = var('facts_as_of', none) -%}
    {%- if moment is not none and moment | string | trim != '' -%}
        toDateTime('{{ moment | string | trim | replace("\\", "\\\\") | replace("'", "\\'") }}', 'UTC')
    {%- endif -%}
{%- endmacro %}

{#-
    For each company, concept, unit and period: what was known at the time as_of.

    as_of is a ClickHouse expression of the type DateTime, for example
    "toDateTime('2026-04-01 00:00:00', 'UTC')"; none or an empty text means "everything which
    is known now". A row
    counts when known_at is not later than as_of. The result is a SELECT statement:

        value_known           the newest value which was known at the time (the argMax form of
                              blueprint 7.10); the accession number decides between two filings
                              with the same known_at
        value_first_reported  the value of the first filing which reported the period
        reports               the number of filings which had reported the period at the time

    The two bounds of the period are part of the key: one filing can report the same concept
    and the same period end for two durations (a quarter and the year to date).
-#}
{% macro facts_as_of(as_of=none, relation=none) -%}
    select
        cik,
        taxonomy,
        concept,
        unit,
        period_start,
        period_end,
        any(is_instant) as is_instant,
        argMin(value, (known_at, accession_number)) as value_first_reported,
        argMin(accession_number, (known_at, accession_number)) as first_accession_number,
        argMin(filed_date, (known_at, accession_number)) as first_filed_date,
        min(known_at) as first_known_at,
        argMax(value, (known_at, accession_number)) as value_known,
        argMax(accession_number, (known_at, accession_number)) as known_accession_number,
        argMax(filed_date, (known_at, accession_number)) as known_filed_date,
        max(known_at) as known_since,
        toUInt32(count()) as reports
    from {{ relation if relation is not none else ref('fct_financial_fact') }}
    {% if as_of is not none and as_of | trim != '' -%}
        where known_at <= {{ as_of | trim }}
    {%- endif %}
    group by
        cik,
        taxonomy,
        concept,
        unit,
        period_start,
        period_end
{%- endmacro %}
