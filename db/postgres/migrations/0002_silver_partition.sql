-- Partition versions (blueprint 7.3: table x partition x snapshot; week 2 plan 3.3 and WP-02):
-- one row for each build of a unit and table. A unit is one slot-hour or one whole slot-date.
-- The newest row for a table and a date is the current version of the date.

CREATE TABLE ops.silver_partition (
    partition_version_id  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source                text        NOT NULL,
    feed                  text        NOT NULL,
    table_name            text        NOT NULL,
    slot_date             date        NOT NULL,
    unit_kind             text        NOT NULL,            -- 'hour' or 'day'
    slot_hour             integer,                         -- 0 to 23 for an hour unit; NULL for a day unit
    -- The Iceberg commit. The three values are NULL when an empty unit needed no commit.
    commit_id             text,
    snapshot_id           bigint,
    previous_snapshot_id  bigint,
    -- The five counts of the pre-commit audit (gate G3). The first four count raw lines and stay
    -- NULL for a table whose rows are not lines of a source file (gdelt_gkg_org).
    raw_lines             bigint,
    parsed_rows           bigint,
    quarantined_rows      bigint,
    duplicate_rows        bigint,
    committed_rows        bigint      NOT NULL,
    -- Evidence of the build (blueprint 8.4): the files, and the content of the unit.
    file_count            integer     NOT NULL,
    fileset_hash          text        NOT NULL,            -- SHA-256 over the lines <stamp>:<md5>, in slot order
    content_digest        text        NOT NULL,
    run_id                text,
    pipeline_version      text        NOT NULL,
    duration_ms           integer,
    detail                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    committed_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    -- "slot_hour IS NOT NULL" is spelled out: a comparison with NULL is NULL, and a check
    -- constraint lets NULL pass.
    CONSTRAINT silver_partition_unit CHECK (
        (unit_kind = 'hour' AND slot_hour IS NOT NULL AND slot_hour BETWEEN 0 AND 23)
        OR (unit_kind = 'day' AND slot_hour IS NULL)),
    CONSTRAINT silver_partition_counts CHECK (committed_rows >= 0 AND file_count >= 0),
    -- The two accounting identities of gate G3, for every table which counts raw lines.
    CONSTRAINT silver_partition_accounting CHECK (
        num_nulls(raw_lines, parsed_rows, quarantined_rows, duplicate_rows) IN (0, 4)
        AND (raw_lines IS NULL
             OR (raw_lines = parsed_rows + quarantined_rows
                 AND committed_rows = parsed_rows - duplicate_rows)))
);

CREATE INDEX silver_partition_current
    ON ops.silver_partition (table_name, slot_date, partition_version_id DESC);
