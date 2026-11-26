-- The ledger (blueprint 7.3 and 8.2; week 2 plan 3.3 and WP-02): one row for every expected
-- source file, and one row for every state change.
--
-- Rules for every file of this directory:
--   * forward only: a file is never changed after it has been applied; a change to the schema
--     is a new file;
--   * no transaction statements: the runner (gbmi db migrate) wraps each file in one transaction;
--   * no array column: ClickHouse cannot read a relation with a nullable array column, so free
--     detail is jsonb.

CREATE FUNCTION ops.touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END
$$;

CREATE FUNCTION ops.refuse_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%.% is append-only: % is not allowed', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END
$$;

CREATE TABLE ops.source_slot (
    source              text        NOT NULL,              -- 'gdelt'
    feed                text        NOT NULL,              -- 'export' | 'mentions' | 'gkg'
    slot_ts             timestamptz NOT NULL,              -- start of the 15-minute slot, UTC
    state               text        NOT NULL DEFAULT 'expected',
    -- what the source lists about the file
    url                 text,
    expected_bytes      bigint,
    expected_md5        text,
    listed_at           timestamptz,
    published_at        timestamptz,                       -- upload time (HTTP header Last-Modified); NULL when bronze already held the file
    -- the download (gate G1)
    bronze_uri          text,
    attempts            integer     NOT NULL DEFAULT 0,    -- failed downloads since the slot was last listed
    last_error          text,
    verified_at         timestamptz,
    unavailable_since   timestamptz,                       -- first HTTP 404 of a listed file
    absent_count        integer     NOT NULL DEFAULT 0,    -- reconciliations in a row which did not find the slot
    -- the silver build: where every raw line of the file went
    loaded_at           timestamptz,
    silver_snapshot_id  bigint,
    raw_lines           bigint,
    parsed_rows         bigint,
    quarantined_rows    bigint,
    -- decisions and claims
    decided_by          text,                              -- 'auto' or the operator who confirmed a missing slot
    decision_note       text,
    reserved_for        text,                              -- claim such as 'spark_backfill'; routine silver jobs skip the slot
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source, feed, slot_ts),
    CONSTRAINT source_slot_state CHECK (state IN
        ('expected', 'listed', 'downloaded', 'verified', 'loaded', 'quarantined', 'confirmed_missing')),
    CONSTRAINT source_slot_quarter_hour CHECK (extract(epoch FROM slot_ts) % 900 = 0),
    CONSTRAINT source_slot_counters CHECK (attempts >= 0 AND absent_count >= 0),
    -- A slot which the source has listed carries the facts of the list.
    CONSTRAINT source_slot_listed CHECK (state IN ('expected', 'confirmed_missing')
        OR (url IS NOT NULL AND expected_bytes IS NOT NULL AND expected_md5 IS NOT NULL
            AND listed_at IS NOT NULL)),
    -- A verified file is in bronze.
    CONSTRAINT source_slot_verified CHECK (state NOT IN ('verified', 'loaded')
        OR (bronze_uri IS NOT NULL AND verified_at IS NOT NULL)),
    -- Accounting (blueprint 3.4): for every loaded file, raw lines = parsed rows + quarantined rows.
    CONSTRAINT source_slot_loaded CHECK (state <> 'loaded'
        OR (loaded_at IS NOT NULL AND raw_lines IS NOT NULL AND parsed_rows IS NOT NULL
            AND quarantined_rows IS NOT NULL AND raw_lines = parsed_rows + quarantined_rows)),
    -- A missing slot is confirmed by somebody: the rule ('auto') or an operator.
    CONSTRAINT source_slot_confirmed CHECK (state <> 'confirmed_missing' OR decided_by IS NOT NULL)
);

-- The slots which still need work (blueprint 7.3).
CREATE INDEX source_slot_pending ON ops.source_slot (state, slot_ts)
    WHERE state NOT IN ('loaded', 'confirmed_missing');

CREATE TRIGGER source_slot_touch_updated_at BEFORE UPDATE ON ops.source_slot
    FOR EACH ROW EXECUTE FUNCTION ops.touch_updated_at();

-- One row for each state change, append-only. The twelve pairs are the twelve transitions of
-- the state diagram (blueprint 8.2); no other pair can be stored.
CREATE TABLE ops.slot_transition (
    transition_id  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source         text        NOT NULL,
    feed           text        NOT NULL,
    slot_ts        timestamptz NOT NULL,
    from_state     text        NOT NULL,
    to_state       text        NOT NULL,
    reason         text,
    run_id         text,
    decided_by     text,
    as_of          timestamptz,                            -- the clock of the run which made the change
    created_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (source, feed, slot_ts) REFERENCES ops.source_slot (source, feed, slot_ts),
    CONSTRAINT slot_transition_allowed CHECK ((from_state, to_state) IN (
        ('expected', 'listed'),
        ('listed', 'downloaded'),
        ('downloaded', 'verified'),
        ('downloaded', 'listed'),
        ('listed', 'quarantined'),
        ('verified', 'loaded'),
        ('verified', 'quarantined'),
        ('quarantined', 'listed'),
        ('expected', 'confirmed_missing'),
        ('listed', 'confirmed_missing'),
        ('quarantined', 'confirmed_missing'),
        ('confirmed_missing', 'listed')))
);

CREATE INDEX slot_transition_by_slot ON ops.slot_transition (source, feed, slot_ts, transition_id);

CREATE TRIGGER slot_transition_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON ops.slot_transition
    FOR EACH STATEMENT EXECUTE FUNCTION ops.refuse_change();
