-- SEC objects, the circuit of a source and the request log (blueprint 7.3 and 4.3; week 3 plan
-- 3.3 and WP-02, with change C14, and with the schema findings of review 1).
--
-- An object is one file which the platform asks from a source which publishes no size and no
-- hash for the file. The states "listed" and "downloaded" of ops.source_slot have no meaning for
-- such a file, so an object has six states of its own and ten transitions. The rules for every
-- file of the directory stand in 0001_ledger.sql.

CREATE TABLE ops.source_object (
    source            text        NOT NULL,              -- 'sec'
    dataset           text        NOT NULL,              -- 'reference' | 'submissions' | 'index' | 'companyfacts' | 'bulk'
    object_date       date        NOT NULL,              -- the date of an index file; for every other object the UTC date of the run
    object_name       text        NOT NULL,              -- for example 'CIK0001326801.json' or 'form.20261125.idx'
    state             text        NOT NULL DEFAULT 'expected',
    cik               bigint,                            -- the company of a submissions document or a facts document
    url               text        NOT NULL,
    -- the download (gate G1): the size and the hash describe the stored bytes
    bronze_uri        text,
    byte_count        bigint,
    md5               text,
    http_status       integer,
    last_modified     timestamptz,                       -- HTTP header Last-Modified
    attempts          integer     NOT NULL DEFAULT 0,    -- failed tries of gate G1 since the object was last expected
    last_error        text,
    verified_at       timestamptz,
    -- the load: where every record of the file went
    loaded_at         timestamptz,
    raw_records       bigint,
    parsed_rows       bigint,
    quarantined_rows  bigint,
    -- decisions
    reason            text,                              -- why the object is quarantined, not_published or superseded
    decided_by        text,                              -- the operator of a requeue
    detail            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source, dataset, object_date, object_name),
    CONSTRAINT source_object_state CHECK (state IN
        ('expected', 'verified', 'quarantined', 'loaded', 'not_published', 'superseded')),
    CONSTRAINT source_object_dataset CHECK (dataset IN
        ('reference', 'submissions', 'index', 'companyfacts', 'bulk')),
    CONSTRAINT source_object_counters CHECK (attempts >= 0
        AND (byte_count IS NULL OR byte_count >= 0)
        AND (raw_records IS NULL OR raw_records >= 0)
        AND (parsed_rows IS NULL OR parsed_rows >= 0)
        AND (quarantined_rows IS NULL OR quarantined_rows >= 0)),
    CONSTRAINT source_object_md5 CHECK (md5 IS NULL OR md5 ~ '^[0-9a-f]{32}$'),
    -- A verified file is in bronze, with the size and the hash of the stored bytes.
    CONSTRAINT source_object_verified CHECK (state NOT IN ('verified', 'loaded')
        OR (bronze_uri IS NOT NULL AND byte_count IS NOT NULL AND md5 IS NOT NULL
            AND verified_at IS NOT NULL)),
    -- Accounting (blueprint 3.4): for every loaded file, raw records = parsed rows + quarantined rows.
    CONSTRAINT source_object_loaded CHECK (state <> 'loaded'
        OR (loaded_at IS NOT NULL AND raw_records IS NOT NULL AND parsed_rows IS NOT NULL
            AND quarantined_rows IS NOT NULL AND raw_records = parsed_rows + quarantined_rows)),
    -- A quarantined object and a final state without a load say why.
    CONSTRAINT source_object_reason CHECK (state NOT IN ('quarantined', 'not_published', 'superseded')
        OR reason IS NOT NULL)
);

-- The objects which still need work.
CREATE INDEX source_object_open ON ops.source_object (source, dataset, object_date)
    WHERE state IN ('expected', 'verified', 'quarantined');
-- The newest object of a name, and of a company.
CREATE INDEX source_object_by_name
    ON ops.source_object (source, dataset, object_name, object_date DESC);
CREATE INDEX source_object_by_cik
    ON ops.source_object (source, dataset, cik, object_date DESC) WHERE cik IS NOT NULL;

CREATE TRIGGER source_object_touch_updated_at BEFORE UPDATE ON ops.source_object
    FOR EACH ROW EXECUTE FUNCTION ops.touch_updated_at();

-- The table itself follows the ten transitions, whoever writes the statement: a new object
-- is 'expected', a state changes only along a transition, the key never changes, and no
-- object is deleted. The log table below can store the same ten pairs and no other pair.
CREATE FUNCTION ops.guard_object_state() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'expected' THEN
            RAISE EXCEPTION 'ops.source_object: a new object is expected, not %', NEW.state;
        END IF;
        RETURN NEW;
    END IF;
    IF (NEW.source, NEW.dataset, NEW.object_date, NEW.object_name)
       IS DISTINCT FROM (OLD.source, OLD.dataset, OLD.object_date, OLD.object_name) THEN
        RAISE EXCEPTION 'ops.source_object: the key of an object never changes';
    END IF;
    IF NEW.state <> OLD.state AND (OLD.state, NEW.state) NOT IN (
        ('expected', 'verified'),
        ('verified', 'loaded'),
        ('expected', 'quarantined'),
        ('verified', 'quarantined'),
        ('quarantined', 'expected'),
        ('expected', 'not_published'),
        ('not_published', 'expected'),
        ('expected', 'superseded'),
        ('verified', 'superseded'),
        ('quarantined', 'superseded')) THEN
        RAISE EXCEPTION 'ops.source_object: no transition from % to %', OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER source_object_guard_state BEFORE INSERT OR UPDATE ON ops.source_object
    FOR EACH ROW EXECUTE FUNCTION ops.guard_object_state();

CREATE TRIGGER source_object_no_delete BEFORE DELETE OR TRUNCATE ON ops.source_object
    FOR EACH STATEMENT EXECUTE FUNCTION ops.refuse_change();

-- One row for each state change, append-only. The ten pairs are the ten transitions of the
-- week 3 plan (3.3); no other pair can be stored. No pair leaves 'loaded' or 'superseded'.
CREATE TABLE ops.object_transition (
    transition_id  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source         text        NOT NULL,
    dataset        text        NOT NULL,
    object_date    date        NOT NULL,
    object_name    text        NOT NULL,
    from_state     text        NOT NULL,
    to_state       text        NOT NULL,
    reason         text,
    run_id         text,
    decided_by     text,
    as_of          timestamptz,                            -- the clock of the run which made the change
    created_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (source, dataset, object_date, object_name)
        REFERENCES ops.source_object (source, dataset, object_date, object_name),
    CONSTRAINT object_transition_allowed CHECK ((from_state, to_state) IN (
        ('expected', 'verified'),
        ('verified', 'loaded'),
        ('expected', 'quarantined'),
        ('verified', 'quarantined'),
        ('quarantined', 'expected'),
        ('expected', 'not_published'),
        ('not_published', 'expected'),
        ('expected', 'superseded'),
        ('verified', 'superseded'),
        ('quarantined', 'superseded')))
);

CREATE INDEX object_transition_by_object
    ON ops.object_transition (source, dataset, object_date, object_name, transition_id);

CREATE TRIGGER object_transition_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON ops.object_transition
    FOR EACH STATEMENT EXECUTE FUNCTION ops.refuse_change();

-- The circuit of a source: one row for each source. A block is recorded as long as
-- blocked_until is not NULL: before the time no request leaves, and after the time the next
-- request is one probe. A refused probe locks the circuit until an operator closes the circuit.
CREATE TABLE ops.source_circuit (
    source         text        PRIMARY KEY,
    blocked_until  timestamptz,                            -- NULL: no block is recorded
    opened_at      timestamptz,
    reason         text,
    http_status    integer,
    open_count     integer     NOT NULL DEFAULT 0,         -- how often the circuit was opened
    locked         boolean     NOT NULL DEFAULT false,     -- a probe was refused
    closed_at      timestamptz,
    closed_by      text,                                   -- 'probe' or the operator
    close_note     text,
    updated_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT source_circuit_counters CHECK (open_count >= 0),
    CONSTRAINT source_circuit_open CHECK (blocked_until IS NULL
        OR (opened_at IS NOT NULL AND reason IS NOT NULL)),
    CONSTRAINT source_circuit_locked CHECK (NOT locked OR blocked_until IS NOT NULL)
);

CREATE TRIGGER source_circuit_touch_updated_at BEFORE UPDATE ON ops.source_circuit
    FOR EACH ROW EXECUTE FUNCTION ops.touch_updated_at();

-- A source without a row has a closed circuit, so a deleted row would forget a block.
CREATE TRIGGER source_circuit_no_delete BEFORE DELETE OR TRUNCATE ON ops.source_circuit
    FOR EACH STATEMENT EXECUTE FUNCTION ops.refuse_change();

-- The history of a circuit, append-only: one row for each opening, lock and close. The row of
-- ops.source_circuit is the state now; the events say how the state came about.
CREATE TABLE ops.circuit_event (
    event_id       bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source         text        NOT NULL,
    event          text        NOT NULL,                   -- 'opened' | 'locked' | 'closed'
    blocked_until  timestamptz,                            -- the end of the block after the event
    reason         text,
    http_status    integer,
    decided_by     text,                                   -- who closed: 'probe' or the operator
    note           text,
    run_id         text,
    event_at       timestamptz NOT NULL,                   -- the clock of the client
    created_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT circuit_event_kind CHECK (event IN ('opened', 'locked', 'closed'))
);

CREATE INDEX circuit_event_by_source ON ops.circuit_event (source, event_id);

CREATE TRIGGER circuit_event_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON ops.circuit_event
    FOR EACH STATEMENT EXECUTE FUNCTION ops.refuse_change();

-- The request log: one row for each attempt to reach a source. The client writes the row
-- before the request leaves, so a killed process leaves the row of every request which left.
-- One later statement adds the outcome. The user agent is not a column.
CREATE TABLE ops.source_request (
    request_id       bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source           text        NOT NULL,
    host             text        NOT NULL,
    path             text        NOT NULL,
    method           text        NOT NULL,
    granted_at       timestamptz NOT NULL,               -- wall clock of the client when the limiter let the request go
    client_session   uuid        NOT NULL,               -- one client: one process which holds the lock of the source
    granted_mono_ns  bigint      NOT NULL,               -- monotonic clock of the client at the same moment; comparable inside one session
    waited_ms        integer     NOT NULL,               -- the wait in the limiter
    attempt          integer     NOT NULL,               -- 1 for the first try of a request
    probe            boolean     NOT NULL DEFAULT false, -- the one request after a block
    run_id           text,
    recorded_at      timestamptz NOT NULL DEFAULT clock_timestamp(), -- clock of the database when the row arrived: a witness beside the clocks of the client
    -- the outcome
    completed_at     timestamptz,
    http_status      integer,                            -- NULL when no answer arrived
    outcome          text,                               -- 'ok' | 'missing' | 'blocked' | 'retry' | 'error'
    duration_ms      integer,
    byte_count       bigint,
    error            text,                               -- the class of a transport error
    CONSTRAINT source_request_method CHECK (method IN ('GET', 'HEAD')),
    CONSTRAINT source_request_counters CHECK (waited_ms >= 0 AND attempt >= 1
        AND granted_mono_ns >= 0
        AND (duration_ms IS NULL OR duration_ms >= 0)
        AND (byte_count IS NULL OR byte_count >= 0)),
    CONSTRAINT source_request_outcome CHECK (outcome IS NULL
        OR outcome IN ('ok', 'missing', 'blocked', 'retry', 'error')),
    CONSTRAINT source_request_completed CHECK ((completed_at IS NULL) = (outcome IS NULL))
);

CREATE INDEX source_request_by_time ON ops.source_request (source, granted_at);
CREATE INDEX source_request_by_session ON ops.source_request (client_session, granted_mono_ns);

-- The log is evidence (exit gate of phase P2, row 1): no row is deleted, the facts of the
-- request never change, and the outcome is written once.
CREATE FUNCTION ops.guard_source_request() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.completed_at IS NOT NULL THEN
        RAISE EXCEPTION 'ops.source_request %: the outcome is recorded and cannot change',
            OLD.request_id;
    END IF;
    IF (NEW.request_id, NEW.source, NEW.host, NEW.path, NEW.method, NEW.granted_at,
        NEW.client_session, NEW.granted_mono_ns, NEW.waited_ms, NEW.attempt, NEW.probe,
        NEW.run_id, NEW.recorded_at)
       IS DISTINCT FROM
       (OLD.request_id, OLD.source, OLD.host, OLD.path, OLD.method, OLD.granted_at,
        OLD.client_session, OLD.granted_mono_ns, OLD.waited_ms, OLD.attempt, OLD.probe,
        OLD.run_id, OLD.recorded_at) THEN
        RAISE EXCEPTION 'ops.source_request %: only the outcome can be set', OLD.request_id;
    END IF;
    -- One statement writes the whole outcome: a second statement finds completed_at set.
    IF NEW.completed_at IS NULL THEN
        RAISE EXCEPTION 'ops.source_request %: the outcome is written whole, with completed_at',
            OLD.request_id;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER source_request_guard BEFORE UPDATE ON ops.source_request
    FOR EACH ROW EXECUTE FUNCTION ops.guard_source_request();

CREATE TRIGGER source_request_no_delete BEFORE DELETE OR TRUNCATE ON ops.source_request
    FOR EACH STATEMENT EXECUTE FUNCTION ops.refuse_change();
