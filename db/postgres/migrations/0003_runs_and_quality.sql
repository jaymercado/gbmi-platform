-- Run records and quality results (blueprint 7.3, 8.1 and 5.7; week 2 plan WP-02).

-- One row for each execution of a task entry point.
CREATE TABLE ops.pipeline_run (
    pipeline_run_id   bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_point       text        NOT NULL,                -- the gbmi function, for example 'gdelt.ingest'
    dag_id            text,
    task_id           text,
    run_id            text,
    try_number        integer,
    as_of             timestamptz,                         -- the clock the run worked with
    started_at        timestamptz NOT NULL,
    ended_at          timestamptz NOT NULL,
    duration_ms       integer     NOT NULL,
    status            text        NOT NULL,                -- 'success' or 'failed'
    row_count         bigint,
    byte_count        bigint,
    pipeline_version  text        NOT NULL,
    error             text,
    detail            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT pipeline_run_status CHECK (status IN ('success', 'failed')),
    CONSTRAINT pipeline_run_times CHECK (ended_at >= started_at AND duration_ms >= 0)
);

CREATE INDEX pipeline_run_by_entry_point ON ops.pipeline_run (entry_point, as_of DESC);
CREATE INDEX pipeline_run_by_run ON ops.pipeline_run (run_id);

-- One row for each execution of a quality check (gates G1 to G3 in phase P1).
CREATE TABLE ops.dq_result (
    dq_result_id    bigint           GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gate            text             NOT NULL,             -- 'G1', 'G2', 'G3', ...
    check_name      text             NOT NULL,
    status          text             NOT NULL,             -- 'passed', 'failed', 'skipped' or 'accepted'
    observed_value  double precision,
    threshold       text,                                  -- the rule in words, for example 'at most 0.01'
    source          text,
    feed            text,
    table_name      text,
    unit            text,                                  -- '2026-11-25T08', '2026-11-25', or the stamp of one slot
    run_id          text,
    detail          jsonb            NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz      NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT dq_result_gate CHECK (gate ~ '^G[1-7]$'),
    CONSTRAINT dq_result_status CHECK (status IN ('passed', 'failed', 'skipped', 'accepted'))
);

CREATE INDEX dq_result_by_time ON ops.dq_result (created_at);
CREATE INDEX dq_result_failures ON ops.dq_result (gate, check_name, created_at) WHERE status = 'failed';
