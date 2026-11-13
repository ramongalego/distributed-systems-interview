-- Lab automation: workflow execution.
--
-- This is what the executor intends and what it has been told. What physically
-- happened is in each instrument's own ledger; jobs.id is the job_id written
-- there, so the two can be laid side by side.

CREATE DATABASE executor_db;

\c executor_db

-- One execution of a workflow.
CREATE TABLE runs (
    id            TEXT PRIMARY KEY,
    workflow_name TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    -- How the run will end, once a failure or a cancel has decided that it
    -- will. It only ends when no instrument is still working for it.
    ending_as     TEXT CHECK (ending_as IN ('failed', 'cancelled')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ
);

CREATE INDEX active_runs ON runs (started_at) WHERE status = 'running';
CREATE INDEX runs_newest_first ON runs (created_at DESC, id);

-- One node of the workflow DAG, for one run.
CREATE TABLE steps (
    run_id     TEXT   NOT NULL REFERENCES runs (id),
    name       TEXT   NOT NULL,
    position   INT    NOT NULL,
    device_id  TEXT   NOT NULL,
    depends_on TEXT[] NOT NULL DEFAULT '{}',
    status     TEXT   NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    PRIMARY KEY (run_id, name)
);

-- One attempt to run a step on its instrument.
--
--   pending    the instrument has been asked to start it, and has not confirmed
--   revoked    no longer asked to start it; what is left is to find out
--              whether it started anyway
--   running    the instrument confirmed the start
--   completed, failed
--   cancelled  the instrument has no record of it: it never ran
CREATE TABLE jobs (
    id          TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL,
    step_name   TEXT NOT NULL,
    attempt     INT  NOT NULL,
    device_id   TEXT NOT NULL,
    state       TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'revoked', 'running', 'completed', 'failed', 'cancelled')),
    -- By the instrument's clock, as it reported them.
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error       TEXT,
    retryable   BOOLEAN,
    data_ref    TEXT,
    data_sha256 TEXT,
    UNIQUE (run_id, step_name, attempt),
    FOREIGN KEY (run_id, step_name) REFERENCES steps (run_id, name)
);

-- An instrument does one thing at a time: at most one job that may be on it.
CREATE UNIQUE INDEX one_open_job_per_device ON jobs (device_id)
    WHERE state IN ('pending', 'revoked', 'running');
