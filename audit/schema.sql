-- Temporal workflow history archive.
--   workflow_execution      one row per closed run
--   workflow_history_event  one row per history event
--   loader_checkpoint       watermark for the poll sweep
-- Run metadata is upserted; duplicate event keys are ignored.

CREATE TABLE IF NOT EXISTS workflow_execution (
    run_id          TEXT PRIMARY KEY,
    workflow_id     TEXT        NOT NULL,
    workflow_type   TEXT        NOT NULL,
    task_queue      TEXT,
    status          TEXT        NOT NULL,        -- COMPLETED | FAILED | TIMED_OUT | TERMINATED | CANCELED | CONTINUED_AS_NEW
    start_time      TIMESTAMPTZ,
    close_time      TIMESTAMPTZ,
    history_length  INT,
    parent_workflow_id TEXT,
    source          TEXT        NOT NULL,        -- export | poll | archiver
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS workflow_execution_wf ON workflow_execution (workflow_id);
CREATE INDEX IF NOT EXISTS workflow_execution_close ON workflow_execution (close_time);

CREATE TABLE IF NOT EXISTS workflow_history_event (
    run_id         TEXT        NOT NULL,
    event_id       BIGINT      NOT NULL,
    workflow_id    TEXT        NOT NULL,
    event_time     TIMESTAMPTZ NOT NULL,
    event_type     TEXT        NOT NULL,
    activity_type  TEXT,                         -- set on ACTIVITY_TASK_SCHEDULED
    attributes     JSONB       NOT NULL,         -- proto JSON; payloads decoded when possible
    PRIMARY KEY (run_id, event_id)
);
CREATE INDEX IF NOT EXISTS workflow_history_event_wf ON workflow_history_event (workflow_id, event_time);

-- Loader checkpoint for the visibility poller (option D).
CREATE TABLE IF NOT EXISTS loader_checkpoint (
    name        TEXT PRIMARY KEY,
    watermark   TIMESTAMPTZ NOT NULL
);
