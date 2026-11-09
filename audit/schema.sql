-- Audit / history projection schema used by the T2 samples.
-- Two families of tables:
--   1. Business-facing, written while workflows run (options B and C):
--        audit_step_event     one row per activity attempt outcome (interceptor, option B)
--        business_projection  latest business state per entity (projection activity, option C)
--   2. Developer-facing, loaded after workflows close (options A and D):
--        workflow_execution      one row per closed run
--        workflow_history_event  one row per history event
-- Every write is an idempotent upsert, because all sources are at-least-once.

CREATE TABLE IF NOT EXISTS audit_step_event (
    event_key      TEXT PRIMARY KEY,            -- runId:activityId:attempt:phase  (idempotency key)
    workflow_id    TEXT        NOT NULL,
    run_id         TEXT        NOT NULL,
    workflow_type  TEXT        NOT NULL,
    task_queue     TEXT        NOT NULL,
    activity_type  TEXT        NOT NULL,
    activity_id    TEXT        NOT NULL,
    attempt        INT         NOT NULL,
    phase          TEXT        NOT NULL CHECK (phase IN ('STARTED', 'COMPLETED', 'FAILED')),
    occurred_at    TIMESTAMPTZ NOT NULL,
    duration_ms    BIGINT,
    error_type     TEXT,
    error_message  TEXT,
    worker_sdk     TEXT        NOT NULL,         -- java | python
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_step_event_wf ON audit_step_event (workflow_id, occurred_at);

CREATE TABLE IF NOT EXISTS business_projection (
    entity_id      TEXT PRIMARY KEY,             -- business key, e.g. order id
    workflow_id    TEXT        NOT NULL,
    stage          TEXT        NOT NULL,
    status         TEXT        NOT NULL,
    stage_seq      INT         NOT NULL,         -- monotonically increasing; stale writes are ignored
    details        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
    source          TEXT        NOT NULL,        -- export | poll
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

-- Convenience view: one entity (e.g. order) "journey" combining business and step data.
CREATE OR REPLACE VIEW entity_journey AS
SELECT bp.entity_id, a.workflow_id, a.activity_type, a.phase, a.attempt, a.occurred_at, a.duration_ms, a.error_message, a.worker_sdk
FROM audit_step_event a
JOIN business_projection bp ON bp.workflow_id = a.workflow_id
ORDER BY a.occurred_at;
