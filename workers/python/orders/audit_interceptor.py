"""Option B: record every activity attempt to Postgres with no code in the workflows.

Activity-side only (I/O is allowed there). Workflow-side interceptors must not do I/O, so workflow
start/close, signals and timers come from the history loader (options A/D) instead.
An audit write failure is logged and swallowed; the activity result is never affected.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any

import psycopg
from temporalio import activity
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
)

log = logging.getLogger(__name__)

INSERT = """
INSERT INTO audit_step_event (event_key, workflow_id, run_id, workflow_type, task_queue, activity_type,
    activity_id, attempt, phase, occurred_at, duration_ms, error_type, error_message, worker_sdk)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'python') ON CONFLICT (event_key) DO NOTHING
"""


class AuditInterceptor(Interceptor):
    def __init__(self, dsn: str, excluded_activity_types: frozenset[str] = frozenset()) -> None:
        self.dsn = dsn
        self.excluded = excluded_activity_types

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _AuditActivityInbound(next, self)


class _AuditActivityInbound(ActivityInboundInterceptor):
    def __init__(self, next: ActivityInboundInterceptor, root: AuditInterceptor) -> None:
        super().__init__(next)
        self.root = root

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        info = activity.info()
        if info.activity_type in self.root.excluded:
            return await super().execute_activity(input)
        start = time.monotonic()
        await self._record(info, "STARTED", None, None)
        try:
            result = await super().execute_activity(input)
        except BaseException as e:
            await self._record(info, "FAILED", start, e)
            raise
        await self._record(info, "COMPLETED", start, None)
        return result

    async def _record(self, info: activity.Info, phase: str, start: float | None, err: BaseException | None) -> None:
        key = f"{info.workflow_run_id}:{info.activity_id}:{info.attempt}:{phase}"
        try:
            async with await psycopg.AsyncConnection.connect(self.root.dsn, connect_timeout=2) as conn:
                await conn.execute(
                    INSERT,
                    (
                        key,
                        info.workflow_id,
                        info.workflow_run_id,
                        info.workflow_type,
                        info.task_queue,
                        info.activity_type,
                        info.activity_id,
                        info.attempt,
                        phase,
                        datetime.now(timezone.utc),
                        None if start is None else int((time.monotonic() - start) * 1000),
                        None if err is None else type(err).__name__,
                        None if err is None else str(err)[:1000],
                    ),
                )
            # Exported through the SDK runtime's metrics pipeline, next to the SDK's own metrics.
            activity.metric_meter().create_counter("audit_writes").add(1, {"sdk": "python"})
        except Exception as e:  # never let auditing break the business activity
            activity.metric_meter().create_counter("audit_write_failures").add(1, {"sdk": "python"})
            log.warning("audit write failed (%s): %s", key, e)
