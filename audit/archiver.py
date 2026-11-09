#!/usr/bin/env python3
"""Option E: fetch a closed workflow's history and project its events into Postgres.

Every worker registers an "archive on close" interceptor (Java: ArchiveOnCloseInterceptor,
Python: orders/archive_interceptor.py). When a workflow's code finishes, the interceptor starts
this HistoryArchiverWorkflow as an ABANDONed child, then lets the parent close. The archiver:

  1. waits until the parent run is closed (activity retries while it is still running),
  2. fetches the complete history, close event included,
  3. upserts it into workflow_execution / workflow_history_event (same loader code as options A/D).

Database writes run in the child with their own retries. The parent waits for child-start
confirmation, not archive completion. Payload conversion is subject to the loader's limits.
Runs that never execute workflow code at the end (terminated, execution timeout) are not covered:
keep `history_loader.py poll` as a periodic sweep for those.

    python archiver_worker.py     # runs the archiver worker on task queue "history-archiver"
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    import psycopg
    from temporalio.api.enums.v1 import WorkflowExecutionStatus
    from temporalio.api.history.v1 import History
    from temporalio.client import Client
    from temporalio.exceptions import ApplicationError

    import history_loader

TASK_QUEUE = "history-archiver"


class ArchiveActivities:
    def __init__(self, client: Client, dsn: str) -> None:
        self.client = client
        self.dsn = dsn

    @activity.defn(name="ArchiveHistory")
    async def archive_history(self, workflow_id: str, run_id: str) -> int:
        handle = self.client.get_workflow_handle(workflow_id, run_id=run_id)
        desc = await handle.describe()
        if desc.raw_description.workflow_execution_info.status == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING:
            # The parent starts us just before it closes; retry (with backoff) until it has.
            raise ApplicationError("parent run still open", type="ParentStillRunning")
        hist = await handle.fetch_history()
        history = History(events=hist.events)

        def write() -> int:
            with psycopg.connect(self.dsn, connect_timeout=5) as conn:
                return history_loader.store(conn, history, "archiver", run_id)

        return await asyncio.to_thread(write)


@workflow.defn(name="HistoryArchiverWorkflow")
class HistoryArchiverWorkflow:
    @workflow.run
    async def run(self, workflow_id: str, run_id: str) -> int:
        return await workflow.execute_activity_method(
            ArchiveActivities.archive_history,
            args=[workflow_id, run_id],
            start_to_close_timeout=timedelta(minutes=2),
            # Keep trying for a week (database outage, slow close); never give up silently sooner.
            schedule_to_close_timeout=timedelta(days=7),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=5),
            ),
        )
