"""Option E (Python side): when any workflow's code finishes, start the history archiver as an
ABANDONed child so the complete history lands in Postgres seconds after the run closes.

No per-workflow code: registered once on the Worker. Never fails the workflow: any error starting
the archiver is logged and swallowed (the periodic poll sweep catches what was missed).
"""

import asyncio
from typing import Any

from temporalio import workflow
from temporalio.workflow import ParentClosePolicy
from temporalio.worker import (
    ExecuteWorkflowInput,
    Interceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)

ARCHIVER_TYPE = "HistoryArchiverWorkflow"
ARCHIVER_QUEUE = "history-archiver"
# Workflows that started before this interceptor was deployed must replay without the new
# child-workflow command; patched() records a marker so old histories skip it.
PATCH_ID = "archive-on-close"


class ArchiveOnCloseInterceptor(Interceptor):
    def workflow_interceptor_class(self, input: WorkflowInterceptorClassInput) -> type[WorkflowInboundInterceptor]:
        return _ArchiveOnCloseInbound


class _ArchiveOnCloseInbound(WorkflowInboundInterceptor):
    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        try:
            result = await super().execute_workflow(input)
        except (Exception, asyncio.CancelledError, workflow.ContinueAsNewError):
            # Failed, cancelled or continued-as-new: archive, then re-raise unchanged.
            # Other BaseExceptions (e.g. the SDK evicting the workflow from cache) pass through untouched.
            await _start_archiver()
            raise
        await _start_archiver()
        return result


async def _start_archiver() -> None:
    info = workflow.info()
    if info.workflow_type == ARCHIVER_TYPE or not workflow.patched(PATCH_ID):
        return
    try:
        await workflow.start_child_workflow(
            ARCHIVER_TYPE,
            args=[info.workflow_id, info.run_id],
            id=f"archive/{info.run_id}",
            task_queue=ARCHIVER_QUEUE,
            parent_close_policy=ParentClosePolicy.ABANDON,
        )
    except (Exception, asyncio.CancelledError) as e:  # never let archiving change the outcome
        workflow.logger.warning("history archiver not started for %s: %r", info.workflow_id, e)
