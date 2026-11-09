"""Business-workflow fixtures; all archival components are the repository implementations."""

from datetime import timedelta

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError


@activity.defn
async def echo(value: dict) -> dict:
    return value


@workflow.defn
class LifecycleWorkflow:
    def __init__(self):
        self.released = False

    @workflow.run
    async def run(self, mode: str) -> dict:
        if mode == "wait":
            await workflow.wait_condition(lambda: self.released)
        if mode == "fail":
            raise ApplicationError("expected business failure", type="BusinessFailure", non_retryable=True)
        if mode == "continue":
            workflow.continue_as_new("complete")
        if mode == "child":
            return await workflow.execute_child_workflow(
                LifecycleWorkflow.run, "complete", id=workflow.info().workflow_id + "/child",
            )
        return await workflow.execute_activity(
            echo, {"order": "test-123", "accepted": True, "optional": None},
            start_to_close_timeout=timedelta(seconds=10),
        )

    @workflow.signal
    def release(self):
        self.released = True

    @workflow.query
    def ready(self) -> bool:
        return True
