"""One scheduled, retryable history sweep; database/API I/O runs in an Activity."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy


@workflow.defn
class HistorySweepWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            "SweepClosedHistories",
            start_to_close_timeout=timedelta(minutes=20),
            schedule_to_close_timeout=timedelta(minutes=55),
            heartbeat_timeout=timedelta(seconds=30),
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                maximum_interval=timedelta(minutes=1),
                maximum_attempts=3,
            ),
        )
