#!/usr/bin/env python3
"""Run the sweep worker or create its hourly Temporal Schedule (once)."""

import argparse
import asyncio
from datetime import timedelta
import os

from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)
from temporalio.worker import Worker

from history_loader import connect
from sweep_activity import sweep_closed_histories
from sweep_workflow import HistorySweepWorkflow


TASK_QUEUE = "history-sweep"
SCHEDULE_ID = "history-sweep"


def hourly_schedule(task_queue: str = TASK_QUEUE, workflow_id: str = SCHEDULE_ID) -> Schedule:
    return Schedule(
        action=ScheduleActionStartWorkflow(
            HistorySweepWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))]),
        policy=SchedulePolicy(
            overlap=ScheduleOverlapPolicy.SKIP,
            catchup_window=timedelta(days=1),
        ),
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["worker", "create-schedule"])
    parser.add_argument("--task-queue", default=TASK_QUEUE)
    parser.add_argument("--schedule-id", default=SCHEDULE_ID)
    args = parser.parse_args()
    if args.command == "worker" and not os.environ.get("PG_DSN"):
        parser.error("PG_DSN is required for the sweep worker")
    client = await connect()
    if args.command == "create-schedule":
        # Fail if the ID already exists rather than silently replacing its policy.
        await client.create_schedule(
            args.schedule_id, hourly_schedule(args.task_queue, args.schedule_id),
        )
        print(f"Created Schedule {args.schedule_id} in {client.namespace}")
    else:
        async with Worker(
            client,
            task_queue=args.task_queue,
            workflows=[HistorySweepWorkflow],
            activities=[sweep_closed_histories],
            max_concurrent_activities=1,
            graceful_shutdown_timeout=timedelta(seconds=30),
        ):
            print(f"Sweep worker: namespace={client.namespace} queue={args.task_queue}", flush=True)
            await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
