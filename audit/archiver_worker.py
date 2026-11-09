#!/usr/bin/env python3
"""Runs the option E history archiver worker (task queue "history-archiver")."""

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from archiver import TASK_QUEUE, ArchiveActivities, HistoryArchiverWorkflow


async def main() -> None:
    api_key = os.environ.get("TEMPORAL_API_KEY")
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        api_key=api_key or None,
        tls=bool(api_key),
    )
    dsn = os.environ.get("PG_DSN", "postgresql://temporal:temporal@localhost:55432/audit")
    acts = ArchiveActivities(client, dsn)
    worker = Worker(client, task_queue=TASK_QUEUE, workflows=[HistoryArchiverWorkflow], activities=[acts.archive_history])
    print(f"history archiver started: namespace={client.namespace} taskQueue={TASK_QUEUE}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
