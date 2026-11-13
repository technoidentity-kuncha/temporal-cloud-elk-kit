"""Converter worker. Temporal connection from TEMPORAL_ADDRESS / TEMPORAL_NAMESPACE / TEMPORAL_API_KEY;
AWS credentials from the standard AWS chain (e.g. AWS_PROFILE)."""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from activities import convert_file, list_unconverted
from shared import TASK_QUEUE
from workflows import ExportConverterWorkflow


async def main() -> None:
    client = await Client.connect(**ClientConfig.load_client_connect_config())
    queue = os.environ.get("CONVERTER_TASK_QUEUE", TASK_QUEUE)
    with ThreadPoolExecutor(max_workers=10) as pool:
        worker = Worker(client, task_queue=queue, workflows=[ExportConverterWorkflow],
                        activities=[list_unconverted, convert_file], activity_executor=pool)
        print(f"converter worker started: namespace={client.namespace} taskQueue={queue}", flush=True)
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
