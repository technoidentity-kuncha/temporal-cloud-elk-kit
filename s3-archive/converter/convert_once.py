"""Run the converter once for explicit export hours (first run, backfill, tests).

  python convert_once.py 2026/11/12/15 2026/11/12/16     # hours are UTC export folders
"""

import asyncio
import os
import sys
import time
from datetime import timedelta

from temporalio.client import Client
from temporalio.envconfig import ClientConfig

from shared import TASK_QUEUE, ConvertInput, ConvertResult
from workflows import ExportConverterWorkflow


async def main(hours: list[str]) -> None:
    client = await Client.connect(**ClientConfig.load_client_connect_config())
    result: ConvertResult = await client.execute_workflow(
        ExportConverterWorkflow.run, ConvertInput(namespace=os.environ["EXPORTED_NAMESPACE"], hours=hours),
        id=f"s3-archive-convert-once-{int(time.time())}",
        task_queue=os.environ.get("CONVERTER_TASK_QUEUE", TASK_QUEUE), result_type=ConvertResult,
        execution_timeout=timedelta(hours=2))
    print(result)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
