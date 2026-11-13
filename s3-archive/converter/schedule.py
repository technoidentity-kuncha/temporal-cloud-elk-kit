"""Manage the hourly converter Schedule.

  python schedule.py create [--paused]          # hourly at :45, converts the previous hour's export folder
  python schedule.py trigger|pause|unpause|describe
"""

import asyncio
import os
import sys
from datetime import timedelta

from temporalio.client import (Client, Schedule, ScheduleActionStartWorkflow, SchedulePolicy, ScheduleSpec,
                               ScheduleState)
from temporalio.envconfig import ClientConfig

from shared import TASK_QUEUE, ConvertInput
from workflows import ExportConverterWorkflow


async def main(cmd: str, paused: bool) -> None:
    namespace = os.environ["EXPORTED_NAMESPACE"]
    client = await Client.connect(**ClientConfig.load_client_connect_config())
    schedule_id = f"s3-archive-converter-{namespace}"
    if cmd == "create":
        await client.create_schedule(schedule_id, Schedule(
            action=ScheduleActionStartWorkflow(
                ExportConverterWorkflow.run, ConvertInput(namespace=namespace),
                id=f"s3-archive-converter-{namespace}",
                task_queue=os.environ.get("CONVERTER_TASK_QUEUE", TASK_QUEUE),
                execution_timeout=timedelta(minutes=50)),  # a stuck run must not block the next hour
            spec=ScheduleSpec(cron_expressions=["45 * * * *"]),  # export lands ~:15 past the next hour
            policy=SchedulePolicy(catchup_window=timedelta(days=1)),
            state=ScheduleState(paused=paused, note="created by s3-archive/converter/schedule.py")))
        print(f"created {schedule_id} (paused={paused})")
        return
    handle = client.get_schedule_handle(schedule_id)
    if cmd == "trigger":
        await handle.trigger()
    elif cmd == "pause":
        await handle.pause()
    elif cmd == "unpause":
        await handle.unpause()
    desc = await handle.describe()
    print(schedule_id, "paused" if desc.schedule.state.paused else "active",
          "| recent:", [a.scheduled_at.isoformat() for a in desc.info.recent_actions][-3:])


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], "--paused" in sys.argv))
