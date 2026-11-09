"""Run with: python -m unittest discover -s audit/tests -v"""

import asyncio
from datetime import timedelta
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Replayer, Worker

import sweep_activity
from sweep_schedule import hourly_schedule
from sweep_workflow import HistorySweepWorkflow


class SweepActivityTests(unittest.IsolatedAsyncioTestCase):
    async def run_loader(self, code):
        spawn = asyncio.create_subprocess_exec
        processes = []

        async def substitute(*args):
            self.assertEqual(args[-3:], ("poll", "--overlap", "300"))
            process = await spawn(sys.executable, "-c", code)
            processes.append(process)
            return process

        with patch.object(sweep_activity.asyncio, "create_subprocess_exec", side_effect=substitute):
            await ActivityEnvironment().run(sweep_activity.sweep_closed_histories)
        return processes

    async def test_successful_loader_completes_activity(self):
        processes = await self.run_loader("pass")
        self.assertEqual(processes[0].returncode, 0)

    async def test_failed_loader_is_retryable(self):
        with self.assertRaises(ApplicationError) as error:
            await self.run_loader("raise SystemExit(2)")
        self.assertEqual(error.exception.type, "HistorySweepFailed")
        self.assertFalse(error.exception.non_retryable)

    async def test_cancel_stops_loader_process(self):
        spawn = asyncio.create_subprocess_exec
        processes = []
        started = asyncio.Event()

        async def substitute(*args):
            process = await spawn(sys.executable, "-c", "import time; time.sleep(300)")
            processes.append(process)
            started.set()
            return process

        with patch.object(sweep_activity.asyncio, "create_subprocess_exec", side_effect=substitute):
            task = asyncio.create_task(ActivityEnvironment().run(sweep_activity.sweep_closed_histories))
            try:
                await asyncio.wait_for(started.wait(), timeout=5)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=10)
                self.assertIsNotNone(processes[0].returncode)
            finally:
                if not task.done():
                    task.cancel()
                for process in processes:
                    if process.returncode is None:
                        process.kill()
                        await process.wait()


@unittest.skipUnless(shutil.which("temporal"), "Temporal CLI required for local-server test")
class SweepScheduleTests(unittest.IsolatedAsyncioTestCase):
    async def test_schedule_skips_overlap_retries_activity_and_replays(self):
        attempts = []
        started = asyncio.Event()
        release = asyncio.Event()

        @activity.defn(name="SweepClosedHistories")
        async def fake_sweep():
            attempts.append(activity.info().attempt)
            if activity.info().attempt == 1:
                raise ApplicationError("simulated loader failure")
            activity.heartbeat()
            started.set()
            await release.wait()

        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal"),
        ) as env:
            queue = f"sweep-test-{uuid4()}"
            async with Worker(
                env.client, task_queue=queue,
                workflows=[HistorySweepWorkflow], activities=[fake_sweep],
            ):
                schedule = hourly_schedule(queue, queue)
                schedule.state.paused = True  # Manual triggers only; avoid an hourly tick racing the test.
                handle = await env.client.create_schedule(queue, schedule)
                description = await handle.describe()
                self.assertEqual(description.schedule.spec.intervals[0].every, timedelta(hours=1))
                self.assertEqual(description.schedule.action.id, queue)
                try:
                    await handle.trigger()
                    await asyncio.wait_for(started.wait(), timeout=40)
                    await handle.trigger()  # SKIP while the first workflow is still open.
                    description = await handle.describe()
                    self.assertEqual(description.info.num_actions, 1)
                    execution = description.info.recent_actions[-1].action
                    workflow_handle = env.client.get_workflow_handle(
                        execution.workflow_id, run_id=execution.first_execution_run_id,
                    )
                    release.set()
                    await asyncio.wait_for(workflow_handle.result(), timeout=10)
                    self.assertEqual(attempts, [1, 2])
                    await Replayer(workflows=[HistorySweepWorkflow]).replay_workflow(
                        await workflow_handle.fetch_history(),
                    )
                finally:
                    release.set()
                    await handle.delete()


if __name__ == "__main__":
    unittest.main()
