"""Run the existing one-pass loader without blocking the Temporal worker loop."""

import asyncio
from pathlib import Path
import sys

from temporalio import activity
from temporalio.exceptions import ApplicationError

LOADER_PATH = Path(__file__).with_name("history_loader.py")


@activity.defn(name="SweepClosedHistories")
async def sweep_closed_histories() -> None:
    # Credentials stay in the worker environment, never in workflow inputs/history.
    # A subprocess isolates the loader's synchronous Postgres calls. Logs inherit
    # worker stdout/stderr; only a successful pass advances the loader checkpoint.
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(LOADER_PATH), "poll", "--overlap", "300",
    )
    try:
        while process.returncode is None:
            activity.heartbeat()
            try:
                await asyncio.wait_for(process.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
        if process.returncode != 0:
            raise ApplicationError(
                f"History loader exited with code {process.returncode}; see worker logs",
                type="HistorySweepFailed",
            )
    finally:
        # Stop the loader on Activity cancellation/timeout before acknowledging it.
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
