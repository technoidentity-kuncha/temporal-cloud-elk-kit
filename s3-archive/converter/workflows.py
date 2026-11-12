"""ExportConverterWorkflow: converts Cloud export folders into per-run archive objects."""

import asyncio
from datetime import datetime, timedelta, timezone

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey

from shared import ConvertInput, ConvertResult, FileInput, FileResult, HourInput

SCHEDULED_START = SearchAttributeKey.for_datetime("TemporalScheduledStartTime")
RETRY = RetryPolicy(initial_interval=timedelta(seconds=5), maximum_interval=timedelta(minutes=5))
PARALLEL_FILES = 10


@workflow.defn
class ExportConverterWorkflow:
    @workflow.run
    async def run(self, inp: ConvertInput) -> ConvertResult:
        hours = inp.hours or self._scheduled_hours(inp)
        keys: list[str] = []
        for hour in hours:
            keys += await workflow.execute_activity(
                "list_unconverted", HourInput(inp.namespace, hour), result_type=list[str],
                start_to_close_timeout=timedelta(minutes=2), retry_policy=RETRY)

        results: list[FileResult] = []
        for i in range(0, len(keys), PARALLEL_FILES):
            results += await asyncio.gather(*[
                workflow.execute_activity(
                    "convert_file", FileInput(inp.namespace, key), result_type=FileResult,
                    start_to_close_timeout=timedelta(minutes=15), heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=RETRY)
                for key in keys[i:i + PARALLEL_FILES]])

        return ConvertResult(hours=hours, files=len(results), runs=sum(r.runs for r in results),
                             quarantined=[r.export_key for r in results if r.quarantined])

    @staticmethod
    def _scheduled_hours(inp: ConvertInput) -> list[str]:
        scheduled = workflow.info().typed_search_attributes.get(SCHEDULED_START) or workflow.now()
        base = scheduled.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return [(base - timedelta(hours=inp.lag_hours + i)).strftime("%Y/%m/%d/%H")
                for i in range(inp.lookback_hours)]
