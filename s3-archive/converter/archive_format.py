"""Pure conversion logic: one Cloud export batch -> one archive object per run. No I/O."""

from dataclasses import dataclass
from urllib.parse import quote

from google.protobuf.json_format import MessageToJson
from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.api.export.v1 import WorkflowExecutions
from temporalio.api.history.v1 import History

CLOSE_STATUS = {
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED: "Completed",
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED: "Failed",
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT: "TimedOut",
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED: "Terminated",
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED: "Canceled",
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW: "ContinuedAsNew",
}


@dataclass(frozen=True)
class ArchivedRun:
    workflow_id: str
    run_id: str
    workflow_type: str
    status: str
    start_time: str
    close_time: str
    history_json: bytes


def run_id_of(history: History) -> str:
    """originalExecutionRunId, except reset runs, which take new_run_id from the last reset task failure."""
    run_id = history.events[0].workflow_execution_started_event_attributes.original_execution_run_id
    for event in history.events:
        failed = event.workflow_task_failed_event_attributes
        if failed.cause == WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW and failed.new_run_id:
            run_id = failed.new_run_id
    return run_id


def to_archived_run(history: History) -> ArchivedRun:
    if not history.events or not history.events[0].HasField("workflow_execution_started_event_attributes"):
        raise ValueError("history does not start with WorkflowExecutionStarted")
    first, last = history.events[0], history.events[-1]
    if last.event_type not in CLOSE_STATUS:
        raise ValueError("history has no terminal event")
    started = first.workflow_execution_started_event_attributes
    run_id = run_id_of(history)
    if not started.workflow_id or not run_id:
        raise ValueError("history is missing workflow or run ID")
    return ArchivedRun(
        workflow_id=started.workflow_id,
        run_id=run_id,
        workflow_type=started.workflow_type.name,
        status=CLOSE_STATUS[last.event_type],
        start_time=first.event_time.ToJsonString(),
        close_time=last.event_time.ToJsonString(),
        history_json=MessageToJson(history, indent=None).encode(),
    )


def runs_in_batch(data: bytes) -> tuple[list[ArchivedRun], list[str]]:
    """Returns the readable runs and one error per unreadable item, so one bad history does not drop the batch."""
    runs, errors = [], []
    for i, item in enumerate(WorkflowExecutions.FromString(data).items):
        try:
            runs.append(to_archived_run(item.history))
        except ValueError as err:
            errors.append(f"item {i}: {err}")
    return runs, errors


def encode_workflow_id(workflow_id: str) -> str:
    """Workflow IDs may contain '/', so they are percent-encoded into a single key segment."""
    return quote(workflow_id, safe="")


def run_key(prefix: str, namespace: str, workflow_id: str, run_id: str) -> str:
    return f"{prefix}/{namespace}/{encode_workflow_id(workflow_id)}/{run_id}.json"


def marker_key(prefix: str, export_key: str) -> str:
    return f"{prefix}/_converted/{export_key}.done"


def export_hour_prefix(namespace: str, hour: str) -> str:
    """hour is 'YYYY/MM/DD/HH' (UTC). Cloud writes each batch under minute folder 00."""
    return f"temporal-workflow-history/export/{namespace}/{hour}/00/"
