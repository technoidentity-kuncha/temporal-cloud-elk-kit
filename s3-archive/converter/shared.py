"""Types shared by the workflow and activities. Keep this module free of I/O imports (workflow sandbox)."""

from dataclasses import dataclass, field

TASK_QUEUE = "s3-archive-converter"


@dataclass
class ConvertInput:
    namespace: str
    hours: list[str] = field(default_factory=list)  # 'YYYY/MM/DD/HH' (UTC); empty = derive from the schedule time
    lag_hours: int = 1  # Cloud writes folder H at about H+1:15, so a run at H+1:45 reads H
    lookback_hours: int = 6  # re-check earlier folders so a failed hour is retried; markers make this cheap


@dataclass
class HourInput:
    namespace: str
    hour: str


@dataclass
class FileInput:
    namespace: str
    export_key: str


@dataclass
class FileResult:
    export_key: str
    runs: int
    quarantined: bool = False


@dataclass
class ConvertResult:
    hours: list[str]
    files: int
    runs: int
    quarantined: list[str]
