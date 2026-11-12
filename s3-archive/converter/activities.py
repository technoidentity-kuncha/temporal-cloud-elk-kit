"""S3 activities: list unconverted export files, convert one file into per-run archive objects."""

import os

import boto3
from temporalio import activity

from archive_format import export_hour_prefix, marker_key, run_key, runs_in_batch
from shared import FileInput, FileResult, HourInput

EXPORT_BUCKET = os.environ["EXPORT_BUCKET"]
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET") or EXPORT_BUCKET
ARCHIVE_PREFIX = os.environ.get("ARCHIVE_PREFIX", "archive").strip("/")

s3 = boto3.client("s3")


def _keys(bucket: str, prefix: str) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


@activity.defn
def list_unconverted(inp: HourInput) -> list[str]:
    prefix = export_hour_prefix(inp.namespace, inp.hour)
    done = set(_keys(ARCHIVE_BUCKET, f"{ARCHIVE_PREFIX}/_converted/{prefix}"))
    return [k for k in _keys(EXPORT_BUCKET, prefix) if marker_key(ARCHIVE_PREFIX, k) not in done]


@activity.defn
def convert_file(inp: FileInput) -> FileResult:
    data = s3.get_object(Bucket=EXPORT_BUCKET, Key=inp.export_key)["Body"].read()
    try:
        runs, errors = runs_in_batch(data)
    except Exception as err:  # the file itself is not a WorkflowExecutions protobuf
        runs, errors = [], [f"file: {err}"]
    if errors:
        activity.logger.error("Export file %s has unreadable items: %s", inp.export_key, errors)
        s3.copy_object(Bucket=ARCHIVE_BUCKET, Key=f"{ARCHIVE_PREFIX}/_quarantine/{inp.export_key}",
                       CopySource={"Bucket": EXPORT_BUCKET, "Key": inp.export_key})
        s3.put_object(Bucket=ARCHIVE_BUCKET, Key=f"{ARCHIVE_PREFIX}/_quarantine/{inp.export_key}.errors.txt",
                      Body="\n".join(errors).encode())
    for run in runs:
        s3.put_object(
            Bucket=ARCHIVE_BUCKET,
            Key=run_key(ARCHIVE_PREFIX, inp.namespace, run.workflow_id, run.run_id),
            Body=run.history_json,
            ContentType="application/json",
            Metadata={
                "workflow-type": run.workflow_type,
                "status": run.status,
                "start-time": run.start_time,
                "close-time": run.close_time,
            },
        )
        activity.heartbeat(run.run_id)
    s3.put_object(Bucket=ARCHIVE_BUCKET, Key=marker_key(ARCHIVE_PREFIX, inp.export_key), Body=str(len(runs)).encode())
    return FileResult(inp.export_key, len(runs), quarantined=bool(errors))
