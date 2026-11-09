#!/usr/bin/env python3
"""Load closed Temporal workflow histories into Postgres (options A and D).

  Option A (Cloud Workflow History Export files, protobuf temporal.api.export.v1.WorkflowExecutions):
      python history_loader.py export --path ./export-files            # local directory (recursive)
      python history_loader.py export --path s3://bucket/temporal-workflow-history/export/<ns>/2026/11/06/

  Option D (visibility query + history fetch, no export needed):
      python history_loader.py poll --since 2h            # one pass
      python history_loader.py poll --interval 60         # keep going, checkpointed in loader_checkpoint

Every row is an idempotent upsert keyed by (run_id, event_id), so re-delivered export files,
overlapping poll windows and re-runs are all safe.
"""

import argparse
import asyncio
import base64
import json
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import psycopg
from google.protobuf.json_format import MessageToDict
from temporalio.api.export.v1 import WorkflowExecutions
from temporalio.api.history.v1 import History
from temporalio.client import Client
from temporalio.envconfig import ClientConfig

CLOSE_STATUS = {
    "WORKFLOW_EXECUTION_COMPLETED": "COMPLETED",
    "WORKFLOW_EXECUTION_FAILED": "FAILED",
    "WORKFLOW_EXECUTION_TIMED_OUT": "TIMED_OUT",
    "WORKFLOW_EXECUTION_CANCELED": "CANCELED",
    "WORKFLOW_EXECUTION_TERMINATED": "TERMINATED",
    "WORKFLOW_EXECUTION_CONTINUED_AS_NEW": "CONTINUED_AS_NEW",
}


# ------------------------------------------------------------------ payload decoding
def decode_payloads(node):
    """Replace proto-JSON payloads ({metadata:{encoding}, data}) with readable values.

    json/plain is decoded to JSON. Anything else (binary/encrypted from a payload codec,
    protobuf, claim-check references) is left as a typed placeholder: decoding it needs the
    customer's codec and keys, which deliberately do not live in the loader.
    """
    if isinstance(node, list):
        return [decode_payloads(x) for x in node]
    if not isinstance(node, dict):
        return node
    meta = node.get("metadata")
    if isinstance(meta, dict) and "encoding" in meta and set(node) <= {"metadata", "data", "externalPayloads"}:
        encoding = base64.b64decode(meta["encoding"]).decode(errors="replace")
        raw = base64.b64decode(node.get("data", "")) if node.get("data") else b""
        if encoding == "json/plain":
            try:
                return json.loads(raw)
            except ValueError:
                return {"_encoding": encoding, "_bytes": len(raw)}
        if encoding == "binary/null":
            return None
        return {"_encoding": encoding, "_bytes": len(raw)}
    return {k: decode_payloads(v) for k, v in node.items()}


# ------------------------------------------------------------------ history -> rows
def history_rows(history: History, run_id_hint: str | None = None):
    """Return (execution_row, event_rows) for one closed history."""
    events = MessageToDict(history)["events"]
    first, last = events[0], events[-1]
    started = first["workflowExecutionStartedEventAttributes"]
    workflow_id = started.get("workflowId")
    # Export files carry no run id. original_execution_run_id is the run id of a normal run, but is
    # preserved across a workflow *reset*, so a reset run shares it with the run it was reset from.
    run_id = run_id_hint or started.get("originalExecutionRunId")
    activity_types = {}
    rows = []
    for e in events:
        etype = e["eventType"].removeprefix("EVENT_TYPE_")
        attrs_key = next((k for k in e if k.endswith("EventAttributes")), None)
        attrs = decode_payloads(e.get(attrs_key, {})) if attrs_key else {}
        activity_type = None
        if etype == "ACTIVITY_TASK_SCHEDULED":
            activity_type = attrs.get("activityType", {}).get("name")
            activity_types[e["eventId"]] = activity_type
        elif etype.startswith("ACTIVITY_TASK_"):
            activity_type = activity_types.get(str(attrs.get("scheduledEventId")))
        rows.append((run_id, int(e["eventId"]), workflow_id, e["eventTime"], etype, activity_type, json.dumps(attrs)))
    status = CLOSE_STATUS.get(last["eventType"].removeprefix("EVENT_TYPE_"), "UNKNOWN")
    execution = (
        run_id,
        workflow_id,
        started.get("workflowType", {}).get("name"),
        started.get("taskQueue", {}).get("name"),
        status,
        first["eventTime"],
        last["eventTime"] if status != "UNKNOWN" else None,
        len(events),
        started.get("parentWorkflowExecution", {}).get("workflowId"),
    )
    return execution, rows


UPSERT_EXECUTION = """
INSERT INTO workflow_execution (run_id, workflow_id, workflow_type, task_queue, status, start_time, close_time,
    history_length, parent_workflow_id, source)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status, close_time = EXCLUDED.close_time,
    history_length = EXCLUDED.history_length, loaded_at = now()
"""
UPSERT_EVENT = """
INSERT INTO workflow_history_event (run_id, event_id, workflow_id, event_time, event_type, activity_type, attributes)
VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (run_id, event_id) DO NOTHING
"""


def store(conn, history: History, source: str, run_id_hint: str | None = None) -> int:
    execution, rows = history_rows(history, run_id_hint)
    with conn.cursor() as cur:
        cur.execute(UPSERT_EXECUTION, (*execution, source))
        cur.executemany(UPSERT_EVENT, rows)
    conn.commit()
    return len(rows)


# ------------------------------------------------------------------ option A: export files
def iter_export_blobs(path: str):
    if path.startswith("s3://"):
        import boto3  # optional dependency, only for S3

        bucket, _, prefix = path[5:].partition("/")
        s3 = boto3.client("s3")
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield f"s3://{bucket}/{obj['Key']}", s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
    else:
        for f in sorted(pathlib.Path(path).rglob("*")):
            if f.is_file():
                yield str(f), f.read_bytes()


def load_export(path: str, dsn: str) -> None:
    files = runs = events = 0
    with psycopg.connect(dsn) as conn:
        for name, blob in iter_export_blobs(path):
            batch = WorkflowExecutions()
            batch.ParseFromString(blob)
            for item in batch.items:
                events += store(conn, item.history, "export")
                runs += 1
            files += 1
            print(f"  {name}: {len(batch.items)} workflow histories")
    print(f"export: {files} files, {runs} runs, {events} events upserted")


# ------------------------------------------------------------------ option D: visibility poll
async def connect() -> Client:
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    return await Client.connect(**config)


async def poll_once(client: Client, conn, since: datetime, overlap: timedelta) -> datetime:
    """Load every run closed after (since - overlap). Visibility is eventually consistent, so the
    window overlaps the previous one; upserts make the overlap free."""
    lower = (since - overlap).strftime("%Y-%m-%dT%H:%M:%SZ")
    query = f"ExecutionStatus != 'Running' AND CloseTime > '{lower}'"
    sem = asyncio.Semaphore(5)  # stay well under Cloud's 30 rps visibility limit and namespace RPS
    newest, count = since, 0

    async def one(wf):
        nonlocal newest, count
        async with sem:
            hist = await client.get_workflow_handle(wf.id, run_id=wf.run_id).fetch_history()
        store(conn, History(events=hist.events), "poll", wf.run_id)
        count += 1
        if wf.close_time and wf.close_time > newest:
            newest = wf.close_time

    await asyncio.gather(*[one(wf) async for wf in client.list_workflows(query)])
    print(f"poll: {count} closed runs since {lower}")
    return newest


async def load_poll(dsn: str, since: str | None, interval: int | None, overlap_s: int) -> None:
    client = await connect()
    with psycopg.connect(dsn) as conn:
        row = conn.execute("SELECT watermark FROM loader_checkpoint WHERE name = 'poll'").fetchone()
        if since:
            unit = since[-1]
            amount = int(since[:-1])
            watermark = datetime.now(timezone.utc) - {"m": timedelta(minutes=amount), "h": timedelta(hours=amount), "d": timedelta(days=amount)}[unit]
        elif row:
            watermark = row[0]
        else:
            watermark = datetime.now(timezone.utc) - timedelta(hours=1)
        while True:
            watermark = await poll_once(client, conn, watermark, timedelta(seconds=overlap_s))
            conn.execute(
                "INSERT INTO loader_checkpoint (name, watermark) VALUES ('poll', %s) "
                "ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark",
                (watermark,),
            )
            conn.commit()
            if not interval:
                return
            await asyncio.sleep(interval)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=os.environ.get("PG_DSN", "postgresql://temporal:temporal@localhost:55432/audit"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("--path", required=True)
    p = sub.add_parser("poll")
    p.add_argument("--since", help="e.g. 30m, 2h, 1d (overrides the checkpoint)")
    p.add_argument("--interval", type=int, help="seconds between passes; omit for a single pass")
    p.add_argument("--overlap", type=int, default=300, help="seconds of window overlap (visibility lag)")
    a = ap.parse_args()

    if a.cmd == "export":
        load_export(a.path, a.dsn)
    else:
        asyncio.run(load_poll(a.dsn, a.since, a.interval, a.overlap))


if __name__ == "__main__":
    sys.exit(main())
