"""Real Temporal dev server + real Postgres. No mocked archive, sweep, API, or DB calls."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg.conninfo import make_conninfo
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType, ResetReapplyType
from temporalio.api.export.v1 import WorkflowExecutions
from temporalio.api.history.v1 import History
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.client import Client, WorkflowFailureError
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError, CancelledError, TerminatedError, TimeoutError as TemporalTimeoutError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "audit"), str(ROOT / "workers/python")]

import history_loader
from archiver import ArchiveActivities, HistoryArchiverWorkflow
from orders.archive_interceptor import ArchiveOnCloseInterceptor
from sweep_activity import sweep_closed_histories
from sweep_schedule import hourly_schedule
from sweep_workflow import HistorySweepWorkflow
from workflows import LifecycleWorkflow, echo
from codec import EncryptedPayloadCodec


class ArchivalE2ETests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        if not shutil.which("temporal") or not os.environ.get("E2E_PG_DSN"):
            self.fail("Temporal CLI and E2E_PG_DSN are required; see the docstring in scripts/test_archival_e2e.py")
        self.stack = AsyncExitStack()
        self.schema = "e2e_" + uuid4().hex
        self.admin_dsn = os.environ["E2E_PG_DSN"]
        with psycopg.connect(self.admin_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{self.schema}"')
        self.addCleanup(self.drop_schema)
        self.addAsyncCleanup(self.stack.aclose)
        self.dsn = make_conninfo(self.admin_dsn, options=f"-csearch_path={self.schema}")
        with psycopg.connect(self.dsn) as conn:
            conn.execute((ROOT / "audit/schema.sql").read_text())
        self.env = await self.stack.enter_async_context(await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal"),
        ))
        self.client = self.env.client
        self.queue = "business-" + uuid4().hex
        self.since = datetime.now(timezone.utc) - timedelta(minutes=5)
        self.histories = {}
        self.tempdir = tempfile.TemporaryDirectory()
        self.stack.callback(self.tempdir.cleanup)
        self.config_file = Path(self.tempdir.name, "temporal.toml")
        self.config_file.write_text('[profile.default]\nnamespace="default"\n')

    def drop_schema(self):
        with psycopg.connect(self.admin_dsn, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')

    def rows(self, query, params=()):
        with psycopg.connect(self.dsn) as conn:
            cur = conn.execute(query, params)
            return cur.fetchall() if cur.description else []

    async def workers(self, *, intercept=True, archiver=True):
        await self.stack.enter_async_context(Worker(
            self.client, task_queue=self.queue, workflows=[LifecycleWorkflow], activities=[echo],
            interceptors=[ArchiveOnCloseInterceptor()] if intercept else [],
        ))
        if archiver:
            await self.archive_worker()

    async def archive_worker(self, dsn=None):
        await self.stack.enter_async_context(Worker(
            self.client, task_queue="history-archiver", workflows=[HistoryArchiverWorkflow],
            activities=[ArchiveActivities(self.client, dsn or self.dsn).archive_history],
        ))

    async def start(self, mode="complete", **kwargs):
        return await self.client.start_workflow(
            LifecycleWorkflow.run, mode, id="case-" + uuid4().hex, task_queue=self.queue, **kwargs,
        )

    async def wait_until(self, predicate, *, timeout=40):
        async with asyncio.timeout(timeout):
            while not await predicate():
                await asyncio.sleep(0.2)

    async def assert_archived(self, handle, status, *, via_child=True):
        run_id = handle.run_id or handle.first_execution_run_id
        handle = self.client.get_workflow_handle(handle.id, run_id=run_id)
        if via_child:
            archive = self.client.get_workflow_handle("archive/" + run_id)
            await asyncio.wait_for(archive.result(), timeout=40)
        hist = await handle.fetch_history()
        self.histories[run_id] = hist
        summary = self.rows(
            "SELECT workflow_id, status, history_length FROM workflow_execution WHERE run_id=%s", (run_id,),
        )
        self.assertEqual(summary, [(handle.id, status, len(hist.events))])
        stored = self.rows(
            "SELECT event_id, event_type FROM workflow_history_event WHERE run_id=%s ORDER BY event_id", (run_id,),
        )
        expected = [(e.event_id, EventType.Name(e.event_type).removeprefix("EVENT_TYPE_")) for e in hist.events]
        self.assertEqual(stored, expected)
        self.assertEqual(stored[-1][1], "WORKFLOW_EXECUTION_" + status)
        return hist

    async def poll_visible(self, handles):
        ids = {h.run_id or h.first_execution_run_id for h in handles}

        async def visible():
            found = {w.run_id async for w in self.client.list_workflows("ExecutionStatus != 'Running'")}
            return ids <= found

        await self.wait_until(visible)
        with psycopg.connect(self.dsn) as conn:
            await history_loader.poll_once(self.client, conn, self.since, timedelta(seconds=300))

    def loader_environment(self):
        # Exclude ambient Cloud credentials and profiles. This only configures the real subprocess.
        return self.loader_environment_from(os.environ)

    async def process(self, *command):
        log_path = Path(self.tempdir.name, f"process-{uuid4().hex}.log")
        log = self.stack.enter_context(log_path.open("w"))
        proc = await asyncio.create_subprocess_exec(
            *command, env=self.loader_environment(), stdout=log, stderr=asyncio.subprocess.STDOUT,
        )

        async def stop():
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=10)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
        self.stack.push_async_callback(stop)
        return proc, log_path

    async def loader(self, *args):
        proc = await asyncio.create_subprocess_exec(
            sys.executable, str(ROOT / "audit/history_loader.py"), *args,
            env=self.loader_environment(), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            output, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except BaseException:
            if proc.returncode is None:
                proc.kill()
            await proc.wait()
            raise
        return proc.returncode, output.decode()

    async def test_completion_failure_children_and_duplicate_ingestion(self):
        await self.workers()
        success = await self.start("child")
        self.assertEqual((await success.result())["order"], "test-123")
        await self.assert_archived(success, "COMPLETED")
        child = await self.client.get_workflow_handle(success.id + "/child").describe()
        await self.assert_archived(self.client.get_workflow_handle(child.id, run_id=child.run_id), "COMPLETED")
        failure = await self.start("fail")
        with self.assertRaises(WorkflowFailureError) as error:
            await failure.result()
        self.assertIsInstance(error.exception.cause, ApplicationError)
        self.assertEqual(error.exception.cause.type, "BusinessFailure")
        await self.assert_archived(failure, "FAILED")
        before = self.rows("SELECT run_id, event_id, attributes FROM workflow_history_event ORDER BY run_id, event_id")
        for run_id, hist in self.histories.items():
            with psycopg.connect(self.dsn) as conn:
                history_loader.store(conn, History(events=hist.events), "poll", run_id)
        self.assertEqual(before, self.rows("SELECT run_id, event_id, attributes FROM workflow_history_event ORDER BY run_id, event_id"))
        self.assertTrue(self.rows("SELECT 1 FROM workflow_history_event WHERE activity_type='echo'"))
        for hist in self.histories.values():
            await Replayer(workflows=[LifecycleWorkflow], interceptors=[ArchiveOnCloseInterceptor()]).replay_workflow(hist)

    async def test_cancellation_preserves_outcome_and_archives(self):
        await self.workers()
        handle = await self.start("wait")
        self.assertTrue(await handle.query(LifecycleWorkflow.ready))
        await handle.cancel()
        with self.assertRaises(WorkflowFailureError) as error:
            await handle.result()
        self.assertIsInstance(error.exception.cause, CancelledError)
        hist = await self.assert_archived(handle, "CANCELED")
        await Replayer(workflows=[LifecycleWorkflow], interceptors=[ArchiveOnCloseInterceptor()]).replay_workflow(hist)

    async def test_continue_as_new_archives_each_run_and_replays(self):
        await self.workers()
        first = await self.start("continue")
        await asyncio.wait_for(first.result(), timeout=40)
        first_hist = await self.assert_archived(first, "CONTINUED_AS_NEW")
        next_id = first_hist.events[-1].workflow_execution_continued_as_new_event_attributes.new_execution_run_id
        last = self.client.get_workflow_handle(first.id, run_id=next_id)
        await self.assert_archived(last, "COMPLETED")
        self.assertNotEqual(first.first_execution_run_id, next_id)
        for hist in self.histories.values():
            await Replayer(workflows=[LifecycleWorkflow], interceptors=[ArchiveOnCloseInterceptor()]).replay_workflow(hist)

    async def test_schedule_recovers_termination_timeout_and_uninstrumented_runs(self):
        await self.workers(intercept=False, archiver=False)
        terminated = await self.start("wait")
        await terminated.query(LifecycleWorkflow.ready)
        await terminated.terminate("E2E termination")
        timeout = await self.start("wait", execution_timeout=timedelta(seconds=2))
        for handle, cause in [(terminated, TerminatedError), (timeout, TemporalTimeoutError)]:
            with self.assertRaises(WorkflowFailureError) as error:
                await handle.result()
            self.assertIsInstance(error.exception.cause, cause)
        completed = await self.start()
        await completed.result()
        # Wait for Visibility, without loading anything into Postgres.
        async def visible():
            ids = {w.run_id async for w in self.client.list_workflows("ExecutionStatus != 'Running'")}
            return {h.first_execution_run_id for h in [terminated, timeout, completed]} <= ids
        await self.wait_until(visible)
        original_env = os.environ.copy()
        os.environ.clear()
        os.environ.update(self.loader_environment_from(original_env))
        try:
            queue = "sweep-" + uuid4().hex
            async with Worker(self.client, task_queue=queue, workflows=[HistorySweepWorkflow], activities=[sweep_closed_histories]):
                schedule = hourly_schedule(queue, queue)
                schedule.state.paused = True
                handle = await self.client.create_schedule(queue, schedule)
                try:
                    await handle.trigger()
                    async def launched():
                        return bool((await handle.describe()).info.recent_actions)
                    await self.wait_until(launched)
                    action = (await handle.describe()).info.recent_actions[-1].action
                    sweep = self.client.get_workflow_handle(action.workflow_id, run_id=action.first_execution_run_id)
                    await asyncio.wait_for(sweep.result(), timeout=45)
                    await Replayer(workflows=[HistorySweepWorkflow]).replay_workflow(await sweep.fetch_history())
                finally:
                    await handle.delete()
        finally:
            os.environ.clear()
            os.environ.update(original_env)
        for h, status in [(terminated, "TERMINATED"), (timeout, "TIMED_OUT"), (completed, "COMPLETED")]:
            await self.assert_archived(h, status, via_child=False)
        self.assertEqual(len(self.rows("SELECT watermark FROM loader_checkpoint WHERE name='poll'")), 1)

    def loader_environment_from(self, original):
        env = {k: v for k, v in original.items() if not k.startswith("TEMPORAL_")}
        env.update(PG_DSN=self.dsn, TEMPORAL_ADDRESS=self.client.service_client.config.target_host,
                   TEMPORAL_NAMESPACE="default", TEMPORAL_CONFIG_FILE=str(self.config_file))
        return env

    async def test_database_failure_retries_without_partial_execution_or_events(self):
        self.rows("""CREATE FUNCTION reject_events() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected database write failure'; END $$""")
        self.rows("CREATE TRIGGER reject_events BEFORE INSERT ON workflow_history_event FOR EACH ROW EXECUTE FUNCTION reject_events()")
        await self.workers()
        handle = await self.start()
        await handle.result()  # The business result is independent of failed archive writes.
        archive = self.client.get_workflow_handle("archive/" + handle.first_execution_run_id)
        async def retried():
            desc = await archive.describe()
            pending = desc.raw_description.pending_activities
            return bool(pending and pending[0].attempt >= 2 and pending[0].last_failure.message)
        await self.wait_until(retried)
        self.assertEqual(self.rows("SELECT * FROM workflow_execution"), [])
        self.assertEqual(self.rows("SELECT * FROM workflow_history_event"), [])
        self.rows("DROP TRIGGER reject_events ON workflow_history_event")
        await self.assert_archived(handle, "COMPLETED")

    async def test_reset_run_identity_survives_export_and_poll(self):
        await self.workers()
        original = await self.start("wait")
        await original.query(LifecycleWorkflow.ready)
        await original.signal(LifecycleWorkflow.release)
        await original.result()
        original_hist = await self.assert_archived(original, "COMPLETED")
        reset_point = next(e.event_id for e in original_hist.events if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
        runs = [original]
        for iteration in range(2):
            previous = runs[-1]
            if iteration:
                events = self.histories[previous.run_id].events
                prior_reset = max(e.event_id for e in events if e.workflow_task_failed_event_attributes.new_run_id)
                reset_point = next(e.event_id for e in events if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED and e.event_id > prior_reset)
            reset = await self.client.workflow_service.reset_workflow_execution(ResetWorkflowExecutionRequest(
                namespace="default", workflow_execution=WorkflowExecution(workflow_id=previous.id, run_id=previous.run_id or previous.first_execution_run_id),
                reason="archive identity E2E", workflow_task_finish_event_id=reset_point, request_id=str(uuid4()),
                reset_reapply_type=ResetReapplyType.RESET_REAPPLY_TYPE_NONE,
            ))
            handle = self.client.get_workflow_handle(original.id, run_id=reset.run_id)
            await handle.signal(LifecycleWorkflow.release)
            await asyncio.wait_for(handle.result(), timeout=40)
            await self.assert_archived(handle, "COMPLETED")
            runs.append(handle)
        self.assertEqual(sum(bool(e.workflow_task_failed_event_attributes.new_run_id)
                             for e in self.histories[runs[-1].run_id].events), 2)
        expected = set(self.histories)
        self.assertEqual(len(expected), 3)
        # Build the official export protobuf from REAL server histories, then use the batch importer.
        batch = WorkflowExecutions()
        for hist in self.histories.values():
            batch.items.add().history.CopyFrom(History(events=hist.events))
        self.rows("TRUNCATE workflow_history_event, workflow_execution")
        with tempfile.TemporaryDirectory() as folder:
            Path(folder, "batch.proto").write_bytes(batch.SerializeToString())
            for _ in range(2):
                await asyncio.to_thread(history_loader.load_export, folder, self.dsn)
        self.assertEqual({r[0] for r in self.rows("SELECT run_id FROM workflow_execution")}, expected)
        for handle in runs:
            await self.assert_archived(handle, "COMPLETED", via_child=False)
        await self.poll_visible(runs)
        for handle in runs:
            await self.assert_archived(handle, "COMPLETED", via_child=False)

    async def test_pre_interceptor_history_replays_without_archiver_commands(self):
        await self.workers(intercept=False, archiver=False)
        handle = await self.start()
        await handle.result()
        hist = await handle.fetch_history()
        await Replayer(workflows=[LifecycleWorkflow], interceptors=[ArchiveOnCloseInterceptor()]).replay_workflow(hist)
        self.assertEqual(self.rows("SELECT * FROM workflow_execution"), [])

    async def test_archiver_waits_for_source_close_including_terminal_event(self):
        await self.workers(intercept=False)
        handle = await self.start("wait")
        await handle.query(LifecycleWorkflow.ready)
        archive = await self.client.start_workflow(
            HistoryArchiverWorkflow.run, args=[handle.id, handle.first_execution_run_id],
            id="archive/" + handle.first_execution_run_id, task_queue="history-archiver",
        )
        async def parent_open():
            pending = (await archive.describe()).raw_description.pending_activities
            return bool(pending and pending[0].last_failure.application_failure_info.type == "ParentStillRunning")
        await self.wait_until(parent_open)
        self.assertEqual(self.rows("SELECT * FROM workflow_execution"), [])
        await handle.signal(LifecycleWorkflow.release)
        await handle.result()
        await self.assert_archived(handle, "COMPLETED")

    async def test_delayed_archiver_worker_does_not_delay_business_completion(self):
        await self.workers(archiver=False)
        handle = await self.start()
        await asyncio.wait_for(handle.result(), timeout=10)
        archive = self.client.get_workflow_handle("archive/" + handle.first_execution_run_id)
        self.assertEqual((await archive.describe()).status.name, "RUNNING")
        self.assertEqual(self.rows("SELECT * FROM workflow_execution"), [])
        await self.archive_worker()
        await self.assert_archived(handle, "COMPLETED")

    async def test_failed_store_rolls_back_and_connection_can_be_reused(self):
        await self.workers(intercept=False, archiver=False)
        handle = await self.start()
        await handle.result()
        hist = History(events=(await handle.fetch_history()).events)
        self.rows("""CREATE FUNCTION reject_events() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected write failure'; END $$""")
        self.rows("CREATE TRIGGER reject_events BEFORE INSERT ON workflow_history_event FOR EACH ROW EXECUTE FUNCTION reject_events()")
        with psycopg.connect(self.dsn) as conn:
            with self.assertRaises(psycopg.errors.RaiseException):
                history_loader.store(conn, hist, "poll", handle.first_execution_run_id)
            self.assertEqual(conn.execute("SELECT count(*) FROM workflow_execution").fetchone(), (0,))
            self.rows("DROP TRIGGER reject_events ON workflow_history_event")
            history_loader.store(conn, hist, "poll", handle.first_execution_run_id)
        await self.assert_archived(handle, "COMPLETED", via_child=False)

    async def test_failed_sweep_keeps_checkpoint_and_recovers_partial_delivery(self):
        await self.workers(intercept=False, archiver=False)
        first, second = await self.start(), await self.start()
        await first.result()
        await second.result()
        await self.poll_visible([first, second])
        # Model a prior successful run followed by a failing run in the same sweep window.
        self.rows("DELETE FROM workflow_history_event WHERE run_id=%s", (second.first_execution_run_id,))
        self.rows("DELETE FROM workflow_execution WHERE run_id=%s", (second.first_execution_run_id,))
        self.rows("INSERT INTO loader_checkpoint VALUES ('poll', %s)", (self.since,))
        self.rows("CREATE TABLE blocked_workflows (workflow_id text PRIMARY KEY)")
        self.rows("INSERT INTO blocked_workflows VALUES (%s)", (second.id,))
        self.rows("""CREATE FUNCTION reject_events() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF EXISTS (SELECT 1 FROM blocked_workflows WHERE workflow_id=NEW.workflow_id) THEN
                RAISE EXCEPTION 'injected partial sweep failure';
            END IF;
            RETURN NEW;
        END $$""")
        self.rows("CREATE TRIGGER reject_events BEFORE INSERT ON workflow_history_event FOR EACH ROW EXECUTE FUNCTION reject_events()")
        code, output = await self.loader("poll")
        self.assertNotEqual(code, 0, output)
        self.assertIn("injected partial sweep failure", output)
        self.assertEqual(self.rows("SELECT watermark FROM loader_checkpoint"), [(self.since,)])
        self.assertEqual(self.rows("SELECT run_id FROM workflow_execution"), [(first.first_execution_run_id,)])
        self.rows("DROP TRIGGER reject_events ON workflow_history_event")
        for _ in range(2):
            code, output = await self.loader("poll")
            self.assertEqual(code, 0, output)
        for h in [first, second]:
            await self.assert_archived(h, "COMPLETED", via_child=False)
        watermark = self.rows("SELECT watermark FROM loader_checkpoint")[0][0]
        self.assertGreater(watermark, self.since)

    async def test_archiver_recovers_when_postgres_connections_are_reenabled(self):
        role = "writer_" + uuid4().hex
        with psycopg.connect(self.admin_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE ROLE "{role}" NOLOGIN PASSWORD \'test-only\'')
        def drop_role():
            with psycopg.connect(self.admin_dsn, autocommit=True) as conn:
                conn.execute(f'DROP OWNED BY "{role}"')
                conn.execute(f'DROP ROLE "{role}"')
        self.stack.callback(drop_role)
        self.rows(f'GRANT USAGE ON SCHEMA "{self.schema}" TO "{role}"')
        self.rows(f'GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA "{self.schema}" TO "{role}"')
        await self.workers(archiver=False)
        await self.archive_worker(make_conninfo(self.dsn, user=role, password="test-only"))
        handle = await self.start()
        await handle.result()
        archive = self.client.get_workflow_handle("archive/" + handle.first_execution_run_id)
        async def failed_connection():
            pending = (await archive.describe()).raw_description.pending_activities
            return bool(pending and pending[0].last_failure.application_failure_info.type == "OperationalError")
        await self.wait_until(failed_connection)
        self.assertEqual(self.rows("SELECT * FROM workflow_execution"), [])
        with psycopg.connect(self.admin_dsn, autocommit=True) as conn:
            conn.execute(f'ALTER ROLE "{role}" LOGIN')
        await self.assert_archived(handle, "COMPLETED")

    async def test_encrypted_payloads_are_placeholders_and_raw_export_remains_decodable(self):
        converter = replace(DataConverter.default, payload_codec=EncryptedPayloadCodec())
        self.client = Client(**{**self.client.config(), "data_converter": converter})
        await self.workers()
        handle = await self.start()
        result = await handle.result()
        self.assertEqual(result, {"order": "test-123", "accepted": True, "optional": None})
        hist = await self.assert_archived(handle, "COMPLETED")
        attrs = self.rows("SELECT attributes FROM workflow_history_event WHERE run_id=%s AND event_id=1", (handle.first_execution_run_id,))[0][0]
        self.assertEqual(attrs["input"]["payloads"][0]["_encoding"], "binary/encrypted")
        self.assertGreater(attrs["input"]["payloads"][0]["_bytes"], 0)
        batch = WorkflowExecutions()
        batch.items.add().history.CopyFrom(History(events=hist.events))
        parsed = WorkflowExecutions.FromString(batch.SerializeToString())
        payloads = parsed.items[0].history.events[-1].workflow_execution_completed_event_attributes.result.payloads
        self.assertEqual(await converter.decode(payloads), [result])
        with tempfile.TemporaryDirectory() as folder:
            Path(folder, "encrypted.proto").write_bytes(batch.SerializeToString())
            await asyncio.to_thread(history_loader.load_export, folder, self.dsn)
        await self.assert_archived(handle, "COMPLETED", via_child=False)

    async def test_s3_batch_ingestion_repeated_delivery_and_malformed_history(self):
        import boto3
        from moto import mock_aws

        await self.workers(intercept=False, archiver=False)
        handle = await self.start()
        await handle.result()
        hist = await handle.fetch_history()
        batch = WorkflowExecutions()
        batch.items.add().history.CopyFrom(History(events=hist.events))
        # Moto replaces AWS transport only; the production boto3 reader and Postgres writer execute.
        safe_env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
        safe_env.update(AWS_DEFAULT_REGION="us-east-1", AWS_EC2_METADATA_DISABLED="true")
        with patch.dict(os.environ, safe_env, clear=True), mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="archive-e2e")
            s3.put_object(Bucket="archive-e2e", Key="histories/batch.proto", Body=batch.SerializeToString())
            for _ in range(2):
                await asyncio.to_thread(history_loader.load_export, "s3://archive-e2e/histories/", self.dsn)
        await self.assert_archived(handle, "COMPLETED", via_child=False)
        attrs = self.rows("SELECT attributes FROM workflow_history_event WHERE event_type='WORKFLOW_EXECUTION_COMPLETED'")[0][0]
        self.assertEqual(attrs["result"]["payloads"], [{"order": "test-123", "accepted": True, "optional": None}])
        for invalid in [History(), History(events=hist.events[:-1])]:
            with self.subTest(events=len(invalid.events)), psycopg.connect(self.dsn) as conn:
                with self.assertRaises(ValueError):
                    history_loader.store(conn, invalid, "export")
        await self.assert_archived(handle, "COMPLETED", via_child=False)

    async def test_java_interceptor_to_python_archiver_for_all_closing_code_paths(self):
        jar = ROOT / "workers/java/target/orders-worker-java-1.0.0-standalone.jar"
        self.assertTrue(jar.exists(), "Build workers/pom.xml before the E2E suite")
        compiler = await asyncio.create_subprocess_exec(
            "javac", "-cp", str(jar), "-d", self.tempdir.name,
            str(Path(__file__).with_name("java") / "ArchivalTestWorker.java"),
        )
        self.assertEqual(await asyncio.wait_for(compiler.wait(), timeout=30), 0)
        await self.archive_worker()
        proc, log = await self.process(
            "java", "-cp", os.pathsep.join([self.tempdir.name, str(jar)]), "ArchivalTestWorker",
            self.client.service_client.config.target_host, self.queue,
        )
        for mode in ["complete", "fail", "wait", "continue"]:
            with self.subTest(mode=mode):
                handle = await self.client.start_workflow("JavaLifecycle", mode, id="java-" + uuid4().hex, task_queue=self.queue)
                try:
                    if mode == "wait":
                        self.assertTrue(await asyncio.wait_for(handle.query("ready"), timeout=30))
                        await handle.cancel()
                    if mode in ["fail", "wait"]:
                        with self.assertRaises(WorkflowFailureError) as error:
                            await asyncio.wait_for(handle.result(), timeout=30)
                        self.assertIsInstance(error.exception.cause, ApplicationError if mode == "fail" else CancelledError)
                    else:
                        self.assertEqual(await asyncio.wait_for(handle.result(), timeout=30), "done")
                except Exception:
                    self.fail(f"Java worker failed (exit={proc.returncode}): {log.read_text()}")
                status = {"complete": "COMPLETED", "fail": "FAILED", "wait": "CANCELED", "continue": "CONTINUED_AS_NEW"}[mode]
                hist = await self.assert_archived(handle, status)
                if mode == "continue":
                    next_id = hist.events[-1].workflow_execution_continued_as_new_event_attributes.new_execution_run_id
                    await self.assert_archived(self.client.get_workflow_handle(handle.id, run_id=next_id), "COMPLETED")

    async def test_packaged_java_and_spring_workers_register_real_archival_delivery(self):
        await self.archive_worker()
        for jar in ["workers/java/target/orders-worker-java-1.0.0-standalone.jar",
                    "workers/spring-boot/target/orders-worker-spring-1.0.0.jar"]:
            with self.subTest(jar=jar):
                self.assertTrue((ROOT / jar).exists(), "Build workers/pom.xml before the E2E suite")
                # Both production entry points read these application settings from the environment.
                previous = {k: os.environ.get(k) for k in ["TASK_QUEUE", "METRICS_PORT"]}
                queue = "packaged-" + uuid4().hex
                os.environ.update(TASK_QUEUE=queue, METRICS_PORT="0")
                try:
                    proc, log = await self.process("java", "-jar", str(ROOT / jar))
                finally:
                    for key, value in previous.items():
                        if value is None:
                            os.environ.pop(key, None)
                        else:
                            os.environ[key] = value
                handle = await self.client.start_workflow("ShipmentWorkflow", "sample-order", id="packaged-" + uuid4().hex, task_queue=queue)
                try:
                    self.assertEqual(await asyncio.wait_for(handle.result(), timeout=45), "ship-sample-order")
                except Exception:
                    self.fail(f"Packaged worker failed (exit={proc.returncode}): {log.read_text()}")
                await self.assert_archived(handle, "COMPLETED")


if __name__ == "__main__":
    unittest.main()
