"""Test sample order behavior without a business-projection database."""

import asyncio
from pathlib import Path
import shutil
import sys
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from orders.workflows import OrderWorkflow, ShipmentWorkflow


@activity.defn(name="ValidateOrder")
async def validate(order_id: str) -> None:
    pass


@activity.defn(name="ReserveInventory")
async def reserve(order_id: str) -> None:
    pass


@activity.defn(name="ChargePayment")
async def charge(order_id: str) -> str:
    if order_id == "declined":
        raise ApplicationError("declined", type="PaymentDeclined", non_retryable=True)
    return f"txn-{order_id}"


@activity.defn(name="ScheduleShipment")
async def ship(order_id: str) -> str:
    return f"ship-{order_id}"


@activity.defn(name="NotifyCustomer")
async def notify(order_id: str, message: str) -> None:
    pass


@unittest.skipUnless(shutil.which("temporal"), "Temporal CLI required for local-server tests")
class OrderWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal"),
        )
        self.queue = f"order-test-{uuid4()}"
        self.worker = Worker(
            self.env.client, task_queue=self.queue,
            workflows=[OrderWorkflow, ShipmentWorkflow],
            activities=[validate, reserve, charge, ship, notify],
        )
        await self.worker.__aenter__()

    async def asyncTearDown(self):
        await self.worker.__aexit__(None, None, None)
        await self.env.shutdown()

    async def execute(self, order_id):
        return await asyncio.wait_for(self.env.client.execute_workflow(
            OrderWorkflow.run, order_id, id=str(uuid4()), task_queue=self.queue,
        ), timeout=20)

    async def test_order_and_shipment_complete_without_projection_writer(self):
        self.assertEqual(await self.execute("123"), "txn-123/ship-123")

    async def test_decline_preserves_order_failure(self):
        with self.assertRaises(WorkflowFailureError) as error:
            await self.execute("declined")
        self.assertEqual(error.exception.cause.type, "OrderFailed")


if __name__ == "__main__":
    unittest.main()
