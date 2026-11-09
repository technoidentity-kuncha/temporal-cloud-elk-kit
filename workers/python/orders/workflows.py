"""Order workflow (parent) and shipment workflow (child), mirroring the Java worker."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from orders.activities import (
        ProjectionActivities,
        StageRecord,
        charge_payment,
        notify_customer,
        reserve_inventory,
        schedule_shipment,
        validate_order,
    )

RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=500),
    maximum_attempts=5,
    non_retryable_error_types=["PaymentDeclined"],
)
OPTS = dict(start_to_close_timeout=timedelta(seconds=10), retry_policy=RETRY)


@workflow.defn(name="ShipmentWorkflow")
class ShipmentWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        shipment_id = await workflow.execute_activity(
            schedule_shipment, order_id, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.sleep(timedelta(seconds=2))  # durable timer
        return shipment_id


@workflow.defn(name="OrderWorkflow")
class OrderWorkflow:
    def __init__(self) -> None:
        self._seq = 0

    @workflow.run
    async def run(self, order_id: str) -> str:
        wf_id = workflow.info().workflow_id
        try:
            await workflow.execute_activity(validate_order, order_id, **OPTS)
            await self._project(order_id, wf_id, "VALIDATED", "IN_PROGRESS")

            await workflow.execute_activity(reserve_inventory, order_id, **OPTS)
            await self._project(order_id, wf_id, "RESERVED", "IN_PROGRESS")

            txn = await workflow.execute_activity(charge_payment, order_id, **OPTS)
            await self._project(order_id, wf_id, "CHARGED", "IN_PROGRESS")

            shipment_id = await workflow.execute_child_workflow(
                ShipmentWorkflow.run, order_id, id=f"{wf_id}/shipment"
            )
            await self._project(order_id, wf_id, "SHIPPED", "IN_PROGRESS")

            await workflow.execute_activity(
                notify_customer, args=[order_id, f"order shipped: {shipment_id}"], **OPTS
            )
            await self._project(order_id, wf_id, "DONE", "COMPLETED")
            return f"{txn}/{shipment_id}"
        except ActivityError as e:
            await self._project(order_id, wf_id, "FAILED", "FAILED")
            raise ApplicationError("order failed", type="OrderFailed") from e

    async def _project(self, order_id: str, wf_id: str, stage: str, status: str) -> None:
        """Non-fatal projection write: if retries are exhausted, log and carry on."""
        self._seq += 1
        try:
            await workflow.execute_activity_method(
                ProjectionActivities.record_stage,
                StageRecord(order_id, wf_id, stage, status, self._seq),
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except ActivityError as e:
            workflow.logger.warning("projection write skipped for %s stage %s: %s", order_id, stage, e)
