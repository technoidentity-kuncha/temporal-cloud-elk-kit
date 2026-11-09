"""Order workflow (parent) and shipment workflow (child), mirroring the Java worker."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from orders.activities import (
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
    @workflow.run
    async def run(self, order_id: str) -> str:
        wf_id = workflow.info().workflow_id
        try:
            await workflow.execute_activity(validate_order, order_id, **OPTS)
            await workflow.execute_activity(reserve_inventory, order_id, **OPTS)
            txn = await workflow.execute_activity(charge_payment, order_id, **OPTS)
            shipment_id = await workflow.execute_child_workflow(
                ShipmentWorkflow.run, order_id, id=f"{wf_id}/shipment"
            )
            await workflow.execute_activity(
                notify_customer, args=[order_id, f"order shipped: {shipment_id}"], **OPTS
            )
            return f"{txn}/{shipment_id}"
        except ActivityError as e:
            raise ApplicationError("order failed", type="OrderFailed") from e
