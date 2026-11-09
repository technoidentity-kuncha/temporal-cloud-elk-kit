"""Simulated side effects with realistic latency and failures, mirroring the Java worker."""

import asyncio
import random

from temporalio import activity
from temporalio.exceptions import ApplicationError


async def _work(min_ms: int, max_ms: int) -> None:
    await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000)


@activity.defn(name="ValidateOrder")
async def validate_order(order_id: str) -> None:
    await _work(20, 120)


@activity.defn(name="ReserveInventory")
async def reserve_inventory(order_id: str) -> None:
    await _work(50, 400)


@activity.defn(name="ChargePayment")
async def charge_payment(order_id: str) -> str:
    await _work(100, 900)
    r = random.random()
    if r < 0.04 and activity.info().attempt == 1:
        raise ApplicationError("card declined", type="PaymentDeclined", non_retryable=True)
    if r < 0.19:
        raise ApplicationError("payment gateway timeout", type="GatewayTimeout")
    return f"txn-{order_id}"


@activity.defn(name="ScheduleShipment")
async def schedule_shipment(order_id: str) -> str:
    await _work(100, 600)
    return f"ship-{order_id}"


@activity.defn(name="NotifyCustomer")
async def notify_customer(order_id: str, message: str) -> None:
    await _work(10, 80)
