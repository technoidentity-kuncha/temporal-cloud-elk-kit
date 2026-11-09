"""Simulated side effects with realistic latency and failures, mirroring the Java worker."""

import asyncio
import random
from dataclasses import dataclass

import psycopg
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


@dataclass
class StageRecord:
    entity_id: str
    workflow_id: str
    stage: str
    status: str
    stage_seq: int


class ProjectionActivities:
    """Option C: explicit business projection, an idempotent upsert guarded by stage_seq."""

    UPSERT = """
        INSERT INTO business_projection (entity_id, workflow_id, stage, status, stage_seq, updated_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (entity_id) DO UPDATE SET workflow_id = EXCLUDED.workflow_id, stage = EXCLUDED.stage,
            status = EXCLUDED.status, stage_seq = EXCLUDED.stage_seq, updated_at = now()
        WHERE business_projection.stage_seq <= EXCLUDED.stage_seq
    """

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    @activity.defn(name="RecordStage")
    async def record_stage(self, rec: StageRecord) -> None:
        async with await psycopg.AsyncConnection.connect(self.dsn, connect_timeout=2) as conn:
            await conn.execute(
                self.UPSERT, (rec.entity_id, rec.workflow_id, rec.stage, rec.status, rec.stage_seq)
            )
