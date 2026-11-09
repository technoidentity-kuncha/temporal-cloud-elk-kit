"""Python worker: Temporal Cloud via API key, Prometheus-format metrics on :METRICS_PORT/metrics,
archive-on-close interceptor registered once for all workflows."""

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from orders.activities import (
    charge_payment,
    notify_customer,
    reserve_inventory,
    schedule_shipment,
    validate_order,
)
from orders.archive_interceptor import ArchiveOnCloseInterceptor
from orders.workflows import OrderWorkflow, ShipmentWorkflow


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    metrics_port = env("METRICS_PORT", "9465")

    # Names line up with the Java/Micrometer output: *_total counters, *_seconds durations.
    runtime = Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(
                bind_address=f"0.0.0.0:{metrics_port}",
                counters_total_suffix=True,
                unit_suffix=True,
                durations_as_seconds=True,
            ),
            global_tags={"sdk": "python", "service": env("SERVICE_NAME", "orders-worker-python")},
        )
    )

    api_key = env("TEMPORAL_API_KEY", "")
    client = await Client.connect(
        env("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=env("TEMPORAL_NAMESPACE", "default"),
        api_key=api_key or None,
        tls=bool(api_key),
        runtime=runtime,
    )

    task_queue = env("TASK_QUEUE", "orders-python")

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[OrderWorkflow, ShipmentWorkflow],
        activities=[
            validate_order,
            reserve_inventory,
            charge_payment,
            schedule_shipment,
            notify_customer,
        ],
        interceptors=[ArchiveOnCloseInterceptor()],
        # Deliberately small so slot exhaustion is visible on the dashboard under load.
        max_concurrent_activities=int(env("MAX_ACTIVITY_SLOTS", "20")),
    )
    print(f"Python worker started: namespace={client.namespace} taskQueue={task_queue} metrics=:{metrics_port}/metrics")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
