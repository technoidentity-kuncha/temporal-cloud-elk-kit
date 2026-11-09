"""Load generator: starts OrderWorkflow on both the Java and Python task queues at a steady rate.

    python load.py --rate 2 --duration 600          # 2 workflows/sec per queue for 10 minutes
"""

import argparse
import asyncio
import os
import time
import uuid

from temporalio.client import Client


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=1.0, help="workflows per second, per task queue")
    p.add_argument("--duration", type=int, default=300, help="seconds")
    p.add_argument("--queues", default="orders-java,orders-python")
    a = p.parse_args()

    api_key = os.environ.get("TEMPORAL_API_KEY")
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        api_key=api_key or None,
        tls=bool(api_key),
    )
    queues = a.queues.split(",")
    started, deadline = 0, time.monotonic() + a.duration
    while time.monotonic() < deadline:
        tick = time.monotonic()
        for q in queues:
            order_id = uuid.uuid4().hex[:10]
            await client.start_workflow(
                "OrderWorkflow", order_id, id=f"order-{q.split('-')[-1]}-{order_id}", task_queue=q
            )
            started += 1
        await asyncio.sleep(max(0.0, 1 / a.rate - (time.monotonic() - tick)))
        if started % 50 < len(queues):
            print(f"started {started} workflows")
    print(f"done: started {started} workflows")


if __name__ == "__main__":
    asyncio.run(main())
