# demo_full_queue.py
import time
import logging
from fastapi.testclient import TestClient
from ezl.core import task, webhook, Pipeline
from pydantic import BaseModel

# enable flow logging
logging.basicConfig(level=25)  # FLOW_LEVEL = 25
logger = logging.getLogger(__name__)

# -------------------------
# 1. Normal pipeline full queue
# -------------------------
called = []


@task(buffer=1, workers=1)
def source():
    for i in range(3):
        yield i


@task(buffer=1, workers=1)
def sink(x):
    time.sleep(0.5)  # slow processing
    called.append(x)
    logger.info(f"SINK processed {x}")
    return x


source >> sink
p = Pipeline(source, sink)

print(
    "Running normal pipeline (will block if downstream full)..."
)
p.run(log_level=25)  # see FLOW logs
print("Pipeline finished, called:", called)


# -------------------------
# 2. Webhook full queue
# -------------------------
class Item(BaseModel):
    value: int


received = []


@webhook(path="/ingest", buffer=1)
def wh(x: dict):
    time.sleep(0.5)  # slow downstream
    received.append(x)
    logger.info(f"Webhook processed {x}")
    return x


@task(buffer=1, workers=1)
def wh_sink(x):
    time.sleep(0.5)
    received.append(x)
    logger.info(f"Sink processed {x}")
    return x


wh >> wh_sink

client = TestClient(
    wh._build_fastapi(
        path="/ingest",
        api_key=None,
        model=Item,
        rate_limit_per_minute=None,
    )
)

# first request: accepted
r1 = client.post("/ingest", json={"value": 1})
print("First webhook request:", r1.status_code, r1.json())

# fill downstream queue manually to simulate full buffer
while not wh.downstream.input_queue.full():
    wh.downstream.input_queue.put({"value": 999})

# second request: should be rejected
r2 = client.post("/ingest", json={"value": 2})
print(
    "Second webhook request (queue full):",
    r2.status_code,
    r2.json(),
)
