from fastapi.testclient import TestClient
from ezl.core import task, webhook
from pydantic import BaseModel


class Item(BaseModel):
    value: int


def test_webhook_rejects_when_downstream_full():
    received = []

    @webhook(path="/ingest", buffer=1)
    def wh(x: dict):
        # slow consumer: append to received after delay
        import time

        time.sleep(0.2)
        received.append(x)
        return x

    # slow downstream
    @task(buffer=1, workers=1)
    def sink(x):
        import time

        time.sleep(0.5)
        return x

    wh >> sink

    # use TestClient instead of starting uvicorn
    client = TestClient(
        wh._build_fastapi(
            "/ingest",
            api_key=None,
            model=Item,
            rate_limit_per_minute=None,
        )
    )

    # first request: accepted
    r1 = client.post("/ingest", json={"value": 1})
    assert r1.status_code == 202
    assert r1.json() == {"accepted": 1, "rejected": 0}

    # fill the queue manually to simulate full buffer
    while not wh.downstream.input_queue.full():
        wh.downstream.input_queue.put({"value": 999})

    # second request: rejected due to full queue
    r2 = client.post("/ingest", json={"value": 2})
    assert r2.status_code == 503
    assert r2.json()["error"] == "Queue full"
    assert r2.headers.get("Retry-After") == "10"
